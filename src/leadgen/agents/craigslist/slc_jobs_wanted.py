"""
Craigslist Salt Lake City - Jobs Wanted Agent

Scrapes the Salt Lake City Craigslist "jobs wanted" section for people
actively seeking employment.  These individuals have explicitly posted
that they want work and may be open to NWM financial advisor recruiting.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlencode

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext

from leadgen.agents.base import BaseAgent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Search configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://saltlakecity.craigslist.org"
SEARCH_PATH = "/search/jjj"  # jobs wanted section

SEARCH_KEYWORDS: list[str] = [
    "career change",
    "seeking employment",
    "sales experience",
    "business",
    "finance",
    "insurance",
    "real estate",
    "entrepreneur",
    "management",
    "self motivated",
    "professional",
]


class CraigslistSLCJobsWantedAgent(BaseAgent):
    """Scrape Craigslist SLC jobs-wanted section for potential NWM recruits.

    These postings reveal people who are:
    - Actively looking for employment
    - Self-motivated enough to post publicly
    - In the Salt Lake City / Utah metro area
    - Potentially open to financial services careers
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="craigslist_slc_jobs_wanted",
            platform="craigslist",
            config=config,
            db=db,
        )
        self.max_pages: int = self.config.get("max_pages", 5)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 150)
        # CL blocks aggressively -- use longer delays
        self.delay_min = self.config.get("delay_min", 5.0)
        self.delay_max = self.config.get("delay_max", 10.0)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build Craigslist search URLs for SLC jobs wanted."""
        urls: list[str] = []

        # Keyword-specific searches
        for keyword in SEARCH_KEYWORDS:
            params = {"query": keyword, "sort": "date"}
            url = f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"
            urls.append(url)

        # Broad (no keyword) search
        params = {"sort": "date"}
        url = f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"
        urls.append(url)

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Scrape Craigslist SLC jobs-wanted listings."""
        search_urls = self.get_search_urls()
        crawler = await self.setup_browser()
        collected: list[dict] = []

        @crawler.router.default_handler
        async def handle_listing_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            # Wait for CL results to load
            try:
                await page.wait_for_selector(
                    "li.cl-static-search-result, li.result-row, "
                    "div.result-info, ol.cl-static-search-results > li",
                    timeout=15_000,
                )
            except Exception:
                logger.debug("[%s] No results on %s", self.name, context.request.url)
                return

            # Extract listing rows
            rows = await page.query_selector_all(
                "li.cl-static-search-result, li.result-row, "
                "ol.cl-static-search-results > li"
            )

            for row in rows:
                if len(collected) >= self.max_results_per_run:
                    break
                try:
                    raw = await self._extract_row(row, page)
                    if raw:
                        listing_url = raw.get("url", "")
                        if listing_url:
                            last = await self.check_last_seen(listing_url)
                            post_id = raw.get("post_id", "")
                            if last and last == post_id:
                                continue
                        collected.append(raw)
                except Exception as exc:
                    logger.warning("[%s] Row extraction failed: %s", self.name, exc)

            # Pagination - CL uses "next" button or range links
            next_btn = await page.query_selector(
                "a.button.next, a.cl-next-page, "
                "button.bd-button.cl-next-page, a[title='next page']"
            )
            if next_btn and len(collected) < self.max_results_per_run:
                next_href = await next_btn.get_attribute("href")
                if next_href:
                    if not next_href.startswith("http"):
                        next_href = f"{BASE_URL}{next_href}"
                    await self.rate_limiter.acquire()
                    await asyncio.sleep(self.get_random_delay())
                    await context.enqueue_links(urls=[next_href])

        urls_to_crawl: list[str] = []
        for url in search_urls:
            if len(collected) >= self.max_results_per_run:
                break
            await self.rate_limiter.acquire()
            await asyncio.sleep(self.get_random_delay())
            urls_to_crawl.append(url)

        if urls_to_crawl:
            await crawler.run(urls_to_crawl)

        logger.info("[%s] Scrape complete: %d raw items", self.name, len(collected))
        return collected

    # ------------------------------------------------------------------
    # Row extraction
    # ------------------------------------------------------------------

    async def _extract_row(self, row: Any, page: Any) -> Optional[dict]:
        """Extract structured data from a single CL result row."""
        raw: dict[str, Any] = {}

        # Title + URL
        title_el = await row.query_selector(
            "a.titlestring, a.result-title, a.posting-title, "
            "div.title a, a.cl-app-anchor"
        )
        if title_el:
            raw["title"] = (await title_el.inner_text()).strip()
            href = await title_el.get_attribute("href")
            if href:
                raw["url"] = href if href.startswith("http") else f"{BASE_URL}{href}"
        else:
            return None

        # Preview / meta text
        meta_el = await row.query_selector(
            "span.result-meta, div.meta, span.result-hood"
        )
        if meta_el:
            raw["meta"] = (await meta_el.inner_text()).strip()

        # Date
        date_el = await row.query_selector(
            "time.result-date, time, span.date, "
            "div.meta span.date"
        )
        if date_el:
            raw["posted_date"] = (await date_el.inner_text()).strip()
            dt_attr = await date_el.get_attribute("datetime")
            if dt_attr:
                raw["posted_date_iso"] = dt_attr
            title_attr = await date_el.get_attribute("title")
            if title_attr and not raw.get("posted_date_iso"):
                raw["posted_date_iso"] = title_attr

        # Location (CL shows in parentheses or in a hood span)
        loc_el = await row.query_selector(
            "span.result-hood, span.nearby, span.supertitle"
        )
        if loc_el:
            loc_text = (await loc_el.inner_text()).strip()
            # Remove parentheses CL wraps around locations
            raw["location"] = loc_text.strip("() ")

        # Price if present
        price_el = await row.query_selector("span.result-price, span.priceinfo")
        if price_el:
            raw["price"] = (await price_el.inner_text()).strip()

        # Generate post_id from URL
        if raw.get("url"):
            match = re.search(r"/(\d+)\.html", raw["url"])
            raw["post_id"] = match.group(1) if match else raw["url"]
        elif raw.get("title"):
            raw["post_id"] = raw["title"]

        # Thumbnail
        img_el = await row.query_selector("img, a.result-image img")
        if img_el:
            raw["image_url"] = await img_el.get_attribute("src")

        return raw

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw CL jobs-wanted data into the standard lead format."""
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)
        posted_date = raw_data.get("posted_date_iso") or raw_data.get("posted_date", "")

        # CL jobs-wanted rarely shows contact info in previews
        contact = raw_data.get("contact_info", "")
        phone, email = self._parse_contact(contact)

        return {
            "name": "",
            "title": raw_data.get("title", ""),
            "description": raw_data.get("meta", ""),
            "location_city": city,
            "location_state": "Utah",
            "posted_date": posted_date,
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "craigslist",
            "contact_info": contact,
            "contact_phone": phone,
            "contact_email": email,
            "price": raw_data.get("price", ""),
            "category": "jobs_wanted",
            "image_url": raw_data.get("image_url", ""),
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_city(location: str) -> str:
        if not location:
            return ""
        city = re.sub(r",?\s*(UT|Utah)\s*$", "", location, flags=re.IGNORECASE)
        city = re.sub(r"\s*\d{5}(-\d{4})?\s*$", "", city)
        return city.strip()

    @staticmethod
    def _parse_contact(contact: str) -> tuple[str, str]:
        phone = ""
        email = ""
        if not contact:
            return phone, email

        email_match = re.search(
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", contact
        )
        if email_match:
            email = email_match.group(0).lower()

        phone_match = re.search(
            r"(\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})", contact
        )
        if phone_match:
            phone = phone_match.group(1).strip()

        tel_match = re.search(r"tel:([+\d-]+)", contact)
        if tel_match and not phone:
            phone = tel_match.group(1)

        mailto_match = re.search(r"mailto:([^\s&]+)", contact)
        if mailto_match and not email:
            email = mailto_match.group(1).lower()

        return phone, email
