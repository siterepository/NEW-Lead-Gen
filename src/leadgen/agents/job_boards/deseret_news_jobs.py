"""
Deseret News Jobs Agent

Scrapes Deseret News classifieds/jobs section for Utah-specific
career opportunities and job seekers.  Deseret News is a major
Utah publication - people using its classifieds are local and
actively seeking, making them prime NWM recruiting targets.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlencode, quote_plus

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext

from leadgen.agents.base import BaseAgent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://www.deseret.com"
CLASSIFIEDS_URL = "https://classifieds.deseret.com"

SEARCH_KEYWORDS: list[str] = [
    "career opportunity",
    "sales",
    "financial",
    "business",
    "insurance",
    "real estate",
    "management",
    "employment",
    "hiring",
    "seeking professionals",
]

CATEGORY_SLUGS: list[str] = [
    "jobs",
    "employment",
    "business-opportunities",
    "services",
]


class DeseretNewsJobsAgent(BaseAgent):
    """Scrape Deseret News classifieds for Utah job seekers.

    Targets:
    - Job listings in classifieds section
    - Career opportunity postings
    - Business-for-sale and services ads
    People reading / posting here are Utah locals looking for
    career movement.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="deseret_news_jobs",
            platform="deseret_news",
            config=config,
            db=db,
        )
        self.max_pages: int = self.config.get("max_pages", 8)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 150)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build search URLs for Deseret News classifieds."""
        urls: list[str] = []

        # Keyword-based searches
        for keyword in SEARCH_KEYWORDS:
            params = {
                "q": keyword,
                "category": "jobs",
                "location": "Utah",
            }
            url = (
                f"{CLASSIFIEDS_URL}/search?"
                f"{urlencode(params, quote_via=quote_plus)}"
            )
            urls.append(url)

        # Category browsing
        for cat in CATEGORY_SLUGS:
            urls.append(f"{CLASSIFIEDS_URL}/{cat}")

        # Also check the main Deseret News jobs section
        urls.append(f"{BASE_URL}/jobs")

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Scrape Deseret News classified listings."""
        collected: list[dict] = []
        search_urls = self.get_search_urls()
        crawler = await self.setup_browser()

        @crawler.router.default_handler
        async def handle_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            # Wait for listings
            try:
                await page.wait_for_selector(
                    "div.classified-listing, div.job-listing, "
                    "article.listing, div.search-result, "
                    "div.classifieds-card, li.listing-item",
                    timeout=15_000,
                )
            except Exception:
                logger.debug(
                    "[%s] No listings on %s", self.name, context.request.url
                )
                return

            cards = await page.query_selector_all(
                "div.classified-listing, div.job-listing, "
                "article.listing, div.search-result, "
                "div.classifieds-card, li.listing-item"
            )

            for card in cards:
                if len(collected) >= self.max_results_per_run:
                    break
                try:
                    raw = await self._extract_card(card, page)
                    if raw:
                        collected.append(raw)
                except Exception as exc:
                    logger.warning("[%s] Card extraction error: %s", self.name, exc)

            # Pagination
            next_btn = await page.query_selector(
                "a.next, a[rel='next'], li.next a, "
                "a.pagination-next, a[aria-label='Next page']"
            )
            if next_btn and len(collected) < self.max_results_per_run:
                next_href = await next_btn.get_attribute("href")
                if next_href:
                    if not next_href.startswith("http"):
                        next_href = f"{CLASSIFIEDS_URL}{next_href}"
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

        logger.info("[%s] Collected %d raw items", self.name, len(collected))
        return collected

    # ------------------------------------------------------------------
    # Card extraction
    # ------------------------------------------------------------------

    async def _extract_card(self, card: Any, page: Any) -> Optional[dict]:
        """Extract data from a Deseret News classified card."""
        raw: dict[str, Any] = {}

        # Title
        title_el = await card.query_selector(
            "h2 a, h3 a, a.listing-title, a.classified-title, "
            "span.title a, div.title a"
        )
        if title_el:
            raw["title"] = (await title_el.inner_text()).strip()
            href = await title_el.get_attribute("href")
            if href:
                if not href.startswith("http"):
                    href = f"{CLASSIFIEDS_URL}{href}"
                raw["url"] = href
        else:
            return None

        # Description
        desc_el = await card.query_selector(
            "p.description, div.listing-description, "
            "div.snippet, span.listing-body"
        )
        if desc_el:
            raw["description"] = (await desc_el.inner_text()).strip()

        # Location
        loc_el = await card.query_selector(
            "span.location, div.listing-location, "
            "span.city"
        )
        if loc_el:
            raw["location"] = (await loc_el.inner_text()).strip()

        # Price
        price_el = await card.query_selector(
            "span.price, div.listing-price"
        )
        if price_el:
            raw["price"] = (await price_el.inner_text()).strip()

        # Date
        date_el = await card.query_selector(
            "span.date, time, span.posted-date, "
            "div.listing-date"
        )
        if date_el:
            raw["posted_date"] = (await date_el.inner_text()).strip()
            dt_attr = await date_el.get_attribute("datetime")
            if dt_attr:
                raw["posted_date_iso"] = dt_attr

        # Contact
        contact_el = await card.query_selector(
            "a[href^='tel:'], a[href^='mailto:'], "
            "span.phone, span.contact"
        )
        if contact_el:
            raw["contact_info"] = (
                (await contact_el.inner_text()).strip()
                or await contact_el.get_attribute("href") or ""
            )

        # Category
        cat_el = await card.query_selector(
            "span.category, a.category-link, div.listing-category"
        )
        if cat_el:
            raw["category"] = (await cat_el.inner_text()).strip()

        # Post ID
        if raw.get("url"):
            match = re.search(r"/(\d+)", raw["url"])
            raw["post_id"] = match.group(1) if match else raw["url"]
        else:
            raw["post_id"] = raw.get("title", "")

        return raw

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw Deseret News data to standard lead format."""
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)

        contact = raw_data.get("contact_info", "")
        phone, email = self._parse_contact(contact)

        posted_date = raw_data.get("posted_date_iso") or raw_data.get(
            "posted_date", ""
        )

        return {
            "name": "",
            "title": raw_data.get("title", ""),
            "description": raw_data.get("description", ""),
            "location_city": city,
            "location_state": "Utah",
            "posted_date": posted_date,
            "price": raw_data.get("price", ""),
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "deseret_news",
            "contact_info": contact,
            "contact_phone": phone,
            "contact_email": email,
            "category": raw_data.get("category", "classified"),
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Helpers
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
