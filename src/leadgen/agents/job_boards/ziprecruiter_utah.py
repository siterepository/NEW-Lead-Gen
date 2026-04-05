"""
ZipRecruiter Utah Agent

Scrapes ZipRecruiter for Utah job listings targeting people
actively seeking career changes or new opportunities.
Strong NWM financial advisor recruiting source.
"""

from __future__ import annotations

import asyncio
import logging
import random
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

BASE_URL = "https://www.ziprecruiter.com"

SEARCH_KEYWORDS: list[str] = [
    "career change",
    "financial advisor",
    "sales representative",
    "insurance agent",
    "business development",
    "real estate",
    "account manager",
    "independent contractor",
    "commission based",
    "wealth management",
    "financial planner",
    "entrepreneur",
]


class ZipRecruiterUtahAgent(BaseAgent):
    """Scrape ZipRecruiter for Utah job seekers.

    URL pattern: https://www.ziprecruiter.com/Jobs/<keyword>-in-Utah
    Alternative: https://www.ziprecruiter.com/candidate/search?search=<kw>&location=Utah
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="ziprecruiter_utah",
            platform="ziprecruiter",
            config=config,
            db=db,
        )
        self.delay_min = self.config.get("delay_min", 3.0)
        self.delay_max = self.config.get("delay_max", 8.0)
        self.max_pages: int = self.config.get("max_pages", 6)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 150)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build ZipRecruiter search URLs for Utah."""
        urls: list[str] = []
        keywords = list(SEARCH_KEYWORDS)
        random.shuffle(keywords)

        for keyword in keywords:
            # Path-based format
            slug = keyword.replace(" ", "-").title()
            urls.append(f"{BASE_URL}/Jobs/{slug}-in-Utah")

            # Query-string format (backup)
            params = {
                "search": keyword,
                "location": "Utah",
            }
            urls.append(
                f"{BASE_URL}/candidate/search?"
                f"{urlencode(params, quote_via=quote_plus)}"
            )

        # Broad Utah jobs page
        urls.append(f"{BASE_URL}/Jobs/-in-Utah")

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Scrape ZipRecruiter Utah job listings."""
        collected: list[dict] = []
        search_urls = self.get_search_urls()
        crawler = await self.setup_browser()

        @crawler.router.default_handler
        async def handle_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            # Wait for job cards
            try:
                await page.wait_for_selector(
                    "article.job_result, div.job_content, "
                    "div.job-listing, li.job-listing, "
                    "div[data-testid='job-result']",
                    timeout=15_000,
                )
            except Exception:
                logger.debug(
                    "[%s] No job cards on %s", self.name, context.request.url
                )
                return

            cards = await page.query_selector_all(
                "article.job_result, div.job_content, "
                "div.job-listing, li.job-listing, "
                "div[data-testid='job-result']"
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
                "a.next, a[title='Next'], li.next a, "
                "a.pagination-next, a[aria-label='Next']"
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

        logger.info("[%s] Collected %d raw items", self.name, len(collected))
        return collected

    # ------------------------------------------------------------------
    # Card extraction
    # ------------------------------------------------------------------

    async def _extract_card(self, card: Any, page: Any) -> Optional[dict]:
        """Extract data from a ZipRecruiter job card."""
        raw: dict[str, Any] = {}

        # Title
        title_el = await card.query_selector(
            "a.job_link, h2.job_title a, a[data-testid='job-title'], "
            "span.just_job_title a, a.job-title"
        )
        if title_el:
            raw["title"] = (await title_el.inner_text()).strip()
            href = await title_el.get_attribute("href")
            if href:
                if not href.startswith("http"):
                    href = f"{BASE_URL}{href}"
                raw["url"] = href
        else:
            return None

        # Company
        company_el = await card.query_selector(
            "a.t_org_link, span.hiring_company, "
            "p.company_name, a[data-testid='company-name']"
        )
        if company_el:
            raw["company"] = (await company_el.inner_text()).strip()

        # Location
        loc_el = await card.query_selector(
            "span.location, p.job_location, "
            "span[data-testid='job-location'], a.t_location_link"
        )
        if loc_el:
            raw["location"] = (await loc_el.inner_text()).strip()

        # Salary
        salary_el = await card.query_selector(
            "span.salary, p.salary_estimate, "
            "span[data-testid='salary']"
        )
        if salary_el:
            raw["salary"] = (await salary_el.inner_text()).strip()

        # Description snippet
        desc_el = await card.query_selector(
            "p.job_snippet, div.job_snippet, "
            "span[data-testid='job-snippet']"
        )
        if desc_el:
            raw["description"] = (await desc_el.inner_text()).strip()

        # Date posted
        date_el = await card.query_selector(
            "span.posted_date, time, span.just_posted, "
            "span[data-testid='posted-date']"
        )
        if date_el:
            raw["posted_date"] = (await date_el.inner_text()).strip()
            dt_attr = await date_el.get_attribute("datetime")
            if dt_attr:
                raw["posted_date_iso"] = dt_attr

        # Post ID for dedup
        job_id = None
        link_el = await card.query_selector("a[data-job-id]")
        if link_el:
            job_id = await link_el.get_attribute("data-job-id")
        if not job_id and raw.get("url"):
            match = re.search(r"/([a-f0-9]{20,})", raw["url"])
            job_id = match.group(1) if match else None
        raw["post_id"] = job_id or raw.get("url", raw.get("title", ""))

        return raw

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw ZipRecruiter data to standard lead format."""
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)

        posted_date = raw_data.get("posted_date_iso") or raw_data.get(
            "posted_date", ""
        )

        return {
            "name": "",
            "company": raw_data.get("company", ""),
            "title": raw_data.get("title", ""),
            "description": raw_data.get("description", ""),
            "location_city": city,
            "location_state": "Utah",
            "posted_date": posted_date,
            "salary": raw_data.get("salary", ""),
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "ziprecruiter",
            "contact_info": "",
            "contact_phone": "",
            "contact_email": "",
            "category": "job_listing",
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
