"""
Utah Workforce Services Agent

Scrapes jobs.utah.gov (Utah Department of Workforce Services) for
job listings.  People using state workforce services are actively
seeking employment and are strong NWM financial advisor recruiting targets.
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

BASE_URL = "https://jobs.utah.gov"

SEARCH_KEYWORDS: list[str] = [
    "sales",
    "finance",
    "insurance",
    "business development",
    "account manager",
    "real estate",
    "customer service",
    "management",
    "marketing",
    "consulting",
    "advisor",
    "representative",
]

# Utah counties with largest population
UTAH_REGIONS: list[str] = [
    "Salt Lake",
    "Utah",
    "Davis",
    "Weber",
    "Washington",
    "Cache",
]


class UtahWorkforceAgent(BaseAgent):
    """Scrape Utah Workforce Services (jobs.utah.gov) for active job seekers.

    The state workforce system is used by people who are:
    - Recently unemployed and actively searching
    - Looking for career pivots
    - Using government resources to find work
    All are strong recruiting prospects.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="utah_workforce",
            platform="utah_workforce",
            config=config,
            db=db,
        )
        self.max_pages: int = self.config.get("max_pages", 10)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 200)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build search URLs for jobs.utah.gov.

        URL pattern: https://jobs.utah.gov/jobseeker/search?q=<keyword>
        """
        urls: list[str] = []

        for keyword in SEARCH_KEYWORDS:
            params = {
                "q": keyword,
                "location": "Utah",
                "sort": "date",
            }
            url = f"{BASE_URL}/jobseeker/search?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)

        # Broad search without keyword
        urls.append(f"{BASE_URL}/jobseeker/search?location=Utah&sort=date")

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Scrape Utah Workforce Services listings."""
        collected: list[dict] = []
        search_urls = self.get_search_urls()
        crawler = await self.setup_browser()

        @crawler.router.default_handler
        async def handle_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            # Wait for job listing cards
            try:
                await page.wait_for_selector(
                    "div.job-listing, div.search-result-item, "
                    "tr.clickable-row, div.job-result, "
                    "div.card.job-card, li.job-item",
                    timeout=15_000,
                )
            except Exception:
                logger.debug(
                    "[%s] No listings on %s", self.name, context.request.url
                )
                return

            cards = await page.query_selector_all(
                "div.job-listing, div.search-result-item, "
                "tr.clickable-row, div.job-result, "
                "div.card.job-card, li.job-item"
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
                "a.next-page, a[aria-label='Next'], "
                "li.next a, a.pagination-next"
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
        """Extract data from a Utah Workforce job listing card."""
        raw: dict[str, Any] = {}

        # Title
        title_el = await card.query_selector(
            "a.job-title, h3.job-title a, h2 a, "
            "td.job-title a, a.job-link, span.title a"
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

        # Company / Employer
        company_el = await card.query_selector(
            "span.company, span.employer, td.company, "
            "div.company-name, a.company-link"
        )
        if company_el:
            raw["company"] = (await company_el.inner_text()).strip()

        # Location
        loc_el = await card.query_selector(
            "span.location, td.location, div.job-location, "
            "span.city-state"
        )
        if loc_el:
            raw["location"] = (await loc_el.inner_text()).strip()

        # Salary
        salary_el = await card.query_selector(
            "span.salary, td.salary, div.salary-range, "
            "span.compensation"
        )
        if salary_el:
            raw["salary"] = (await salary_el.inner_text()).strip()

        # Description
        desc_el = await card.query_selector(
            "div.description, p.snippet, td.description, "
            "div.job-snippet"
        )
        if desc_el:
            raw["description"] = (await desc_el.inner_text()).strip()

        # Date posted
        date_el = await card.query_selector(
            "span.date, span.posted-date, td.date, "
            "time, span.post-date"
        )
        if date_el:
            raw["posted_date"] = (await date_el.inner_text()).strip()
            dt_attr = await date_el.get_attribute("datetime")
            if dt_attr:
                raw["posted_date_iso"] = dt_attr

        # Job ID for dedup
        job_id_el = await card.query_selector(
            "span.job-id, span.order-number"
        )
        if job_id_el:
            raw["post_id"] = (await job_id_el.inner_text()).strip()
        elif raw.get("url"):
            match = re.search(r"[/=](\d{5,})", raw["url"])
            raw["post_id"] = match.group(1) if match else raw["url"]
        else:
            raw["post_id"] = raw.get("title", "")

        return raw

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw workforce data to standard lead format."""
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
            "platform": "utah_workforce",
            "contact_info": "",
            "contact_phone": "",
            "contact_email": "",
            "category": "government_job_board",
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_city(location: str) -> str:
        """Extract city from Utah Workforce location string."""
        if not location:
            return ""
        city = re.sub(r",?\s*(UT|Utah)\s*$", "", location, flags=re.IGNORECASE)
        city = re.sub(r"\s*\d{5}(-\d{4})?\s*$", "", city)
        return city.strip()
