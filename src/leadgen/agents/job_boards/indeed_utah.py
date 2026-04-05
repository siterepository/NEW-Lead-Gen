"""
Indeed Utah Agent

Scrapes Indeed.com for Utah job listings to find people actively
seeking career changes - prime NWM financial advisor recruiting targets.

NOTE: Indeed has aggressive anti-bot detection.  This agent uses
extended delays (5-15s), aggressive UA rotation, and conservative
page limits to minimise detection risk.
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
# Search configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://www.indeed.com"

SEARCH_KEYWORDS: list[str] = [
    "career change",
    "financial advisor",
    "insurance agent",
    "sales representative",
    "business development",
    "entrepreneur",
    "real estate agent",
    "self employed",
    "independent contractor",
    "commission sales",
    "wealth management",
    "financial planner",
    "account manager",
]


class IndeedUtahAgent(BaseAgent):
    """Scrape Indeed.com for Utah job seekers and career changers.

    People browsing Indeed for opportunity-type roles or posting
    resumes in sales/finance categories are strong NWM recruits.

    Anti-bot strategy:
    - Long random delays between requests (5-15s)
    - Aggressive user-agent rotation every request
    - Conservative pagination (max 5 pages per keyword)
    - Randomised keyword order each run
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="indeed_utah",
            platform="indeed",
            config=config,
            db=db,
        )
        # Override base delays for Indeed's stricter anti-bot
        self.delay_min = self.config.get("delay_min", 5.0)
        self.delay_max = self.config.get("delay_max", 15.0)
        self.max_pages: int = self.config.get("max_pages", 5)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 150)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build Indeed search URLs for Utah job seekers.

        URL pattern: https://www.indeed.com/jobs?q=<keyword>&l=Utah&sort=date
        """
        urls: list[str] = []
        keywords = list(SEARCH_KEYWORDS)
        random.shuffle(keywords)  # randomise order to vary crawl pattern

        for keyword in keywords:
            params = {
                "q": keyword,
                "l": "Utah",
                "sort": "date",
                "fromage": "14",  # last 14 days
            }
            url = f"{BASE_URL}/jobs?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Scrape Indeed Utah listings with extra-cautious rate limiting."""
        collected: list[dict] = []
        search_urls = self.get_search_urls()
        crawler = await self.setup_browser()

        @crawler.router.default_handler
        async def handle_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            # Rotate UA on every page load for Indeed
            ua = self.get_random_user_agent()
            await page.set_extra_http_headers({"User-Agent": ua})

            # Wait for job cards
            try:
                await page.wait_for_selector(
                    "div.job_seen_beacon, div.jobsearch-ResultsList "
                    "div.result, td.resultContent, div.slider_container",
                    timeout=20_000,
                )
            except Exception:
                logger.debug(
                    "[%s] No job cards on %s", self.name, context.request.url
                )
                return

            cards = await page.query_selector_all(
                "div.job_seen_beacon, td.resultContent, "
                "div.slider_container div.job_seen_beacon"
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

            # Pagination - "Next" link
            next_btn = await page.query_selector(
                "a[data-testid='pagination-page-next'], "
                "a[aria-label='Next Page'], ul.pagination-list li:last-child a"
            )
            if next_btn and len(collected) < self.max_results_per_run:
                next_href = await next_btn.get_attribute("href")
                if next_href:
                    if not next_href.startswith("http"):
                        next_href = f"{BASE_URL}{next_href}"
                    await self.rate_limiter.acquire()
                    await asyncio.sleep(self.get_random_delay())
                    await context.enqueue_links(urls=[next_href])

        # Enqueue URLs with long delays
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
        """Extract data from a single Indeed job card."""
        raw: dict[str, Any] = {}

        # Title
        title_el = await card.query_selector(
            "h2.jobTitle a, a.jcs-JobTitle, "
            "h2.jobTitle span[title], a[data-jk]"
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

        # Company name
        company_el = await card.query_selector(
            "span.companyName, span[data-testid='company-name'], "
            "span.css-1h7lukg"
        )
        if company_el:
            raw["company"] = (await company_el.inner_text()).strip()

        # Location
        loc_el = await card.query_selector(
            "div.companyLocation, div[data-testid='text-location'], "
            "span.companyLocation"
        )
        if loc_el:
            raw["location"] = (await loc_el.inner_text()).strip()

        # Salary
        salary_el = await card.query_selector(
            "div.salary-snippet-container, "
            "div[data-testid='attribute_snippet_testid'], "
            "span.estimated-salary"
        )
        if salary_el:
            raw["salary"] = (await salary_el.inner_text()).strip()

        # Description snippet
        desc_el = await card.query_selector(
            "div.job-snippet, div[class*='job-snippet'], "
            "table.jobCardShelfContainer td"
        )
        if desc_el:
            raw["description"] = (await desc_el.inner_text()).strip()

        # Date posted
        date_el = await card.query_selector(
            "span.date, span[data-testid='myJobsStateDate'], "
            "span.css-qvloho"
        )
        if date_el:
            raw["posted_date"] = (await date_el.inner_text()).strip()

        # Job key for dedup
        jk = None
        link_el = await card.query_selector("a[data-jk]")
        if link_el:
            jk = await link_el.get_attribute("data-jk")
        raw["post_id"] = jk or raw.get("url", raw.get("title", ""))

        return raw

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw Indeed data into the standard lead format."""
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)

        return {
            "name": "",
            "title": raw_data.get("title", ""),
            "company": raw_data.get("company", ""),
            "description": raw_data.get("description", ""),
            "location_city": city,
            "location_state": "Utah",
            "posted_date": raw_data.get("posted_date", ""),
            "salary": raw_data.get("salary", ""),
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "indeed",
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
        """Extract city from Indeed location like 'Salt Lake City, UT'."""
        if not location:
            return ""
        city = re.sub(r",?\s*(UT|Utah)\s*$", "", location, flags=re.IGNORECASE)
        city = re.sub(r"\s*\d{5}(-\d{4})?\s*$", "", city)
        # Indeed sometimes adds "+1 location" suffix
        city = re.sub(r"\s*\+\d+\s*location.*$", "", city, flags=re.IGNORECASE)
        return city.strip()
