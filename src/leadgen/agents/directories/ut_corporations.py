"""
Utah Division of Corporations Agent

Scrapes the Utah Division of Corporations (secure.utah.gov/bes/)
for new business filings.  New business owners are entrepreneurial
by nature and make excellent NWM financial advisor recruiting targets.
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

BASE_URL = "https://secure.utah.gov/bes"

# Entity types most likely to be small/new business owners
ENTITY_TYPES: list[str] = [
    "LLC",
    "Corporation - Domestic",
    "Corporation - Professional",
    "DBA",
]

# Search by recently filed (sorted newest first)
SEARCH_STATUSES: list[str] = [
    "Active",
]


class UtahCorporationsAgent(BaseAgent):
    """Scrape Utah Division of Corporations for new business filings.

    New business owners are:
    - Entrepreneurial and risk-tolerant
    - Often looking for additional income streams
    - Networked in their communities
    - Prime NWM recruiting targets
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="ut_corporations",
            platform="utah_bes",
            config=config,
            db=db,
        )
        self.max_pages: int = self.config.get("max_pages", 10)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 200)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build search URLs for Utah Business Entity Search.

        The BES portal uses a form-based search.  We target recent
        filings by searching with common business-name keywords.
        """
        urls: list[str] = []

        # Common industry keywords for new filings
        keywords = [
            "consulting",
            "financial",
            "insurance",
            "real estate",
            "sales",
            "marketing",
            "coaching",
            "services",
            "management",
            "group",
            "solutions",
            "enterprise",
        ]

        for keyword in keywords:
            params = {
                "BusinessName": keyword,
                "State": "UT",
                "Status": "Active",
                "FilingType": "",
            }
            url = f"{BASE_URL}/action/search?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Scrape Utah BES for new business filings."""
        collected: list[dict] = []
        search_urls = self.get_search_urls()
        crawler = await self.setup_browser()

        @crawler.router.default_handler
        async def handle_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            # Wait for results table
            try:
                await page.wait_for_selector(
                    "table.searchResults, table#searchResultsTable, "
                    "div.search-results table, div.results-list, "
                    "table.table tbody tr",
                    timeout=15_000,
                )
            except Exception:
                logger.debug(
                    "[%s] No results on %s", self.name, context.request.url
                )
                return

            rows = await page.query_selector_all(
                "table.searchResults tbody tr, "
                "table#searchResultsTable tbody tr, "
                "div.results-list div.result-item, "
                "table.table tbody tr"
            )

            for row in rows:
                if len(collected) >= self.max_results_per_run:
                    break
                try:
                    raw = await self._extract_row(row, page)
                    if raw:
                        collected.append(raw)
                except Exception as exc:
                    logger.warning("[%s] Row extraction error: %s", self.name, exc)

            # Pagination
            next_btn = await page.query_selector(
                "a.next, a[aria-label='Next'], "
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
    # Row extraction
    # ------------------------------------------------------------------

    async def _extract_row(self, row: Any, page: Any) -> Optional[dict]:
        """Extract data from a single BES search result row."""
        raw: dict[str, Any] = {}

        cells = await row.query_selector_all("td")
        if len(cells) < 2:
            return None

        # Business name (usually first column with a link)
        name_el = await row.query_selector("td a, a.entity-name")
        if name_el:
            raw["business_name"] = (await name_el.inner_text()).strip()
            href = await name_el.get_attribute("href")
            if href:
                if not href.startswith("http"):
                    href = f"{BASE_URL}{href}"
                raw["url"] = href
        else:
            # Try first cell text
            first_text = (await cells[0].inner_text()).strip()
            if first_text:
                raw["business_name"] = first_text
            else:
                return None

        # Entity number
        entity_el = await row.query_selector(
            "td.entity-number, td:nth-child(2)"
        )
        if entity_el:
            raw["entity_number"] = (await entity_el.inner_text()).strip()

        # Entity type
        type_el = await row.query_selector(
            "td.entity-type, td:nth-child(3)"
        )
        if type_el:
            raw["entity_type"] = (await type_el.inner_text()).strip()

        # Status
        status_el = await row.query_selector(
            "td.status, td:nth-child(4)"
        )
        if status_el:
            raw["status"] = (await status_el.inner_text()).strip()

        # Filing date
        date_el = await row.query_selector(
            "td.filing-date, td:nth-child(5)"
        )
        if date_el:
            raw["filing_date"] = (await date_el.inner_text()).strip()

        # Registered agent
        agent_el = await row.query_selector(
            "td.registered-agent, td:nth-child(6)"
        )
        if agent_el:
            raw["registered_agent"] = (await agent_el.inner_text()).strip()

        # Post ID for dedup
        raw["post_id"] = raw.get(
            "entity_number", raw.get("url", raw.get("business_name", ""))
        )

        return raw

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw BES data to standard lead format."""
        # Try to extract owner name from registered agent
        name = raw_data.get("registered_agent", "")

        return {
            "name": name,
            "business_name": raw_data.get("business_name", ""),
            "title": f"New Business: {raw_data.get('business_name', '')}",
            "description": (
                f"Entity type: {raw_data.get('entity_type', '')}. "
                f"Status: {raw_data.get('status', '')}. "
                f"Filed: {raw_data.get('filing_date', '')}."
            ),
            "entity_number": raw_data.get("entity_number", ""),
            "entity_type": raw_data.get("entity_type", ""),
            "filing_date": raw_data.get("filing_date", ""),
            "location_city": "",
            "location_state": "Utah",
            "posted_date": raw_data.get("filing_date", ""),
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "utah_bes",
            "contact_info": "",
            "contact_phone": "",
            "contact_email": "",
            "category": "new_business_filing",
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
