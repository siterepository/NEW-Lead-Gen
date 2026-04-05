"""
Utah DOPL Expired Licenses Agent

Scrapes the Utah Division of Occupational and Professional Licensing
(dopl.utah.gov) for recently expired professional licenses.
People whose licenses have expired may be open to career changes -
strong NWM financial advisor recruiting signal.
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

BASE_URL = "https://dopl.utah.gov"
SEARCH_URL = "https://dopl.utah.gov/license-lookup"

# License types where expired holders make good NWM recruits
LICENSE_TYPES: list[str] = [
    "Real Estate",
    "Insurance",
    "Securities",
    "Mortgage",
    "Financial",
    "Accounting",
    "Contractor",
    "Appraiser",
]

# Statuses indicating expired or lapsed
EXPIRED_STATUSES: list[str] = [
    "Expired",
    "Lapsed",
    "Inactive",
    "Revoked",
]


class ExpiredLicensesAgent(BaseAgent):
    """Scrape Utah DOPL for expired professional licenses.

    People with expired licenses are:
    - Transitioning between careers
    - May have left a regulated industry
    - Have professional experience and networks
    - Potentially open to a new career path like NWM
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="expired_licenses",
            platform="utah_dopl",
            config=config,
            db=db,
        )
        self.max_pages: int = self.config.get("max_pages", 10)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 200)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build search URLs for Utah DOPL license lookup.

        Targets expired licenses in relevant professions.
        """
        urls: list[str] = []

        for license_type in LICENSE_TYPES:
            params = {
                "profession": license_type,
                "status": "Expired",
                "state": "UT",
            }
            url = f"{SEARCH_URL}?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)

        # Also search for "Lapsed" and "Inactive"
        for license_type in LICENSE_TYPES[:4]:  # top professions only
            for status in ["Lapsed", "Inactive"]:
                params = {
                    "profession": license_type,
                    "status": status,
                    "state": "UT",
                }
                url = f"{SEARCH_URL}?{urlencode(params, quote_via=quote_plus)}"
                urls.append(url)

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Scrape Utah DOPL for expired license holders."""
        collected: list[dict] = []
        search_urls = self.get_search_urls()
        crawler = await self.setup_browser()

        @crawler.router.default_handler
        async def handle_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            # DOPL uses a form-based search; may need to interact
            # Wait for results table
            try:
                await page.wait_for_selector(
                    "table.license-results, table#resultsTable, "
                    "div.search-results table, div.results, "
                    "table.table tbody tr, div.license-list",
                    timeout=20_000,
                )
            except Exception:
                logger.debug(
                    "[%s] No results on %s", self.name, context.request.url
                )
                return

            rows = await page.query_selector_all(
                "table.license-results tbody tr, "
                "table#resultsTable tbody tr, "
                "div.results div.result-row, "
                "table.table tbody tr, "
                "div.license-list div.license-item"
            )

            for row in rows:
                if len(collected) >= self.max_results_per_run:
                    break
                try:
                    raw = await self._extract_row(row, page)
                    if raw:
                        # Only keep expired/lapsed/inactive
                        status = raw.get("license_status", "").lower()
                        if any(s.lower() in status for s in EXPIRED_STATUSES):
                            collected.append(raw)
                except Exception as exc:
                    logger.warning("[%s] Row extraction error: %s", self.name, exc)

            # Pagination
            next_btn = await page.query_selector(
                "a.next, a[aria-label='Next'], "
                "li.next a, button.next-page"
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
        """Extract data from a single DOPL license result row."""
        raw: dict[str, Any] = {}

        cells = await row.query_selector_all("td")
        if len(cells) < 2:
            return None

        # Name (usually first column)
        name_el = await row.query_selector("td a, td:first-child")
        if name_el:
            raw["name"] = (await name_el.inner_text()).strip()
            href = await name_el.get_attribute("href")
            if href:
                if not href.startswith("http"):
                    href = f"{BASE_URL}{href}"
                raw["url"] = href
        else:
            return None

        if not raw.get("name"):
            return None

        # License number
        lic_el = await row.query_selector(
            "td.license-number, td:nth-child(2)"
        )
        if lic_el:
            raw["license_number"] = (await lic_el.inner_text()).strip()

        # Profession / License type
        prof_el = await row.query_selector(
            "td.profession, td:nth-child(3)"
        )
        if prof_el:
            raw["profession"] = (await prof_el.inner_text()).strip()

        # Status
        status_el = await row.query_selector(
            "td.status, td:nth-child(4)"
        )
        if status_el:
            raw["license_status"] = (await status_el.inner_text()).strip()

        # Expiration date
        exp_el = await row.query_selector(
            "td.expiration-date, td:nth-child(5)"
        )
        if exp_el:
            raw["expiration_date"] = (await exp_el.inner_text()).strip()

        # City
        city_el = await row.query_selector(
            "td.city, td:nth-child(6)"
        )
        if city_el:
            raw["city"] = (await city_el.inner_text()).strip()

        # Post ID for dedup
        raw["post_id"] = raw.get(
            "license_number", raw.get("url", raw.get("name", ""))
        )

        return raw

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw DOPL data to standard lead format."""
        return {
            "name": raw_data.get("name", ""),
            "title": f"Expired {raw_data.get('profession', '')} License",
            "description": (
                f"License #{raw_data.get('license_number', '')} "
                f"({raw_data.get('profession', '')}). "
                f"Status: {raw_data.get('license_status', '')}. "
                f"Expired: {raw_data.get('expiration_date', '')}."
            ),
            "license_number": raw_data.get("license_number", ""),
            "profession": raw_data.get("profession", ""),
            "license_status": raw_data.get("license_status", ""),
            "expiration_date": raw_data.get("expiration_date", ""),
            "location_city": raw_data.get("city", ""),
            "location_state": "Utah",
            "posted_date": raw_data.get("expiration_date", ""),
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "utah_dopl",
            "contact_info": "",
            "contact_phone": "",
            "contact_email": "",
            "category": "expired_license",
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
