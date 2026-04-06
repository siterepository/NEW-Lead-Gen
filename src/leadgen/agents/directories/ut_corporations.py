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

import httpx

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
        """Fetch listing pages with httpx and extract items via regex."""
        search_urls = self.get_search_urls()
        collected: list[dict] = []
        headers = {"User-Agent": self.get_random_user_agent()}

        async with httpx.AsyncClient(
            headers=headers, follow_redirects=True, timeout=30.0
        ) as client:
            for url in search_urls:
                if len(collected) >= self.max_results_per_run:
                    break
                await self.rate_limiter.acquire()
                await asyncio.sleep(self.get_random_delay())

                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    html = resp.text
                except httpx.HTTPError as exc:
                    logger.warning("[%s] HTTP error for %s: %s", self.name, url, exc)
                    continue

                items = self._parse_html(html, url)
                for item in items:
                    if len(collected) >= self.max_results_per_run:
                        break
                    collected.append(item)

        logger.info("[%s] Scrape complete: %d raw items", self.name, len(collected))
        return collected

    def _parse_html(self, html: str, source_url: str) -> list[dict]:
        """Extract directory listings from HTML via regex."""
        items: list[dict] = []
        for m in re.finditer(
            r'<a[^>]+href="([^"]+)"[^>]*>([^<]{5,})</a>',
            html,
        ):
            href, title = m.group(1), m.group(2).strip()
            if any(skip in title.lower() for skip in [
                "privacy", "terms", "cookie", "about", "contact",
                "sign in", "log in", "home", "back", "next", "previous",
            ]):
                continue
            url = href if href.startswith("http") else f"{source_url.split('/')[0]}//{source_url.split('/')[2]}{href}"
            items.append({
                "title": title,
                "url": url,
                "post_id": url,
                "description": "",
                "location": "",
                "contact_info": "",
            })
        return items

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
