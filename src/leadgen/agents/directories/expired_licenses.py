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

import httpx

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
