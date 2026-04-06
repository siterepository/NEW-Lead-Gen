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

import httpx

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
        """Extract job listings from HTML via regex."""
        items: list[dict] = []
        # Generic job listing pattern: links with title text
        for m in re.finditer(
            r'<a[^>]+href="([^"]+)"[^>]*>([^<]{5,})</a>',
            html,
        ):
            href, title = m.group(1), m.group(2).strip()
            # Skip navigation / footer links
            if any(skip in title.lower() for skip in [
                "privacy", "terms", "cookie", "about", "contact",
                "sign in", "log in", "home", "back",
            ]):
                continue
            url = href if href.startswith("http") else f"{BASE_URL}{href}"
            post_id_match = re.search(r"/(\d{4,})", href)
            post_id = post_id_match.group(1) if post_id_match else url
            items.append({
                "title": title,
                "url": url,
                "post_id": post_id,
                "company": "",
                "location": "",
                "salary": "",
                "description": "",
                "posted_date": "",
                "contact_info": "",
            })
        return items

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
