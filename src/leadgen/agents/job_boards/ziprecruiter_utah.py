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

import httpx

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
