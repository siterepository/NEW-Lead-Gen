"""
Better Business Bureau Utah Agent

Scrapes BBB Utah (bbb.org/us/ut) for businesses with complaints
or low ratings.  Owners of struggling businesses may be open to
career changes - strong NWM financial advisor recruiting signal.
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

BASE_URL = "https://www.bbb.org"
UTAH_URL = "https://www.bbb.org/us/ut"

# Business categories with high turnover / dissatisfaction
CATEGORIES: list[str] = [
    "insurance",
    "financial-services",
    "real-estate",
    "contractors",
    "sales",
    "marketing",
    "consulting",
    "auto-dealers",
    "home-improvement",
    "retail",
]

# Utah cities to search
UTAH_CITIES: list[str] = [
    "salt-lake-city",
    "provo",
    "ogden",
    "orem",
    "sandy",
    "west-jordan",
    "st-george",
    "layton",
    "logan",
    "lehi",
]


class BBBUtahAgent(BaseAgent):
    """Scrape BBB Utah for businesses with complaints or low ratings.

    Struggling business owners are:
    - Under financial pressure
    - May be considering career alternatives
    - Have business experience and networks
    - Open to conversations about NWM opportunities
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="bbb_utah",
            platform="bbb",
            config=config,
            db=db,
        )
        self.max_pages: int = self.config.get("max_pages", 8)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 150)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build BBB search URLs for Utah businesses.

        URL pattern: https://www.bbb.org/search?find_country=US&find_loc=Utah&find_type=Category&find_text=<category>
        """
        urls: list[str] = []

        # Category-based search across Utah
        for category in CATEGORIES:
            params = {
                "find_country": "US",
                "find_loc": "Utah",
                "find_type": "Category",
                "find_text": category.replace("-", " "),
                "sort": "Relevance",
            }
            url = f"{BASE_URL}/search?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)

        # City-specific pages
        for city in UTAH_CITIES[:5]:  # top 5 cities
            urls.append(f"{UTAH_URL}/{city}")

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
        """Extract BBB business listings from HTML via regex."""
        items: list[dict] = []
        # BBB business profile links
        for m in re.finditer(
            r'<a[^>]+href="(https?://www\.bbb\.org/us/[^"]+)"[^>]*>([^<]{3,})</a>',
            html,
        ):
            href, name = m.group(1), m.group(2).strip()
            if any(skip in name.lower() for skip in [
                "privacy", "terms", "cookie", "about us", "file a complaint",
            ]):
                continue
            items.append({
                "business_name": name,
                "url": href,
                "bbb_rating": "",
                "complaint_count": "",
                "location": "",
                "phone": "",
                "category": "",
                "accredited": False,
                "post_id": href,
            })
        return items

    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw BBB data to standard lead format."""
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)

        phone = raw_data.get("phone", "")
        # Clean phone from tel: prefix
        phone = re.sub(r"^tel:", "", phone).strip()

        return {
            "name": "",
            "business_name": raw_data.get("business_name", ""),
            "title": f"BBB: {raw_data.get('business_name', '')}",
            "description": (
                f"BBB Rating: {raw_data.get('bbb_rating', 'N/A')}. "
                f"Complaints: {raw_data.get('complaint_count', '0')}. "
                f"Category: {raw_data.get('category', '')}."
            ),
            "bbb_rating": raw_data.get("bbb_rating", ""),
            "complaint_count": raw_data.get("complaint_count", ""),
            "accredited": raw_data.get("accredited", False),
            "location_city": city,
            "location_state": "Utah",
            "posted_date": "",
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "bbb",
            "contact_info": phone,
            "contact_phone": phone,
            "contact_email": "",
            "category": raw_data.get("category", "business"),
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
