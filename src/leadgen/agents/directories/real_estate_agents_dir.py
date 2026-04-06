"""
Utah Real Estate Agent Directory Agent

Scrapes Utah real estate agent directories for agent listings.
Real estate agents are prime NWM financial advisor recruits because:
- They have large networks
- Commission-based income mindset
- Entrepreneurial and self-motivated
- Often looking for supplemental or replacement income
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

# Primary source: Utah Association of Realtors / utahrealestate.com
UTRE_BASE = "https://www.utahrealestate.com"
REALTOR_BASE = "https://www.realtor.com"

# Utah counties / areas for targeted search
UTAH_AREAS: list[str] = [
    "Salt Lake County",
    "Utah County",
    "Davis County",
    "Weber County",
    "Washington County",
    "Cache County",
    "Summit County",
    "Iron County",
    "Tooele County",
    "Box Elder County",
]

UTAH_CITIES: list[str] = [
    "Salt Lake City",
    "Provo",
    "Ogden",
    "Orem",
    "Sandy",
    "West Jordan",
    "St George",
    "Layton",
    "Logan",
    "Lehi",
    "Draper",
    "Park City",
]


class RealEstateAgentDirAgent(BaseAgent):
    """Scrape Utah real estate agent directories for agent listings.

    Sources:
    - utahrealestate.com agent directory
    - realtor.com Utah agent search (fallback)
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="real_estate_agents_dir",
            platform="real_estate_dir",
            config=config,
            db=db,
        )
        self.max_pages: int = self.config.get("max_pages", 10)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 200)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build search URLs for real estate agent directories."""
        urls: list[str] = []

        # Utah Real Estate agent directory
        urls.append(f"{UTRE_BASE}/agents")

        # City-based agent searches on utahrealestate.com
        for city in UTAH_CITIES:
            encoded = quote_plus(city)
            urls.append(f"{UTRE_BASE}/agents/search?location={encoded}")

        # Realtor.com fallback - agent finder for Utah cities
        for city in UTAH_CITIES[:6]:
            slug = city.lower().replace(" ", "-")
            urls.append(
                f"{REALTOR_BASE}/realestateagents/{slug}_ut"
            )

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
        """Convert raw agent directory data to standard lead format."""
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)

        phone = raw_data.get("phone", "")
        email = raw_data.get("email", "")

        return {
            "name": raw_data.get("name", ""),
            "title": f"Real Estate Agent: {raw_data.get('name', '')}",
            "brokerage": raw_data.get("brokerage", ""),
            "description": (
                f"Brokerage: {raw_data.get('brokerage', 'N/A')}. "
                f"Specialties: {raw_data.get('specialties', 'N/A')}."
            ),
            "specialties": raw_data.get("specialties", ""),
            "listings_count": raw_data.get("listings_count", ""),
            "rating": raw_data.get("rating", ""),
            "location_city": city,
            "location_state": "Utah",
            "posted_date": "",
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "real_estate_dir",
            "contact_info": phone or email,
            "contact_phone": phone,
            "contact_email": email,
            "image_url": raw_data.get("image_url", ""),
            "category": "real_estate_agent",
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
        # Remove county suffix
        city = re.sub(r"\s+County\s*$", "", city, flags=re.IGNORECASE)
        return city.strip()
