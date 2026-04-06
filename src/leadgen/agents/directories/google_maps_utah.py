"""
Google Maps Utah Agent

Finds new businesses in Utah via Google Maps / Places.  Uses the
Google Places API (free $200/mo credit) when an API key is available,
or falls back to scraping Google Maps search results.

New business owners are entrepreneurial and make strong NWM
financial advisor recruiting targets.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
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

GOOGLE_MAPS_URL = "https://www.google.com/maps"
PLACES_API_URL = "https://maps.googleapis.com/maps/api/place"

# Business types to search for in Utah
BUSINESS_QUERIES: list[str] = [
    "new business Utah",
    "financial services Utah",
    "insurance agency Utah",
    "real estate agency Utah",
    "consulting firm Utah",
    "small business Utah",
    "startup Utah",
    "entrepreneur Utah",
    "coaching business Utah",
    "marketing agency Utah",
]

# Utah cities for geographically-targeted searches
UTAH_LOCATIONS: list[dict[str, Any]] = [
    {"name": "Salt Lake City", "lat": 40.7608, "lng": -111.8910},
    {"name": "Provo", "lat": 40.2338, "lng": -111.6585},
    {"name": "Ogden", "lat": 41.2230, "lng": -111.9738},
    {"name": "St George", "lat": 37.0965, "lng": -113.5684},
    {"name": "Orem", "lat": 40.2969, "lng": -111.6946},
    {"name": "Sandy", "lat": 40.5649, "lng": -111.8389},
    {"name": "Lehi", "lat": 40.3916, "lng": -111.8508},
    {"name": "Logan", "lat": 41.7370, "lng": -111.8338},
]


class GoogleMapsUtahAgent(BaseAgent):
    """Find new businesses in Utah via Google Maps / Places API.

    Two modes of operation:
    1. API mode (preferred): Uses Google Places API with free $200/mo credit
       Requires GOOGLE_PLACES_API_KEY environment variable.
    2. Scrape mode (fallback): Scrapes Google Maps search results via browser.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="google_maps_utah",
            platform="google_maps",
            config=config,
            db=db,
        )
        self.api_key: str = self.config.get(
            "google_places_api_key",
            os.environ.get("GOOGLE_PLACES_API_KEY", ""),
        )
        self.use_api: bool = bool(self.api_key)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 200)
        self.search_radius_meters: int = self.config.get(
            "search_radius_meters", 30_000
        )

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build search URLs for Google Maps / Places API.

        In API mode, returns Places Text Search URLs.
        In scrape mode, returns Google Maps search URLs.
        """
        urls: list[str] = []

        if self.use_api:
            # Google Places Text Search API
            for query in BUSINESS_QUERIES:
                params = {
                    "query": query,
                    "key": self.api_key,
                    "region": "us",
                }
                url = (
                    f"{PLACES_API_URL}/textsearch/json?"
                    f"{urlencode(params, quote_via=quote_plus)}"
                )
                urls.append(url)

            # Nearby Search for each Utah city
            for loc in UTAH_LOCATIONS[:4]:
                for btype in ["accounting", "insurance_agency", "real_estate_agency"]:
                    params = {
                        "location": f"{loc['lat']},{loc['lng']}",
                        "radius": str(self.search_radius_meters),
                        "type": btype,
                        "key": self.api_key,
                    }
                    url = (
                        f"{PLACES_API_URL}/nearbysearch/json?"
                        f"{urlencode(params, quote_via=quote_plus)}"
                    )
                    urls.append(url)
        else:
            # Fallback: Google Maps browser URLs
            for query in BUSINESS_QUERIES:
                encoded = quote_plus(query)
                urls.append(f"{GOOGLE_MAPS_URL}/search/{encoded}")

        logger.info("[%s] Generated %d search URLs (api=%s)", self.name, len(urls), self.use_api)
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
        """Extract business listings from Google Maps / search HTML via regex."""
        items: list[dict] = []
        # Google search result links
        for m in re.finditer(
            r'<a[^>]+href="([^"]+)"[^>]*><h3[^>]*>([^<]+)</h3></a>',
            html,
        ):
            href, title = m.group(1), m.group(2).strip()
            if not title or "google" in href.lower():
                continue
            items.append({
                "business_name": title,
                "url": href,
                "location": "",
                "phone": "",
                "category": "",
                "post_id": href,
            })
        return items

    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw Google Maps/Places data to standard lead format."""
        location_raw = raw_data.get("location", raw_data.get("address", ""))
        city = self._parse_city(location_raw)

        types_list = raw_data.get("types", [])
        category = ", ".join(types_list) if isinstance(types_list, list) else str(types_list)

        return {
            "name": "",
            "business_name": raw_data.get("business_name", ""),
            "title": f"Business: {raw_data.get('business_name', '')}",
            "description": (
                f"Rating: {raw_data.get('rating', 'N/A')} "
                f"({raw_data.get('user_ratings_total', raw_data.get('review_count', '0'))} reviews). "
                f"Address: {raw_data.get('address', raw_data.get('location', ''))}."
            ),
            "rating": raw_data.get("rating", ""),
            "review_count": raw_data.get(
                "user_ratings_total", raw_data.get("review_count", "")
            ),
            "place_id": raw_data.get("place_id", ""),
            "location_city": city,
            "location_state": "Utah",
            "posted_date": "",
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "google_maps",
            "contact_info": "",
            "contact_phone": "",
            "contact_email": "",
            "category": category or "business",
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
        # Try to extract city from address like "123 Main St, Salt Lake City, UT 84101"
        match = re.search(
            r"(?:,\s*)([A-Z][a-zA-Z\s]+?)(?:,\s*(?:UT|Utah))", location
        )
        if match:
            return match.group(1).strip()
        # Fallback: strip state/zip
        city = re.sub(r",?\s*(UT|Utah)\s*$", "", location, flags=re.IGNORECASE)
        city = re.sub(r"\s*\d{5}(-\d{4})?\s*$", "", city)
        return city.strip()
