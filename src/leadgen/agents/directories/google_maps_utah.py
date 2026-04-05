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

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext

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
        """Scrape Google Maps or use Places API for Utah businesses."""
        if self.use_api:
            return await self._scrape_api()
        return await self._scrape_browser()

    async def _scrape_api(self) -> list[dict]:
        """Use Google Places API to find businesses."""
        import aiohttp  # noqa: F811

        collected: list[dict] = []
        search_urls = self.get_search_urls()

        async with aiohttp.ClientSession() as session:
            for url in search_urls:
                if len(collected) >= self.max_results_per_run:
                    break
                await self.rate_limiter.acquire()
                await asyncio.sleep(self.get_random_delay())

                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        if resp.status != 200:
                            logger.warning(
                                "[%s] API returned %d for %s",
                                self.name, resp.status, url[:80],
                            )
                            continue
                        data = await resp.json()
                        results = data.get("results", [])
                        for place in results:
                            if len(collected) >= self.max_results_per_run:
                                break
                            raw = self._parse_api_result(place)
                            if raw:
                                collected.append(raw)
                except Exception as exc:
                    logger.warning("[%s] API request failed: %s", self.name, exc)

        logger.info("[%s] API collected %d raw items", self.name, len(collected))
        return collected

    async def _scrape_browser(self) -> list[dict]:
        """Fallback: scrape Google Maps search results via browser."""
        collected: list[dict] = []
        search_urls = self.get_search_urls()
        crawler = await self.setup_browser()

        @crawler.router.default_handler
        async def handle_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            # Wait for Maps results to load
            try:
                await page.wait_for_selector(
                    "div.Nv2PK, div[role='feed'] div.Nv2PK, "
                    "div.section-result, a.hfpxzc",
                    timeout=20_000,
                )
            except Exception:
                logger.debug(
                    "[%s] No Maps results on %s", self.name, context.request.url
                )
                return

            # Scroll to load more results
            feed = await page.query_selector("div[role='feed']")
            if feed:
                for _ in range(5):
                    await feed.evaluate("el => el.scrollTop = el.scrollHeight")
                    await asyncio.sleep(1.5)

            cards = await page.query_selector_all(
                "div.Nv2PK, a.hfpxzc"
            )

            for card in cards:
                if len(collected) >= self.max_results_per_run:
                    break
                try:
                    raw = await self._extract_maps_card(card, page)
                    if raw:
                        collected.append(raw)
                except Exception as exc:
                    logger.warning("[%s] Card extraction error: %s", self.name, exc)

        urls_to_crawl: list[str] = []
        for url in search_urls:
            if len(collected) >= self.max_results_per_run:
                break
            await self.rate_limiter.acquire()
            await asyncio.sleep(self.get_random_delay())
            urls_to_crawl.append(url)

        if urls_to_crawl:
            await crawler.run(urls_to_crawl)

        logger.info("[%s] Browser collected %d raw items", self.name, len(collected))
        return collected

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    def _parse_api_result(self, place: dict) -> Optional[dict]:
        """Parse a Google Places API result into raw dict."""
        name = place.get("name", "")
        if not name:
            return None

        location = place.get("geometry", {}).get("location", {})
        address = place.get("formatted_address", "")

        return {
            "business_name": name,
            "address": address,
            "location": address,
            "lat": location.get("lat"),
            "lng": location.get("lng"),
            "rating": place.get("rating"),
            "user_ratings_total": place.get("user_ratings_total"),
            "types": place.get("types", []),
            "place_id": place.get("place_id", ""),
            "business_status": place.get("business_status", ""),
            "url": f"https://www.google.com/maps/place/?q=place_id:{place.get('place_id', '')}",
            "post_id": place.get("place_id", name),
        }

    async def _extract_maps_card(
        self, card: Any, page: Any
    ) -> Optional[dict]:
        """Extract data from a Google Maps search result card."""
        raw: dict[str, Any] = {}

        # Business name
        name_el = await card.query_selector(
            "div.qBF1Pd, div.fontHeadlineSmall, "
            "span.fontHeadlineSmall"
        )
        if name_el:
            raw["business_name"] = (await name_el.inner_text()).strip()
        else:
            # Try aria-label on the card itself
            label = await card.get_attribute("aria-label")
            if label:
                raw["business_name"] = label.strip()
            else:
                return None

        # Rating
        rating_el = await card.query_selector(
            "span.MW4etd, span.fontBodyMedium span[role='img']"
        )
        if rating_el:
            raw["rating"] = (await rating_el.inner_text()).strip()

        # Review count
        review_el = await card.query_selector(
            "span.UY7F9, span.fontBodyMedium span"
        )
        if review_el:
            raw["review_count"] = (await review_el.inner_text()).strip()

        # Address / category info
        info_els = await card.query_selector_all(
            "div.W4Efsd span, div.fontBodyMedium > span"
        )
        info_parts = []
        for el in info_els:
            text = (await el.inner_text()).strip()
            if text and text != "\u00b7":
                info_parts.append(text)
        if info_parts:
            raw["location"] = " ".join(info_parts)

        # URL from href
        href = await card.get_attribute("href")
        if href:
            raw["url"] = href

        # Post ID
        raw["post_id"] = raw.get("url", raw.get("business_name", ""))

        return raw

    # ------------------------------------------------------------------
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
