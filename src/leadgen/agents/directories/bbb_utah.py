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

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext

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
        """Scrape BBB Utah for businesses with complaints."""
        collected: list[dict] = []
        search_urls = self.get_search_urls()
        crawler = await self.setup_browser()

        @crawler.router.default_handler
        async def handle_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            # Wait for business cards
            try:
                await page.wait_for_selector(
                    "div.search-results div.result-item, "
                    "div.bds-body a.text-blue-medium, "
                    "li.search-result, div.result-card, "
                    "div[data-testid='search-result']",
                    timeout=15_000,
                )
            except Exception:
                logger.debug(
                    "[%s] No results on %s", self.name, context.request.url
                )
                return

            cards = await page.query_selector_all(
                "div.search-results div.result-item, "
                "li.search-result, div.result-card, "
                "div[data-testid='search-result'], "
                "div.listing-item"
            )

            for card in cards:
                if len(collected) >= self.max_results_per_run:
                    break
                try:
                    raw = await self._extract_card(card, page)
                    if raw:
                        collected.append(raw)
                except Exception as exc:
                    logger.warning("[%s] Card extraction error: %s", self.name, exc)

            # Pagination
            next_btn = await page.query_selector(
                "a.next, a[aria-label='Next'], "
                "li.pagination-next a, a.pagination-next"
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
    # Card extraction
    # ------------------------------------------------------------------

    async def _extract_card(self, card: Any, page: Any) -> Optional[dict]:
        """Extract data from a single BBB business listing card."""
        raw: dict[str, Any] = {}

        # Business name
        name_el = await card.query_selector(
            "a.text-blue-medium, h3.result-name a, "
            "a.business-name, h4 a, span.org-name a"
        )
        if name_el:
            raw["business_name"] = (await name_el.inner_text()).strip()
            href = await name_el.get_attribute("href")
            if href:
                if not href.startswith("http"):
                    href = f"{BASE_URL}{href}"
                raw["url"] = href
        else:
            return None

        # BBB rating
        rating_el = await card.query_selector(
            "span.result-rating, span.bbb-rating, "
            "div.rating-letter, span[data-testid='rating']"
        )
        if rating_el:
            raw["bbb_rating"] = (await rating_el.inner_text()).strip()

        # Complaint count
        complaint_el = await card.query_selector(
            "span.complaint-count, a.complaints-link, "
            "span[data-testid='complaints']"
        )
        if complaint_el:
            raw["complaint_count"] = (await complaint_el.inner_text()).strip()

        # Location
        loc_el = await card.query_selector(
            "span.result-location, div.address, "
            "p.location, span.city-state"
        )
        if loc_el:
            raw["location"] = (await loc_el.inner_text()).strip()

        # Phone
        phone_el = await card.query_selector(
            "a.result-phone, a[href^='tel:'], "
            "span.phone-number"
        )
        if phone_el:
            phone_text = (await phone_el.inner_text()).strip()
            phone_href = await phone_el.get_attribute("href") or ""
            raw["phone"] = phone_text or phone_href.replace("tel:", "")

        # Category
        cat_el = await card.query_selector(
            "span.category, span.business-category, "
            "p.categories"
        )
        if cat_el:
            raw["category"] = (await cat_el.inner_text()).strip()

        # Accreditation status
        accred_el = await card.query_selector(
            "span.accreditation, img.accredited-badge, "
            "span[data-testid='accreditation']"
        )
        if accred_el:
            raw["accredited"] = True
        else:
            raw["accredited"] = False

        # Post ID
        raw["post_id"] = raw.get("url", raw.get("business_name", ""))

        return raw

    # ------------------------------------------------------------------
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
