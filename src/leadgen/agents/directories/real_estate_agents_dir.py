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

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext

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
        """Scrape real estate agent directories."""
        collected: list[dict] = []
        search_urls = self.get_search_urls()
        crawler = await self.setup_browser()

        @crawler.router.default_handler
        async def handle_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            # Wait for agent cards
            try:
                await page.wait_for_selector(
                    "div.agent-card, div.agent-listing, "
                    "div.agent-info, li.agent-item, "
                    "div[data-testid='agent-card'], "
                    "div.component_agentCard, "
                    "div.broker-card, div.member-card",
                    timeout=15_000,
                )
            except Exception:
                logger.debug(
                    "[%s] No agent cards on %s", self.name, context.request.url
                )
                return

            cards = await page.query_selector_all(
                "div.agent-card, div.agent-listing, "
                "div.agent-info, li.agent-item, "
                "div[data-testid='agent-card'], "
                "div.component_agentCard, "
                "div.broker-card, div.member-card"
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
                "a.next, a[aria-label='Next'], a[rel='next'], "
                "li.next a, a.pagination-next, "
                "button[aria-label='Next page']"
            )
            if next_btn and len(collected) < self.max_results_per_run:
                next_href = await next_btn.get_attribute("href")
                if next_href:
                    if not next_href.startswith("http"):
                        # Determine base from current URL
                        current = context.request.url
                        if "realtor.com" in current:
                            next_href = f"{REALTOR_BASE}{next_href}"
                        else:
                            next_href = f"{UTRE_BASE}{next_href}"
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
        """Extract data from a single real estate agent card."""
        raw: dict[str, Any] = {}

        # Agent name
        name_el = await card.query_selector(
            "a.agent-name, h3.agent-name, span.agent-name, "
            "a[data-testid='agent-name'], div.agent-title a, "
            "h2 a, h3 a, span.name a"
        )
        if name_el:
            raw["name"] = (await name_el.inner_text()).strip()
            href = await name_el.get_attribute("href")
            if href:
                if not href.startswith("http"):
                    raw["url"] = f"{UTRE_BASE}{href}"
                else:
                    raw["url"] = href
        else:
            return None

        # Brokerage / Company
        company_el = await card.query_selector(
            "span.brokerage, span.company, div.office-name, "
            "span.agent-group, p.brokerage-name"
        )
        if company_el:
            raw["brokerage"] = (await company_el.inner_text()).strip()

        # Phone
        phone_el = await card.query_selector(
            "a[href^='tel:'], span.phone, span.agent-phone, "
            "a.phone-link"
        )
        if phone_el:
            phone_text = (await phone_el.inner_text()).strip()
            phone_href = await phone_el.get_attribute("href") or ""
            raw["phone"] = phone_text or phone_href.replace("tel:", "")

        # Email
        email_el = await card.query_selector(
            "a[href^='mailto:'], span.email, a.email-link"
        )
        if email_el:
            email_text = (await email_el.inner_text()).strip()
            email_href = await email_el.get_attribute("href") or ""
            raw["email"] = email_text or email_href.replace("mailto:", "")

        # Location / service area
        loc_el = await card.query_selector(
            "span.location, div.service-area, span.area, "
            "p.agent-location"
        )
        if loc_el:
            raw["location"] = (await loc_el.inner_text()).strip()

        # Specialties
        spec_el = await card.query_selector(
            "span.specialties, div.agent-specialties, "
            "p.designations"
        )
        if spec_el:
            raw["specialties"] = (await spec_el.inner_text()).strip()

        # Listings count / experience
        listings_el = await card.query_selector(
            "span.listing-count, span.sales-count, "
            "div.agent-stats"
        )
        if listings_el:
            raw["listings_count"] = (await listings_el.inner_text()).strip()

        # Rating
        rating_el = await card.query_selector(
            "span.rating, div.agent-rating, span.stars"
        )
        if rating_el:
            raw["rating"] = (await rating_el.inner_text()).strip()

        # Photo URL
        img_el = await card.query_selector(
            "img.agent-photo, img.agent-image, img.headshot"
        )
        if img_el:
            raw["image_url"] = await img_el.get_attribute("src")

        # Post ID
        raw["post_id"] = raw.get("url", raw.get("name", ""))

        return raw

    # ------------------------------------------------------------------
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
