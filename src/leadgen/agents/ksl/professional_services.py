"""
KSL Classifieds - Professional Services Agent

Scrapes KSL Classifieds for accountants, financial planners, lawyers, and
other professionals offering services.  These individuals have transferable
skills and client-facing experience that maps well to NWM financial advising.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import quote_plus, urlencode

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext

from leadgen.agents.base import BaseAgent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Search configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://classifieds.ksl.com"

SEARCH_KEYWORDS: list[str] = [
    "accountant",
    "CPA",
    "bookkeeper",
    "financial planner",
    "financial advisor",
    "tax preparation",
    "tax services",
    "lawyer",
    "attorney",
    "paralegal",
    "real estate agent",
    "insurance agent",
    "mortgage broker",
    "investment",
    "wealth management",
    "estate planning",
    "notary",
]

CATEGORY_SLUGS: list[str] = [
    "Services",
    "Business-Opportunities",
    "Career-Services",
]


class KSLProfessionalServicesAgent(BaseAgent):
    """Scrape KSL Classifieds for professional service providers in Utah.

    These listings reveal people who are:
    - Licensed professionals with client bases
    - Experienced in financial / legal services
    - Potentially looking for career changes
    - Already comfortable with client relationship management
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="ksl_professional_services",
            platform="ksl",
            config=config,
            db=db,
        )
        self.max_pages: int = self.config.get("max_pages", 10)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 200)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build search URLs targeting professional service providers."""
        urls: list[str] = []

        for keyword in SEARCH_KEYWORDS:
            for category in CATEGORY_SLUGS:
                params = {
                    "keyword": keyword,
                    "category": category,
                    "state": "Utah",
                    "sort": "newest",
                }
                url = f"{BASE_URL}/search/?{urlencode(params, quote_via=quote_plus)}"
                urls.append(url)

        for category in CATEGORY_SLUGS:
            params = {"category": category, "state": "Utah", "sort": "newest"}
            url = f"{BASE_URL}/search/?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Scrape KSL for professional service provider listings."""
        search_urls = self.get_search_urls()
        crawler = await self.setup_browser()
        collected: list[dict] = []

        @crawler.router.default_handler
        async def handle_listing_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            try:
                await page.wait_for_selector(
                    "div.listing-item, div.search-result, article.listing",
                    timeout=15_000,
                )
            except Exception:
                logger.debug("[%s] No listings on %s", self.name, context.request.url)
                return

            cards = await page.query_selector_all(
                "div.listing-item, div.search-result, article.listing"
            )

            for card in cards:
                if len(collected) >= self.max_results_per_run:
                    break
                try:
                    raw = await self._extract_card(card, page)
                    if raw:
                        listing_url = raw.get("url", "")
                        if listing_url:
                            last = await self.check_last_seen(listing_url)
                            post_id = raw.get("post_id", "")
                            if last and last == post_id:
                                continue
                        collected.append(raw)
                except Exception as exc:
                    logger.warning("[%s] Card extraction failed: %s", self.name, exc)

            next_btn = await page.query_selector(
                "a.pagination-next, a[rel='next'], a.next-page, button.pagination-next"
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

        logger.info("[%s] Scrape complete: %d raw items", self.name, len(collected))
        return collected

    # ------------------------------------------------------------------
    # Card extraction
    # ------------------------------------------------------------------

    async def _extract_card(self, card: Any, page: Any) -> Optional[dict]:
        """Extract structured data from a single listing card."""
        raw: dict[str, Any] = {}

        title_el = await card.query_selector(
            "h2.item-title a, h3.listing-title a, a.item-link, "
            "[data-testid='listing-title']"
        )
        if title_el:
            raw["title"] = (await title_el.inner_text()).strip()
            href = await title_el.get_attribute("href")
            if href:
                raw["url"] = href if href.startswith("http") else f"{BASE_URL}{href}"
        else:
            return None

        desc_el = await card.query_selector(
            "div.item-description, p.listing-description, "
            "div.item-body, span.description-text"
        )
        if desc_el:
            raw["description"] = (await desc_el.inner_text()).strip()

        date_el = await card.query_selector(
            "span.item-date, time, span.listing-date, [data-testid='listing-date']"
        )
        if date_el:
            raw["posted_date"] = (await date_el.inner_text()).strip()
            dt_attr = await date_el.get_attribute("datetime")
            if dt_attr:
                raw["posted_date_iso"] = dt_attr

        loc_el = await card.query_selector(
            "span.item-location, span.listing-location, "
            "div.location, [data-testid='listing-location']"
        )
        if loc_el:
            raw["location"] = (await loc_el.inner_text()).strip()

        price_el = await card.query_selector(
            "span.listing-price, span.item-price, div.price, "
            "[data-testid='listing-price']"
        )
        if price_el:
            raw["price"] = (await price_el.inner_text()).strip()

        contact_el = await card.query_selector(
            "a.contact-seller, span.phone-number, a[href^='tel:'], a[href^='mailto:']"
        )
        if contact_el:
            contact_text = (await contact_el.inner_text()).strip()
            contact_href = await contact_el.get_attribute("href") or ""
            raw["contact_info"] = contact_text or contact_href

        if raw.get("url"):
            match = re.search(r"/(\d+)", raw["url"])
            raw["post_id"] = match.group(1) if match else raw["url"]
        elif raw.get("title"):
            raw["post_id"] = raw["title"]

        img_el = await card.query_selector(
            "div.item-image img, img.listing-image, [data-testid='listing-image'] img"
        )
        if img_el:
            raw["image_url"] = await img_el.get_attribute("src")

        cat_el = await card.query_selector(
            "span.category-tag, span.item-category, a.category-link"
        )
        if cat_el:
            raw["category"] = (await cat_el.inner_text()).strip()

        return raw

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw KSL professional services data into the standard lead format."""
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)
        name = raw_data.get("author", raw_data.get("seller_name", ""))
        contact = raw_data.get("contact_info", "")
        phone, email = self._parse_contact(contact)
        posted_date = raw_data.get("posted_date_iso") or raw_data.get("posted_date", "")

        return {
            "name": name,
            "title": raw_data.get("title", ""),
            "description": raw_data.get("description", ""),
            "location_city": city,
            "location_state": "Utah",
            "posted_date": posted_date,
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "ksl",
            "contact_info": contact,
            "contact_phone": phone,
            "contact_email": email,
            "price": raw_data.get("price", ""),
            "category": raw_data.get("category", ""),
            "image_url": raw_data.get("image_url", ""),
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_city(location: str) -> str:
        if not location:
            return ""
        city = re.sub(r",?\s*(UT|Utah)\s*$", "", location, flags=re.IGNORECASE)
        city = re.sub(r"\s*\d{5}(-\d{4})?\s*$", "", city)
        return city.strip()

    @staticmethod
    def _parse_contact(contact: str) -> tuple[str, str]:
        phone = ""
        email = ""
        if not contact:
            return phone, email

        email_match = re.search(
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", contact
        )
        if email_match:
            email = email_match.group(0).lower()

        phone_match = re.search(
            r"(\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})", contact
        )
        if phone_match:
            phone = phone_match.group(1).strip()

        tel_match = re.search(r"tel:([+\d-]+)", contact)
        if tel_match and not phone:
            phone = tel_match.group(1)

        mailto_match = re.search(r"mailto:([^\s&]+)", contact)
        if mailto_match and not email:
            email = mailto_match.group(1).lower()

        return phone, email
