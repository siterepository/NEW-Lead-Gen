"""
KSL Classifieds - Business For Sale Agent

Scrapes KSL Classifieds for people selling businesses.  Business sellers
are prime NWM recruiting targets because they are:
- Entrepreneurial (they started or ran a business)
- Looking for their next venture
- Potentially have capital from the sale
- Have sales, people, and leadership skills
"""

from __future__ import annotations

import asyncio
import logging
import random
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

# Keywords targeting business-for-sale and business-opportunity listings
SEARCH_KEYWORDS: list[str] = [
    "business for sale",
    "franchise",
    "turnkey business",
    "established business",
    "business opportunity",
    "retiring owner",
    "owner financing",
    "revenue",
    "profitable business",
    "client list",
    "customer base",
    "inventory included",
    "relocating",
    "must sell",
    "partnership",
    "investment opportunity",
]

# KSL classifieds categories for business sales
CATEGORY_SLUGS: list[str] = [
    "Business-Opportunities",
    "Businesses-For-Sale",
    "Commercial",
    "Franchise",
]


class KSLBusinessForSaleAgent(BaseAgent):
    """Scrape KSL Classifieds for businesses listed for sale.

    People selling businesses are high-value recruiting targets:
    - Entrepreneurial mindset (built/ran a business)
    - In transition -- looking for what comes next
    - May have capital from the sale
    - Proven sales and leadership skills
    - Often have large professional networks
    """

    # ------------------------------------------------------------------
    # Agent identity
    # ------------------------------------------------------------------

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="ksl_business_for_sale",
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
        """Build search URLs for KSL classifieds targeting business-for-sale.

        Combines business-sale keywords with relevant category slugs.
        """
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

        # Broad category-only URLs (no keyword filter)
        for category in CATEGORY_SLUGS:
            params = {
                "category": category,
                "state": "Utah",
                "sort": "newest",
            }
            url = f"{BASE_URL}/search/?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Execute scraping against KSL Classifieds business-for-sale listings.

        Workflow:
        1. Build search URLs via get_search_urls().
        2. Use Crawlee PlaywrightCrawler to load each page.
        3. Extract listing cards from the results.
        4. Handle pagination up to self.max_pages per URL.
        5. Respect rate limits with random 2-8 second delays.
        6. De-duplicate against last_seen records.
        7. Return list of raw listing dicts.
        """
        all_items: list[dict] = []
        search_urls = self.get_search_urls()

        crawler = await self.setup_browser()

        collected: list[dict] = []

        @crawler.router.default_handler
        async def handle_listing_page(context: PlaywrightCrawlingContext) -> None:
            """Process a single search-results page."""
            page = context.page
            await self.apply_stealth(page)

            # Wait for listing cards
            try:
                await page.wait_for_selector(
                    "div.listing-item, div.search-result, article.listing",
                    timeout=15_000,
                )
            except Exception:
                logger.debug(
                    "[%s] No listings found on %s", self.name, context.request.url
                )
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
                    logger.warning(
                        "[%s] Failed to extract card: %s", self.name, exc
                    )

            # Pagination
            next_btn = await page.query_selector(
                "a.pagination-next, a[rel='next'], a.next-page, "
                "button.pagination-next"
            )
            if next_btn and len(collected) < self.max_results_per_run:
                next_href = await next_btn.get_attribute("href")
                if next_href:
                    if not next_href.startswith("http"):
                        next_href = f"{BASE_URL}{next_href}"
                    await self.rate_limiter.acquire()
                    delay = self.get_random_delay()
                    await asyncio.sleep(delay)
                    await context.enqueue_links(urls=[next_href])

        # Enqueue all search URLs with rate limiting
        urls_to_crawl: list[str] = []
        for url in search_urls:
            if len(collected) >= self.max_results_per_run:
                break
            await self.rate_limiter.acquire()
            delay = self.get_random_delay()
            await asyncio.sleep(delay)
            urls_to_crawl.append(url)

        if urls_to_crawl:
            await crawler.run(urls_to_crawl)

        all_items = collected
        logger.info(
            "[%s] Scrape complete: %d raw items collected", self.name, len(all_items)
        )
        return all_items

    # ------------------------------------------------------------------
    # Card extraction helper
    # ------------------------------------------------------------------

    async def _extract_card(self, card: Any, page: Any) -> Optional[dict]:
        """Extract structured data from a single business-for-sale card."""
        raw: dict[str, Any] = {}

        # Title (required)
        title_el = await card.query_selector(
            "h2.item-title a, h3.listing-title a, a.item-link, "
            "[data-testid='listing-title']"
        )
        if title_el:
            raw["title"] = (await title_el.inner_text()).strip()
            href = await title_el.get_attribute("href")
            if href:
                if not href.startswith("http"):
                    href = f"{BASE_URL}{href}"
                raw["url"] = href
        else:
            return None

        # Description / preview text
        desc_el = await card.query_selector(
            "div.item-description, p.listing-description, "
            "div.item-body, span.description-text"
        )
        if desc_el:
            raw["description"] = (await desc_el.inner_text()).strip()

        # Posted date
        date_el = await card.query_selector(
            "span.item-date, time, span.listing-date, "
            "[data-testid='listing-date']"
        )
        if date_el:
            raw["posted_date"] = (await date_el.inner_text()).strip()
            dt_attr = await date_el.get_attribute("datetime")
            if dt_attr:
                raw["posted_date_iso"] = dt_attr

        # Location
        loc_el = await card.query_selector(
            "span.item-location, span.listing-location, "
            "div.location, [data-testid='listing-location']"
        )
        if loc_el:
            raw["location"] = (await loc_el.inner_text()).strip()

        # Price -- critical for business-for-sale (indicates business size)
        price_el = await card.query_selector(
            "span.listing-price, span.item-price, "
            "div.price, [data-testid='listing-price']"
        )
        if price_el:
            raw["price"] = (await price_el.inner_text()).strip()
            raw["price_numeric"] = self._parse_price(raw["price"])

        # Contact info
        contact_el = await card.query_selector(
            "a.contact-seller, span.phone-number, a[href^='tel:'], "
            "a[href^='mailto:']"
        )
        if contact_el:
            contact_text = (await contact_el.inner_text()).strip()
            contact_href = await contact_el.get_attribute("href") or ""
            raw["contact_info"] = contact_text or contact_href

        # Post ID for dedup
        if raw.get("url"):
            match = re.search(r"/(\d+)", raw["url"])
            if match:
                raw["post_id"] = match.group(1)
            else:
                raw["post_id"] = raw["url"]
        elif raw.get("title"):
            raw["post_id"] = raw["title"]

        # Image
        img_el = await card.query_selector(
            "div.item-image img, img.listing-image, "
            "[data-testid='listing-image'] img"
        )
        if img_el:
            raw["image_url"] = await img_el.get_attribute("src")

        # Category tag
        cat_el = await card.query_selector(
            "span.category-tag, span.item-category, a.category-link"
        )
        if cat_el:
            raw["category"] = (await cat_el.inner_text()).strip()

        # Business-specific fields
        raw["listing_type"] = "business_for_sale"

        return raw

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw KSL business-for-sale listing into the standard format."""
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)

        name = raw_data.get("author", raw_data.get("seller_name", ""))

        contact = raw_data.get("contact_info", "")
        phone, email = self._parse_contact(contact)

        posted_date = raw_data.get("posted_date_iso") or raw_data.get(
            "posted_date", ""
        )

        # Detect business type from title/description
        business_type = self._detect_business_type(
            raw_data.get("title", ""), raw_data.get("description", "")
        )

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
            "price_numeric": raw_data.get("price_numeric"),
            "category": raw_data.get("category", ""),
            "image_url": raw_data.get("image_url", ""),
            "listing_type": "business_for_sale",
            "business_type": business_type,
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_city(location: str) -> str:
        """Extract city name from a KSL location string."""
        if not location:
            return ""
        city = re.sub(r",?\s*(UT|Utah)\s*$", "", location, flags=re.IGNORECASE)
        city = re.sub(r"\s*\d{5}(-\d{4})?\s*$", "", city)
        return city.strip()

    @staticmethod
    def _parse_contact(contact: str) -> tuple[str, str]:
        """Extract phone number and email from a contact string."""
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

    @staticmethod
    def _parse_price(price_str: str) -> Optional[float]:
        """Parse a price string into a numeric value.

        Handles formats like '$50,000', '$1.2M', '50000', 'Call for price'.
        Returns None if the price cannot be parsed.
        """
        if not price_str:
            return None

        text = price_str.strip().lower()

        # Remove currency symbols and commas
        cleaned = re.sub(r"[$,]", "", text)

        # Handle 'M' for millions, 'K' for thousands
        mult_match = re.search(r"([\d.]+)\s*(m|million)", cleaned)
        if mult_match:
            try:
                return float(mult_match.group(1)) * 1_000_000
            except ValueError:
                return None

        k_match = re.search(r"([\d.]+)\s*(k|thousand)", cleaned)
        if k_match:
            try:
                return float(k_match.group(1)) * 1_000
            except ValueError:
                return None

        # Plain number
        num_match = re.search(r"[\d.]+", cleaned)
        if num_match:
            try:
                return float(num_match.group(0))
            except ValueError:
                return None

        return None

    @staticmethod
    def _detect_business_type(title: str, description: str) -> str:
        """Classify the type of business being sold.

        Returns a tag that the scoring pipeline can use to prioritize
        leads with transferable skills (e.g. service businesses, sales,
        consulting over manufacturing or agriculture).
        """
        text = f"{title} {description}".lower()

        if any(kw in text for kw in ["restaurant", "food", "cafe", "catering"]):
            return "food_service"
        if any(kw in text for kw in ["salon", "spa", "beauty", "barber"]):
            return "personal_care"
        if any(kw in text for kw in ["retail", "store", "shop", "ecommerce"]):
            return "retail"
        if any(kw in text for kw in ["construction", "contractor", "plumbing", "hvac"]):
            return "construction_trades"
        if any(kw in text for kw in ["consulting", "agency", "marketing", "advertising"]):
            return "professional_services"
        if any(kw in text for kw in ["insurance", "financial", "accounting", "tax"]):
            return "financial_services"
        if any(kw in text for kw in ["real estate", "property", "rental"]):
            return "real_estate"
        if any(kw in text for kw in ["franchise", "chain", "brand"]):
            return "franchise"
        if any(kw in text for kw in ["tech", "software", "saas", "app"]):
            return "technology"
        if any(kw in text for kw in ["fitness", "gym", "training"]):
            return "fitness"
        if any(kw in text for kw in ["cleaning", "janitorial", "maid"]):
            return "cleaning"
        if any(kw in text for kw in ["landscape", "lawn", "tree"]):
            return "landscaping"

        return "general"
