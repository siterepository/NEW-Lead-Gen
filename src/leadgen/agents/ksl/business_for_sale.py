"""
KSL Classifieds - Business For Sale Agent

Scrapes KSL Classifieds for people selling businesses.  Business sellers
are prime NWM recruiting targets because they are:
- Entrepreneurial (they started or ran a business)
- Looking for their next venture
- Potentially have capital from the sale
- Have sales, people, and leadership skills

NOTE: This agent intentionally stays on classifieds.ksl.com (not ksl.com/jobs)
because business-for-sale listings belong in the classifieds marketplace.
Other KSL agents were moved to ksl.com/jobs to avoid getting general
marketplace items (cars, appliances) for professional keyword searches.
Be aware that KSL classifieds may return mixed results for some keywords.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import quote_plus, urlencode

import httpx

from leadgen.agents.base import BaseAgent

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Search configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://classifieds.ksl.com"

# Keywords targeting businesses for sale and franchise opportunities
SEARCH_KEYWORDS: list[str] = [
    "business for sale",
    "franchise",
    "established business",
    "profitable business",
    "turnkey business",
    "restaurant for sale",
    "retail store",
    "service business",
    "franchise opportunity",
    "business opportunity",
    "owner retiring",
    "client base included",
    "agency for sale",
    "insurance book",
]

# KSL classifieds categories for business sales
CATEGORY_SLUGS: list[str] = [
    "Businesses-For-Sale",
    "Business-Opportunities",
    "Franchise",
    "Commercial",
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

        Uses httpx to fetch KSL pages and extracts listing data from
        the embedded Next.js RSC payload (no browser rendering needed).
        """
        all_items: list[dict] = []
        search_urls = self.get_search_urls()
        max_urls = self.config.get("max_pages", 5)

        headers = {
            "User-Agent": self.get_random_user_agent(),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        }

        async with httpx.AsyncClient(
            follow_redirects=True, timeout=30, headers=headers
        ) as client:
            for i, url in enumerate(search_urls[:max_urls]):
                if len(all_items) >= self.max_results_per_run:
                    break

                await self.rate_limiter.acquire()
                delay = self.get_random_delay()
                await asyncio.sleep(delay)

                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        logger.warning(
                            "[%s] HTTP %d for %s", self.name, resp.status_code, url
                        )
                        continue

                    # Extract listings from Next.js RSC embedded data
                    ids = re.findall(r'\\?"id\\?":\s*(\d{7,})', resp.text)
                    titles = re.findall(
                        r'\\?"title\\?":\\?"((?:[^\\]|\\.)*?)\\?"', resp.text
                    )
                    cities = re.findall(
                        r'\\?"city\\?":\\?"((?:[^\\]|\\.)*?)\\?"', resp.text
                    )
                    prices = re.findall(
                        r'\\?"price\\?":\\?"((?:[^\\]|\\.)*?)\\?"', resp.text
                    )

                    for j, title in enumerate(titles):
                        if len(all_items) >= self.max_results_per_run:
                            break
                        clean_title = title.replace('\\"', '"').replace("\\\\", "\\")
                        lid = ids[j] if j < len(ids) else ""
                        city = (
                            cities[j].replace('\\"', "") if j < len(cities) else ""
                        )
                        price = (
                            prices[j].replace('\\"', "") if j < len(prices) else ""
                        )

                        all_items.append({
                            "post_id": lid,
                            "title": clean_title,
                            "location_city": city,
                            "location_state": "Utah",
                            "price": price,
                            "source_url": f"{BASE_URL}/listing/{lid}" if lid else url,
                            "platform": "ksl",
                            "category": "business_for_sale",
                            "scraped_at": datetime.now(timezone.utc).isoformat(),
                        })

                    logger.info(
                        "[%s] URL %d/%d: %d listings from %s",
                        self.name, i + 1, min(len(search_urls), max_urls),
                        len(titles), url[:80],
                    )

                except Exception as exc:
                    logger.warning("[%s] Error fetching %s: %s", self.name, url[:60], exc)
        logger.info(
            "[%s] Scrape complete: %d raw items collected", self.name, len(all_items)
        )
        return all_items

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
