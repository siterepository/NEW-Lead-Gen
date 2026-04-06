"""
KSL Classifieds - Job Seekers Agent

Scrapes KSL Classifieds (classifieds.ksl.com) for people posting in
job/career/services categories who might be good NWM financial advisor
recruits.  Targets people actively looking for work, career changes,
or new opportunities in Utah.
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

# Keywords that surface people seeking work / career change / opportunities
SEARCH_KEYWORDS: list[str] = [
    "career change",
    "looking for work",
    "seeking employment",
    "opportunity",
    "available for hire",
    "sales experience",
    "business owner",
    "entrepreneur",
    "real estate",
    "insurance",
    "coaching",
    "freelance",
    "consultant",
    "side hustle",
    "self employed",
]

# KSL classifieds category IDs / slugs relevant to job seekers
CATEGORY_SLUGS: list[str] = [
    "Services",
    "Jobs",
    "Business-Opportunities",
    "Career-Services",
]


class KSLJobSeekersAgent(BaseAgent):
    """Scrape KSL Classifieds for job seekers and career-changers in Utah.

    These listings often reveal people who are:
    - Between jobs (open to new career paths)
    - Actively seeking better opportunities
    - Entrepreneurial and self-starting
    - Located in the Utah market
    """

    # ------------------------------------------------------------------
    # Agent identity
    # ------------------------------------------------------------------

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="ksl_job_seekers",
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
        """Build search URLs for KSL classifieds targeting job seekers.

        Combines keywords with category slugs to produce a comprehensive
        list of search URLs.  Uses the KSL query-string pattern:
            https://classifieds.ksl.com/search/?keyword=...&category=...&state=Utah
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

        # Also add broad category-only URLs (no keyword filter)
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
        """Execute the main scraping logic against KSL Classifieds.

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
                            "category": "services",
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
    # Card extraction helper
    # ------------------------------------------------------------------

    async def _extract_card(self, card: Any, page: Any) -> Optional[dict]:
        """Extract structured data from a single listing card element.

        Returns a raw dict or None if extraction fails.
        """
        raw: dict[str, Any] = {}

        # Title
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
            # Title is required
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
            # Also try datetime attribute
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

        # Price (indicates business size if selling a business)
        price_el = await card.query_selector(
            "span.listing-price, span.item-price, "
            "div.price, [data-testid='listing-price']"
        )
        if price_el:
            raw["price"] = (await price_el.inner_text()).strip()

        # Contact info (phone/email if visible on card)
        contact_el = await card.query_selector(
            "a.contact-seller, span.phone-number, a[href^='tel:'], "
            "a[href^='mailto:']"
        )
        if contact_el:
            contact_text = (await contact_el.inner_text()).strip()
            contact_href = await contact_el.get_attribute("href") or ""
            raw["contact_info"] = contact_text or contact_href

        # Generate a post_id for dedup from the URL or title
        if raw.get("url"):
            # Extract numeric ID from URL if present
            match = re.search(r"/(\d+)", raw["url"])
            if match:
                raw["post_id"] = match.group(1)
            else:
                raw["post_id"] = raw["url"]
        elif raw.get("title"):
            raw["post_id"] = raw["title"]

        # Image URL (sometimes useful for profile detection)
        img_el = await card.query_selector(
            "div.item-image img, img.listing-image, "
            "[data-testid='listing-image'] img"
        )
        if img_el:
            raw["image_url"] = await img_el.get_attribute("src")

        # Category tag if present
        cat_el = await card.query_selector(
            "span.category-tag, span.item-category, "
            "a.category-link"
        )
        if cat_el:
            raw["category"] = (await cat_el.inner_text()).strip()

        return raw

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw KSL listing data into the standard format the
        normalizer pipeline expects.

        Maps raw scrape fields to the canonical schema:
            name, title, description, location_city, location_state,
            posted_date, url, platform, contact_info, price, category
        """
        # Parse location into city component
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)

        # Try to extract a name from the listing (author/seller)
        name = raw_data.get("author", raw_data.get("seller_name", ""))

        # Parse contact info
        contact = raw_data.get("contact_info", "")
        phone, email = self._parse_contact(contact)

        # Normalize posted date
        posted_date = raw_data.get("posted_date_iso") or raw_data.get(
            "posted_date", ""
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
        """Extract city name from a KSL location string.

        KSL formats locations as 'City, UT' or 'City, Utah' or just 'City'.
        """
        if not location:
            return ""
        # Remove state suffixes
        city = re.sub(r",?\s*(UT|Utah)\s*$", "", location, flags=re.IGNORECASE)
        # Remove zip codes
        city = re.sub(r"\s*\d{5}(-\d{4})?\s*$", "", city)
        return city.strip()

    @staticmethod
    def _parse_contact(contact: str) -> tuple[str, str]:
        """Extract phone number and email from a contact string.

        Returns (phone, email) tuple.  Either may be empty.
        """
        phone = ""
        email = ""

        if not contact:
            return phone, email

        # Look for email
        email_match = re.search(
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", contact
        )
        if email_match:
            email = email_match.group(0).lower()

        # Look for phone (US formats)
        phone_match = re.search(
            r"(\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})", contact
        )
        if phone_match:
            phone = phone_match.group(1).strip()

        # Also check for tel: href
        tel_match = re.search(r"tel:([+\d-]+)", contact)
        if tel_match and not phone:
            phone = tel_match.group(1)

        # Also check for mailto: href
        mailto_match = re.search(r"mailto:([^\s&]+)", contact)
        if mailto_match and not email:
            email = mailto_match.group(1).lower()

        return phone, email
