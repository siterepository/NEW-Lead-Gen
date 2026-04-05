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

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext

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

        Workflow:
        1. Build search URLs via get_search_urls().
        2. For each URL, use Crawlee PlaywrightCrawler to load the page.
        3. Extract listing cards from the results.
        4. Handle pagination (up to self.max_pages per search URL).
        5. Respect rate limits with random delays.
        6. De-duplicate against last_seen records.
        7. Return list of raw listing dicts.
        """
        all_items: list[dict] = []
        search_urls = self.get_search_urls()

        crawler = await self.setup_browser()

        # Shared state across the request handler closures
        collected: list[dict] = []

        @crawler.router.default_handler
        async def handle_listing_page(context: PlaywrightCrawlingContext) -> None:
            """Process a single search-results page."""
            page = context.page
            await self.apply_stealth(page)

            # Wait for listing cards to appear
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

            # Extract all listing cards on the page
            cards = await page.query_selector_all(
                "div.listing-item, div.search-result, article.listing"
            )

            for card in cards:
                if len(collected) >= self.max_results_per_run:
                    break

                try:
                    raw = await self._extract_card(card, page)
                    if raw:
                        # Skip already-seen listings
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

            # Handle pagination -- look for "next page" link
            next_btn = await page.query_selector(
                "a.pagination-next, a[rel='next'], a.next-page, "
                "button.pagination-next"
            )
            if next_btn and len(collected) < self.max_results_per_run:
                next_href = await next_btn.get_attribute("href")
                if next_href:
                    if not next_href.startswith("http"):
                        next_href = f"{BASE_URL}{next_href}"
                    # Rate-limit before following pagination
                    await self.rate_limiter.acquire()
                    delay = self.get_random_delay()
                    await asyncio.sleep(delay)
                    await context.enqueue_links(
                        urls=[next_href],
                    )

        # Enqueue all search URLs with rate limiting
        urls_to_crawl: list[str] = []
        pages_per_url = 0

        for url in search_urls:
            if len(collected) >= self.max_results_per_run:
                break

            await self.rate_limiter.acquire()
            delay = self.get_random_delay()
            await asyncio.sleep(delay)
            urls_to_crawl.append(url)

        # Run the crawler
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
