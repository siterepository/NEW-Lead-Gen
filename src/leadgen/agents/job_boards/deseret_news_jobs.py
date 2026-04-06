"""
Deseret News Jobs Agent

Scrapes Deseret News classifieds/jobs section for Utah-specific
career opportunities and job seekers.  Deseret News is a major
Utah publication - people using its classifieds are local and
actively seeking, making them prime NWM recruiting targets.
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

BASE_URL = "https://www.deseret.com"
CLASSIFIEDS_URL = "https://classifieds.deseret.com"

SEARCH_KEYWORDS: list[str] = [
    "career opportunity",
    "sales",
    "financial",
    "business",
    "insurance",
    "real estate",
    "management",
    "employment",
    "hiring",
    "seeking professionals",
]

CATEGORY_SLUGS: list[str] = [
    "jobs",
    "employment",
    "business-opportunities",
    "services",
]


class DeseretNewsJobsAgent(BaseAgent):
    """Scrape Deseret News classifieds for Utah job seekers.

    Targets:
    - Job listings in classifieds section
    - Career opportunity postings
    - Business-for-sale and services ads
    People reading / posting here are Utah locals looking for
    career movement.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="deseret_news_jobs",
            platform="deseret_news",
            config=config,
            db=db,
        )
        self.max_pages: int = self.config.get("max_pages", 8)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 150)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build search URLs for Deseret News classifieds."""
        urls: list[str] = []

        # Keyword-based searches
        for keyword in SEARCH_KEYWORDS:
            params = {
                "q": keyword,
                "category": "jobs",
                "location": "Utah",
            }
            url = (
                f"{CLASSIFIEDS_URL}/search?"
                f"{urlencode(params, quote_via=quote_plus)}"
            )
            urls.append(url)

        # Category browsing
        for cat in CATEGORY_SLUGS:
            urls.append(f"{CLASSIFIEDS_URL}/{cat}")

        # Also check the main Deseret News jobs section
        urls.append(f"{BASE_URL}/jobs")

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
        """Extract job listings from HTML via regex."""
        items: list[dict] = []
        # Generic job listing pattern: links with title text
        for m in re.finditer(
            r'<a[^>]+href="([^"]+)"[^>]*>([^<]{5,})</a>',
            html,
        ):
            href, title = m.group(1), m.group(2).strip()
            # Skip navigation / footer links
            if any(skip in title.lower() for skip in [
                "privacy", "terms", "cookie", "about", "contact",
                "sign in", "log in", "home", "back",
            ]):
                continue
            url = href if href.startswith("http") else f"{CLASSIFIEDS_URL}{href}"
            post_id_match = re.search(r"/(\d{4,})", href)
            post_id = post_id_match.group(1) if post_id_match else url
            items.append({
                "title": title,
                "url": url,
                "post_id": post_id,
                "company": "",
                "location": "",
                "salary": "",
                "description": "",
                "posted_date": "",
                "contact_info": "",
            })
        return items

    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw Deseret News data to standard lead format."""
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)

        contact = raw_data.get("contact_info", "")
        phone, email = self._parse_contact(contact)

        posted_date = raw_data.get("posted_date_iso") or raw_data.get(
            "posted_date", ""
        )

        return {
            "name": "",
            "title": raw_data.get("title", ""),
            "description": raw_data.get("description", ""),
            "location_city": city,
            "location_state": "Utah",
            "posted_date": posted_date,
            "price": raw_data.get("price", ""),
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "deseret_news",
            "contact_info": contact,
            "contact_phone": phone,
            "contact_email": email,
            "category": raw_data.get("category", "classified"),
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
