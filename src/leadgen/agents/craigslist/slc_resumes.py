"""
Craigslist Salt Lake City - Resumes Agent

Scrapes the Salt Lake City Craigslist "resumes" section for people posting
their resumes publicly.  These individuals are actively marketing themselves
for employment and represent high-intent recruiting prospects.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import httpx

from leadgen.agents.base import BaseAgent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Search configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://saltlakecity.craigslist.org"
SEARCH_PATH = "/search/res"  # resumes section

SEARCH_KEYWORDS: list[str] = [
    "sales",
    "business development",
    "finance",
    "accounting",
    "management",
    "marketing",
    "insurance",
    "real estate",
    "customer service",
    "leadership",
    "entrepreneur",
    "professional",
]


class CraigslistSLCResumesAgent(BaseAgent):
    """Scrape Craigslist SLC resumes section for potential NWM recruits.

    These postings reveal people who are:
    - Actively advertising their skills and experience
    - High-intent job seekers (took effort to post resume)
    - Detailing their professional background publicly
    - Located in the Salt Lake City metro area
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="craigslist_slc_resumes",
            platform="craigslist",
            config=config,
            db=db,
        )
        self.max_pages: int = self.config.get("max_pages", 5)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 150)
        self.delay_min = self.config.get("delay_min", 5.0)
        self.delay_max = self.config.get("delay_max", 10.0)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build Craigslist search URLs for SLC resumes."""
        urls: list[str] = []

        for keyword in SEARCH_KEYWORDS:
            params = {"query": keyword, "sort": "date"}
            url = f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"
            urls.append(url)

        # Broad search
        params = {"sort": "date"}
        url = f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"
        urls.append(url)

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Scrape Craigslist SLC resumes listings.

        Uses httpx to fetch pages and regex to parse the server-rendered
        HTML (no browser needed -- CL is fully SSR).
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

                    html = resp.text
                    items = self._parse_listings(html)

                    for item in items:
                        if len(all_items) >= self.max_results_per_run:
                            break
                        all_items.append(item)

                    logger.info(
                        "[%s] URL %d/%d: %d listings from %s",
                        self.name, i + 1, min(len(search_urls), max_urls),
                        len(items), url[:80],
                    )

                except Exception as exc:
                    logger.warning("[%s] Error fetching %s: %s", self.name, url[:60], exc)

        logger.info(
            "[%s] Scrape complete: %d raw items collected", self.name, len(all_items)
        )
        return all_items

    # ------------------------------------------------------------------
    # HTML parsing helpers
    # ------------------------------------------------------------------

    def _parse_listings(self, html: str) -> list[dict]:
        """Extract listings from Craigslist server-rendered HTML via regex."""
        items: list[dict] = []

        row_pattern = re.compile(
            r'<li[^>]*class="[^"]*(?:cl-static-search-result|result-row)[^"]*"[^>]*>'
            r'(.*?)</li>',
            re.DOTALL,
        )

        for m in row_pattern.finditer(html):
            block = m.group(0)

            # -- Title + URL --
            title_match = re.search(
                r'<a[^>]*class="[^"]*(?:titlestring|result-title|cl-app-anchor)[^"]*"'
                r'[^>]*href="([^"]*)"[^>]*>(.*?)</a>',
                block, re.DOTALL,
            )
            if not title_match:
                title_match = re.search(
                    r'<a[^>]*href="([^"]*)"[^>]*>(.*?)</a>',
                    block, re.DOTALL,
                )
            if not title_match:
                continue

            href = title_match.group(1).strip()
            title = re.sub(r'<[^>]+>', '', title_match.group(2)).strip()
            if not title:
                continue

            url = href if href.startswith("http") else f"{BASE_URL}{href}"

            # -- Location --
            location = ""
            loc_match = re.search(
                r'<span[^>]*class="[^"]*(?:result-hood|nearby|supertitle)[^"]*"[^>]*>'
                r'(.*?)</span>',
                block, re.DOTALL,
            )
            if loc_match:
                location = re.sub(r'<[^>]+>', '', loc_match.group(1)).strip().strip("() ")

            # -- Date --
            posted_date = ""
            date_match = re.search(
                r'<time[^>]*datetime="([^"]*)"', block
            )
            if date_match:
                posted_date = date_match.group(1).strip()

            # -- Post ID from URL --
            post_id = ""
            id_match = re.search(r'/(\d+)\.html', url)
            if id_match:
                post_id = id_match.group(1)

            items.append({
                "title": title,
                "url": url,
                "source_url": url,
                "post_id": post_id or url,
                "price": "",
                "location": location,
                "posted_date_iso": posted_date,
                "posted_date": posted_date,
                "platform": "craigslist",
                "category": "resumes",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })

        return items

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw CL resumes data into the standard lead format."""
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)
        posted_date = raw_data.get("posted_date_iso") or raw_data.get("posted_date", "")
        contact = raw_data.get("contact_info", "")
        phone, email = self._parse_contact(contact)

        return {
            "name": "",
            "title": raw_data.get("title", ""),
            "description": raw_data.get("meta", ""),
            "location_city": city,
            "location_state": "Utah",
            "posted_date": posted_date,
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "craigslist",
            "contact_info": contact,
            "contact_phone": phone,
            "contact_email": email,
            "price": "",
            "category": "resumes",
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
