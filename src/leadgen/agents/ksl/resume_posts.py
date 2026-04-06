"""
KSL Jobs - Resume Posts Agent

Scrapes KSL Jobs (ksl.com/jobs) for people actively posting resumes and
"jobs wanted" ads.  These individuals are explicitly seeking new employment
and may be open to a career in financial services with NWM.

NOTE: Switched from classifieds.ksl.com to ksl.com/jobs because the
classifieds general marketplace returns cars/appliances instead of
professional listings when searching by keyword.
"""

from __future__ import annotations

import asyncio
import logging
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

BASE_URL = "https://www.ksl.com/jobs"

SEARCH_KEYWORDS: list[str] = [
    "sales manager",
    "business development",
    "account executive",
    "real estate agent",
    "insurance agent",
    "financial services",
    "sales director",
    "financial advisor",
]


class KSLResumePostsAgent(BaseAgent):
    """Scrape KSL Jobs for people posting resumes / jobs-wanted ads.

    These listings reveal people who are:
    - Actively seeking employment (posted a resume)
    - Between jobs and urgently available
    - Willing to consider new career paths
    - Self-motivated enough to market themselves
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="ksl_resume_posts",
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
        """Build search URLs targeting resume posts and jobs-wanted listings on KSL Jobs.

        Uses the KSL Jobs search endpoint:
            https://www.ksl.com/jobs/search?keyword=...&location=Utah
        """
        urls: list[str] = []

        for keyword in SEARCH_KEYWORDS:
            params = {
                "keyword": keyword,
                "location": "Utah",
            }
            url = f"{BASE_URL}/search?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Scrape KSL Jobs for resume / jobs-wanted posts.

        Uses httpx to fetch KSL Jobs pages and extracts listing data from
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
                            "category": "resume_posts",
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
        """Convert raw KSL resume post data into the standard lead format."""
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
