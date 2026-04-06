"""
Glassdoor Utah Agent

Scrapes Glassdoor company reviews in Utah for employees expressing
dissatisfaction - a recruiting signal for NWM financial advisor prospects.
People unhappy at their current job are open to career conversations.
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

BASE_URL = "https://www.glassdoor.com"

# Utah state search ID on Glassdoor
UTAH_REVIEWS_URL = (
    "https://www.glassdoor.com/Reviews/"
    "utah-reviews-SRCH_IL.0,4_IS937.htm"
)

# Industries where dissatisfied employees make good NWM recruits
INDUSTRY_FILTERS: list[str] = [
    "Banking",
    "Insurance",
    "Real Estate",
    "Financial Services",
    "Accounting",
    "Sales",
    "Retail",
    "Education",
]

# Low star ratings indicate unhappy employees
LOW_RATING_THRESHOLD = 3.0


class GlassdoorUtahAgent(BaseAgent):
    """Scrape Glassdoor for Utah company reviews indicating dissatisfaction.

    Strategy:
    - Find companies in Utah with low overall ratings
    - Extract review snippets mentioning dissatisfaction, low pay,
      poor management, limited growth
    - These signal employees open to career changes
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="glassdoor_utah",
            platform="glassdoor",
            config=config,
            db=db,
        )
        self.max_pages: int = self.config.get("max_pages", 8)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 150)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build Glassdoor review search URLs for Utah companies.

        Returns the base Utah reviews URL plus industry-filtered variants.
        """
        urls: list[str] = [UTAH_REVIEWS_URL]

        # Add page variants for deeper crawling
        for page_num in range(2, self.max_pages + 1):
            page_url = UTAH_REVIEWS_URL.replace(
                ".htm", f"_IP{page_num}.htm"
            )
            urls.append(page_url)

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
        """Extract company review cards from Glassdoor HTML via regex."""
        items: list[dict] = []
        # Glassdoor company links: /Reviews/<company>-Reviews-...htm
        for m in re.finditer(
            r'<a[^>]+href="(/Reviews/[^"]+\.htm)"[^>]*>([^<]{3,})</a>',
            html,
        ):
            href, company = m.group(1), m.group(2).strip()
            url = f"{BASE_URL}{href}" if not href.startswith("http") else href
            # Try to extract rating nearby
            rating = ""
            context = html[max(0, m.start()-200):m.end()+200]
            rating_match = re.search(r'(\d\.\d)', context)
            if rating_match:
                rating = rating_match.group(1)
            items.append({
                "company": company,
                "url": url,
                "rating": rating,
                "review_count": "",
                "location": "",
                "industry": "",
                "review_snippet": "",
                "post_id": url,
            })
        return items

    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw Glassdoor data to standard lead format."""
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)

        return {
            "name": "",
            "company": raw_data.get("company", ""),
            "title": f"Reviews: {raw_data.get('company', '')}",
            "description": raw_data.get("review_snippet", ""),
            "rating": raw_data.get("rating", ""),
            "review_count": raw_data.get("review_count", ""),
            "industry": raw_data.get("industry", ""),
            "location_city": city,
            "location_state": "Utah",
            "posted_date": "",
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "glassdoor",
            "contact_info": "",
            "contact_phone": "",
            "contact_email": "",
            "category": "company_review",
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_city(location: str) -> str:
        """Extract city from Glassdoor location string."""
        if not location:
            return ""
        city = re.sub(r",?\s*(UT|Utah)\s*$", "", location, flags=re.IGNORECASE)
        city = re.sub(r"\s*\d{5}(-\d{4})?\s*$", "", city)
        return city.strip()
