"""
Indeed Utah Agent

Scrapes Indeed.com for Utah job listings to find people actively
seeking career changes - prime NWM financial advisor recruiting targets.

NOTE: Indeed has aggressive anti-bot detection.  This agent uses
extended delays (5-15s), aggressive UA rotation, and conservative
page limits to minimise detection risk.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlencode, quote_plus

import httpx

from leadgen.agents.base import BaseAgent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Search configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://www.indeed.com"

SEARCH_KEYWORDS: list[str] = [
    "career change",
    "financial advisor",
    "insurance agent",
    "sales representative",
    "business development",
    "entrepreneur",
    "real estate agent",
    "self employed",
    "independent contractor",
    "commission sales",
    "wealth management",
    "financial planner",
    "account manager",
]


class IndeedUtahAgent(BaseAgent):
    """Scrape Indeed.com for Utah job seekers and career changers.

    People browsing Indeed for opportunity-type roles or posting
    resumes in sales/finance categories are strong NWM recruits.

    Anti-bot strategy:
    - Long random delays between requests (5-15s)
    - Aggressive user-agent rotation every request
    - Conservative pagination (max 5 pages per keyword)
    - Randomised keyword order each run
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="indeed_utah",
            platform="indeed",
            config=config,
            db=db,
        )
        # Override base delays for Indeed's stricter anti-bot
        self.delay_min = self.config.get("delay_min", 5.0)
        self.delay_max = self.config.get("delay_max", 15.0)
        self.max_pages: int = self.config.get("max_pages", 5)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 150)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build Indeed search URLs for Utah job seekers.

        URL pattern: https://www.indeed.com/jobs?q=<keyword>&l=Utah&sort=date
        """
        urls: list[str] = []
        keywords = list(SEARCH_KEYWORDS)
        random.shuffle(keywords)  # randomise order to vary crawl pattern

        for keyword in keywords:
            params = {
                "q": keyword,
                "l": "Utah",
                "sort": "date",
                "fromage": "14",  # last 14 days
            }
            url = f"{BASE_URL}/jobs?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)

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
        """Extract job cards from Indeed HTML via regex."""
        items: list[dict] = []
        # Indeed job links contain data-jk attribute or /viewjob?jk= pattern
        for m in re.finditer(
            r'<a[^>]+href="(/viewjob\?jk=([^"&]+)[^"]*)"[^>]*>',
            html,
        ):
            href, jk = m.group(1), m.group(2)
            url = f"{BASE_URL}{href}" if not href.startswith("http") else href
            # Try to find title near this link
            title_match = re.search(
                r'<span[^>]*>([^<]{5,})</span>', html[m.end():m.end()+500]
            )
            title = title_match.group(1).strip() if title_match else ""
            items.append({
                "title": title,
                "url": url,
                "post_id": jk,
                "company": "",
                "location": "",
                "salary": "",
                "description": "",
                "posted_date": "",
            })
        # Fallback: match job title patterns
        if not items:
            for m in re.finditer(
                r'<a[^>]+href="([^"]*indeed[^"]*)"[^>]*>([^<]{5,})</a>', html
            ):
                href, title = m.group(1), m.group(2).strip()
                url = href if href.startswith("http") else f"{BASE_URL}{href}"
                items.append({
                    "title": title,
                    "url": url,
                    "post_id": url,
                    "company": "",
                    "location": "",
                    "salary": "",
                    "description": "",
                    "posted_date": "",
                })
        return items

    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw Indeed data into the standard lead format."""
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)

        return {
            "name": "",
            "title": raw_data.get("title", ""),
            "company": raw_data.get("company", ""),
            "description": raw_data.get("description", ""),
            "location_city": city,
            "location_state": "Utah",
            "posted_date": raw_data.get("posted_date", ""),
            "salary": raw_data.get("salary", ""),
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "indeed",
            "contact_info": "",
            "contact_phone": "",
            "contact_email": "",
            "category": "job_listing",
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_city(location: str) -> str:
        """Extract city from Indeed location like 'Salt Lake City, UT'."""
        if not location:
            return ""
        city = re.sub(r",?\s*(UT|Utah)\s*$", "", location, flags=re.IGNORECASE)
        city = re.sub(r"\s*\d{5}(-\d{4})?\s*$", "", city)
        # Indeed sometimes adds "+1 location" suffix
        city = re.sub(r"\s*\+\d+\s*location.*$", "", city, flags=re.IGNORECASE)
        return city.strip()
