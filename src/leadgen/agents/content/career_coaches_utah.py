"""
Utah Career Coaches Agent

Scrapes Utah career coaching websites to find their clients and testimonials.
People actively working with career coaches are in the midst of career
transitions and may be strong NWM financial advisor recruiting prospects.
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
# Target coach sites and discovery queries
# ---------------------------------------------------------------------------

# Google searches to discover Utah career coaches and their testimonials
GOOGLE_SEARCH_QUERIES: list[str] = [
    '"career coach" "utah" testimonials',
    '"career coach" "salt lake city" reviews',
    '"career coaching" "utah" client results',
    '"executive coach" "utah" testimonials',
    '"life coach" "career change" "utah"',
    '"resume writer" "utah" testimonials',
    '"career counselor" "utah" reviews',
    '"career transition coach" "utah"',
    'site:thumbtack.com "career coach" "utah"',
    'site:yelp.com "career coach" "utah"',
]

# Known Utah career coaching sites to scrape directly
KNOWN_COACH_SITES: list[str] = [
    "https://www.thumbtack.com/ut/salt-lake-city/career-counseling/",
    "https://www.yelp.com/search?find_desc=career+coaching&find_loc=Salt+Lake+City%2C+UT",
]


class UtahCareerCoachesAgent(BaseAgent):
    """Scrape Utah career coaching websites for client testimonials.

    Targets:
    - Testimonial pages showing people in career transitions
    - Review sites with Utah career coaching clients
    - Coach websites listing their specialties and client types
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="career_coaches_utah",
            platform="content",
            config=config,
            db=db,
        )
        self.max_results_per_run: int = self.config.get("max_results_per_run", 100)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build Google search URLs and direct coach site URLs."""
        urls: list[str] = []

        for query in GOOGLE_SEARCH_QUERIES:
            params = {"q": query}
            url = f"https://www.google.com/search?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)

        urls.extend(KNOWN_COACH_SITES)

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
        """Extract article links from blog / content site HTML via regex."""
        items: list[dict] = []
        if "google.com/search" in source_url:
            # Google search results
            for m in re.finditer(
                r'<a[^>]+href="(https?://(?!www\.google)[^"]+)"[^>]*>'
                r'(?:<h3[^>]*>([^<]+)</h3>)?',
                html,
            ):
                href = m.group(1)
                title = m.group(2).strip() if m.group(2) else ""
                if "/search" in href or not title:
                    continue
                items.append({
                    "title": title,
                    "url": href,
                    "source": "google_search",
                    "description": "",
                    "author": "",
                    "posted_date": "",
                    "post_id": href,
                })
        else:
            # Blog / article pages
            for m in re.finditer(
                r'<a[^>]+href="([^"]+)"[^>]*>([^<]{10,})</a>',
                html,
            ):
                href, title = m.group(1), m.group(2).strip()
                if any(skip in title.lower() for skip in [
                    "privacy", "terms", "cookie", "about", "contact",
                    "sign in", "log in", "menu", "navigation",
                ]):
                    continue
                url = href if href.startswith("http") else f"{source_url.rstrip('/')}/{href.lstrip('/')}"
                items.append({
                    "title": title,
                    "url": url,
                    "source": "utah_biz_blog",
                    "description": "",
                    "author": "",
                    "posted_date": "",
                    "post_id": url,
                })
        return items

    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw coach/testimonial data into the standard lead format."""
        return {
            "name": raw_data.get("client_name", ""),
            "title": raw_data.get("title", ""),
            "description": raw_data.get("description", ""),
            "location_city": "",
            "location_state": "Utah",
            "posted_date": "",
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", raw_data.get("url", "")),
            "platform": "content",
            "source_site": raw_data.get("source", ""),
            "coach_name": raw_data.get("coach_name", ""),
            "testimonial_text": raw_data.get("testimonial_text", ""),
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
