"""
Medium Career Stories Agent

Searches Medium for career change stories by Utah authors.  Uses Google search
with site:medium.com to discover public Medium articles without requiring
Medium authentication.
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
# Google search queries targeting Medium articles
# ---------------------------------------------------------------------------

GOOGLE_SEARCH_QUERIES: list[str] = [
    'site:medium.com "utah" "career change" OR "new career"',
    'site:medium.com "utah" "career transition"',
    'site:medium.com "utah" "financial planning" OR "financial advisor"',
    'site:medium.com "utah" "entrepreneur" OR "startup"',
    'site:medium.com "salt lake city" "career change"',
    'site:medium.com "salt lake city" "new job" OR "career"',
    'site:medium.com "utah" "leaving corporate" OR "quit my job"',
    'site:medium.com "utah" "side hustle" OR "freelance"',
    'site:medium.com "utah" "business owner" OR "self employed"',
    'site:medium.com "provo" OR "ogden" "career change"',
]


class MediumCareersAgent(BaseAgent):
    """Search Medium for career change stories by Utah authors.

    Uses Google to discover public Medium articles.  Does NOT require
    Medium API access or authentication.

    Targets:
    - Utah residents writing about career changes
    - Entrepreneurs sharing startup / business stories from Utah
    - People documenting transitions into financial services
    - Authors discussing Utah job market and opportunities
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="medium_careers",
            platform="medium",
            config=config,
            db=db,
        )
        self.max_results_per_run: int = self.config.get("max_results_per_run", 100)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build Google search URLs targeting Medium articles."""
        urls: list[str] = []

        for query in GOOGLE_SEARCH_QUERIES:
            params = {"q": query, "num": "20"}
            url = f"https://www.google.com/search?{urlencode(params, quote_via=quote_plus)}"
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
        """Convert raw Medium article data into the standard lead format."""
        return {
            "name": raw_data.get("author", ""),
            "title": raw_data.get("title", ""),
            "description": raw_data.get("description", ""),
            "location_city": "",
            "location_state": "Utah",
            "posted_date": raw_data.get("posted_date", ""),
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "medium",
            "source_site": raw_data.get("source", ""),
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
