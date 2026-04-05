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

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext

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
        """Scrape Google search results for Medium career articles."""
        all_items: list[dict] = []
        crawler = await self.setup_browser()
        collected: list[dict] = []

        @crawler.router.default_handler
        async def handle_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            try:
                await page.wait_for_load_state("domcontentloaded", timeout=15_000)
            except Exception:
                return

            # Extract Google search results
            results = await page.query_selector_all("div.g")
            for result in results:
                if len(collected) >= self.max_results_per_run:
                    break
                try:
                    link_el = await result.query_selector("a[href]")
                    if not link_el:
                        continue

                    href = await link_el.get_attribute("href") or ""

                    # Only keep Medium URLs
                    if "medium.com" not in href:
                        continue

                    title_el = await result.query_selector("h3")
                    title = ""
                    if title_el:
                        title = (await title_el.inner_text()).strip()

                    # Extract snippet
                    snippet_el = await result.query_selector(
                        "div.VwiC3b, span.aCOpRe, div[data-sncf]"
                    )
                    snippet = ""
                    if snippet_el:
                        snippet = (await snippet_el.inner_text()).strip()

                    # Try to extract author from URL or snippet
                    author = self._extract_author(href, snippet)

                    # Extract date from snippet if present
                    posted_date = self._extract_date(snippet)

                    collected.append({
                        "title": title,
                        "url": href,
                        "description": snippet[:500],
                        "author": author,
                        "posted_date": posted_date,
                        "source": "medium_article",
                        "post_id": href,
                    })
                except Exception as exc:
                    logger.debug("[%s] Result extraction failed: %s", self.name, exc)

        search_urls = self.get_search_urls()
        urls_to_crawl: list[str] = []

        for url in search_urls:
            if len(collected) >= self.max_results_per_run:
                break
            await self.rate_limiter.acquire()
            await asyncio.sleep(self.get_random_delay())
            urls_to_crawl.append(url)

        if urls_to_crawl:
            await crawler.run(urls_to_crawl)

        all_items = collected
        logger.info("[%s] Scrape complete: %d items collected", self.name, len(all_items))
        return all_items

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_author(url: str, snippet: str) -> str:
        """Try to extract the author name from Medium article metadata.

        Medium URLs often follow: medium.com/@username/article-title
        """
        author = ""

        # Extract username from Medium URL
        username_match = re.search(r"medium\.com/@([^/]+)", url)
        if username_match:
            author = username_match.group(1).replace("-", " ").title()

        # Check for "by Name" in snippet
        if not author:
            by_match = re.search(r"\bby\s+([A-Z][a-z]+ [A-Z][a-z]+)", snippet)
            if by_match:
                author = by_match.group(1)

        return author

    @staticmethod
    def _extract_date(snippet: str) -> str:
        """Try to extract a date from the Google snippet."""
        # Google often prepends dates like "Jan 15, 2024 ---"
        date_match = re.search(
            r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})",
            snippet,
        )
        if date_match:
            return date_match.group(1)
        return ""

    # ------------------------------------------------------------------
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
