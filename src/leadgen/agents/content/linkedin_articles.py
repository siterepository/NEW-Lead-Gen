"""
LinkedIn Articles Agent

Searches for LinkedIn articles and posts by Utah authors about career changes,
financial planning, and entrepreneurship.  Uses Google search with
site:linkedin.com/pulse to discover public LinkedIn content without
requiring LinkedIn authentication.
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
# Google search queries targeting LinkedIn articles
# ---------------------------------------------------------------------------

GOOGLE_SEARCH_QUERIES: list[str] = [
    'site:linkedin.com/pulse "utah" "career change"',
    'site:linkedin.com/pulse "utah" "financial planning"',
    'site:linkedin.com/pulse "utah" "entrepreneurship"',
    'site:linkedin.com/pulse "utah" "career transition"',
    'site:linkedin.com/pulse "salt lake city" "new career"',
    'site:linkedin.com/pulse "utah" "leaving corporate"',
    'site:linkedin.com/pulse "utah" "financial advisor"',
    'site:linkedin.com/pulse "utah" "business owner"',
    'site:linkedin.com/posts "utah" "career change"',
    'site:linkedin.com/posts "utah" "new opportunity"',
    'site:linkedin.com/posts "salt lake" "career"',
    'site:linkedin.com/posts "utah" "financial planning"',
]


class LinkedInArticlesAgent(BaseAgent):
    """Search for LinkedIn articles by Utah authors about career topics.

    Uses Google to discover public LinkedIn Pulse articles and posts.
    Does NOT require LinkedIn login or API access.

    Targets:
    - Utah professionals writing about career changes
    - Financial planning thought leaders in Utah
    - Entrepreneurs and business owners sharing stories
    - People publicly documenting career transitions
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="linkedin_articles",
            platform="linkedin",
            config=config,
            db=db,
        )
        self.max_results_per_run: int = self.config.get("max_results_per_run", 100)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build Google search URLs targeting LinkedIn articles."""
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
        """Scrape Google search results for LinkedIn articles."""
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

                    # Only keep LinkedIn URLs
                    if "linkedin.com" not in href:
                        continue

                    title_el = await result.query_selector("h3")
                    title = ""
                    if title_el:
                        title = (await title_el.inner_text()).strip()

                    # Extract snippet / description
                    snippet_el = await result.query_selector(
                        "div.VwiC3b, span.aCOpRe, div[data-sncf]"
                    )
                    snippet = ""
                    if snippet_el:
                        snippet = (await snippet_el.inner_text()).strip()

                    # Try to extract author name from title or snippet
                    author = self._extract_author(title, snippet, href)

                    collected.append({
                        "title": title,
                        "url": href,
                        "description": snippet[:500],
                        "author": author,
                        "source": "linkedin_article",
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
    def _extract_author(title: str, snippet: str, url: str) -> str:
        """Try to extract the author name from LinkedIn article metadata.

        LinkedIn Pulse URLs often contain the author name in the URL path,
        and Google snippets may include "by Author Name" or "Author Name on LinkedIn".
        """
        author = ""

        # Check URL for author slug: linkedin.com/pulse/title-author-name
        url_match = re.search(
            r"linkedin\.com/pulse/[^/]+-([a-z]+-[a-z]+(?:-[a-z]+)?)\b", url
        )
        if url_match:
            slug = url_match.group(1)
            author = slug.replace("-", " ").title()

        # Check for "by Name" pattern in snippet
        if not author:
            by_match = re.search(r"\bby\s+([A-Z][a-z]+ [A-Z][a-z]+)", snippet)
            if by_match:
                author = by_match.group(1)

        # Check for "Name on LinkedIn" pattern
        if not author:
            on_match = re.search(r"([A-Z][a-z]+ [A-Z][a-z]+)\s+on LinkedIn", title)
            if on_match:
                author = on_match.group(1)

        return author

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw LinkedIn article data into the standard lead format."""
        return {
            "name": raw_data.get("author", ""),
            "title": raw_data.get("title", ""),
            "description": raw_data.get("description", ""),
            "location_city": "",
            "location_state": "Utah",
            "posted_date": "",
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "linkedin",
            "source_site": raw_data.get("source", ""),
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
