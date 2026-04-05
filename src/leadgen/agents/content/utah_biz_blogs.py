"""
Utah Business Blogs & News Agent

Monitors Utah business blogs and news sites for articles about career changes,
entrepreneurship, and business closures.  People featured in or engaging with
these articles may be good NWM financial advisor recruiting prospects.
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
# Target sites and search terms
# ---------------------------------------------------------------------------

TARGET_SITES: list[dict[str, str]] = [
    {
        "name": "Utah Business",
        "base_url": "https://www.utahbusiness.com",
        "search_path": "/?s={query}",
    },
    {
        "name": "Silicon Slopes",
        "base_url": "https://www.siliconslopes.com",
        "search_path": "/?s={query}",
    },
]

# Google search queries to find relevant articles across Utah biz sites
GOOGLE_SEARCH_QUERIES: list[str] = [
    'site:utahbusiness.com "career change" OR "new career"',
    'site:utahbusiness.com "entrepreneur" OR "startup founder"',
    'site:utahbusiness.com "business closure" OR "layoffs"',
    'site:siliconslopes.com "career change" OR "hiring"',
    'site:siliconslopes.com "entrepreneur" OR "startup"',
    '"utah business" "career transition" OR "new opportunity"',
    '"utah" "business news" "career change" OR "laid off"',
]

SEARCH_KEYWORDS: list[str] = [
    "career change",
    "entrepreneur",
    "startup",
    "business closure",
    "layoffs utah",
    "new career",
    "hiring",
]


class UtahBizBlogsAgent(BaseAgent):
    """Scrape Utah business blogs for career-change and entrepreneurship articles.

    Targets content about:
    - Career transitions and job market shifts in Utah
    - New entrepreneurs and startup founders
    - Business closures (employees now job-seeking)
    - People featured in career-related articles
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="utah_biz_blogs",
            platform="content",
            config=config,
            db=db,
        )
        self.max_results_per_run: int = self.config.get("max_results_per_run", 100)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build search URLs for Utah business blogs and Google searches."""
        urls: list[str] = []

        # Direct site searches
        for site in TARGET_SITES:
            for keyword in SEARCH_KEYWORDS:
                search_path = site["search_path"].format(
                    query=quote_plus(keyword)
                )
                url = f"{site['base_url']}{search_path}"
                urls.append(url)

        # Google searches for cross-site discovery
        for query in GOOGLE_SEARCH_QUERIES:
            params = {"q": query}
            url = f"https://www.google.com/search?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Scrape Utah business blogs for career-related articles."""
        all_items: list[dict] = []
        crawler = await self.setup_browser()
        collected: list[dict] = []

        @crawler.router.default_handler
        async def handle_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)
            url = context.request.url

            try:
                await page.wait_for_load_state("domcontentloaded", timeout=15_000)
            except Exception:
                return

            if "google.com/search" in url:
                # Extract Google search results
                results = await page.query_selector_all("div.g a[href]")
                for result in results:
                    if len(collected) >= self.max_results_per_run:
                        break
                    try:
                        href = await result.get_attribute("href") or ""
                        title_el = await result.query_selector("h3")
                        title = ""
                        if title_el:
                            title = (await title_el.inner_text()).strip()

                        if href and not href.startswith("/search"):
                            collected.append({
                                "title": title,
                                "url": href,
                                "source": "google_search",
                                "description": "",
                                "author": "",
                                "posted_date": "",
                            })
                    except Exception:
                        continue
            else:
                # Extract articles from Utah business sites
                articles = await page.query_selector_all(
                    "article, div.post, div.entry, div.article-card, "
                    "div.blog-post, div.search-result"
                )
                for article in articles:
                    if len(collected) >= self.max_results_per_run:
                        break
                    try:
                        raw = await self._extract_article(article, page, url)
                        if raw:
                            collected.append(raw)
                    except Exception as exc:
                        logger.debug("[%s] Article extraction failed: %s", self.name, exc)

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

    async def _extract_article(
        self, article: Any, page: Any, base_url: str
    ) -> Optional[dict]:
        """Extract article data from a blog post element."""
        raw: dict[str, Any] = {}

        # Title and link
        title_el = await article.query_selector(
            "h2 a, h3 a, h1 a, a.entry-title, a.post-title"
        )
        if title_el:
            raw["title"] = (await title_el.inner_text()).strip()
            href = await title_el.get_attribute("href") or ""
            if href and not href.startswith("http"):
                href = f"{base_url.rstrip('/')}/{href.lstrip('/')}"
            raw["url"] = href
        else:
            return None

        # Description / excerpt
        desc_el = await article.query_selector(
            "p.excerpt, div.entry-summary, div.post-excerpt, "
            "p.description, div.summary"
        )
        if desc_el:
            raw["description"] = (await desc_el.inner_text()).strip()[:500]

        # Author
        author_el = await article.query_selector(
            "span.author, a.author, span.byline, a.byline"
        )
        if author_el:
            raw["author"] = (await author_el.inner_text()).strip()

        # Date
        date_el = await article.query_selector(
            "time, span.date, span.published, span.post-date"
        )
        if date_el:
            raw["posted_date"] = (await date_el.inner_text()).strip()
            dt_attr = await date_el.get_attribute("datetime")
            if dt_attr:
                raw["posted_date_iso"] = dt_attr

        raw["source"] = "utah_biz_blog"
        raw["post_id"] = raw.get("url", raw.get("title", ""))

        return raw

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw article data into the standard lead format."""
        posted_date = raw_data.get("posted_date_iso") or raw_data.get(
            "posted_date", ""
        )

        return {
            "name": raw_data.get("author", ""),
            "title": raw_data.get("title", ""),
            "description": raw_data.get("description", ""),
            "location_city": "",
            "location_state": "Utah",
            "posted_date": posted_date,
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "content",
            "source_site": raw_data.get("source", ""),
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
