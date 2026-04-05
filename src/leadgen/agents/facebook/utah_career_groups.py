"""
Facebook Utah Career Groups Agent

Finds and lists public Utah career/job Facebook groups using Google search.
Extracts group names, descriptions, member counts, and URLs.  These groups
are flagged for manual monitoring since private posts cannot be scraped.

IMPORTANT: This agent can ONLY access publicly visible content.
It does NOT log into Facebook or access private content.
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
# Google search queries to find public Facebook groups
# ---------------------------------------------------------------------------

GOOGLE_SEARCH_QUERIES: list[str] = [
    'site:facebook.com/groups "utah" "career"',
    'site:facebook.com/groups "utah" "jobs"',
    'site:facebook.com/groups "salt lake city" "career"',
    'site:facebook.com/groups "utah" "job seekers"',
    'site:facebook.com/groups "utah" "employment"',
    'site:facebook.com/groups "utah" "hiring"',
    'site:facebook.com/groups "utah" "career change"',
    'site:facebook.com/groups "utah" "professional networking"',
    'site:facebook.com/groups "utah county" "jobs"',
    'site:facebook.com/groups "davis county" "jobs"',
]


class FBUtahCareerGroupsAgent(BaseAgent):
    """Find public Utah career/job Facebook groups via Google search.

    This agent discovers publicly listed Facebook groups related to
    careers and employment in Utah.  Groups are flagged for manual
    monitoring -- actual group posts cannot be scraped without login.

    Outputs:
    - Group name, URL, description, estimated member count
    - Public/private status indicator
    - Recommendation for manual monitoring
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="fb_utah_career_groups",
            platform="facebook",
            config=config,
            db=db,
        )
        self.max_results_per_run: int = self.config.get("max_results_per_run", 50)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build Google search URLs to discover Facebook groups."""
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
        """Find public Utah career Facebook groups via Google search."""
        all_items: list[dict] = []
        crawler = await self.setup_browser()
        collected: list[dict] = []
        seen_urls: set[str] = set()

        @crawler.router.default_handler
        async def handle_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            try:
                await page.wait_for_load_state("domcontentloaded", timeout=15_000)
            except Exception:
                return

            results = await page.query_selector_all("div.g")
            for result in results:
                if len(collected) >= self.max_results_per_run:
                    break
                try:
                    link_el = await result.query_selector("a[href]")
                    if not link_el:
                        continue

                    href = await link_el.get_attribute("href") or ""

                    # Only keep Facebook group URLs
                    if "facebook.com/groups" not in href:
                        continue

                    # Deduplicate by URL
                    clean_url = re.sub(r"[?#].*$", "", href)
                    if clean_url in seen_urls:
                        continue
                    seen_urls.add(clean_url)

                    title_el = await result.query_selector("h3")
                    title = ""
                    if title_el:
                        title = (await title_el.inner_text()).strip()

                    snippet_el = await result.query_selector(
                        "div.VwiC3b, span.aCOpRe"
                    )
                    snippet = ""
                    if snippet_el:
                        snippet = (await snippet_el.inner_text()).strip()

                    # Extract member count from snippet if present
                    member_count = _extract_member_count(snippet)

                    collected.append({
                        "group_name": title,
                        "url": clean_url,
                        "description": snippet[:500],
                        "member_count": member_count,
                        "source": "google_fb_search",
                        "group_type": "career",
                        "requires_manual_monitoring": True,
                        "post_id": clean_url,
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
        logger.info("[%s] Scrape complete: %d groups found", self.name, len(all_items))
        return all_items

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw Facebook group data into the standard lead format."""
        return {
            "name": raw_data.get("group_name", ""),
            "title": raw_data.get("group_name", ""),
            "description": raw_data.get("description", ""),
            "location_city": "",
            "location_state": "Utah",
            "posted_date": "",
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "facebook",
            "group_type": raw_data.get("group_type", "career"),
            "member_count": raw_data.get("member_count", ""),
            "requires_manual_monitoring": True,
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _extract_member_count(text: str) -> str:
    """Extract member count from a Google snippet about a Facebook group."""
    patterns = [
        r"([\d,]+(?:\.\d+)?[KkMm]?)\s*members",
        r"([\d,]+)\s*people",
        r"Group\s*.*?([\d,]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""
