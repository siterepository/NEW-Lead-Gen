"""
Facebook Utah Veteran Career Transition Groups Agent

Finds public Utah veteran career transition Facebook groups using Google search.
Military veterans transitioning to civilian careers are excellent NWM recruiting
prospects due to their discipline, leadership skills, and desire for
meaningful second careers.

IMPORTANT: Only accesses publicly visible content. No Facebook login.
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
# Google search queries
# ---------------------------------------------------------------------------

GOOGLE_SEARCH_QUERIES: list[str] = [
    'site:facebook.com/groups "utah" "veteran" "career"',
    'site:facebook.com/groups "utah" "veteran" "transition"',
    'site:facebook.com/groups "utah" "military" "career"',
    'site:facebook.com/groups "utah" "veteran" "employment"',
    'site:facebook.com/groups "salt lake" "veteran" "jobs"',
    'site:facebook.com/groups "utah" "military transition"',
    'site:facebook.com/groups "utah" "veteran" "networking"',
    'site:facebook.com/groups "hill air force base" "career"',
    'site:facebook.com/groups "utah" "military spouse" "career"',
]


class FBVeteranTransitionAgent(BaseAgent):
    """Find public Utah veteran career transition Facebook groups.

    Discovers groups where veterans transitioning to civilian careers
    gather for support and networking.  These individuals often have
    strong leadership and discipline that aligns with financial services.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="fb_veteran_transition",
            platform="facebook",
            config=config,
            db=db,
        )
        self.max_results_per_run: int = self.config.get("max_results_per_run", 50)

    def get_search_urls(self) -> list[str]:
        urls: list[str] = []
        for query in GOOGLE_SEARCH_QUERIES:
            params = {"q": query, "num": "20"}
            url = f"https://www.google.com/search?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)
        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    async def scrape(self) -> list[dict]:
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
                    if "facebook.com/groups" not in href:
                        continue

                    clean_url = re.sub(r"[?#].*$", "", href)
                    if clean_url in seen_urls:
                        continue
                    seen_urls.add(clean_url)

                    title_el = await result.query_selector("h3")
                    title = (await title_el.inner_text()).strip() if title_el else ""

                    snippet_el = await result.query_selector("div.VwiC3b, span.aCOpRe")
                    snippet = (await snippet_el.inner_text()).strip() if snippet_el else ""

                    member_count = _extract_member_count(snippet)

                    collected.append({
                        "group_name": title,
                        "url": clean_url,
                        "description": snippet[:500],
                        "member_count": member_count,
                        "source": "google_fb_search",
                        "group_type": "veteran_transition",
                        "requires_manual_monitoring": True,
                        "post_id": clean_url,
                    })
                except Exception:
                    continue

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

    def parse_item(self, raw_data: dict) -> dict:
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
            "group_type": raw_data.get("group_type", "veteran_transition"),
            "member_count": raw_data.get("member_count", ""),
            "requires_manual_monitoring": True,
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }


def _extract_member_count(text: str) -> str:
    patterns = [
        r"([\d,]+(?:\.\d+)?[KkMm]?)\s*members",
        r"([\d,]+)\s*people",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""
