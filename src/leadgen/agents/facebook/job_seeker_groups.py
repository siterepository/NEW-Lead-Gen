"""
Facebook Utah Job Seeker Support Groups Agent

Finds public Utah job seeker support Facebook groups using Google search.
People in these groups are actively seeking employment and may be open
to NWM financial advisor career opportunities.

IMPORTANT: Only accesses publicly visible content. No Facebook login.
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
# Google search queries
# ---------------------------------------------------------------------------

GOOGLE_SEARCH_QUERIES: list[str] = [
    'site:facebook.com/groups "utah" "job seekers"',
    'site:facebook.com/groups "utah" "looking for work"',
    'site:facebook.com/groups "utah" "unemployment"',
    'site:facebook.com/groups "utah" "job search"',
    'site:facebook.com/groups "salt lake" "job seekers"',
    'site:facebook.com/groups "utah" "career support"',
    'site:facebook.com/groups "utah" "job help"',
    'site:facebook.com/groups "utah" "employment support"',
    'site:facebook.com/groups "utah county" "job seekers"',
]


class FBJobSeekerGroupsAgent(BaseAgent):
    """Find public Utah job seeker support Facebook groups.

    Discovers groups where job seekers gather for support and leads.
    Members are actively seeking employment and open to new career paths.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="fb_job_seeker_groups",
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
        """Extract Facebook group links from Google search results HTML."""
        items: list[dict] = []
        seen_urls: set[str] = set()
        # Google search results containing facebook.com/groups
        for m in re.finditer(
            r'<a[^>]+href="(https?://[^"]*facebook\.com/groups/[^"]*)"[^>]*>',
            html,
        ):
            raw_url = m.group(1)
            clean_url = re.sub(r"[?#].*$", "", raw_url)
            if clean_url in seen_urls:
                continue
            seen_urls.add(clean_url)
            # Try to extract title from nearby <h3>
            context = html[m.end():m.end()+500]
            title_match = re.search(r"<h3[^>]*>([^<]+)</h3>", context)
            title = title_match.group(1).strip() if title_match else clean_url
            # Try to extract snippet
            snippet_match = re.search(
                r'<(?:span|div)[^>]*class="[^"]*(?:VwiC3b|aCOpRe)[^"]*"[^>]*>([^<]+)',
                context,
            )
            snippet = snippet_match.group(1).strip()[:500] if snippet_match else ""
            # Extract member count
            member_count = ""
            mc_match = re.search(r"([\d,]+(?:\.\d+)?[KkMm]?)\s*members", snippet)
            if mc_match:
                member_count = mc_match.group(1)
            items.append({
                "group_name": title,
                "url": clean_url,
                "description": snippet,
                "member_count": member_count,
                "source": "google_fb_search",
                "group_type": "career",
                "requires_manual_monitoring": True,
                "post_id": clean_url,
            })
        return items

    # ------------------------------------------------------------------
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
            "group_type": raw_data.get("group_type", "job_seeker"),
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
