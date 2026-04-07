"""
Reddit Career Guidance / Career Change Agent

Monitors r/careerguidance and r/careerchange for Utah users posting about
career transitions.  Uses the Reddit public JSON API to find people
contemplating or actively making career changes who may be open to NWM
financial advisor recruiting.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import quote_plus, urlencode

import httpx

from leadgen.agents.base import BaseAgent, USER_AGENTS

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Search configuration
# ---------------------------------------------------------------------------

SUBREDDITS: list[str] = ["careerguidance", "careerchange"]

SEARCH_QUERIES: list[str] = [
    "utah",
    "salt lake",
    "provo",
    "ogden",
    "utah county",
    "utah career",
    "financial advisor",
    "insurance career",
    "sales career utah",
    "leaving corporate",
    "career transition finance",
]


class RedditCareerGuidanceAgent(BaseAgent):
    """Monitor r/careerguidance and r/careerchange for Utah users.

    Targets people who are:
    - Posting about career transitions from Utah
    - Asking for guidance on switching to finance/sales/insurance
    - Discussing leaving corporate jobs for entrepreneurship
    - Expressing interest in financial planning careers
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="reddit_career_guidance",
            platform="reddit",
            config=config,
            db=db,
        )
        self.max_results_per_run: int = self.config.get("max_results_per_run", 100)
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=30.0,
                headers={
                    "User-Agent": "script:leadgen:v0.1.0 (NWM lead research tool)",
                    "Accept": "application/json",
                },
                follow_redirects=True,
            )
        return self._client

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build Reddit JSON API search URLs for career subreddits."""
        urls: list[str] = []
        for subreddit in SUBREDDITS:
            base = f"https://www.reddit.com/r/{subreddit}"
            for query in SEARCH_QUERIES:
                params = {
                    "q": query,
                    "restrict_sr": "1",
                    "sort": "new",
                    "limit": "25",
                }
                url = f"{base}/search.json?{urlencode(params, quote_via=quote_plus)}"
                urls.append(url)

            # Also grab newest posts
            urls.append(f"{base}/new.json?limit=25")

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Fetch Utah career-change posts from guidance subreddits."""
        all_items: list[dict] = []
        seen_ids: set[str] = set()
        client = await self._get_client()
        search_urls = self.get_search_urls()

        for url in search_urls:
            if len(all_items) >= self.max_results_per_run:
                break

            await self.rate_limiter.acquire()
            delay = self.get_random_delay()
            await asyncio.sleep(delay)

            try:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()

                posts = data.get("data", {}).get("children", [])
                for post in posts:
                    if len(all_items) >= self.max_results_per_run:
                        break

                    post_data = post.get("data", {})
                    post_id = post_data.get("id", "")

                    if post_id in seen_ids:
                        continue
                    seen_ids.add(post_id)

                    if self._is_utah_relevant(post_data):
                        all_items.append(self._extract_post(post_data))

            except Exception as exc:
                logger.warning("[%s] Failed to fetch %s: %s", self.name, url, exc)

        logger.info(
            "[%s] Scrape complete: %d raw items collected",
            self.name, len(all_items),
        )
        return all_items

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_utah_relevant(self, post: dict) -> bool:
        """Check if a post mentions Utah or is career-transition relevant."""
        text = (
            f"{post.get('title', '')} {post.get('selftext', '')}"
        ).lower()

        utah_indicators = [
            "utah", "salt lake", "slc", "provo", "ogden", "orem",
            "sandy", "west jordan", "layton", "lehi", "st. george",
            "st george", "logan", "bountiful", "draper", "murray",
            "utah county", "davis county", "weber county",
        ]
        has_utah = any(loc in text for loc in utah_indicators)

        # For search results that already filtered on Utah terms,
        # also accept posts about financial career transitions
        finance_keywords = [
            "financial advisor", "financial planner", "insurance agent",
            "wealth management", "financial services", "northwestern mutual",
        ]
        has_finance = any(kw in text for kw in finance_keywords)

        return has_utah or has_finance

    def _extract_post(self, post: dict) -> dict:
        """Extract relevant fields from a Reddit post."""
        return {
            "post_id": post.get("id", ""),
            "title": post.get("title", ""),
            "selftext": post.get("selftext", "")[:2000],
            "author": post.get("author", ""),
            "url": f"https://www.reddit.com{post.get('permalink', '')}",
            "created_utc": post.get("created_utc", 0),
            "score": post.get("score", 0),
            "num_comments": post.get("num_comments", 0),
            "subreddit": post.get("subreddit", ""),
            "link_flair_text": post.get("link_flair_text", ""),
        }

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw Reddit post data into the standard lead format."""
        created_utc = raw_data.get("created_utc", 0)
        posted_date = ""
        if created_utc:
            posted_date = datetime.fromtimestamp(
                created_utc, tz=timezone.utc
            ).isoformat()

        return {
            "name": raw_data.get("author", ""),
            "title": raw_data.get("title", ""),
            "description": raw_data.get("selftext", ""),
            "location_city": "",
            "location_state": "Utah",
            "posted_date": posted_date,
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "reddit",
            "subreddit": raw_data.get("subreddit", ""),
            "score": raw_data.get("score", 0),
            "num_comments": raw_data.get("num_comments", 0),
            "flair": raw_data.get("link_flair_text", ""),
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
