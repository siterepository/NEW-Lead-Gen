"""
Reddit r/SaltLakeCity - Career Posts Agent

Monitors r/SaltLakeCity for career-related posts (job seeking, career change,
new opportunities) using the Reddit public JSON API.  Identifies Utah residents
who may be open to NWM financial advisor recruiting.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import quote_plus, urlencode

import httpx

from leadgen.agents.base import BaseAgent, USER_AGENTS

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Search configuration
# ---------------------------------------------------------------------------

SUBREDDIT = "SaltLakeCity"
BASE_JSON_URL = f"https://www.reddit.com/r/{SUBREDDIT}"

SEARCH_QUERIES: list[str] = [
    "career change OR new job",
    "looking for work OR job hunting",
    "hiring OR opportunity",
    "career advice OR career help",
    "leaving my job OR quit my job",
    "new career OR career transition",
    "financial advisor OR financial planning",
    "entrepreneur OR side hustle",
    "sales job OR sales career",
]


class RedditSLCCareersAgent(BaseAgent):
    """Monitor r/SaltLakeCity for career-related posts via Reddit JSON API.

    Targets posts from Utah residents who are:
    - Actively seeking new career paths
    - Asking for career advice or job leads
    - Discussing job changes or transitions
    - Showing entrepreneurial or financial ambition
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="reddit_slc_careers",
            platform="reddit",
            config=config,
            db=db,
        )
        self.max_results_per_run: int = self.config.get("max_results_per_run", 100)
        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------
    # HTTP client
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=30.0,
                headers={
                    "User-Agent": self.get_random_user_agent(),
                    "Accept": "application/json",
                },
                follow_redirects=True,
            )
        return self._client

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build Reddit JSON API search URLs for r/SaltLakeCity."""
        urls: list[str] = []
        for query in SEARCH_QUERIES:
            params = {
                "q": query,
                "restrict_sr": "1",
                "sort": "new",
                "limit": "25",
            }
            url = f"{BASE_JSON_URL}/search.json?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)

        # Also grab newest posts from the subreddit directly
        urls.append(f"{BASE_JSON_URL}/new.json?limit=50")

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Fetch career-related posts from r/SaltLakeCity via JSON API."""
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

                    # Filter for career-relevant content
                    if self._is_career_relevant(post_data):
                        all_items.append(self._extract_post(post_data))

            except Exception as exc:
                logger.warning(
                    "[%s] Failed to fetch %s: %s", self.name, url, exc
                )

        logger.info(
            "[%s] Scrape complete: %d raw items collected",
            self.name, len(all_items),
        )
        return all_items

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_career_relevant(self, post: dict) -> bool:
        """Check if a post is career-related based on title and body."""
        text = (
            f"{post.get('title', '')} {post.get('selftext', '')}"
        ).lower()

        career_keywords = [
            "career", "job", "hiring", "work", "employment", "opportunity",
            "resume", "interview", "salary", "profession", "occupation",
            "side hustle", "entrepreneur", "business", "freelance",
            "financial advisor", "financial planning", "insurance",
            "sales", "recruiting", "quit", "fired", "laid off", "layoff",
        ]
        return any(kw in text for kw in career_keywords)

    def _extract_post(self, post: dict) -> dict:
        """Extract relevant fields from a Reddit post JSON object."""
        return {
            "post_id": post.get("id", ""),
            "title": post.get("title", ""),
            "selftext": post.get("selftext", "")[:2000],
            "author": post.get("author", ""),
            "url": f"https://www.reddit.com{post.get('permalink', '')}",
            "created_utc": post.get("created_utc", 0),
            "score": post.get("score", 0),
            "num_comments": post.get("num_comments", 0),
            "subreddit": post.get("subreddit", SUBREDDIT),
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
            "location_city": "Salt Lake City",
            "location_state": "Utah",
            "posted_date": posted_date,
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "reddit",
            "subreddit": raw_data.get("subreddit", SUBREDDIT),
            "score": raw_data.get("score", 0),
            "num_comments": raw_data.get("num_comments", 0),
            "flair": raw_data.get("link_flair_text", ""),
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
