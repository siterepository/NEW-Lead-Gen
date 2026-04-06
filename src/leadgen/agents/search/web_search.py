"""
WebSearchAgent - Scrapes search engines for professional leads in Utah.

Uses the multi-provider SearchEngine to execute pre-built queries targeting
LinkedIn profiles, Facebook business pages, job postings, and local
directories.  Results are normalised into the standard lead schema and
fed through the relevance filter before storage.

Provider fallback order: Brave API > DuckDuckGo HTML > Google HTML.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from leadgen.agents.base import BaseAgent
from leadgen.search.engine import LEAD_SEARCH_QUERIES, SearchEngine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _detect_platform_from_url(url: str) -> str:
    """Infer the originating platform from a result URL.

    Returns a short label such as ``linkedin``, ``facebook``, ``indeed``,
    or ``web`` as a catch-all.
    """
    if not url:
        return "web"

    host = urlparse(url).netloc.lower()

    if "linkedin.com" in host:
        return "linkedin"
    if "facebook.com" in host:
        return "facebook"
    if "indeed.com" in host:
        return "indeed"
    if "glassdoor.com" in host:
        return "glassdoor"
    if "ksl.com" in host:
        return "ksl"
    if "craigslist.org" in host:
        return "craigslist"
    if "yelp.com" in host:
        return "yelp"

    return "web"


def _generate_post_id(url: str, title: str) -> str:
    """Create a deterministic dedup key from URL + title."""
    raw = f"{url}|{title}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# WebSearchAgent
# ---------------------------------------------------------------------------

class WebSearchAgent(BaseAgent):
    """Scrapes search engines for professional leads in Utah.

    Iterates through :data:`LEAD_SEARCH_QUERIES`, collects results from
    the first available search provider, and normalises them into the
    standard lead dict schema.

    Config keys (optional overrides via ``config`` dict):
        ``brave_api_key``       -- Brave Search API key (or env BRAVE_API_KEY)
        ``max_results_per_run`` -- Cap total results collected (default 500)
        ``queries``             -- Override the default query list
        ``requests_per_minute`` -- Throttle for the search engine (default 10)
        ``inter_query_delay``   -- Extra seconds between queries (default 2-5)
    """

    # ------------------------------------------------------------------
    # Agent identity
    # ------------------------------------------------------------------

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="web_search",
            platform="search",
            config=config,
            db=db,
        )
        self.max_results_per_run: int = self.config.get("max_results_per_run", 500)
        self.queries: list[str] = self.config.get("queries", LEAD_SEARCH_QUERIES)
        self._engine: SearchEngine | None = None

    # ------------------------------------------------------------------
    # Lazy engine init (uses config / env for API key)
    # ------------------------------------------------------------------

    def _get_engine(self) -> SearchEngine:
        if self._engine is None:
            self._engine = SearchEngine(
                brave_api_key=self.config.get("brave_api_key"),
                requests_per_minute=self.config.get("requests_per_minute", 10),
            )
        return self._engine

    # ------------------------------------------------------------------
    # get_search_urls  (not used directly -- queries go through SearchEngine)
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Return the raw query strings (not URLs) this agent will execute.

        The SearchEngine builds provider-specific URLs internally, so this
        method returns the textual queries for logging / debugging.
        """
        return list(self.queries)

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Execute all search queries and collect raw result dicts.

        Each query is sent to :class:`SearchEngine.search`, which handles
        provider fallback internally.  A random delay is inserted between
        queries to reduce the chance of rate-limiting.
        """
        engine = self._get_engine()
        all_results: list[dict] = []
        seen_urls: set[str] = set()

        total_queries = len(self.queries)

        for idx, query in enumerate(self.queries, start=1):
            if len(all_results) >= self.max_results_per_run:
                logger.info(
                    "[%s] Hit max_results_per_run (%d). Stopping early.",
                    self.name,
                    self.max_results_per_run,
                )
                break

            logger.info(
                "[%s] Query %d/%d: %s",
                self.name, idx, total_queries, query[:80],
            )

            try:
                results = await engine.search(query, num_results=10)
            except Exception as exc:
                logger.warning(
                    "[%s] Search failed for query %d: %s", self.name, idx, exc
                )
                results = []

            for r in results:
                result_url = r.get("url", "")

                # Dedup within a single run
                if result_url in seen_urls:
                    continue
                seen_urls.add(result_url)

                detected_platform = _detect_platform_from_url(result_url)
                post_id = _generate_post_id(result_url, r.get("title", ""))

                all_results.append({
                    "post_id": post_id,
                    "title": r.get("title", ""),
                    "source_url": result_url,
                    "source_post_text": r.get("snippet", ""),
                    "platform": "search",
                    "detected_platform": detected_platform,
                    "category": "web_search",
                    "search_query": query,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })

            logger.info(
                "[%s] Query %d returned %d results (running total: %d)",
                self.name, idx, len(results), len(all_results),
            )

            # Random inter-query delay to be polite
            if idx < total_queries:
                delay_min = self.config.get("inter_query_delay_min", 2.0)
                delay_max = self.config.get("inter_query_delay_max", 5.0)
                delay = random.uniform(delay_min, delay_max)
                await asyncio.sleep(delay)

        logger.info(
            "[%s] Scrape complete: %d raw results from %d queries",
            self.name, len(all_results), total_queries,
        )
        return all_results

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert a raw search result dict into the standard lead schema.

        Search results are intentionally sparse -- the enrichment pipeline
        will later visit each ``source_url`` to extract contact info,
        names, and more detailed descriptions.
        """
        return {
            "name": "",  # Populated later by enrichment
            "title": raw_data.get("title", ""),
            "description": raw_data.get("source_post_text", ""),
            "source_post_text": raw_data.get("source_post_text", ""),
            "location_city": "",  # Populated later by enrichment
            "location_state": "Utah",
            "posted_date": "",
            "url": raw_data.get("source_url", ""),
            "source_url": raw_data.get("source_url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": raw_data.get("platform", "search"),
            "detected_platform": raw_data.get("detected_platform", "web"),
            "contact_info": "",
            "contact_phone": "",
            "contact_email": "",
            "price": "",
            "category": raw_data.get("category", "web_search"),
            "search_query": raw_data.get("search_query", ""),
            "image_url": "",
            "agent": self.name,
            "scraped_at": raw_data.get(
                "scraped_at", datetime.now(timezone.utc).isoformat()
            ),
        }
