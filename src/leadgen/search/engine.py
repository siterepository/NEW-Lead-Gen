"""
Multi-provider search engine with automatic fallback.

Provides a unified async interface for searching across Brave API,
DuckDuckGo HTML, and Google HTML.  Providers are tried in order of
reliability; if one fails or is rate-limited the next is attempted.

Provider priority:
    1. Brave Search API  -- best quality, requires free API key (2000 queries/month)
    2. DuckDuckGo HTML   -- no key needed, rarely blocks
    3. Google HTML        -- last resort, most likely to block

Usage:
    engine = SearchEngine(brave_api_key="...")
    results = await engine.search('site:linkedin.com/in "sales manager" "Utah"')
    # results -> [{"title": ..., "url": ..., "snippet": ...}, ...]
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import time
from html import unescape
from typing import Any
from urllib.parse import quote_plus, urlencode, urlparse

import httpx

from leadgen.agents.base import RateLimiter, USER_AGENTS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pre-built search queries targeting Utah professionals
# ---------------------------------------------------------------------------

LEAD_SEARCH_QUERIES: list[str] = [
    # ---- PEOPLE looking for work on LinkedIn ----
    'site:linkedin.com/in "open to work" "sales" "Utah"',
    'site:linkedin.com/in "seeking new opportunities" "Utah"',
    'site:linkedin.com/in "looking for" "sales" "Utah"',
    'site:linkedin.com/in "open to work" "insurance" "Utah"',
    'site:linkedin.com/in "open to work" "financial" "Utah"',
    'site:linkedin.com/in "open to work" "real estate" "Utah"',
    'site:linkedin.com/in "career transition" "Utah"',
    'site:linkedin.com/in "former" "door to door" "Utah"',
    # ---- People expressing dissatisfaction / seeking change ----
    'site:reddit.com "Utah" "hate my sales job"',
    'site:reddit.com "Utah" "leaving door to door"',
    'site:reddit.com "Utah" "burned out" "sales"',
    'site:reddit.com "tired of" "commission" "Utah"',
    'site:reddit.com "quit" "sales job" "Utah" OR "SLC"',
    # ---- Door-to-door / direct sales people looking to leave ----
    '"door to door" "looking for" "Utah"',
    '"pest control" "tired" OR "leaving" OR "quit" "Utah"',
    '"solar sales" "burned out" OR "looking for" "Utah"',
    '"alarm sales" "done with" OR "leaving" "Utah"',
    # ---- Entrepreneurs / business owners in transition ----
    '"selling my business" "Utah"',
    '"business for sale" "owner" "Utah"',
    '"closing my business" "Utah"',
    # ---- Insurance / financial people seeking change ----
    '"insurance agent" "looking for" "Utah"',
    '"financial advisor" "career change" "Utah"',
    '"life insurance" "tired" OR "leaving" OR "burned out"',
]


# ---------------------------------------------------------------------------
# SearchEngine
# ---------------------------------------------------------------------------

class SearchEngine:
    """Multi-provider search engine with automatic fallback.

    Tries providers in order: Brave API > DuckDuckGo HTML > Google HTML.
    Falls back to the next provider when one fails, times out, or is
    rate-limited.

    Args:
        brave_api_key: Optional Brave Search API key.  Falls back to the
            ``BRAVE_API_KEY`` environment variable, then to empty string
            (which disables Brave and starts with DuckDuckGo).
        requests_per_minute: Rate-limit ceiling shared across all providers.
        timeout: HTTP request timeout in seconds.
    """

    def __init__(
        self,
        brave_api_key: str | None = None,
        requests_per_minute: int = 10,
        timeout: float = 20.0,
    ) -> None:
        self.brave_api_key: str = brave_api_key or os.environ.get("BRAVE_API_KEY", "")
        self.serper_api_key: str = os.environ.get("SERPER_API_KEY", "")
        self.rate_limiter = RateLimiter(requests_per_minute=requests_per_minute)
        self.timeout = timeout

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def search(self, query: str, num_results: int = 10) -> list[dict[str, str]]:
        """Search and return results as a list of ``{title, url, snippet}``.

        Tries each provider in order, returning the first non-empty result
        set.  Returns an empty list only when every provider fails.
        """
        # 1. Serper.dev API (best for site:linkedin.com, 2500 free queries)
        if self.serper_api_key:
            results = await self._search_serper(query, num_results)
            if results:
                return results
            logger.debug("Serper.dev returned no results; falling back.")

        # 2. Brave API (if key is available)
        if self.brave_api_key:
            results = await self._search_brave_api(query, num_results)
            if results:
                return results
            logger.debug("Brave API returned no results; falling back.")

        # 3. DuckDuckGo HTML (no key needed, rarely blocks)
        results = await self._search_duckduckgo(query, num_results)
        if results:
            return results
        logger.debug("DuckDuckGo returned no results; falling back.")

        # 4. Google HTML (last resort -- most likely to block)
        results = await self._search_google(query, num_results)
        if results:
            return results
        logger.debug("Google returned no results. All providers exhausted.")

        return []

    # ------------------------------------------------------------------
    # Provider: Serper.dev (RECOMMENDED - 2500 free, best for LinkedIn)
    # ------------------------------------------------------------------

    async def _search_serper(
        self, query: str, num: int
    ) -> list[dict[str, str]]:
        """Use Serper.dev Google Search API (free: 2,500 queries).

        Best for site:linkedin.com queries. Returns Google-quality results
        via official API without risk of blocking.
        """
        await self.rate_limiter.acquire()

        url = "https://google.serper.dev/search"
        headers = {
            "X-API-KEY": self.serper_api_key,
            "Content-Type": "application/json",
        }
        payload = {
            "q": query,
            "num": min(num, 20),
            "gl": "us",
            "hl": "en",
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(url, json=payload, headers=headers)
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            logger.warning("Serper.dev search error: %s", exc)
            return []

        results: list[dict[str, str]] = []
        for item in data.get("organic", []):
            results.append({
                "title": item.get("title", ""),
                "url": item.get("link", ""),
                "snippet": item.get("snippet", ""),
            })

        logger.info("Serper.dev: %d results for '%s'", len(results), query[:50])
        return results

    # ------------------------------------------------------------------
    # Provider: Brave Search API
    # ------------------------------------------------------------------

    async def _search_brave_api(
        self, query: str, num: int
    ) -> list[dict[str, str]]:
        """Use the Brave Search API (free tier: 2 000 queries / month).

        Endpoint:
            GET https://api.search.brave.com/res/v1/web/search?q=QUERY&count=NUM
        Header:
            X-Subscription-Token: <API_KEY>
        """
        await self.rate_limiter.acquire()

        url = "https://api.search.brave.com/res/v1/web/search"
        params = {"q": query, "count": min(num, 20)}
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": self.brave_api_key,
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(url, params=params, headers=headers)

            if resp.status_code == 429:
                logger.warning("Brave API rate-limited (429).")
                return []

            if resp.status_code != 200:
                logger.warning(
                    "Brave API HTTP %d: %s", resp.status_code, resp.text[:200]
                )
                return []

            data = resp.json()
            web_results = data.get("web", {}).get("results", [])

            results: list[dict[str, str]] = []
            for item in web_results[:num]:
                results.append({
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                    "snippet": _strip_html(item.get("description", "")),
                })
            return results

        except httpx.TimeoutException:
            logger.warning("Brave API request timed out.")
            return []
        except Exception as exc:
            logger.warning("Brave API error: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Provider: DuckDuckGo HTML
    # ------------------------------------------------------------------

    async def _search_duckduckgo(
        self, query: str, num: int
    ) -> list[dict[str, str]]:
        """Scrape DuckDuckGo's HTML-only endpoint (no API key needed).

        Endpoint:
            GET https://html.duckduckgo.com/html/?q=QUERY
        Parse:
            ``<a class="result__a">`` for title/URL,
            ``<a class="result__snippet">`` for snippet.
        """
        await self.rate_limiter.acquire()

        url = "https://html.duckduckgo.com/html/"
        params = {"q": query}
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://duckduckgo.com/",
        }

        try:
            async with httpx.AsyncClient(
                timeout=self.timeout, follow_redirects=True
            ) as client:
                resp = await client.get(url, params=params, headers=headers)

            if resp.status_code != 200:
                logger.warning(
                    "DuckDuckGo HTTP %d for query: %s",
                    resp.status_code, query[:60],
                )
                return []

            html = resp.text
            results: list[dict[str, str]] = []

            # Extract result blocks:
            # <a rel="nofollow" class="result__a" href="...">TITLE</a>
            # <a class="result__snippet" href="...">SNIPPET</a>
            link_pattern = re.compile(
                r'<a[^>]*class="result__a"[^>]*href="([^"]*)"[^>]*>(.*?)</a>',
                re.DOTALL,
            )
            snippet_pattern = re.compile(
                r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>',
                re.DOTALL,
            )

            links = link_pattern.findall(html)
            snippets = snippet_pattern.findall(html)

            for i, (href, title_html) in enumerate(links[:num]):
                # DuckDuckGo wraps actual URLs in a redirect
                real_url = _extract_ddg_url(href)
                if not real_url:
                    continue

                snippet_text = ""
                if i < len(snippets):
                    snippet_text = _strip_html(snippets[i])

                results.append({
                    "title": _strip_html(title_html),
                    "url": real_url,
                    "snippet": snippet_text,
                })

            return results

        except httpx.TimeoutException:
            logger.warning("DuckDuckGo request timed out for: %s", query[:60])
            return []
        except Exception as exc:
            logger.warning("DuckDuckGo error: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Provider: Google HTML
    # ------------------------------------------------------------------

    async def _search_google(
        self, query: str, num: int
    ) -> list[dict[str, str]]:
        """Scrape Google's HTML search results (last resort, may block).

        Endpoint:
            GET https://www.google.com/search?q=QUERY&num=NUM
        Parse:
            ``<div class="g">`` blocks for individual results.
            Extracts first ``<a>`` href, ``<h3>`` title, snippet from
            inner ``<span>`` / ``<div>`` content.
        """
        await self.rate_limiter.acquire()

        url = "https://www.google.com/search"
        params = {"q": query, "num": min(num, 20), "hl": "en"}
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Referer": "https://www.google.com/",
        }

        try:
            async with httpx.AsyncClient(
                timeout=self.timeout, follow_redirects=True
            ) as client:
                resp = await client.get(url, params=params, headers=headers)

            if resp.status_code == 429:
                logger.warning("Google rate-limited (429) for: %s", query[:60])
                return []

            if resp.status_code != 200:
                logger.warning(
                    "Google HTTP %d for query: %s",
                    resp.status_code, query[:60],
                )
                return []

            html = resp.text

            # Detect CAPTCHA / block page
            if "detected unusual traffic" in html.lower() or "/sorry/" in html:
                logger.warning("Google CAPTCHA/block detected for: %s", query[:60])
                return []

            return _parse_google_html(html, num)

        except httpx.TimeoutException:
            logger.warning("Google request timed out for: %s", query[:60])
            return []
        except Exception as exc:
            logger.warning("Google error: %s", exc)
            return []


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities from a string."""
    clean = re.sub(r"<[^>]+>", "", text)
    clean = unescape(clean)
    return clean.strip()


def _extract_ddg_url(href: str) -> str:
    """Extract the real destination URL from a DuckDuckGo redirect link.

    DDG wraps links like:
        //duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com&rut=...
    We need the ``uddg`` parameter value.
    """
    if not href:
        return ""

    # Direct URL (no redirect wrapper)
    if href.startswith("http://") or href.startswith("https://"):
        parsed = urlparse(href)
        # Skip DDG internal pages
        if "duckduckgo.com" not in parsed.netloc:
            return href

    # Extract uddg parameter from redirect
    match = re.search(r"[?&]uddg=([^&]+)", href)
    if match:
        from urllib.parse import unquote
        return unquote(match.group(1))

    return ""


def _parse_google_html(html: str, num: int) -> list[dict[str, str]]:
    """Parse Google search result HTML into structured results.

    Extracts from ``<div class="g">`` blocks:
        - URL from the first ``<a href="...">``
        - Title from ``<h3>``
        - Snippet from remaining text content
    """
    results: list[dict[str, str]] = []

    # Split on result blocks -- Google wraps each in <div class="g">
    # We use a broad pattern to find result containers.
    block_pattern = re.compile(
        r'<div\s+class="g"[^>]*>(.*?)</div>\s*(?=<div\s+class="g"|$)',
        re.DOTALL,
    )
    blocks = block_pattern.findall(html)

    # Fallback: try <div class="Gx5Zad"> (mobile/alternative layout)
    if not blocks:
        block_pattern_alt = re.compile(
            r'<div\s+class="[^"]*Gx5Zad[^"]*"[^>]*>(.*?)</div>\s*</div>',
            re.DOTALL,
        )
        blocks = block_pattern_alt.findall(html)

    for block in blocks[:num]:
        # Extract URL
        url_match = re.search(r'<a\s+href="(/url\?q=|)(https?://[^"&]+)', block)
        if not url_match:
            continue
        result_url = url_match.group(2)

        # Skip Google internal links
        parsed = urlparse(result_url)
        if "google.com" in parsed.netloc:
            continue

        # Extract title from <h3>
        title_match = re.search(r"<h3[^>]*>(.*?)</h3>", block, re.DOTALL)
        title = _strip_html(title_match.group(1)) if title_match else ""

        # Extract snippet -- look for common snippet containers
        snippet = ""
        snippet_patterns = [
            r'<span[^>]*class="[^"]*(?:st|aCOpRe)[^"]*"[^>]*>(.*?)</span>',
            r'<div[^>]*class="[^"]*VwiC3b[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*data-sncf="[^"]*"[^>]*>(.*?)</div>',
        ]
        for sp in snippet_patterns:
            sm = re.search(sp, block, re.DOTALL)
            if sm:
                snippet = _strip_html(sm.group(1))
                break

        if not snippet:
            # Fallback: take all text after the title
            remaining = block
            if title_match:
                remaining = block[title_match.end():]
            snippet = _strip_html(remaining)[:300]

        results.append({
            "title": title,
            "url": result_url,
            "snippet": snippet,
        })

    return results
