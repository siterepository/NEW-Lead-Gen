"""
BaseAgent - Abstract base class for all 50 scraping agents.

Every platform-specific agent (KSL, Craigslist, LinkedIn, etc.) inherits
from this class and implements scrape(), parse_item(), and get_search_urls().
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Optional

try:
    from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext
except ImportError:
    PlaywrightCrawler = None  # type: ignore
    PlaywrightCrawlingContext = None  # type: ignore

import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rate Limiter (token-bucket, async-compatible)
# ---------------------------------------------------------------------------

class RateLimiter:
    """Simple async token-bucket rate limiter.

    Args:
        requests_per_minute: Maximum sustained request rate.
    """

    def __init__(self, requests_per_minute: int = 30) -> None:
        self.requests_per_minute = requests_per_minute
        self.interval = 60.0 / requests_per_minute
        self.tokens = requests_per_minute
        self.max_tokens = requests_per_minute
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a request token is available."""
        while True:
            async with self._lock:
                self._refill()
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = self.interval - (time.monotonic() - self.last_refill)
            # Sleep OUTSIDE the lock so other coroutines aren't blocked
            await asyncio.sleep(max(wait, 0.05))

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        new_tokens = elapsed / self.interval
        if new_tokens > 0:
            self.tokens = min(self.max_tokens, self.tokens + new_tokens)
            self.last_refill = now


# ---------------------------------------------------------------------------
# User-Agent rotation list (20+ real browser strings)
# ---------------------------------------------------------------------------

USER_AGENTS: list[str] = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    # Chrome on Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Firefox on Linux
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    # Chrome on Android (mobile variety)
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    # Safari on iOS
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
]


# ---------------------------------------------------------------------------
# BaseAgent
# ---------------------------------------------------------------------------

class BaseAgent(ABC):
    """Abstract base class that all scraping agents inherit from.

    Subclasses must implement:
        - scrape()         : Main scraping logic returning raw item dicts.
        - parse_item()     : Convert a single raw dict into structured data.
        - get_search_urls(): Return the list of URLs to crawl.
    """

    MAX_RETRIES: int = 3
    BACKOFF_BASE: float = 2.0  # seconds; actual wait = base ** attempt

    def __init__(
        self,
        name: str,
        platform: str,
        config: dict[str, Any],
        db: Any,  # JobQueue instance
    ) -> None:
        self.name = name
        self.platform = platform
        self.config = config
        self.db = db

        # Rate limiter - honour per-agent config or default to 30 rpm
        rpm = self.config.get("requests_per_minute", 30)
        self.rate_limiter = RateLimiter(requests_per_minute=rpm)

        # Delay range between page loads (seconds)
        self.delay_min: float = self.config.get("delay_min", 1.0)
        self.delay_max: float = self.config.get("delay_max", 3.0)

        self._crawler: Optional[PlaywrightCrawler] = None

    # ------------------------------------------------------------------
    # Abstract methods (subclasses MUST implement)
    # ------------------------------------------------------------------

    @abstractmethod
    async def scrape(self) -> list[dict]:
        """Execute the main scraping logic and return raw item dicts."""
        ...

    @abstractmethod
    def parse_item(self, raw_data: dict) -> dict:
        """Parse a single raw HTML/JSON blob into structured lead data."""
        ...

    @abstractmethod
    def get_search_urls(self) -> list[str]:
        """Return the list of URLs this agent should crawl."""
        ...

    # ------------------------------------------------------------------
    # Concrete: full run lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> list[dict]:
        """Full agent lifecycle with retry logic.

        1. Log run start.
        2. Call scrape() with up to MAX_RETRIES attempts.
        3. Store raw results via the job queue.
        4. Log completion (or error).

        Returns:
            List of parsed item dicts (may be empty on failure).
        """
        attempt = 0
        items: list[dict] = []
        last_error: Optional[Exception] = None

        while attempt < self.MAX_RETRIES:
            try:
                logger.info(
                    "[%s] Starting scrape (attempt %d/%d)",
                    self.name,
                    attempt + 1,
                    self.MAX_RETRIES,
                )
                items = await self.scrape()
                last_error = None
                break  # success
            except Exception as exc:
                last_error = exc
                attempt += 1
                wait = self.BACKOFF_BASE ** attempt
                logger.warning(
                    "[%s] Scrape failed (attempt %d/%d): %s  -- retrying in %.1fs",
                    self.name,
                    attempt,
                    self.MAX_RETRIES,
                    exc,
                    wait,
                )
                if attempt < self.MAX_RETRIES:
                    await asyncio.sleep(wait)

        # ------- post-scrape bookkeeping -------
        if last_error is not None:
            logger.error(
                "[%s] All %d attempts failed. Last error: %s",
                self.name,
                self.MAX_RETRIES,
                last_error,
            )
            await self.log_run(
                status="failed",
                items_found=0,
                items_new=0,
                items_dup=0,
                error=str(last_error),
            )
            return []

        # Parse raw items
        parsed: list[dict] = []
        for raw in items:
            try:
                parsed.append(self.parse_item(raw))
            except Exception as exc:
                logger.warning("[%s] Failed to parse item: %s", self.name, exc)

        # Persist
        new_count = 0
        dup_count = 0
        try:
            new_count, dup_count = await self.store_raw_scrapes(parsed)
        except Exception as exc:
            logger.error("[%s] Failed to store results: %s", self.name, exc)
            await self.log_run(
                status="failed",
                items_found=len(parsed),
                items_new=0,
                items_dup=0,
                error=f"Storage error: {exc}",
            )
            return parsed

        logger.info(
            "[%s] Scrape complete. found=%d new=%d dup=%d",
            self.name,
            len(parsed),
            new_count,
            dup_count,
        )
        await self.log_run(
            status="success",
            items_found=len(parsed),
            items_new=new_count,
            items_dup=dup_count,
        )
        return parsed

    # ------------------------------------------------------------------
    # Concrete: data persistence
    # ------------------------------------------------------------------

    async def store_raw_scrapes(self, items: list[dict]) -> tuple[int, int]:
        """Enqueue parsed items into the job queue as 'raw_scrape' jobs.

        Returns:
            (new_count, duplicate_count)
        """
        new_count = 0
        dup_count = 0

        for item in items:
            # Use source_url as dedup key when available
            source_url = item.get("source_url", "")
            if source_url:
                last = await self.check_last_seen(source_url)
                post_id = item.get("post_id", "")
                if last and last == post_id:
                    dup_count += 1
                    continue
                if post_id:
                    await self.update_last_seen(source_url, post_id)

            payload = {
                "agent": self.name,
                "platform": self.platform,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "data": item,
            }
            await self.db.enqueue(job_type="raw_scrape", payload=payload)
            new_count += 1

        return new_count, dup_count

    # ------------------------------------------------------------------
    # Concrete: run logging
    # ------------------------------------------------------------------

    async def log_run(
        self,
        status: str,
        items_found: int,
        items_new: int,
        items_dup: int,
        error: Optional[str] = None,
    ) -> None:
        """Record a row in the agent_runs table via the job queue."""
        payload = {
            "agent_name": self.name,
            "platform": self.platform,
            "status": status,
            "items_found": items_found,
            "items_new": items_new,
            "items_dup": items_dup,
            "error": error,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            await self.db.enqueue(job_type="agent_run_log", payload=payload)
        except Exception as exc:
            logger.error("[%s] Failed to log run: %s", self.name, exc)

    # ------------------------------------------------------------------
    # Concrete: change detection helpers
    # ------------------------------------------------------------------

    async def check_last_seen(self, url: str) -> Optional[str]:
        """Return the last_seen post ID for *url*, or None."""
        try:
            return await self.db.get_last_seen(agent_name=self.name, url=url)
        except Exception as exc:
            logger.warning("[%s] check_last_seen error: %s", self.name, exc)
            return None

    async def update_last_seen(self, url: str, post_id: str) -> None:
        """Persist the latest post ID for change-detection."""
        try:
            await self.db.set_last_seen(
                agent_name=self.name, url=url, post_id=post_id
            )
        except Exception as exc:
            logger.warning("[%s] update_last_seen error: %s", self.name, exc)

    # ------------------------------------------------------------------
    # Concrete: timing / stealth helpers
    # ------------------------------------------------------------------

    def get_random_delay(self) -> float:
        """Return a random delay between the configured min and max."""
        return random.uniform(self.delay_min, self.delay_max)

    def get_random_user_agent(self) -> str:
        """Pick a random User-Agent string from the rotation list."""
        return random.choice(USER_AGENTS)

    # ------------------------------------------------------------------
    # Concrete: Crawlee / Playwright browser setup
    # ------------------------------------------------------------------

    async def setup_browser(self) -> PlaywrightCrawler:
        """Configure and return a PlaywrightCrawler with stealth settings.

        The crawler is cached on the instance so repeated calls reuse the
        same browser context within a single run.
        """
        if self._crawler is not None:
            return self._crawler

        user_agent = self.get_random_user_agent()

        crawler = PlaywrightCrawler(
            # Headless by default; override via config
            headless=self.config.get("headless", True),
            browser_type=self.config.get("browser_type", "chromium"),
            max_request_retries=self.MAX_RETRIES,
            request_handler_timeout=self.config.get(
                "page_timeout", 60_000
            ),  # ms
        )

        # Store stealth overrides that subclasses can apply inside their
        # request handler via context.page.
        self._browser_stealth = {
            "user_agent": user_agent,
            "viewport": {"width": 1920, "height": 1080},
            "locale": "en-US",
            "timezone_id": "America/Denver",
            "extra_http_headers": {
                "Accept-Language": "en-US,en;q=0.9",
            },
        }

        self._crawler = crawler
        return crawler

    async def apply_stealth(self, page: Any) -> None:
        """Apply stealth overrides to a Playwright page object.

        Call this inside the request handler that Crawlee provides:

            async def handler(context: PlaywrightCrawlingContext):
                await self.apply_stealth(context.page)
        """
        stealth = getattr(self, "_browser_stealth", {})
        if not stealth:
            return

        try:
            await page.set_extra_http_headers(
                stealth.get("extra_http_headers", {})
            )
            # Mask webdriver flag
            await page.evaluate(
                "() => { Object.defineProperty(navigator, 'webdriver', { get: () => false }) }"
            )
        except Exception as exc:
            logger.debug("[%s] Stealth override warning: %s", self.name, exc)

    # ------------------------------------------------------------------
    # repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={self.name!r} platform={self.platform!r}>"
