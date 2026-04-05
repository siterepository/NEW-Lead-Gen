"""
Glassdoor Utah Agent

Scrapes Glassdoor company reviews in Utah for employees expressing
dissatisfaction - a recruiting signal for NWM financial advisor prospects.
People unhappy at their current job are open to career conversations.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlencode, quote_plus

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext

from leadgen.agents.base import BaseAgent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://www.glassdoor.com"

# Utah state search ID on Glassdoor
UTAH_REVIEWS_URL = (
    "https://www.glassdoor.com/Reviews/"
    "utah-reviews-SRCH_IL.0,4_IS937.htm"
)

# Industries where dissatisfied employees make good NWM recruits
INDUSTRY_FILTERS: list[str] = [
    "Banking",
    "Insurance",
    "Real Estate",
    "Financial Services",
    "Accounting",
    "Sales",
    "Retail",
    "Education",
]

# Low star ratings indicate unhappy employees
LOW_RATING_THRESHOLD = 3.0


class GlassdoorUtahAgent(BaseAgent):
    """Scrape Glassdoor for Utah company reviews indicating dissatisfaction.

    Strategy:
    - Find companies in Utah with low overall ratings
    - Extract review snippets mentioning dissatisfaction, low pay,
      poor management, limited growth
    - These signal employees open to career changes
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="glassdoor_utah",
            platform="glassdoor",
            config=config,
            db=db,
        )
        self.max_pages: int = self.config.get("max_pages", 8)
        self.max_results_per_run: int = self.config.get("max_results_per_run", 150)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build Glassdoor review search URLs for Utah companies.

        Returns the base Utah reviews URL plus industry-filtered variants.
        """
        urls: list[str] = [UTAH_REVIEWS_URL]

        # Add page variants for deeper crawling
        for page_num in range(2, self.max_pages + 1):
            page_url = UTAH_REVIEWS_URL.replace(
                ".htm", f"_IP{page_num}.htm"
            )
            urls.append(page_url)

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Scrape Glassdoor Utah reviews for dissatisfied employees."""
        collected: list[dict] = []
        search_urls = self.get_search_urls()
        crawler = await self.setup_browser()

        @crawler.router.default_handler
        async def handle_page(context: PlaywrightCrawlingContext) -> None:
            page = context.page
            await self.apply_stealth(page)

            # Wait for company/review cards
            try:
                await page.wait_for_selector(
                    "div[data-test='employer-card-single'], "
                    "div.single-company-result, "
                    "div.eiCell, div.review-container",
                    timeout=20_000,
                )
            except Exception:
                logger.debug(
                    "[%s] No review cards on %s", self.name, context.request.url
                )
                return

            # Extract company cards from search results
            cards = await page.query_selector_all(
                "div[data-test='employer-card-single'], "
                "div.single-company-result, div.eiCell"
            )

            for card in cards:
                if len(collected) >= self.max_results_per_run:
                    break
                try:
                    raw = await self._extract_company_card(card, page)
                    if raw:
                        # Filter to low-rated companies
                        rating = raw.get("rating", 5.0)
                        try:
                            if float(rating) > LOW_RATING_THRESHOLD:
                                continue
                        except (ValueError, TypeError):
                            pass
                        collected.append(raw)
                except Exception as exc:
                    logger.warning("[%s] Card extraction error: %s", self.name, exc)

        # Enqueue with rate limiting
        urls_to_crawl: list[str] = []
        for url in search_urls:
            if len(collected) >= self.max_results_per_run:
                break
            await self.rate_limiter.acquire()
            await asyncio.sleep(self.get_random_delay())
            urls_to_crawl.append(url)

        if urls_to_crawl:
            await crawler.run(urls_to_crawl)

        logger.info("[%s] Collected %d raw items", self.name, len(collected))
        return collected

    # ------------------------------------------------------------------
    # Card extraction
    # ------------------------------------------------------------------

    async def _extract_company_card(
        self, card: Any, page: Any
    ) -> Optional[dict]:
        """Extract data from a Glassdoor company/review card."""
        raw: dict[str, Any] = {}

        # Company name
        name_el = await card.query_selector(
            "h2 a.eiCell-master-wrap, a[data-test='cell-Reviews-url'], "
            "h2.employerName, a.employerName"
        )
        if name_el:
            raw["company"] = (await name_el.inner_text()).strip()
            href = await name_el.get_attribute("href")
            if href:
                if not href.startswith("http"):
                    href = f"{BASE_URL}{href}"
                raw["url"] = href
        else:
            return None

        # Overall rating
        rating_el = await card.query_selector(
            "span[data-test='rating'], span.ratingNumber, "
            "span.bigRating, span.rating"
        )
        if rating_el:
            raw["rating"] = (await rating_el.inner_text()).strip()

        # Number of reviews
        reviews_el = await card.query_selector(
            "span[data-test='cell-Reviews-count'], "
            "span.reviewCount, a.eiCell-master-count"
        )
        if reviews_el:
            raw["review_count"] = (await reviews_el.inner_text()).strip()

        # Location
        loc_el = await card.query_selector(
            "span.loc, span.employer-location, "
            "span[data-test='employer-location']"
        )
        if loc_el:
            raw["location"] = (await loc_el.inner_text()).strip()

        # Industry
        ind_el = await card.query_selector(
            "span.industry, span[data-test='employer-industry']"
        )
        if ind_el:
            raw["industry"] = (await ind_el.inner_text()).strip()

        # Review snippet (cons / negative highlights)
        snippet_el = await card.query_selector(
            "div.review-snippet, p.reviewSnippet, "
            "span[data-test='review-snippet']"
        )
        if snippet_el:
            raw["review_snippet"] = (await snippet_el.inner_text()).strip()

        # Post ID for dedup
        raw["post_id"] = raw.get("url", raw.get("company", ""))

        return raw

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw Glassdoor data to standard lead format."""
        location_raw = raw_data.get("location", "")
        city = self._parse_city(location_raw)

        return {
            "name": "",
            "company": raw_data.get("company", ""),
            "title": f"Reviews: {raw_data.get('company', '')}",
            "description": raw_data.get("review_snippet", ""),
            "rating": raw_data.get("rating", ""),
            "review_count": raw_data.get("review_count", ""),
            "industry": raw_data.get("industry", ""),
            "location_city": city,
            "location_state": "Utah",
            "posted_date": "",
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", ""),
            "platform": "glassdoor",
            "contact_info": "",
            "contact_phone": "",
            "contact_email": "",
            "category": "company_review",
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_city(location: str) -> str:
        """Extract city from Glassdoor location string."""
        if not location:
            return ""
        city = re.sub(r",?\s*(UT|Utah)\s*$", "", location, flags=re.IGNORECASE)
        city = re.sub(r"\s*\d{5}(-\d{4})?\s*$", "", city)
        return city.strip()
