"""
Utah Career Coaches Agent

Scrapes Utah career coaching websites to find their clients and testimonials.
People actively working with career coaches are in the midst of career
transitions and may be strong NWM financial advisor recruiting prospects.
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
# Target coach sites and discovery queries
# ---------------------------------------------------------------------------

# Google searches to discover Utah career coaches and their testimonials
GOOGLE_SEARCH_QUERIES: list[str] = [
    '"career coach" "utah" testimonials',
    '"career coach" "salt lake city" reviews',
    '"career coaching" "utah" client results',
    '"executive coach" "utah" testimonials',
    '"life coach" "career change" "utah"',
    '"resume writer" "utah" testimonials',
    '"career counselor" "utah" reviews',
    '"career transition coach" "utah"',
    'site:thumbtack.com "career coach" "utah"',
    'site:yelp.com "career coach" "utah"',
]

# Known Utah career coaching sites to scrape directly
KNOWN_COACH_SITES: list[str] = [
    "https://www.thumbtack.com/ut/salt-lake-city/career-counseling/",
    "https://www.yelp.com/search?find_desc=career+coaching&find_loc=Salt+Lake+City%2C+UT",
]


class UtahCareerCoachesAgent(BaseAgent):
    """Scrape Utah career coaching websites for client testimonials.

    Targets:
    - Testimonial pages showing people in career transitions
    - Review sites with Utah career coaching clients
    - Coach websites listing their specialties and client types
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="career_coaches_utah",
            platform="content",
            config=config,
            db=db,
        )
        self.max_results_per_run: int = self.config.get("max_results_per_run", 100)

    # ------------------------------------------------------------------
    # get_search_urls
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Build Google search URLs and direct coach site URLs."""
        urls: list[str] = []

        for query in GOOGLE_SEARCH_QUERIES:
            params = {"q": query}
            url = f"https://www.google.com/search?{urlencode(params, quote_via=quote_plus)}"
            urls.append(url)

        urls.extend(KNOWN_COACH_SITES)

        logger.info("[%s] Generated %d search URLs", self.name, len(urls))
        return urls

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------

    async def scrape(self) -> list[dict]:
        """Scrape career coaching sites for testimonials and client data."""
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
                # Extract Google search results pointing to coach sites
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

                        snippet_el = await result.query_selector(
                            "div.VwiC3b, span.aCOpRe"
                        )
                        snippet = ""
                        if snippet_el:
                            snippet = (await snippet_el.inner_text()).strip()

                        if href and not href.startswith("/search"):
                            collected.append({
                                "title": title,
                                "url": href,
                                "description": snippet[:500],
                                "source": "google_coach_search",
                                "coach_name": "",
                                "testimonial_text": "",
                                "client_name": "",
                            })
                    except Exception:
                        continue
            else:
                # Extract testimonials/reviews from coach sites
                testimonials = await page.query_selector_all(
                    "div.testimonial, div.review, blockquote, "
                    "div.client-story, div.success-story, "
                    "div[class*='testimonial'], div[class*='review']"
                )
                for testimonial in testimonials:
                    if len(collected) >= self.max_results_per_run:
                        break
                    try:
                        text = (await testimonial.inner_text()).strip()
                        if len(text) < 20:
                            continue

                        # Try to extract client name
                        name_el = await testimonial.query_selector(
                            "cite, span.name, span.author, strong, "
                            "p.author, div.reviewer-name"
                        )
                        client_name = ""
                        if name_el:
                            client_name = (await name_el.inner_text()).strip()

                        collected.append({
                            "title": f"Testimonial from {client_name or 'anonymous'}",
                            "url": url,
                            "description": text[:500],
                            "source": "coach_testimonial",
                            "coach_name": "",
                            "testimonial_text": text[:1000],
                            "client_name": client_name,
                            "post_id": f"{url}#{client_name or text[:30]}",
                        })
                    except Exception:
                        continue

                # Also extract coach business listings
                listings = await page.query_selector_all(
                    "div.business-listing, div.search-result, "
                    "div.professional-card, li.regular-search-result"
                )
                for listing in listings:
                    if len(collected) >= self.max_results_per_run:
                        break
                    try:
                        name_el = await listing.query_selector(
                            "h2 a, h3 a, a.business-name, a[class*='name']"
                        )
                        if not name_el:
                            continue
                        name = (await name_el.inner_text()).strip()
                        href = await name_el.get_attribute("href") or ""

                        desc_el = await listing.query_selector(
                            "p, div.snippet, div.description"
                        )
                        desc = ""
                        if desc_el:
                            desc = (await desc_el.inner_text()).strip()

                        collected.append({
                            "title": name,
                            "url": href if href.startswith("http") else url,
                            "description": desc[:500],
                            "source": "coach_listing",
                            "coach_name": name,
                            "testimonial_text": "",
                            "client_name": "",
                            "post_id": href or name,
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
        logger.info("[%s] Scrape complete: %d items collected", self.name, len(all_items))
        return all_items

    # ------------------------------------------------------------------
    # parse_item
    # ------------------------------------------------------------------

    def parse_item(self, raw_data: dict) -> dict:
        """Convert raw coach/testimonial data into the standard lead format."""
        return {
            "name": raw_data.get("client_name", ""),
            "title": raw_data.get("title", ""),
            "description": raw_data.get("description", ""),
            "location_city": "",
            "location_state": "Utah",
            "posted_date": "",
            "url": raw_data.get("url", ""),
            "source_url": raw_data.get("url", ""),
            "post_id": raw_data.get("post_id", raw_data.get("url", "")),
            "platform": "content",
            "source_site": raw_data.get("source", ""),
            "coach_name": raw_data.get("coach_name", ""),
            "testimonial_text": raw_data.get("testimonial_text", ""),
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
