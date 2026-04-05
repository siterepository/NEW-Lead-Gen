"""
Shared Apollo API base class for all LinkedIn/Apollo agents.

All 8 LinkedIn agents inherit from this class instead of directly from
BaseAgent.  It provides:
  - Apollo People Search API integration via httpx (no Playwright)
  - Shared credit tracking (60 credits/month across ALL agents)
  - Standard response parsing into the normalizer schema
  - Rate limiting that respects the free-tier budget
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx

from leadgen.agents.base import BaseAgent

logger = logging.getLogger(__name__)

# Apollo API constants
APOLLO_API_URL = "https://api.apollo.io/api/v1/mixed_people/search"
APOLLO_CREDIT_FILE = Path.home() / ".leadgen" / "apollo_credits.json"

# Free-tier monthly budget (shared across ALL 8 agents)
MONTHLY_CREDIT_BUDGET = 60
DEFAULT_PER_RUN_LIMIT = 5  # Each agent should use at most 5-8 credits per run


class ApolloLinkedInBase(BaseAgent):
    """Base class for all LinkedIn/Apollo search agents.

    Subclasses define their specific search filters by overriding
    ``get_apollo_params()``.  Everything else -- API calls, credit
    tracking, response parsing -- is handled here.
    """

    def __init__(
        self,
        name: str,
        config: dict[str, Any],
        db: Any,
    ) -> None:
        super().__init__(
            name=name,
            platform="linkedin_apollo",
            config=config,
            db=db,
        )
        self.api_key: str = os.environ.get("APOLLO_API_KEY", "")
        self.per_run_limit: int = self.config.get("per_run_credit_limit", DEFAULT_PER_RUN_LIMIT)
        self.per_page: int = self.config.get("results_per_page", 10)

    # ------------------------------------------------------------------
    # Abstract hook -- subclasses MUST implement
    # ------------------------------------------------------------------

    def get_apollo_params(self) -> dict[str, Any]:
        """Return Apollo People Search filters for this agent.

        Must return a dict suitable for merging into the API payload,
        e.g.::

            {
                "person_titles": ["Sales Manager", "Account Executive"],
                "person_locations": ["Utah, United States"],
            }
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # BaseAgent interface
    # ------------------------------------------------------------------

    def get_search_urls(self) -> list[str]:
        """Apollo agents don't scrape URLs -- return empty list."""
        return []

    async def scrape(self) -> list[dict]:
        """Call the Apollo People Search API and return raw person dicts.

        Respects credit budget: stops early when credits are exhausted.
        """
        if not self.api_key:
            logger.warning(
                "[%s] APOLLO_API_KEY not set -- skipping API search.",
                self.name,
            )
            return []

        # Check remaining budget
        credits_used_this_month = self._load_credit_usage()
        remaining = MONTHLY_CREDIT_BUDGET - credits_used_this_month
        if remaining <= 0:
            logger.warning(
                "[%s] Monthly Apollo credit budget exhausted (%d/%d used).",
                self.name,
                credits_used_this_month,
                MONTHLY_CREDIT_BUDGET,
            )
            return []

        # Cap this run's spend
        run_budget = min(self.per_run_limit, remaining)
        logger.info(
            "[%s] Apollo credit budget: %d remaining, %d allocated for this run.",
            self.name,
            remaining,
            run_budget,
        )

        # Build API payload
        params = self.get_apollo_params()
        payload: dict[str, Any] = {
            "api_key": self.api_key,
            "page": 1,
            "per_page": min(self.per_page, run_budget),
            **params,
        }

        all_people: list[dict] = []
        credits_spent = 0
        page = 1

        while credits_spent < run_budget:
            payload["page"] = page
            payload["per_page"] = min(self.per_page, run_budget - credits_spent)

            if payload["per_page"] <= 0:
                break

            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.post(APOLLO_API_URL, json=payload)
                    response.raise_for_status()
                    data = response.json()

                people = data.get("people", [])
                if not people:
                    logger.info(
                        "[%s] No more results on page %d.", self.name, page
                    )
                    break

                # Each person returned costs 1 email credit
                credits_spent += len(people)
                all_people.extend(people)

                logger.info(
                    "[%s] Page %d: got %d people (total credits this run: %d).",
                    self.name,
                    page,
                    len(people),
                    credits_spent,
                )

                # Check if there are more pages
                pagination = data.get("pagination", {})
                total_pages = pagination.get("total_pages", 1)
                if page >= total_pages:
                    break

                page += 1

            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[%s] Apollo API HTTP error: %s %s",
                    self.name,
                    exc.response.status_code,
                    exc.response.text[:300],
                )
                break
            except httpx.RequestError as exc:
                logger.error("[%s] Apollo API request error: %s", self.name, exc)
                break
            except Exception as exc:
                logger.error("[%s] Apollo unexpected error: %s", self.name, exc)
                break

        # Persist updated credit count
        self._save_credit_usage(credits_used_this_month + credits_spent)

        logger.info(
            "[%s] Apollo search complete: %d people found, %d credits used.",
            self.name,
            len(all_people),
            credits_spent,
        )

        return all_people

    def parse_item(self, raw_data: dict) -> dict:
        """Convert an Apollo person record into the standard lead dict.

        Maps Apollo fields to the canonical normalizer schema.
        """
        # Organization info
        org = raw_data.get("organization") or {}

        # Extract best phone number
        phone = ""
        phone_numbers = raw_data.get("phone_numbers") or []
        for pn in phone_numbers:
            if pn.get("type") == "mobile":
                phone = pn.get("sanitized_number", "")
                break
        if not phone and phone_numbers:
            phone = phone_numbers[0].get("sanitized_number", "")

        # Education (most recent)
        education = ""
        edu_list = raw_data.get("education") or []
        if edu_list:
            edu = edu_list[0]
            school = edu.get("school_name", "")
            degree = edu.get("degree", "")
            if school:
                education = f"{degree} - {school}".strip(" -")

        # Location
        city = raw_data.get("city", "")
        state = raw_data.get("state", "Utah")

        # Build a unique post_id from Apollo person ID
        apollo_id = raw_data.get("id", "")

        return {
            "name": f'{raw_data.get("first_name", "")} {raw_data.get("last_name", "")}'.strip(),
            "first_name": raw_data.get("first_name", ""),
            "last_name": raw_data.get("last_name", ""),
            "title": raw_data.get("title", ""),
            "description": raw_data.get("headline", ""),
            "current_company": org.get("name", ""),
            "current_role": raw_data.get("title", ""),
            "email": (raw_data.get("email") or "").lower().strip(),
            "phone": phone,
            "linkedin_url": raw_data.get("linkedin_url", ""),
            "location_city": city,
            "location_state": state,
            "education": education,
            "source_url": raw_data.get("linkedin_url", ""),
            "post_id": apollo_id,
            "platform": "linkedin_apollo",
            "agent": self.name,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "industry": org.get("industry", ""),
            "company_size": org.get("estimated_num_employees", ""),
            "seniority": raw_data.get("seniority", ""),
            "departments": raw_data.get("departments", []),
        }

    # ------------------------------------------------------------------
    # Credit tracking (file-based, shared across all agents)
    # ------------------------------------------------------------------

    @staticmethod
    def _load_credit_usage() -> int:
        """Load the current month's total Apollo credit usage from disk."""
        try:
            if APOLLO_CREDIT_FILE.exists():
                data = json.loads(APOLLO_CREDIT_FILE.read_text())
                # Reset if we're in a new month
                saved_month = data.get("month", "")
                current_month = datetime.now(timezone.utc).strftime("%Y-%m")
                if saved_month == current_month:
                    return data.get("credits_used", 0)
                # New month -- reset
                return 0
        except Exception as exc:
            logger.warning("Failed to load Apollo credit file: %s", exc)
        return 0

    @staticmethod
    def _save_credit_usage(total_used: int) -> None:
        """Persist the current month's total Apollo credit usage to disk."""
        try:
            APOLLO_CREDIT_FILE.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "month": datetime.now(timezone.utc).strftime("%Y-%m"),
                "credits_used": total_used,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            APOLLO_CREDIT_FILE.write_text(json.dumps(data, indent=2))
        except Exception as exc:
            logger.warning("Failed to save Apollo credit file: %s", exc)
