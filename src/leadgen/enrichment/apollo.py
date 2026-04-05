"""
Apollo.io free-tier integration for lead enrichment.

Free tier limits:
  - 60 mobile credits per month
  - 600 email credits per month

Only enriches A-tier leads to conserve credits.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

import httpx

from leadgen.models.lead import Lead, ApiCreditUsage

logger = logging.getLogger(__name__)

_APOLLO_BASE_URL = "https://api.apollo.io/v1"

# Free-tier monthly limits
_MOBILE_CREDITS_MONTHLY = 60
_EMAIL_CREDITS_MONTHLY = 600


class ApolloEnricher:
    """Enrich leads using the Apollo.io People API (free tier)."""

    def __init__(self):
        self.api_key: str = os.environ.get("APOLLO_API_KEY", "")
        if not self.api_key:
            logger.warning("APOLLO_API_KEY not set -- Apollo enrichment will be unavailable.")

        # Credit tracking (in-memory; sync with DB on startup in production)
        self._mobile_credits_used: int = 0
        self._email_credits_used: int = 0

        # Credit log buffer
        self._usage_log: list[ApiCreditUsage] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def enrich_lead(self, lead: Lead) -> Lead:
        """
        Enrich a lead with Apollo data. Only processes A-tier leads.

        Updates lead fields: email, phone, current_company, current_role,
        linkedin_url, education. Sets enriched=True on success.

        Returns:
            The updated lead (modified in-place and returned).
        """
        if lead.tier != "A":
            logger.debug(
                "Skipping non-A-tier lead for Apollo enrichment: tier=%s",
                lead.tier,
            )
            return lead

        if not self.api_key:
            logger.warning("Apollo API key not configured, skipping enrichment.")
            return lead

        # Check remaining credits
        credits = await self.check_credits()
        if credits["email_remaining"] <= 0 and credits["mobile_remaining"] <= 0:
            logger.warning("Apollo free-tier credits exhausted for this month.")
            return lead

        # Search Apollo for this person
        person_data = await self.search_person(
            name=f"{lead.first_name or ''} {lead.last_name or ''}".strip(),
            location=lead.location_city,
            company=lead.current_company,
        )

        if person_data is None:
            logger.debug(
                "Apollo returned no results for %s %s",
                lead.first_name, lead.last_name,
            )
            return lead

        # Map Apollo response to Lead fields
        lead = self._apply_enrichment(lead, person_data)
        lead.enriched = True
        lead.enrichment_date = datetime.now(timezone.utc)

        logger.info(
            "Enriched lead via Apollo: %s %s (email=%s, phone=%s)",
            lead.first_name, lead.last_name,
            "yes" if lead.email else "no",
            "yes" if lead.phone else "no",
        )

        return lead

    async def check_credits(self) -> dict:
        """
        Return current credit usage and remaining counts.

        Returns:
            {
                "service": "apollo",
                "mobile_used": int,
                "mobile_remaining": int,
                "email_used": int,
                "email_remaining": int,
            }
        """
        return {
            "service": "apollo",
            "mobile_used": self._mobile_credits_used,
            "mobile_remaining": max(0, _MOBILE_CREDITS_MONTHLY - self._mobile_credits_used),
            "email_used": self._email_credits_used,
            "email_remaining": max(0, _EMAIL_CREDITS_MONTHLY - self._email_credits_used),
        }

    async def search_person(
        self,
        name: str,
        location: str = None,
        company: str = None,
    ) -> Optional[dict]:
        """
        Call the Apollo People Search API.

        Args:
            name: Full name to search for.
            location: City/region hint.
            company: Current company hint.

        Returns:
            Enrichment data dict or None if not found / error.
        """
        if not self.api_key:
            return None

        # Build search payload
        payload: dict = {
            "api_key": self.api_key,
            "q_person_name": name,
            "page": 1,
            "per_page": 1,
        }
        if location:
            payload["person_locations[]"] = location
        if company:
            payload["q_organization_name"] = company

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{_APOLLO_BASE_URL}/mixed_people/search",
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()

            people = data.get("people", [])
            if not people:
                return None

            person = people[0]

            # Track credit usage
            self._email_credits_used += 1
            if person.get("phone_numbers"):
                self._mobile_credits_used += 1

            # Log usage
            self._log_credit_usage(
                credits_used=1,
                operation="people_search",
                lead_name=name,
            )

            return person

        except httpx.HTTPStatusError as exc:
            logger.error(
                "Apollo API HTTP error: %s %s",
                exc.response.status_code, exc.response.text[:200],
            )
            return None
        except httpx.RequestError as exc:
            logger.error("Apollo API request error: %s", exc)
            return None
        except Exception as exc:
            logger.error("Apollo enrichment unexpected error: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_enrichment(lead: Lead, person: dict) -> Lead:
        """Map Apollo person response fields onto the Lead model."""
        # Email
        email = person.get("email")
        if email and not lead.email:
            lead.email = email.lower().strip()

        # Phone
        phone_numbers = person.get("phone_numbers") or []
        if phone_numbers and not lead.phone:
            # Prefer mobile, then work
            for pn in phone_numbers:
                if pn.get("type") == "mobile":
                    lead.phone = pn.get("sanitized_number", "")
                    break
            if not lead.phone and phone_numbers:
                lead.phone = phone_numbers[0].get("sanitized_number", "")

        # Company and title
        org = person.get("organization") or {}
        if org.get("name") and not lead.current_company:
            lead.current_company = org["name"]
        if person.get("title") and not lead.current_role:
            lead.current_role = person["title"]

        # LinkedIn
        linkedin = person.get("linkedin_url")
        if linkedin and not lead.linkedin_url:
            lead.linkedin_url = linkedin

        # Education
        if not lead.education:
            education_entries = person.get("education") or []
            if education_entries:
                # Take most recent
                edu = education_entries[0]
                school = edu.get("school_name", "")
                degree = edu.get("degree", "")
                if school:
                    lead.education = f"{degree} - {school}".strip(" -")

        return lead

    def _log_credit_usage(self, credits_used: int, operation: str, lead_name: str = "") -> None:
        """Record API credit consumption."""
        usage = ApiCreditUsage(
            service="apollo",
            credits_used=credits_used,
            credits_remaining=max(
                0,
                _EMAIL_CREDITS_MONTHLY - self._email_credits_used,
            ),
            operation=operation,
        )
        self._usage_log.append(usage)

        logger.debug(
            "Apollo credit used: op=%s name=%s remaining_email=%d remaining_mobile=%d",
            operation, lead_name,
            _EMAIL_CREDITS_MONTHLY - self._email_credits_used,
            _MOBILE_CREDITS_MONTHLY - self._mobile_credits_used,
        )
        # TODO: persist to Supabase api_credit_usage table
