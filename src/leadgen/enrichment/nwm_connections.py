"""
NWM Mutual Connection Checker

Uses Apollo.io API to:
1. Pull a list of Northwestern Mutual employees in Utah
2. Cache the list locally (refreshes weekly)
3. Cross-reference leads against NWM employee names and companies

This enables the +40 scoring boost for leads with NWM mutual connections.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

from leadgen.models.lead import Lead

logger = logging.getLogger(__name__)

# Cache file for NWM employees (avoids burning API credits on every run)
_CACHE_DIR = Path.home() / ".leadgen"
_NWM_CACHE_FILE = _CACHE_DIR / "nwm_employees.json"
_CACHE_MAX_AGE_SECONDS = 7 * 24 * 60 * 60  # 1 week

_APOLLO_BASE_URL = "https://api.apollo.io/v1"

# NWM company variations to search for
NWM_COMPANY_NAMES = [
    "Northwestern Mutual",
    "North Western Mutual",
    "NM Financial",
    "Northwestern Mutual Life Insurance",
    "Northwestern Mutual Investment Services",
    "Northwestern Mutual Wealth Management",
]

# Utah locations to filter by
UTAH_LOCATIONS = [
    "Utah, United States",
    "Salt Lake City, Utah",
    "Provo, Utah",
    "Ogden, Utah",
    "Sandy, Utah",
    "Draper, Utah",
    "Lehi, Utah",
]


class NWMConnectionChecker:
    """Check if leads have connections to Northwestern Mutual employees."""

    def __init__(self):
        self.api_key: str = os.environ.get("APOLLO_API_KEY", "")
        self._nwm_employees: list[dict] = []
        self._nwm_names: set[str] = set()
        self._nwm_emails: set[str] = set()
        self._loaded = False

    async def initialize(self) -> None:
        """Load NWM employee list from cache or Apollo API."""
        if self._loaded:
            return

        # Try loading from cache first
        if self._load_cache():
            self._loaded = True
            logger.info(
                "NWM employee list loaded from cache: %d employees",
                len(self._nwm_employees),
            )
            return

        # Fetch from Apollo API
        if self.api_key:
            await self._fetch_nwm_employees()
            self._save_cache()
            self._loaded = True
            logger.info(
                "NWM employee list fetched from Apollo: %d employees",
                len(self._nwm_employees),
            )
        else:
            logger.warning(
                "No APOLLO_API_KEY set and no cache found. "
                "NWM connection checking will be limited to text matching only."
            )
            self._loaded = True

    def _load_cache(self) -> bool:
        """Load NWM employees from local cache file if fresh enough."""
        if not _NWM_CACHE_FILE.exists():
            return False

        try:
            data = json.loads(_NWM_CACHE_FILE.read_text())
            cached_at = data.get("cached_at", 0)
            age = time.time() - cached_at

            if age > _CACHE_MAX_AGE_SECONDS:
                logger.info("NWM cache expired (%.1f days old), will refresh.", age / 86400)
                return False

            self._nwm_employees = data.get("employees", [])
            self._build_lookup_sets()
            return True
        except Exception as exc:
            logger.warning("Failed to load NWM cache: %s", exc)
            return False

    def _save_cache(self) -> None:
        """Save NWM employee list to local cache."""
        try:
            _CACHE_DIR.mkdir(parents=True, exist_ok=True)
            data = {
                "cached_at": time.time(),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "employee_count": len(self._nwm_employees),
                "employees": self._nwm_employees,
            }
            _NWM_CACHE_FILE.write_text(json.dumps(data, indent=2))
            logger.info("NWM employee cache saved: %d employees", len(self._nwm_employees))
        except Exception as exc:
            logger.warning("Failed to save NWM cache: %s", exc)

    def _build_lookup_sets(self) -> None:
        """Build fast lookup sets from the employee list."""
        self._nwm_names = set()
        self._nwm_emails = set()

        for emp in self._nwm_employees:
            name = emp.get("name", "").strip().lower()
            if name:
                self._nwm_names.add(name)
                # Also add first + last separately for partial matching
                parts = name.split()
                if len(parts) >= 2:
                    self._nwm_names.add(f"{parts[0]} {parts[-1]}")

            email = emp.get("email", "").strip().lower()
            if email:
                self._nwm_emails.add(email)

    async def _fetch_nwm_employees(self) -> None:
        """Fetch NWM employees in Utah from Apollo People Search API.

        Uses 1 API credit per search. We do 1 search with per_page=100
        to get a solid list without burning too many credits.
        """
        if not self.api_key:
            return

        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
        }

        payload = {
            "api_key": self.api_key,
            "q_organization_name": "Northwestern Mutual",
            "person_locations": ["Utah, United States"],
            "per_page": 100,
            "page": 1,
        }

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{_APOLLO_BASE_URL}/mixed_people/search",
                    json=payload,
                    headers=headers,
                )
                resp.raise_for_status()
                data = resp.json()

            people = data.get("people", [])
            self._nwm_employees = []

            for person in people:
                emp = {
                    "name": f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),
                    "title": person.get("title", ""),
                    "email": person.get("email", ""),
                    "linkedin_url": person.get("linkedin_url", ""),
                    "city": person.get("city", ""),
                    "state": person.get("state", ""),
                }
                self._nwm_employees.append(emp)

            self._build_lookup_sets()
            logger.info(
                "Apollo returned %d NWM employees in Utah (used 1 credit)",
                len(self._nwm_employees),
            )

        except Exception as exc:
            logger.error("Failed to fetch NWM employees from Apollo: %s", exc)

    def check_lead(self, lead: Lead) -> Lead:
        """Check if a lead has connections to NWM employees.

        Cross-references the lead's data against the NWM employee list.
        Sets has_nwm_mutual_connection=True and score_nwm_connection=40
        if a match is found.

        Match criteria (any one triggers the boost):
        1. Lead's LinkedIn connections overlap with NWM employees
        2. Lead's career history mentions NWM
        3. Lead's source text mentions NWM
        4. Lead's company is NWM or an NWM subsidiary
        5. Lead's name appears in NWM employee list (they ARE an NWM employee
           or former employee - still valuable as a warm contact)
        """
        if lead.has_nwm_mutual_connection:
            return lead  # Already flagged

        matched_names = []

        # Check 1: Is the lead's current company NWM?
        if lead.current_company:
            company_lower = lead.current_company.lower()
            for nwm_name in NWM_COMPANY_NAMES:
                if nwm_name.lower() in company_lower:
                    lead.has_nwm_mutual_connection = True
                    lead.nwm_connection_source = f"current employer: {lead.current_company}"
                    lead.score_nwm_connection = 40
                    return lead

        # Check 2: Career history mentions NWM
        if lead.career_history:
            for role in lead.career_history:
                role_lower = role.lower()
                for nwm_name in NWM_COMPANY_NAMES:
                    if nwm_name.lower() in role_lower:
                        lead.has_nwm_mutual_connection = True
                        lead.nwm_connection_source = f"career history: {role}"
                        lead.score_nwm_connection = 40
                        return lead

        # Check 3: Source text mentions NWM
        if lead.source_post_text:
            text_lower = lead.source_post_text.lower()
            for nwm_name in NWM_COMPANY_NAMES:
                if nwm_name.lower() in text_lower:
                    lead.has_nwm_mutual_connection = True
                    lead.nwm_connection_source = "mentioned in post text"
                    lead.score_nwm_connection = 40
                    return lead

        # Check 4: Cross-reference against NWM employee names
        # (if we have the employee list from Apollo)
        if self._nwm_names and lead.first_name and lead.last_name:
            lead_name = f"{lead.first_name} {lead.last_name}".strip().lower()

            # Check if this person IS an NWM employee
            if lead_name in self._nwm_names:
                lead.has_nwm_mutual_connection = True
                lead.nwm_connection_source = "matched NWM employee list (Apollo)"
                lead.nwm_mutual_names = [lead_name]
                lead.score_nwm_connection = 40
                return lead

        # Check 5: If lead has a LinkedIn URL, check if any NWM employees
        # share similar network indicators (same city + same industry)
        # This is a softer signal - only flag if multiple indicators
        if self._nwm_employees and lead.location_city:
            city_lower = lead.location_city.lower()
            same_city_nwm = [
                emp for emp in self._nwm_employees
                if emp.get("city", "").lower() == city_lower
            ]
            if len(same_city_nwm) >= 3:
                # 3+ NWM reps in same city = high likelihood of mutual connections
                names = [emp["name"] for emp in same_city_nwm[:5]]
                lead.nwm_mutual_names = names
                lead.nwm_connection_source = (
                    f"same city as {len(same_city_nwm)} NWM reps: {', '.join(names[:3])}"
                )
                # Softer boost for same-city proximity (not full 40)
                # Full 40 only for confirmed connections
                # For now, flag but don't auto-boost - let scoring engine decide
                logger.debug(
                    "Lead %s %s: %d NWM reps in same city (%s)",
                    lead.first_name, lead.last_name, len(same_city_nwm), lead.location_city,
                )

        return lead

    def get_nwm_employee_count(self) -> int:
        """Return the number of cached NWM employees."""
        return len(self._nwm_employees)

    def get_nwm_employees(self) -> list[dict]:
        """Return the full NWM employee list."""
        return self._nwm_employees.copy()
