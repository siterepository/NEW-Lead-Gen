"""
Deduplication stage: detects and merges duplicate leads.

Uses exact fingerprint matching first, then falls back to
fuzzy matching via rapidfuzz for near-duplicates.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from rapidfuzz import fuzz

from leadgen.models.lead import Lead

logger = logging.getLogger(__name__)

# Fuzzy match threshold (0-100). 90 means very high similarity required.
_FUZZY_THRESHOLD = 90.0

# Multi-source score bonuses
_BONUS_TWO_SOURCES = 20
_BONUS_THREE_PLUS_SOURCES = 35


class Deduplicator:
    """Detects duplicate leads and merges multi-source records."""

    async def is_duplicate(
        self, lead: Lead, existing_leads: list[Lead]
    ) -> Optional[Lead]:
        """
        Check whether *lead* duplicates any entry in *existing_leads*.

        Strategy (short-circuit on first match):
          1. Exact fingerprint match
          2. Exact email match (if both have emails)
          3. Fuzzy name + location match at >=90% threshold

        Returns the matching existing Lead, or None if the lead is new.
        """
        if not existing_leads:
            return None

        # Ensure fingerprint is computed
        if not lead.fingerprint:
            lead.compute_fingerprint()

        # --- Pass 1: exact fingerprint ---
        for existing in existing_leads:
            if not existing.fingerprint:
                existing.compute_fingerprint()
            if lead.fingerprint and lead.fingerprint == existing.fingerprint:
                logger.debug("Exact fingerprint match: %s", lead.fingerprint[:12])
                return existing

        # --- Pass 2: exact email ---
        if lead.email:
            for existing in existing_leads:
                if existing.email and lead.email.lower() == existing.email.lower():
                    logger.debug("Exact email match: %s", lead.email)
                    return existing

        # --- Pass 3: fuzzy name + location ---
        lead_name = self._full_name(lead)
        lead_location = self._location_key(lead)

        if lead_name:
            for existing in existing_leads:
                existing_name = self._full_name(existing)
                if not existing_name:
                    continue

                name_score = self.fuzzy_match_name(lead_name, existing_name)
                if name_score < _FUZZY_THRESHOLD:
                    continue

                # Name matches -- check location for extra confidence
                existing_location = self._location_key(existing)
                if lead_location and existing_location:
                    loc_score = fuzz.ratio(lead_location, existing_location)
                    if loc_score >= _FUZZY_THRESHOLD:
                        logger.debug(
                            "Fuzzy match: name=%.1f%% location=%.1f%% (%s ~ %s)",
                            name_score, loc_score, lead_name, existing_name,
                        )
                        return existing
                elif name_score >= 95.0:
                    # Very high name match even without location
                    logger.debug(
                        "Fuzzy match (name only, high confidence): %.1f%% (%s ~ %s)",
                        name_score, lead_name, existing_name,
                    )
                    return existing

        return None

    async def merge_leads(self, existing: Lead, new: Lead) -> Lead:
        """
        Merge a new duplicate lead into an existing record.

        Rules:
          - Prefer non-None fields from whichever source has them
          - Increment sources_count
          - Update last_seen
          - Boost score for multi-source corroboration
        """
        # Merge identity fields (prefer existing, fill gaps from new)
        existing.first_name = existing.first_name or new.first_name
        existing.last_name = existing.last_name or new.last_name
        existing.email = existing.email or new.email
        existing.phone = existing.phone or new.phone
        existing.linkedin_url = existing.linkedin_url or new.linkedin_url

        # Merge location
        existing.location_city = existing.location_city or new.location_city
        existing.location_state = existing.location_state or new.location_state
        existing.location_zip = existing.location_zip or new.location_zip

        # Merge professional info
        existing.current_role = existing.current_role or new.current_role
        existing.current_company = existing.current_company or new.current_company
        existing.education = existing.education or new.education

        if new.career_history:
            if existing.career_history:
                # Combine, deduplicate
                combined = list(dict.fromkeys(existing.career_history + new.career_history))
                existing.career_history = combined
            else:
                existing.career_history = new.career_history

        # Merge recruiting signals
        if new.recruiting_signals:
            if existing.recruiting_signals:
                combined = list(dict.fromkeys(existing.recruiting_signals + new.recruiting_signals))
                existing.recruiting_signals = combined
            else:
                existing.recruiting_signals = new.recruiting_signals

        if new.motivation_keywords:
            if existing.motivation_keywords:
                combined = list(dict.fromkeys(existing.motivation_keywords + new.motivation_keywords))
                existing.motivation_keywords = combined
            else:
                existing.motivation_keywords = new.motivation_keywords

        # Merge life events
        if new.life_events:
            if existing.life_events:
                existing.life_events = {**existing.life_events, **new.life_events}
            else:
                existing.life_events = new.life_events

        # Merge source post text (keep existing, append new if different)
        if new.source_post_text and new.source_post_text != existing.source_post_text:
            if existing.source_post_text:
                # Truncate combined text to model limit
                combined = f"{existing.source_post_text}\n---\n{new.source_post_text}"
                existing.source_post_text = combined[:5000]
            else:
                existing.source_post_text = new.source_post_text

        # Update source tracking
        existing.sources_count += 1
        existing.last_seen = datetime.now(timezone.utc)
        existing.updated_at = datetime.now(timezone.utc)

        # Multi-source score boost applied to data_quality dimension
        self._apply_multi_source_boost(existing)

        # Recompute fingerprint with potentially more data
        existing.compute_fingerprint()

        logger.info(
            "Merged lead %s (now %d sources)",
            existing.id, existing.sources_count,
        )
        return existing

    def fuzzy_match_name(self, name1: str, name2: str) -> float:
        """
        Compute fuzzy similarity between two names using RapidFuzz token_sort_ratio.

        Returns a score from 0 to 100.
        """
        if not name1 or not name2:
            return 0.0
        return fuzz.token_sort_ratio(name1.lower().strip(), name2.lower().strip())

    def compute_fingerprint(self, lead: Lead) -> str:
        """
        Compute SHA-256 fingerprint from normalized identity fields.

        Uses: first_name + last_name + location_zip + email
        """
        raw = (
            self._normalize(lead.first_name)
            + self._normalize(lead.last_name)
            + self._normalize(lead.location_zip)
            + self._normalize(lead.email)
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize(value: str | None) -> str:
        """Lowercase, strip whitespace and non-alphanumeric chars."""
        if not value:
            return ""
        return re.sub(r"[^a-z0-9]", "", value.strip().lower())

    @staticmethod
    def _full_name(lead: Lead) -> str:
        """Build 'first last' string for comparison."""
        parts = []
        if lead.first_name:
            parts.append(lead.first_name.strip())
        if lead.last_name:
            parts.append(lead.last_name.strip())
        return " ".join(parts).lower()

    @staticmethod
    def _location_key(lead: Lead) -> str:
        """Build a location string for comparison."""
        parts = []
        if lead.location_city:
            parts.append(lead.location_city.strip().lower())
        if lead.location_state:
            parts.append(lead.location_state.strip().lower())
        if lead.location_zip:
            parts.append(lead.location_zip.strip())
        return " ".join(parts)

    @staticmethod
    def _apply_multi_source_boost(lead: Lead) -> None:
        """
        Boost data_quality score for leads confirmed by multiple sources.

        +20 for 2 sources, +35 for 3+ sources.
        Applied as an increment to score_data_quality, capped at 10.
        """
        if lead.sources_count >= 3:
            bonus = _BONUS_THREE_PLUS_SOURCES
        elif lead.sources_count >= 2:
            bonus = _BONUS_TWO_SOURCES
        else:
            return

        # Apply bonus across dimensions proportionally to avoid exceeding caps
        # Primary boost goes to data_quality (max 10)
        dq_room = 10 - lead.score_data_quality
        dq_boost = min(bonus, dq_room)
        lead.score_data_quality += dq_boost
        remaining = bonus - dq_boost

        # Overflow goes to demographics (max 10)
        if remaining > 0:
            demo_room = 10 - lead.score_demographics
            demo_boost = min(remaining, demo_room)
            lead.score_demographics += demo_boost
            remaining -= demo_boost

        # Any further overflow goes to motivation (max 25)
        if remaining > 0:
            mot_room = 25 - lead.score_motivation
            mot_boost = min(remaining, mot_room)
            lead.score_motivation += mot_boost

        # Cap total at 100 and use plan-aligned tier thresholds (75/50/25)
        raw_total = (
            lead.score_career_fit
            + lead.score_motivation
            + lead.score_people_skills
            + lead.score_demographics
            + lead.score_data_quality
        )
        lead.total_score = min(raw_total, 100)
        if lead.total_score >= 75:
            lead.tier = "A"
        elif lead.total_score >= 50:
            lead.tier = "B"
        elif lead.total_score >= 25:
            lead.tier = "C"
        else:
            lead.tier = "D"
