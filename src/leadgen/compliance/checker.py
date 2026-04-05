"""
Compliance checking gate: validates leads against DNC lists,
age restrictions, and data-minimization rules before they enter
the pipeline.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Optional

from leadgen.models.lead import Lead

logger = logging.getLogger(__name__)

# Patterns for sensitive data that should never be stored
_SSN_PATTERN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_CREDIT_CARD_PATTERN = re.compile(r"\b(?:\d[ -]*?){13,16}\b")


class ComplianceChecker:
    """
    Runs compliance checks on leads before pipeline processing.

    Checks:
      - Do Not Contact (DNC) list
      - Minor detection (under 18)
      - Data minimization (strip sensitive PII)
    """

    def __init__(self):
        # In-memory DNC list. In production, backed by Supabase table.
        self._dnc_emails: set[str] = set()
        self._dnc_phones: set[str] = set()
        self._dnc_names: set[str] = set()

        # Compliance log buffer (flush to DB periodically)
        self._log_buffer: list[dict] = []

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def check_lead(self, lead: Lead) -> tuple[bool, list[str]]:
        """
        Run all compliance checks on a lead.

        Returns:
            (passed, issues) -- passed is True if all checks clear,
            issues is a list of human-readable failure reasons.
        """
        issues: list[str] = []

        # DNC check
        dnc_clear = await self.check_dnc(lead)
        if not dnc_clear:
            issues.append("Lead is on the Do Not Contact list")
            lead.dnc_listed = True

        # Minor check
        minor_clear = await self.check_minor(lead)
        if not minor_clear:
            issues.append("Lead appears to be a minor (under 18)")

        # Data minimization
        data_clean = await self.check_data_minimization(lead)
        if not data_clean:
            issues.append("Sensitive data patterns were found and scrubbed")

        passed = len(issues) == 0

        # Update lead compliance fields
        lead.compliance_cleared = passed
        lead.compliance_date = datetime.now(timezone.utc)

        # Log the check
        await self.log_check(
            lead_id=lead.id or "unknown",
            check_type="full_compliance",
            result=passed,
            details="; ".join(issues) if issues else "All checks passed",
        )

        if not passed:
            logger.info(
                "Compliance FAILED for %s %s: %s",
                lead.first_name, lead.last_name, "; ".join(issues),
            )
        else:
            logger.debug(
                "Compliance passed for %s %s",
                lead.first_name, lead.last_name,
            )

        return passed, issues

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    async def check_dnc(self, lead: Lead) -> bool:
        """
        Check if the lead's email, phone, or name is on the DNC list.

        Returns True if the lead is CLEAR (not on DNC).
        """
        if lead.email and lead.email.lower() in self._dnc_emails:
            await self.log_check(
                lead_id=lead.id or "unknown",
                check_type="dnc_email",
                result=False,
                details=f"Email {lead.email} is on DNC list",
            )
            return False

        if lead.phone and lead.phone in self._dnc_phones:
            await self.log_check(
                lead_id=lead.id or "unknown",
                check_type="dnc_phone",
                result=False,
                details=f"Phone {lead.phone} is on DNC list",
            )
            return False

        # Name-based DNC (less precise, used for explicit opt-outs)
        if lead.first_name and lead.last_name:
            full_name = f"{lead.first_name} {lead.last_name}".lower().strip()
            if full_name in self._dnc_names:
                await self.log_check(
                    lead_id=lead.id or "unknown",
                    check_type="dnc_name",
                    result=False,
                    details=f"Name '{full_name}' is on DNC list",
                )
                return False

        return True

    async def check_minor(self, lead: Lead) -> bool:
        """
        If age is determinable and under 18, reject the lead.

        Returns True if the lead is CLEAR (not a minor or age unknown).
        """
        # Check life_events for age/DOB signals
        if lead.life_events:
            age = lead.life_events.get("age")
            if age is not None:
                try:
                    age_int = int(age)
                    if age_int < 18:
                        await self.log_check(
                            lead_id=lead.id or "unknown",
                            check_type="minor_check",
                            result=False,
                            details=f"Lead age is {age_int} (under 18)",
                        )
                        return False
                except (ValueError, TypeError):
                    pass

            # Check for date of birth
            dob = lead.life_events.get("date_of_birth") or lead.life_events.get("dob")
            if dob:
                try:
                    if isinstance(dob, str):
                        birth_date = datetime.fromisoformat(dob)
                    elif isinstance(dob, datetime):
                        birth_date = dob
                    else:
                        birth_date = None

                    if birth_date:
                        today = datetime.now(timezone.utc)
                        age_years = (
                            today.year - birth_date.year
                            - ((today.month, today.day) < (birth_date.month, birth_date.day))
                        )
                        if age_years < 18:
                            await self.log_check(
                                lead_id=lead.id or "unknown",
                                check_type="minor_check",
                                result=False,
                                details=f"Lead DOB indicates age {age_years} (under 18)",
                            )
                            return False
                except (ValueError, TypeError):
                    pass

        # Check for age keywords in source text
        if lead.source_post_text:
            age_patterns = [
                r"\b(\d{1,2})\s*(?:year|yr)s?\s*old\b",
                r"\bage[:\s]+(\d{1,2})\b",
            ]
            for pattern in age_patterns:
                match = re.search(pattern, lead.source_post_text, re.IGNORECASE)
                if match:
                    try:
                        mentioned_age = int(match.group(1))
                        if mentioned_age < 18:
                            await self.log_check(
                                lead_id=lead.id or "unknown",
                                check_type="minor_check",
                                result=False,
                                details=f"Source text mentions age {mentioned_age} (under 18)",
                            )
                            return False
                    except ValueError:
                        pass

        return True

    async def check_data_minimization(self, lead: Lead) -> bool:
        """
        Ensure no unnecessary sensitive data is stored.

        Strips SSN patterns and credit card patterns from raw text fields.
        Returns True if the data is clean (after scrubbing).
        """
        found_sensitive = False

        # Check and scrub source_post_text
        if lead.source_post_text:
            scrubbed, had_ssn = self._scrub_ssn(lead.source_post_text)
            scrubbed, had_cc = self._scrub_credit_card(scrubbed)
            if had_ssn or had_cc:
                lead.source_post_text = scrubbed
                found_sensitive = True
                details = []
                if had_ssn:
                    details.append("SSN pattern found and scrubbed")
                if had_cc:
                    details.append("Credit card pattern found and scrubbed")
                await self.log_check(
                    lead_id=lead.id or "unknown",
                    check_type="data_minimization",
                    result=False,
                    details="; ".join(details),
                )

        # Check recruiting signals text
        if lead.recruiting_signals:
            clean_signals = []
            for signal in lead.recruiting_signals:
                cleaned, _ = self._scrub_ssn(signal)
                cleaned, _ = self._scrub_credit_card(cleaned)
                clean_signals.append(cleaned)
            lead.recruiting_signals = clean_signals

        # Check motivation keywords
        if lead.motivation_keywords:
            clean_keywords = []
            for kw in lead.motivation_keywords:
                cleaned, _ = self._scrub_ssn(kw)
                cleaned, _ = self._scrub_credit_card(cleaned)
                clean_keywords.append(cleaned)
            lead.motivation_keywords = clean_keywords

        if found_sensitive:
            logger.warning(
                "Sensitive data scrubbed from lead %s %s",
                lead.first_name, lead.last_name,
            )

        # Return True means clean (we scrubbed it), but we note it happened
        return not found_sensitive

    # ------------------------------------------------------------------
    # DNC management
    # ------------------------------------------------------------------

    async def add_to_dnc(
        self,
        email: str = None,
        phone: str = None,
        name: str = None,
        reason: str = "opt-out",
    ) -> None:
        """
        Add a contact to the Do Not Contact list.

        Args:
            email: Email address to block.
            phone: Phone number to block.
            name: Full name to block (less precise).
            reason: Reason for DNC (e.g., "opt-out", "complaint", "minor").
        """
        if email:
            self._dnc_emails.add(email.lower().strip())
            logger.info("Added email to DNC: %s (reason: %s)", email, reason)

        if phone:
            self._dnc_phones.add(phone.strip())
            logger.info("Added phone to DNC: %s (reason: %s)", phone, reason)

        if name:
            self._dnc_names.add(name.lower().strip())
            logger.info("Added name to DNC: %s (reason: %s)", name, reason)

        # TODO: persist to Supabase dnc_list table
        #   await supabase.table("dnc_list").insert({
        #       "email": email, "phone": phone, "name": name,
        #       "reason": reason, "added_at": datetime.now(timezone.utc).isoformat(),
        #   })

    # ------------------------------------------------------------------
    # Audit logging
    # ------------------------------------------------------------------

    async def log_check(
        self,
        lead_id: str,
        check_type: str,
        result: bool,
        details: str,
    ) -> None:
        """
        Log a compliance check to the audit trail.

        Args:
            lead_id: UUID of the lead checked.
            check_type: Type of check (dnc_email, minor_check, etc.).
            result: True if check passed.
            details: Human-readable description.
        """
        entry = {
            "lead_id": lead_id,
            "check_type": check_type,
            "result": "pass" if result else "fail",
            "details": details,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        self._log_buffer.append(entry)

        logger.debug(
            "Compliance log: lead=%s check=%s result=%s detail=%s",
            lead_id[:8] if lead_id else "?", check_type, result, details,
        )

        # TODO: flush to Supabase compliance_log table periodically
        #   if len(self._log_buffer) >= 50:
        #       await supabase.table("compliance_log").insert(self._log_buffer)
        #       self._log_buffer.clear()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _scrub_ssn(text: str) -> tuple[str, bool]:
        """Replace SSN patterns with [REDACTED-SSN]. Returns (scrubbed, had_match)."""
        scrubbed = _SSN_PATTERN.sub("[REDACTED-SSN]", text)
        had_match = scrubbed != text
        return scrubbed, had_match

    @staticmethod
    def _scrub_credit_card(text: str) -> tuple[str, bool]:
        """Replace credit card patterns with [REDACTED-CC]. Returns (scrubbed, had_match)."""
        scrubbed = _CREDIT_CARD_PATTERN.sub("[REDACTED-CC]", text)
        had_match = scrubbed != text
        return scrubbed, had_match
