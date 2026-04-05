"""
Hunter.io free-tier integration for email finding and verification.

Free tier limits:
  - 25 email searches per month
  - 50 email verifications per month
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

import httpx

from leadgen.models.lead import ApiCreditUsage

logger = logging.getLogger(__name__)

_HUNTER_BASE_URL = "https://api.hunter.io/v2"

# Free-tier monthly limits
_SEARCHES_MONTHLY = 25
_VERIFICATIONS_MONTHLY = 50


class HunterEnricher:
    """Find and verify professional emails using the Hunter.io API (free tier)."""

    def __init__(self):
        self.api_key: str = os.environ.get("HUNTER_API_KEY", "")
        if not self.api_key:
            logger.warning("HUNTER_API_KEY not set -- Hunter enrichment will be unavailable.")

        # Credit tracking
        self._searches_used: int = 0
        self._verifications_used: int = 0

        # Usage log buffer
        self._usage_log: list[ApiCreditUsage] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def find_email(
        self,
        first_name: str,
        last_name: str,
        domain: str,
    ) -> Optional[str]:
        """
        Use the Hunter Email Finder API to discover a professional email.

        Args:
            first_name: Person's first name.
            last_name: Person's last name.
            domain: Company domain (e.g. "acme.com").

        Returns:
            The found email address, or None if not found / credits exhausted.
        """
        if not self.api_key:
            logger.warning("Hunter API key not configured.")
            return None

        credits = await self.check_credits()
        if credits["searches_remaining"] <= 0:
            logger.warning("Hunter search credits exhausted for this month.")
            return None

        params = {
            "api_key": self.api_key,
            "first_name": first_name.strip(),
            "last_name": last_name.strip(),
            "domain": domain.strip(),
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    f"{_HUNTER_BASE_URL}/email-finder",
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

            result = data.get("data", {})
            email = result.get("email")
            confidence = result.get("confidence", 0)

            # Track credit
            self._searches_used += 1
            self._log_credit_usage(
                credits_used=1,
                operation="email_finder",
                detail=f"{first_name} {last_name} @ {domain}",
            )

            if email and confidence >= 50:
                logger.info(
                    "Hunter found email for %s %s @ %s: %s (confidence=%d)",
                    first_name, last_name, domain, email, confidence,
                )
                return email.lower().strip()
            else:
                logger.debug(
                    "Hunter no confident result for %s %s @ %s (confidence=%d)",
                    first_name, last_name, domain, confidence,
                )
                return None

        except httpx.HTTPStatusError as exc:
            logger.error(
                "Hunter API HTTP error: %s %s",
                exc.response.status_code, exc.response.text[:200],
            )
            return None
        except httpx.RequestError as exc:
            logger.error("Hunter API request error: %s", exc)
            return None
        except Exception as exc:
            logger.error("Hunter email finder unexpected error: %s", exc)
            return None

    async def verify_email(self, email: str) -> dict:
        """
        Verify email deliverability using the Hunter Email Verifier API.

        Args:
            email: Email address to verify.

        Returns:
            {
                "valid": bool,
                "score": int (0-100),
                "status": str ("valid", "invalid", "accept_all", "unknown"),
                "disposable": bool,
                "webmail": bool,
            }
        """
        default_result = {
            "valid": False,
            "score": 0,
            "status": "unknown",
            "disposable": False,
            "webmail": False,
        }

        if not self.api_key:
            logger.warning("Hunter API key not configured.")
            return default_result

        credits = await self.check_credits()
        if credits["verifications_remaining"] <= 0:
            logger.warning("Hunter verification credits exhausted for this month.")
            return default_result

        params = {
            "api_key": self.api_key,
            "email": email.strip().lower(),
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    f"{_HUNTER_BASE_URL}/email-verifier",
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

            result = data.get("data", {})

            # Track credit
            self._verifications_used += 1
            self._log_credit_usage(
                credits_used=1,
                operation="email_verifier",
                detail=email,
            )

            status = result.get("status", "unknown")
            score = result.get("score", 0)
            is_valid = status in ("valid", "accept_all") and score >= 50

            verification_result = {
                "valid": is_valid,
                "score": score,
                "status": status,
                "disposable": result.get("disposable", False),
                "webmail": result.get("webmail", False),
            }

            logger.info(
                "Hunter verified %s: valid=%s score=%d status=%s",
                email, is_valid, score, status,
            )

            return verification_result

        except httpx.HTTPStatusError as exc:
            logger.error(
                "Hunter verifier HTTP error: %s %s",
                exc.response.status_code, exc.response.text[:200],
            )
            return default_result
        except httpx.RequestError as exc:
            logger.error("Hunter verifier request error: %s", exc)
            return default_result
        except Exception as exc:
            logger.error("Hunter verifier unexpected error: %s", exc)
            return default_result

    async def check_credits(self) -> dict:
        """
        Return current credit usage and remaining counts.

        Returns:
            {
                "service": "hunter",
                "searches_used": int,
                "searches_remaining": int,
                "verifications_used": int,
                "verifications_remaining": int,
            }
        """
        return {
            "service": "hunter",
            "searches_used": self._searches_used,
            "searches_remaining": max(0, _SEARCHES_MONTHLY - self._searches_used),
            "verifications_used": self._verifications_used,
            "verifications_remaining": max(0, _VERIFICATIONS_MONTHLY - self._verifications_used),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _log_credit_usage(self, credits_used: int, operation: str, detail: str = "") -> None:
        """Record API credit consumption."""
        # Calculate remaining (searches + verifications combined)
        remaining = max(0, _SEARCHES_MONTHLY - self._searches_used) + max(
            0, _VERIFICATIONS_MONTHLY - self._verifications_used
        )

        usage = ApiCreditUsage(
            service="hunter",
            credits_used=credits_used,
            credits_remaining=remaining,
            operation=operation,
        )
        self._usage_log.append(usage)

        logger.debug(
            "Hunter credit used: op=%s detail=%s searches_left=%d verifications_left=%d",
            operation, detail,
            _SEARCHES_MONTHLY - self._searches_used,
            _VERIFICATIONS_MONTHLY - self._verifications_used,
        )
        # TODO: persist to Supabase api_credit_usage table
