"""
SupabaseClient - Async wrapper around the Supabase Python client.

Provides typed CRUD methods for leads, raw scrapes, agent runs,
DNC list, API credit tracking, and compliance logging.

Falls back to local-only mode (no-op) when SUPABASE_URL / SUPABASE_KEY
environment variables are not set.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

from leadgen.models.lead import Lead

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy import & connection flag
# ---------------------------------------------------------------------------

_supabase_available = True
try:
    from supabase import create_client, Client
except ImportError:
    _supabase_available = False
    logger.warning(
        "supabase-py package not installed. "
        "Run `pip install supabase` to enable Supabase integration."
    )


# ---------------------------------------------------------------------------
# SupabaseClient
# ---------------------------------------------------------------------------

class SupabaseClient:
    """Async-style Supabase client for the NWM Lead Generation system.

    Reads ``SUPABASE_URL`` and ``SUPABASE_KEY`` from the environment (or
    a ``.env`` file via *python-dotenv* if available).  When either variable
    is missing the client enters **local-only mode**: every method returns a
    safe default and logs a debug message instead of raising.

    Usage::

        client = SupabaseClient()
        if client.is_connected:
            await client.upsert_lead(lead)
    """

    def __init__(self) -> None:
        self._client: Optional[Client] = None
        self.is_connected: bool = False

        # Try loading .env via python-dotenv (optional dependency)
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass

        url = os.environ.get("SUPABASE_URL", "").strip()
        key = os.environ.get("SUPABASE_KEY", "").strip()

        if not _supabase_available:
            logger.warning(
                "SupabaseClient running in local-only mode: "
                "supabase package not installed."
            )
            return

        if not url or not key:
            logger.warning(
                "SupabaseClient running in local-only mode: "
                "SUPABASE_URL and/or SUPABASE_KEY environment variables not set."
            )
            return

        try:
            self._client = create_client(url, key)
            self.is_connected = True
            logger.info("SupabaseClient connected to %s", url)
        except Exception as exc:
            logger.error("Failed to initialise Supabase client: %s", exc)
            self.is_connected = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_connection(self) -> Client:
        """Return the underlying client or raise if not connected."""
        if not self.is_connected or self._client is None:
            raise RuntimeError(
                "Supabase is not connected. "
                "Set SUPABASE_URL and SUPABASE_KEY environment variables."
            )
        return self._client

    @staticmethod
    def _utcnow_iso() -> str:
        """ISO-8601 timestamp string in UTC."""
        return datetime.now(timezone.utc).isoformat()

    # ==================================================================
    # Leads CRUD
    # ==================================================================

    async def upsert_lead(self, lead: Lead) -> None:
        """Upsert a lead by its fingerprint.

        If a row with the same ``fingerprint`` already exists it is updated;
        otherwise a new row is inserted.
        """
        if not self.is_connected:
            logger.debug("upsert_lead skipped (local-only mode)")
            return

        client = self._require_connection()

        # Ensure fingerprint is computed
        if not lead.fingerprint:
            lead.compute_fingerprint()

        data = lead.model_dump(mode="json", exclude_none=True)
        # Convert datetime fields to ISO strings for Supabase
        for key, value in list(data.items()):
            if isinstance(value, datetime):
                data[key] = value.isoformat()

        data["updated_at"] = self._utcnow_iso()

        client.table("leads").upsert(
            data,
            on_conflict="fingerprint",
        ).execute()

        logger.debug("Upserted lead fingerprint=%s", lead.fingerprint)

    async def get_lead_by_fingerprint(self, fingerprint: str) -> Optional[Lead]:
        """Fetch a single lead by its deduplication fingerprint."""
        if not self.is_connected:
            logger.debug("get_lead_by_fingerprint skipped (local-only mode)")
            return None

        client = self._require_connection()

        response = (
            client.table("leads")
            .select("*")
            .eq("fingerprint", fingerprint)
            .limit(1)
            .execute()
        )

        if response.data:
            return Lead(**response.data[0])
        return None

    async def get_leads(
        self,
        tier: Optional[str] = None,
        limit: int = 100,
    ) -> list[Lead]:
        """Fetch leads, optionally filtered by tier.

        Results are ordered by ``total_score`` descending.
        """
        if not self.is_connected:
            logger.debug("get_leads skipped (local-only mode)")
            return []

        client = self._require_connection()

        query = (
            client.table("leads")
            .select("*")
            .order("total_score", desc=True)
            .limit(limit)
        )

        if tier:
            query = query.eq("tier", tier.upper())

        response = query.execute()
        return [Lead(**row) for row in response.data]

    # ==================================================================
    # Raw scrapes
    # ==================================================================

    async def store_raw_scrape(self, scrape: dict) -> None:
        """Insert a raw scrape payload into the ``raw_scrapes`` table.

        Expected keys: ``agent_name``, ``platform``, ``url``, ``raw_data``.
        """
        if not self.is_connected:
            logger.debug("store_raw_scrape skipped (local-only mode)")
            return

        client = self._require_connection()

        row = {
            "agent_name": scrape.get("agent_name", "unknown"),
            "platform": scrape.get("platform", "unknown"),
            "url": scrape.get("url", ""),
            "raw_data": scrape.get("raw_data", {}),
            "scraped_at": scrape.get("scraped_at", self._utcnow_iso()),
            "processed": scrape.get("processed", False),
        }

        client.table("raw_scrapes").insert(row).execute()
        logger.debug("Stored raw scrape from agent=%s url=%s", row["agent_name"], row["url"])

    # ==================================================================
    # Agent runs
    # ==================================================================

    async def log_agent_run(self, run: dict) -> None:
        """Insert an agent run record into the ``agent_runs`` table.

        Expected keys: ``agent_name``, ``status``, and optionally
        ``items_found``, ``items_new``, ``items_duplicate``,
        ``error_message``, ``started_at``, ``finished_at``.
        """
        if not self.is_connected:
            logger.debug("log_agent_run skipped (local-only mode)")
            return

        client = self._require_connection()

        row = {
            "agent_name": run.get("agent_name", "unknown"),
            "status": run.get("status", "running"),
            "items_found": run.get("items_found", 0),
            "items_new": run.get("items_new", 0),
            "items_duplicate": run.get("items_duplicate", 0),
            "error_message": run.get("error_message"),
            "started_at": run.get("started_at", self._utcnow_iso()),
            "finished_at": run.get("finished_at"),
        }

        client.table("agent_runs").insert(row).execute()
        logger.debug("Logged agent run: agent=%s status=%s", row["agent_name"], row["status"])

    # ==================================================================
    # DNC list
    # ==================================================================

    async def check_dnc(
        self,
        email: Optional[str] = None,
        phone: Optional[str] = None,
    ) -> bool:
        """Check whether an email or phone appears on the Do Not Contact list.

        Returns ``True`` if at least one match is found.
        """
        if not self.is_connected:
            logger.debug("check_dnc skipped (local-only mode)")
            return False

        client = self._require_connection()

        if email:
            response = (
                client.table("dnc_list")
                .select("id")
                .eq("email", email.strip().lower())
                .limit(1)
                .execute()
            )
            if response.data:
                return True

        if phone:
            response = (
                client.table("dnc_list")
                .select("id")
                .eq("phone", phone.strip())
                .limit(1)
                .execute()
            )
            if response.data:
                return True

        return False

    async def add_to_dnc(
        self,
        email: Optional[str] = None,
        phone: Optional[str] = None,
        reason: str = "opt-out",
    ) -> None:
        """Add an email and/or phone to the Do Not Contact list."""
        if not self.is_connected:
            logger.debug("add_to_dnc skipped (local-only mode)")
            return

        if not email and not phone:
            logger.warning("add_to_dnc called with no email or phone; skipping")
            return

        client = self._require_connection()

        row: dict = {
            "reason": reason,
            "added_at": self._utcnow_iso(),
        }
        if email:
            row["email"] = email.strip().lower()
        if phone:
            row["phone"] = phone.strip()

        client.table("dnc_list").insert(row).execute()
        logger.info("Added to DNC list: email=%s phone=%s reason=%s", email, phone, reason)

    # ==================================================================
    # API credits
    # ==================================================================

    async def log_credit_usage(
        self,
        service: str,
        credits_used: int,
        operation: str,
    ) -> None:
        """Log a single API credit usage event to the ``api_credits`` table."""
        if not self.is_connected:
            logger.debug("log_credit_usage skipped (local-only mode)")
            return

        client = self._require_connection()

        row = {
            "service": service,
            "credits_used": credits_used,
            "credits_remaining": 0,  # caller should update if known
            "operation": operation,
            "used_at": self._utcnow_iso(),
        }

        client.table("api_credits").insert(row).execute()
        logger.debug("Logged credit usage: service=%s credits=%d op=%s", service, credits_used, operation)

    async def get_credit_usage(self, service: str) -> dict:
        """Get aggregated credit usage for a service in the current calendar month.

        Returns a dict with ``total_credits_used`` and ``call_count``.
        """
        if not self.is_connected:
            logger.debug("get_credit_usage skipped (local-only mode)")
            return {"total_credits_used": 0, "call_count": 0}

        client = self._require_connection()

        # First day of current month in UTC
        now = datetime.now(timezone.utc)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()

        response = (
            client.table("api_credits")
            .select("credits_used")
            .eq("service", service)
            .gte("used_at", month_start)
            .execute()
        )

        rows = response.data or []
        total = sum(row.get("credits_used", 0) for row in rows)

        return {
            "total_credits_used": total,
            "call_count": len(rows),
        }

    # ==================================================================
    # Compliance
    # ==================================================================

    async def log_compliance_check(self, entry: dict) -> None:
        """Log a compliance check result to the ``compliance_log`` table.

        Expected keys: ``lead_id``, ``check_type``, ``result``
        (``'pass'`` or ``'fail'``), and optionally ``details``.
        """
        if not self.is_connected:
            logger.debug("log_compliance_check skipped (local-only mode)")
            return

        client = self._require_connection()

        row = {
            "lead_id": entry.get("lead_id"),
            "check_type": entry.get("check_type", "unknown"),
            "result": entry.get("result", "fail"),
            "details": entry.get("details"),
            "checked_at": entry.get("checked_at", self._utcnow_iso()),
        }

        client.table("compliance_log").insert(row).execute()
        logger.debug(
            "Logged compliance check: lead=%s type=%s result=%s",
            row["lead_id"],
            row["check_type"],
            row["result"],
        )
