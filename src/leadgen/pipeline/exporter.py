"""
Export stage: produces CSVs matching the NWM recruiting workflow.

Supports per-tier exports and full-list exports with audit logging.
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from leadgen.models.lead import Lead, ExportRecord

logger = logging.getLogger(__name__)

# Columns matching the NWM recruiting workflow
_EXPORT_COLUMNS = [
    "Name",
    "Phone",
    "Email",
    "Current Role",
    "LinkedIn URL",
    "Recruiting Score",
    "Top Signal",
    "Source Platform",
    "Last Active Date",
    "Tier",
    "Location",
]

_TIER_ORDER = ["A", "B", "C", "D"]


class Exporter:
    """Exports scored leads to CSV files for the NWM recruiting workflow."""

    async def export_csv(
        self,
        leads: list[Lead],
        filepath: str,
        tier_filter: Optional[str] = None,
    ) -> str:
        """
        Export leads to a single CSV file.

        Args:
            leads: List of Lead models to export.
            filepath: Output CSV path.
            tier_filter: If set (e.g. "A"), export only that tier.

        Returns:
            The filepath written.
        """
        if tier_filter:
            tier_filter = tier_filter.upper()
            leads = [ld for ld in leads if ld.tier == tier_filter]

        # Sort by score descending within each tier
        leads = sorted(leads, key=lambda ld: ld.total_score, reverse=True)

        # Ensure output directory exists
        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)

        with open(filepath, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_EXPORT_COLUMNS)
            writer.writeheader()

            for lead in leads:
                writer.writerow(self._lead_to_row(lead))

        logger.info(
            "Exported %d leads to %s (tier_filter=%s)",
            len(leads), filepath, tier_filter or "all",
        )

        # Log the export
        await self.log_export(
            filename=os.path.basename(filepath),
            leads_count=len(leads),
            tier_filter=tier_filter or "all",
            filters={"tier": tier_filter} if tier_filter else {},
        )

        return filepath

    async def export_all_tiers(
        self,
        leads: list[Lead],
        output_dir: str,
    ) -> dict[str, str]:
        """
        Create separate CSVs per tier (a_tier.csv, b_tier.csv, etc.).

        Args:
            leads: Full list of leads.
            output_dir: Directory to write tier files into.

        Returns:
            Dict mapping tier letter to the filepath written.
        """
        os.makedirs(output_dir, exist_ok=True)
        results: dict[str, str] = {}

        for tier in _TIER_ORDER:
            tier_leads = [ld for ld in leads if ld.tier == tier]
            if not tier_leads:
                logger.debug("No leads for tier %s, skipping file.", tier)
                continue

            filename = f"{tier.lower()}_tier.csv"
            filepath = os.path.join(output_dir, filename)
            await self.export_csv(tier_leads, filepath)
            results[tier] = filepath

        logger.info(
            "Exported %d tier files to %s: %s",
            len(results), output_dir, list(results.keys()),
        )
        return results

    async def log_export(
        self,
        filename: str,
        leads_count: int,
        tier_filter: str,
        filters: dict,
    ) -> None:
        """
        Record an export event for audit trail.

        Creates an ExportRecord that can be persisted to the exports table.
        """
        record = ExportRecord(
            filename=filename,
            format="csv",
            leads_count=leads_count,
            tier_filter=tier_filter if tier_filter in _TIER_ORDER else None,
            filters=filters,
            exported_at=datetime.now(timezone.utc),
        )
        logger.info(
            "Export logged: %s (%d leads, tier=%s)",
            record.filename, record.leads_count, record.tier_filter or "all",
        )
        # TODO: persist record to Supabase exports table
        #   await supabase.table("exports").insert(record.model_dump())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _lead_to_row(lead: Lead) -> dict[str, str]:
        """Convert a Lead instance to a flat CSV row dict."""
        full_name = " ".join(
            part for part in [lead.first_name, lead.last_name] if part
        )

        # Build location string
        location_parts = []
        if lead.location_city:
            location_parts.append(lead.location_city)
        if lead.location_state:
            location_parts.append(lead.location_state)
        if lead.location_zip:
            location_parts.append(lead.location_zip)
        location = ", ".join(location_parts)

        # Top signal: highest-value recruiting signal
        top_signal = ""
        if lead.recruiting_signals:
            top_signal = lead.recruiting_signals[0]
        elif lead.motivation_keywords:
            top_signal = lead.motivation_keywords[0]

        # Last active date
        last_active = ""
        if lead.last_seen:
            last_active = lead.last_seen.strftime("%Y-%m-%d")

        return {
            "Name": full_name,
            "Phone": lead.phone or "",
            "Email": lead.email or "",
            "Current Role": lead.current_role or "",
            "LinkedIn URL": lead.linkedin_url or "",
            "Recruiting Score": str(lead.total_score),
            "Top Signal": top_signal,
            "Source Platform": lead.source_platform,
            "Last Active Date": last_active,
            "Tier": lead.tier or "",
            "Location": location,
        }
