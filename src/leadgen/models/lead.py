"""
Pydantic v2 models for the NWM Recruiting Lead Generation system.

Defines the core data models used across scraping, enrichment, scoring,
compliance, and export pipelines.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, computed_field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utcnow() -> datetime:
    """Return current UTC time (timezone-aware for TIMESTAMPTZ compat)."""
    return datetime.now(timezone.utc)


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _normalize(value: str | None) -> str:
    """Lowercase, strip whitespace and non-alphanumeric chars."""
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]", "", value.strip().lower())


# ---------------------------------------------------------------------------
# Lead  (main entity)
# ---------------------------------------------------------------------------

class Lead(BaseModel):
    """
    Primary lead record representing a potential NWM financial advisor recruit.

    The scoring system allocates 100 total points across five dimensions:
      - career_fit   (35)  Role relevance, entrepreneurial history
      - motivation    (25)  Life events, expressed dissatisfaction, ambition signals
      - people_skills (20)  Sales/coaching/leadership indicators
      - demographics  (10)  Location, age-range proxy, market potential
      - data_quality  (10)  Completeness and freshness of data
    """

    # --- Identity --------------------------------------------------------
    id: Optional[str] = Field(default_factory=_new_uuid, description="UUID primary key")
    fingerprint: Optional[str] = Field(
        default=None,
        description="SHA-256 deduplication hash of normalized(first+last+zip+email). Auto-computed before DB insert.",
    )
    first_name: Optional[str] = Field(default=None, max_length=120)
    last_name: Optional[str] = Field(default=None, max_length=120)
    email: Optional[str] = Field(default=None, max_length=254)
    phone: Optional[str] = Field(default=None, max_length=30)
    linkedin_url: Optional[str] = Field(default=None, max_length=500)

    # --- Location --------------------------------------------------------
    location_city: Optional[str] = Field(default=None, max_length=120)
    location_state: Optional[str] = Field(default=None, max_length=60)
    location_zip: Optional[str] = Field(default=None, max_length=20)

    # --- Professional background -----------------------------------------
    current_role: Optional[str] = Field(default=None, max_length=200)
    current_company: Optional[str] = Field(default=None, max_length=200)
    career_history: Optional[list[str]] = Field(
        default=None,
        description="List of prior roles/companies (most recent first)",
    )
    education: Optional[str] = Field(default=None, max_length=300)

    # --- Recruiting intelligence -----------------------------------------
    life_events: Optional[dict] = Field(
        default=None,
        description="JSONB-style dict of life events: job_change, business_sale, relocation, etc.",
    )
    recruiting_signals: Optional[list[str]] = Field(
        default=None,
        description="Keywords/phrases that indicate recruiting potential",
    )
    sentiment_score: Optional[float] = Field(
        default=None,
        ge=-1.0,
        le=1.0,
        description="NLP sentiment about current career (-1 negative, +1 positive)",
    )
    motivation_keywords: Optional[list[str]] = Field(
        default=None,
        description="Extracted motivation-related keywords (e.g. 'looking for change', 'entrepreneurial')",
    )

    # --- NWM Connection Intelligence ----------------------------------------
    has_nwm_mutual_connection: bool = Field(
        default=False,
        description="True if lead shares a mutual connection with someone at Northwestern Mutual",
    )
    nwm_mutual_names: Optional[list[str]] = Field(
        default=None,
        description="Names of mutual connections at NWM (if known)",
    )
    nwm_connection_source: Optional[str] = Field(
        default=None, max_length=200,
        description="How the NWM connection was detected (linkedin, referral, etc.)",
    )

    # --- Scoring (100-point system + 40pt NWM boost) ----------------------
    score_career_fit: int = Field(default=0, ge=0, le=35, description="Career fit score out of 35")
    score_motivation: int = Field(default=0, ge=0, le=25, description="Motivation score out of 25")
    score_people_skills: int = Field(default=0, ge=0, le=20, description="People/leadership skills score out of 20")
    score_demographics: int = Field(default=0, ge=0, le=10, description="Demographics score out of 10")
    score_data_quality: int = Field(default=0, ge=0, le=10, description="Data quality/completeness score out of 10")
    score_nwm_connection: int = Field(default=0, ge=0, le=40, description="NWM mutual connection boost (0 or 40)")
    total_score: int = Field(default=0, ge=0, le=140, description="Composite score out of 140 (100 base + 40 NWM boost)")
    tier: Optional[str] = Field(
        default=None,
        pattern=r"^[A-D]$",
        description="Lead tier: A (80-100), B (60-79), C (40-59), D (0-39)",
    )

    # --- Source tracking --------------------------------------------------
    source_platform: str = Field(
        ...,
        max_length=60,
        description="Platform the lead was sourced from (linkedin, reddit, facebook, etc.)",
    )
    source_url: Optional[str] = Field(default=None, max_length=1000)
    source_post_text: Optional[str] = Field(
        default=None,
        max_length=5000,
        description="Original post/comment text that surfaced this lead",
    )
    sources_count: int = Field(
        default=1,
        ge=1,
        description="Number of independent sources corroborating this lead",
    )

    # --- Enrichment & compliance -----------------------------------------
    enriched: bool = Field(default=False, description="Whether enrichment pipeline has run")
    enrichment_date: Optional[datetime] = None
    compliance_cleared: bool = Field(default=False, description="Whether compliance checks passed")
    compliance_date: Optional[datetime] = None
    dnc_listed: bool = Field(default=False, description="Lead is on the Do Not Contact list")

    # --- Timestamps -------------------------------------------------------
    first_seen: datetime = Field(default_factory=_utcnow)
    last_seen: datetime = Field(default_factory=_utcnow)
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    # --- Validators -------------------------------------------------------

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str | None) -> str | None:
        if v is not None:
            v = v.strip().lower()
            if v and "@" not in v:
                raise ValueError("email must contain '@'")
        return v

    @field_validator("tier")
    @classmethod
    def validate_tier(cls, v: str | None) -> str | None:
        if v is not None:
            v = v.upper()
            if v not in {"A", "B", "C", "D"}:
                raise ValueError("tier must be A, B, C, or D")
        return v

    @model_validator(mode="after")
    def auto_compute_total_and_tier(self) -> "Lead":
        """Recompute total_score and tier from sub-scores on every validation.

        Includes the +40 NWM mutual connection boost if applicable.
        """
        base_score = (
            self.score_career_fit
            + self.score_motivation
            + self.score_people_skills
            + self.score_demographics
            + self.score_data_quality
        )
        self.total_score = base_score + self.score_nwm_connection

        # Tier thresholds (75/50/25 on base 100, NWM boost can push higher)
        if self.total_score >= 75:
            self.tier = "A"
        elif self.total_score >= 50:
            self.tier = "B"
        elif self.total_score >= 25:
            self.tier = "C"
        else:
            self.tier = "D"
        return self

    # --- Methods ----------------------------------------------------------

    def compute_fingerprint(self) -> str:
        """
        Create a SHA-256 dedup fingerprint from normalized identity fields.

        Uses: first_name + last_name + location_zip + email
        Stores result on the instance and returns it.
        """
        raw = (
            _normalize(self.first_name)
            + _normalize(self.last_name)
            + _normalize(self.location_zip)
            + _normalize(self.email)
        )
        self.fingerprint = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        return self.fingerprint

    class Config:
        json_schema_extra = {
            "example": {
                "first_name": "Jane",
                "last_name": "Smith",
                "email": "jane.smith@example.com",
                "location_zip": "53202",
                "current_role": "Insurance Agent",
                "source_platform": "linkedin",
                "score_career_fit": 28,
                "score_motivation": 18,
                "score_people_skills": 14,
                "score_demographics": 7,
                "score_data_quality": 8,
            }
        }


# ---------------------------------------------------------------------------
# RawScrape
# ---------------------------------------------------------------------------

class RawScrape(BaseModel):
    """
    Raw data captured by a scraping agent before processing.

    Stored as JSONB in Supabase so agents can dump arbitrary
    platform-specific payloads.
    """

    id: Optional[str] = Field(default_factory=_new_uuid)
    agent_name: str = Field(..., max_length=60, description="Name of the scraping agent")
    platform: str = Field(..., max_length=60, description="Source platform identifier")
    url: str = Field(..., max_length=1000, description="URL that was scraped")
    raw_data: dict = Field(
        default_factory=dict,
        description="Arbitrary JSON payload from the scraper",
    )
    scraped_at: datetime = Field(default_factory=_utcnow)
    processed: bool = Field(default=False, description="Whether the pipeline has processed this scrape")


# ---------------------------------------------------------------------------
# AgentRun
# ---------------------------------------------------------------------------

class AgentRun(BaseModel):
    """
    Execution log for a single agent run.

    Used to track throughput, error rates, and dedup efficiency.
    """

    id: Optional[str] = Field(default_factory=_new_uuid)
    agent_name: str = Field(..., max_length=60)
    started_at: datetime = Field(default_factory=_utcnow)
    finished_at: Optional[datetime] = None
    status: str = Field(
        default="running",
        pattern=r"^(running|completed|failed|error)$",
        description="Current run status",
    )
    items_found: int = Field(default=0, ge=0, description="Total items discovered")
    items_new: int = Field(default=0, ge=0, description="New unique leads created")
    items_duplicate: int = Field(default=0, ge=0, description="Duplicate items skipped")
    error_message: Optional[str] = Field(default=None, max_length=2000)

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        allowed = {"running", "completed", "failed", "error"}
        v = v.lower()
        if v not in allowed:
            raise ValueError(f"status must be one of {allowed}")
        return v


# ---------------------------------------------------------------------------
# ApiCreditUsage
# ---------------------------------------------------------------------------

class ApiCreditUsage(BaseModel):
    """
    Tracks API credit consumption for paid enrichment services
    (Apollo, Hunter, etc.) to stay within budget.
    """

    id: Optional[str] = Field(default_factory=_new_uuid)
    service: str = Field(
        ...,
        max_length=60,
        description="Enrichment service name (apollo, hunter, etc.)",
    )
    credits_used: int = Field(..., ge=0)
    credits_remaining: int = Field(..., ge=0)
    operation: str = Field(
        ...,
        max_length=120,
        description="Operation performed (email_lookup, company_enrich, etc.)",
    )
    lead_id: Optional[str] = Field(default=None, description="Associated lead UUID")
    used_at: datetime = Field(default_factory=_utcnow)


# ---------------------------------------------------------------------------
# ExportRecord
# ---------------------------------------------------------------------------

class ExportRecord(BaseModel):
    """
    Metadata for each export job (CSV/Excel downloads).
    """

    id: Optional[str] = Field(default_factory=_new_uuid)
    filename: str = Field(..., max_length=255)
    format: str = Field(default="csv", pattern=r"^(csv|xlsx|json)$")
    filters: dict = Field(
        default_factory=dict,
        description="Filter criteria used for this export",
    )
    leads_count: int = Field(..., ge=0, description="Number of leads in the export")
    tier_filter: Optional[str] = Field(
        default=None,
        pattern=r"^[A-D]$",
        description="If export was filtered to a specific tier",
    )
    exported_at: datetime = Field(default_factory=_utcnow)
