"""
Unit tests for leadgen.models.lead -- Lead, RawScrape, AgentRun models.

10 tests covering model creation, validation, fingerprinting, scoring tiers.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from leadgen.models.lead import Lead, RawScrape, AgentRun
from tests.conftest import sample_lead


# ---------------------------------------------------------------------------
# 1. Lead creation with required fields only
# ---------------------------------------------------------------------------

class TestLeadCreation:

    def test_lead_creation_required_fields_only(self):
        """Lead can be created with just source_platform (the only required field)."""
        lead = Lead(source_platform="linkedin")
        assert lead.source_platform == "linkedin"
        assert lead.id is not None  # UUID auto-generated
        assert lead.first_name is None
        assert lead.total_score == 0
        assert lead.tier == "D"  # zero score -> tier D

    def test_lead_creation_with_all_fields(self):
        """Lead can be created with every field populated."""
        lead = sample_lead(
            career_history=["Sales Rep at Acme", "BDR at TechCo"],
            life_events={"job_change": True},
            recruiting_signals=["open to work"],
            sentiment_score=0.5,
            motivation_keywords=["career change"],
            score_career_fit=30,
            score_motivation=20,
            score_people_skills=15,
            score_demographics=8,
            score_data_quality=7,
            enriched=True,
        )
        assert lead.first_name == "Jane"
        assert lead.last_name == "Smith"
        assert lead.email == "jane.smith@example.com"
        assert lead.career_history == ["Sales Rep at Acme", "BDR at TechCo"]
        assert lead.life_events == {"job_change": True}
        assert lead.sentiment_score == 0.5
        assert lead.enriched is True
        # total_score auto-computed by model_validator
        assert lead.total_score == 80
        assert lead.tier == "A"


# ---------------------------------------------------------------------------
# 2. Fingerprint
# ---------------------------------------------------------------------------

class TestFingerprint:

    def test_compute_fingerprint_deterministic(self):
        """Same inputs produce the same fingerprint every time."""
        lead = sample_lead()
        fp1 = lead.compute_fingerprint()
        fp2 = lead.compute_fingerprint()
        assert fp1 == fp2
        assert len(fp1) == 64  # SHA-256 hex digest length

    def test_compute_fingerprint_different_for_different_inputs(self):
        """Different name/email/zip produces a different fingerprint."""
        lead_a = sample_lead(first_name="Alice", email="alice@example.com")
        lead_b = sample_lead(first_name="Bob", email="bob@example.com")
        fp_a = lead_a.compute_fingerprint()
        fp_b = lead_b.compute_fingerprint()
        assert fp_a != fp_b

    def test_compute_fingerprint_handles_none_fields(self):
        """Fingerprint computation does not crash when fields are None."""
        lead = Lead(source_platform="reddit")
        assert lead.first_name is None
        assert lead.email is None
        fp = lead.compute_fingerprint()
        assert isinstance(fp, str)
        assert len(fp) == 64


# ---------------------------------------------------------------------------
# 3. Email validation
# ---------------------------------------------------------------------------

class TestEmailValidation:

    def test_validate_email_rejects_invalid(self):
        """An email without '@' raises a ValidationError."""
        with pytest.raises(ValidationError, match="email must contain"):
            Lead(source_platform="linkedin", email="not-an-email")

    def test_validate_email_lowercases(self):
        """Email is automatically lowercased and stripped."""
        lead = Lead(source_platform="linkedin", email="  Jane.Smith@Example.COM  ")
        assert lead.email == "jane.smith@example.com"


# ---------------------------------------------------------------------------
# 4. Auto-compute total and tier
# ---------------------------------------------------------------------------

class TestAutoComputeTotalAndTier:

    def test_tier_a_threshold(self):
        """Score >= 75 yields tier A."""
        lead = sample_lead(
            score_career_fit=35,
            score_motivation=20,
            score_people_skills=15,
            score_demographics=5,
            score_data_quality=5,
        )
        assert lead.total_score == 80
        assert lead.tier == "A"

    def test_tier_b_threshold(self):
        """Score >= 50 and < 75 yields tier B."""
        lead = sample_lead(
            score_career_fit=20,
            score_motivation=15,
            score_people_skills=10,
            score_demographics=5,
            score_data_quality=5,
        )
        assert lead.total_score == 55
        assert lead.tier == "B"

    def test_tier_c_threshold(self):
        """Score >= 25 and < 50 yields tier C."""
        lead = sample_lead(
            score_career_fit=10,
            score_motivation=10,
            score_people_skills=5,
            score_demographics=3,
            score_data_quality=2,
        )
        assert lead.total_score == 30
        assert lead.tier == "C"

    def test_tier_d_threshold(self):
        """Score < 25 yields tier D."""
        lead = sample_lead(
            score_career_fit=5,
            score_motivation=5,
            score_people_skills=5,
            score_demographics=2,
            score_data_quality=2,
        )
        assert lead.total_score == 19
        assert lead.tier == "D"


# ---------------------------------------------------------------------------
# 5. Score field bounds enforcement
# ---------------------------------------------------------------------------

class TestScoreBounds:

    def test_score_career_fit_rejects_over_max(self):
        """score_career_fit > 35 raises ValidationError."""
        with pytest.raises(ValidationError):
            sample_lead(score_career_fit=36)

    def test_score_motivation_rejects_over_max(self):
        """score_motivation > 25 raises ValidationError."""
        with pytest.raises(ValidationError):
            sample_lead(score_motivation=26)

    def test_score_rejects_negative(self):
        """Negative score values raise ValidationError."""
        with pytest.raises(ValidationError):
            sample_lead(score_career_fit=-1)


# ---------------------------------------------------------------------------
# 6. RawScrape and AgentRun model creation
# ---------------------------------------------------------------------------

class TestAuxiliaryModels:

    def test_raw_scrape_creation(self):
        """RawScrape can be created with required fields."""
        scrape = RawScrape(
            agent_name="ksl_job_seekers",
            platform="ksl",
            url="https://ksl.com/listing/123",
            raw_data={"title": "Sales Manager", "name": "John Doe"},
        )
        assert scrape.agent_name == "ksl_job_seekers"
        assert scrape.platform == "ksl"
        assert scrape.processed is False
        assert scrape.id is not None

    def test_agent_run_creation(self):
        """AgentRun can be created and defaults are applied correctly."""
        run = AgentRun(agent_name="ksl_job_seekers")
        assert run.agent_name == "ksl_job_seekers"
        assert run.status == "running"
        assert run.items_found == 0
        assert run.items_new == 0
        assert run.items_duplicate == 0
        assert run.error_message is None
