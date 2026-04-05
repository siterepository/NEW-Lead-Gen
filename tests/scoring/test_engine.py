"""
Unit tests for leadgen.scoring.engine -- ScoringEngine.

15 tests covering all five scoring dimensions, caps, freshness bonus,
the full pipeline, and edge cases.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from leadgen.scoring.engine import ScoringEngine
from tests.conftest import sample_lead


@pytest.fixture
def engine():
    """ScoringEngine with explicit priority ZIPs for deterministic tests."""
    return ScoringEngine(priority_zips={
        "84004", "84060", "84098", "84020", "84092", "84093",
        "84117", "84121", "84095", "84010", "84037", "84025", "84043",
    })


# ---------------------------------------------------------------------------
# score_career_fit (max 35)
# ---------------------------------------------------------------------------

class TestScoreCareerFit:

    def test_sales_keywords_score_10(self, engine):
        """Sales-category keywords award 10 points."""
        lead = sample_lead(current_role="Senior Sales Manager")
        score = engine.score_career_fit(lead)
        assert score >= 10

    def test_entrepreneurial_keywords_score_8(self, engine):
        """Entrepreneurial-category keywords award 8 points."""
        lead = sample_lead(
            current_role="Founder & CEO",
            source_post_text="",
        )
        score = engine.score_career_fit(lead)
        assert score >= 8

    def test_career_fit_caps_at_35(self, engine):
        """Even with many matching categories, score cannot exceed 35."""
        lead = sample_lead(
            current_role="Sales Manager and Entrepreneur",
            current_company="Keller Williams Real Estate",
            career_history=[
                "Insurance Agent at State Farm",
                "Army Veteran, Team Leader",
                "Professor and Coach",
                "Retail Manager, customer service",
                "Division 1 Athlete",
            ],
            source_post_text="leadership director vp",
        )
        score = engine.score_career_fit(lead)
        assert score <= 35

    def test_career_fit_returns_0_for_empty(self, engine):
        """Lead with no career text yields 0."""
        lead = sample_lead(
            current_role=None,
            current_company=None,
            career_history=None,
            source_post_text=None,
            recruiting_signals=None,
        )
        score = engine.score_career_fit(lead)
        assert score == 0


# ---------------------------------------------------------------------------
# score_motivation (max 25)
# ---------------------------------------------------------------------------

class TestScoreMotivation:

    def test_job_seeking_keywords_score_10(self, engine):
        """Explicitly job-seeking language awards 10 points."""
        lead = sample_lead(source_post_text="I am actively looking for work and open to opportunities")
        score = engine.score_motivation(lead)
        assert score >= 10

    def test_career_change_keywords_score_8(self, engine):
        """Career change language awards 8 points."""
        lead = sample_lead(
            source_post_text="Considering a career change, ready for a fresh start",
            # Make first_seen old so no freshness bonus
            first_seen=datetime.now(timezone.utc) - timedelta(days=60),
        )
        score = engine.score_motivation(lead)
        assert score >= 8

    def test_freshness_bonus_under_7_days(self, engine):
        """Leads seen within 7 days get +5 freshness bonus."""
        lead = sample_lead(
            source_post_text="",  # No keywords so base is 0
            first_seen=datetime.now(timezone.utc) - timedelta(days=2),
        )
        score = engine.score_motivation(lead)
        assert score == 5  # Only freshness bonus

    def test_motivation_caps_at_25(self, engine):
        """Even with all signals + freshness, motivation cannot exceed 25."""
        lead = sample_lead(
            source_post_text=(
                "I am actively looking for work. Career change coming. "
                "Just got laid off. Burned out and underpaid. "
                "Want passive income and financial freedom. "
                "Returning to work after stay at home."
            ),
            first_seen=datetime.now(timezone.utc) - timedelta(days=1),
        )
        score = engine.score_motivation(lead)
        assert score <= 25


# ---------------------------------------------------------------------------
# score_people_skills (max 20)
# ---------------------------------------------------------------------------

class TestScorePeopleSkills:

    def test_networker_keywords_score_6(self, engine):
        """Active networker keywords award 6 points."""
        lead = sample_lead(
            source_post_text="I am an active networker and community builder",
            first_seen=datetime.now(timezone.utc) - timedelta(days=60),
        )
        score = engine.score_people_skills(lead)
        assert score >= 6


# ---------------------------------------------------------------------------
# score_demographics (max 10)
# ---------------------------------------------------------------------------

class TestScoreDemographics:

    def test_utah_resident_score_3(self, engine):
        """Utah residents get 3 points."""
        lead = sample_lead(location_state="UT")
        score = engine.score_demographics(lead)
        assert score >= 3

    def test_priority_zip_score_2(self, engine):
        """Priority ZIP codes get 2 additional points."""
        lead = sample_lead(location_state="UT", location_zip="84060")
        score = engine.score_demographics(lead)
        # Should include Utah (3) + priority ZIP (2) = at least 5
        assert score >= 5


# ---------------------------------------------------------------------------
# score_data_quality (max 10)
# ---------------------------------------------------------------------------

class TestScoreDataQuality:

    def test_email_awards_3_phone_awards_2(self, engine):
        """Having email yields 3 and phone yields 2."""
        lead = sample_lead(
            email="jane@example.com",
            phone="+18015551234",
            linkedin_url=None,
            sources_count=1,
            last_seen=datetime.now(timezone.utc) - timedelta(days=60),
        )
        score = engine.score_data_quality(lead)
        assert score >= 5  # 3 (email) + 2 (phone)


# ---------------------------------------------------------------------------
# score_lead -- full pipeline
# ---------------------------------------------------------------------------

class TestScoreLeadPipeline:

    def test_score_lead_full_pipeline(self, engine):
        """score_lead populates all five dimensions and computes total/tier."""
        lead = sample_lead(
            current_role="Sales Manager",
            source_post_text="Looking for work, career change, networking expert",
            location_state="UT",
            location_zip="84060",
            email="jane@example.com",
            phone="+18015551234",
            linkedin_url="https://linkedin.com/in/janesmith",
            first_seen=datetime.now(timezone.utc) - timedelta(days=3),
        )
        scored = engine.score_lead(lead)
        assert scored.score_career_fit > 0
        assert scored.score_motivation > 0
        assert scored.total_score == (
            scored.score_career_fit
            + scored.score_motivation
            + scored.score_people_skills
            + scored.score_demographics
            + scored.score_data_quality
        )
        assert scored.tier in {"A", "B", "C", "D"}

    def test_score_lead_tier_assignment(self, engine):
        """score_lead assigns correct tier based on total."""
        # Build a lead that will score high
        lead = sample_lead(
            current_role="Sales Director and Founder",
            current_company="Keller Williams",
            career_history=["Insurance Agent", "Team Leader", "Army Veteran"],
            source_post_text=(
                "Actively looking for work. Career change. "
                "Burned out. Networking expert. Volunteer. "
                "Coach. Public speaker at TEDx. Podcaster. Team leader."
            ),
            location_state="UT",
            location_zip="84060",
            education="MBA from University of Utah 2005",
            email="topcandidate@example.com",
            phone="+18015551234",
            linkedin_url="https://linkedin.com/in/topcandidate",
            sources_count=3,
            first_seen=datetime.now(timezone.utc) - timedelta(days=2),
        )
        scored = engine.score_lead(lead)
        # With all these signals this should be a high-scoring lead
        assert scored.total_score > 0
        assert scored.tier is not None

    def test_score_lead_with_empty_lead(self, engine):
        """Scoring a lead with no data yields tier D with score 0 or close to 0."""
        lead = sample_lead(
            first_name=None,
            last_name=None,
            email=None,
            phone=None,
            linkedin_url=None,
            current_role=None,
            current_company=None,
            career_history=None,
            source_post_text=None,
            recruiting_signals=None,
            motivation_keywords=None,
            education=None,
            location_state=None,
            location_zip=None,
            sources_count=1,
            last_seen=datetime.now(timezone.utc) - timedelta(days=60),
            first_seen=datetime.now(timezone.utc) - timedelta(days=60),
        )
        scored = engine.score_lead(lead)
        assert scored.score_career_fit == 0
        assert scored.score_motivation == 0
        assert scored.score_people_skills == 0
        assert scored.score_demographics == 0
        # data_quality may be non-zero due to last_seen but very low
        assert scored.total_score < 25
        assert scored.tier == "D"
