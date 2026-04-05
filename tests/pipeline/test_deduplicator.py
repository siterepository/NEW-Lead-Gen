"""
Unit tests for leadgen.pipeline.deduplicator -- Deduplicator.

5 tests covering exact fingerprint match, fuzzy name match,
no-match, merge data combination, and sources_count increment.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from leadgen.pipeline.deduplicator import Deduplicator
from tests.conftest import sample_lead


@pytest.fixture
def dedup():
    return Deduplicator()


# ---------------------------------------------------------------------------
# 1. is_duplicate -- exact fingerprint match
# ---------------------------------------------------------------------------

class TestIsDuplicateExact:

    @pytest.mark.asyncio
    async def test_exact_fingerprint_match(self, dedup):
        """Two leads with identical identity fields match on fingerprint."""
        lead_a = sample_lead()
        lead_a.compute_fingerprint()

        # Create an identical lead (same name, email, zip)
        lead_b = sample_lead()
        lead_b.compute_fingerprint()

        result = await dedup.is_duplicate(lead_b, [lead_a])
        assert result is not None
        assert result.fingerprint == lead_a.fingerprint


# ---------------------------------------------------------------------------
# 2. is_duplicate -- fuzzy name match
# ---------------------------------------------------------------------------

class TestIsDuplicateFuzzy:

    @pytest.mark.asyncio
    async def test_fuzzy_name_match(self, dedup):
        """Leads with very similar names and same location match fuzzy."""
        lead_existing = sample_lead(
            first_name="Jane",
            last_name="Smith",
            location_city="Salt Lake City",
            location_state="UT",
            location_zip="84101",
            email="different1@example.com",  # Different email so fingerprint won't match
        )
        lead_existing.compute_fingerprint()

        lead_new = sample_lead(
            first_name="Jane",
            last_name="Smth",  # Slight typo
            location_city="Salt Lake City",
            location_state="UT",
            location_zip="84101",
            email="different2@example.com",
        )
        lead_new.compute_fingerprint()

        result = await dedup.is_duplicate(lead_new, [lead_existing])
        # This should match on fuzzy name + location since name similarity
        # is very high (Jane Smth vs Jane Smith) and location matches exactly
        assert result is not None


# ---------------------------------------------------------------------------
# 3. is_duplicate -- no match returns None
# ---------------------------------------------------------------------------

class TestIsDuplicateNoMatch:

    @pytest.mark.asyncio
    async def test_no_match_returns_none(self, dedup):
        """Completely different leads return None."""
        lead_a = sample_lead(
            first_name="Alice",
            last_name="Johnson",
            email="alice@example.com",
            location_city="Los Angeles",
            location_state="CA",
            location_zip="90210",
        )
        lead_a.compute_fingerprint()

        lead_b = sample_lead(
            first_name="Bob",
            last_name="Williams",
            email="bob@example.com",
            location_city="New York",
            location_state="NY",
            location_zip="10001",
        )
        lead_b.compute_fingerprint()

        result = await dedup.is_duplicate(lead_b, [lead_a])
        assert result is None

    @pytest.mark.asyncio
    async def test_no_match_empty_list(self, dedup):
        """Empty existing_leads list returns None."""
        lead = sample_lead()
        result = await dedup.is_duplicate(lead, [])
        assert result is None


# ---------------------------------------------------------------------------
# 4. merge_leads combines data
# ---------------------------------------------------------------------------

class TestMergeLeads:

    @pytest.mark.asyncio
    async def test_merge_combines_data(self, dedup):
        """Merge fills gaps in existing lead from new lead."""
        existing = sample_lead(
            first_name="Jane",
            last_name="Smith",
            email="jane@example.com",
            phone=None,  # Gap
            linkedin_url=None,  # Gap
            education=None,  # Gap
        )
        new = sample_lead(
            first_name="Jane",
            last_name="Smith",
            email="jane@example.com",
            phone="+18015559999",
            linkedin_url="https://linkedin.com/in/janesmith",
            education="MBA from BYU",
            source_post_text="New additional text from another source",
        )

        merged = await dedup.merge_leads(existing, new)
        assert merged.phone == "+18015559999"  # Filled from new
        assert merged.linkedin_url == "https://linkedin.com/in/janesmith"
        assert merged.education == "MBA from BYU"
        assert merged.first_name == "Jane"  # Kept existing


# ---------------------------------------------------------------------------
# 5. merge_leads increments sources_count
# ---------------------------------------------------------------------------

class TestMergeSourcesCount:

    @pytest.mark.asyncio
    async def test_merge_increments_sources_count(self, dedup):
        """Each merge increments sources_count by 1."""
        existing = sample_lead(sources_count=1)
        new = sample_lead()

        merged = await dedup.merge_leads(existing, new)
        assert merged.sources_count == 2

        # Merge again
        another = sample_lead(source_post_text="Yet another source")
        merged2 = await dedup.merge_leads(merged, another)
        assert merged2.sources_count == 3
