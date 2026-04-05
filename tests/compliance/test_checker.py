"""
Unit tests for leadgen.compliance.checker -- ComplianceChecker.

5 tests covering clean lead pass, DNC blocking, minor detection,
SSN scrubbing, and add_to_dnc.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from leadgen.compliance.checker import ComplianceChecker
from tests.conftest import sample_lead


@pytest_asyncio.fixture
async def checker():
    return ComplianceChecker()


# ---------------------------------------------------------------------------
# 1. check_lead passes clean lead
# ---------------------------------------------------------------------------

class TestCheckLeadPasses:

    @pytest.mark.asyncio
    async def test_check_lead_passes_clean_lead(self, checker):
        """A normal lead with no issues passes all compliance checks."""
        lead = sample_lead()
        passed, issues = await checker.check_lead(lead)
        assert passed is True
        assert issues == []
        assert lead.compliance_cleared is True
        assert lead.compliance_date is not None


# ---------------------------------------------------------------------------
# 2. check_dnc blocks DNC email
# ---------------------------------------------------------------------------

class TestCheckDnc:

    @pytest.mark.asyncio
    async def test_dnc_blocks_email(self, checker):
        """A lead whose email is on the DNC list fails compliance."""
        await checker.add_to_dnc(email="blocked@example.com", reason="opt-out")
        lead = sample_lead(email="blocked@example.com")

        passed, issues = await checker.check_lead(lead)
        assert passed is False
        assert any("Do Not Contact" in issue for issue in issues)
        assert lead.dnc_listed is True

    @pytest.mark.asyncio
    async def test_dnc_blocks_phone(self, checker):
        """A lead whose phone is on the DNC list fails compliance."""
        await checker.add_to_dnc(phone="+18005551234", reason="complaint")
        lead = sample_lead(phone="+18005551234")

        passed, issues = await checker.check_lead(lead)
        assert passed is False
        assert any("Do Not Contact" in issue for issue in issues)


# ---------------------------------------------------------------------------
# 3. check_minor blocks minors
# ---------------------------------------------------------------------------

class TestCheckMinor:

    @pytest.mark.asyncio
    async def test_minor_blocked_by_age_in_life_events(self, checker):
        """A lead with age < 18 in life_events is rejected."""
        lead = sample_lead(life_events={"age": 16})
        passed, issues = await checker.check_lead(lead)
        assert passed is False
        assert any("minor" in issue.lower() for issue in issues)

    @pytest.mark.asyncio
    async def test_minor_blocked_by_age_in_source_text(self, checker):
        """A lead mentioning age < 18 in source post text is rejected."""
        lead = sample_lead(source_post_text="I am 15 years old looking for work")
        passed, issues = await checker.check_lead(lead)
        assert passed is False
        assert any("minor" in issue.lower() for issue in issues)

    @pytest.mark.asyncio
    async def test_adult_passes_minor_check(self, checker):
        """A lead with age >= 18 passes the minor check."""
        lead = sample_lead(life_events={"age": 30})
        passed, issues = await checker.check_lead(lead)
        assert passed is True


# ---------------------------------------------------------------------------
# 4. check_data_minimization scrubs SSN
# ---------------------------------------------------------------------------

class TestDataMinimization:

    @pytest.mark.asyncio
    async def test_ssn_scrubbed_from_source_text(self, checker):
        """SSN patterns in source_post_text are replaced with [REDACTED-SSN]."""
        lead = sample_lead(
            source_post_text="My SSN is 123-45-6789, please contact me"
        )
        passed, issues = await checker.check_lead(lead)
        assert passed is False
        assert "[REDACTED-SSN]" in lead.source_post_text
        assert "123-45-6789" not in lead.source_post_text
        assert any("Sensitive data" in issue or "scrubbed" in issue.lower() for issue in issues)

    @pytest.mark.asyncio
    async def test_clean_text_passes_minimization(self, checker):
        """Source text without sensitive patterns passes data minimization."""
        lead = sample_lead(
            source_post_text="Looking for opportunities in financial services."
        )
        passed, issues = await checker.check_lead(lead)
        assert passed is True


# ---------------------------------------------------------------------------
# 5. add_to_dnc adds correctly
# ---------------------------------------------------------------------------

class TestAddToDnc:

    @pytest.mark.asyncio
    async def test_add_to_dnc_email(self, checker):
        """add_to_dnc registers an email so subsequent checks block it."""
        await checker.add_to_dnc(email="nope@example.com")
        assert "nope@example.com" in checker._dnc_emails

    @pytest.mark.asyncio
    async def test_add_to_dnc_phone(self, checker):
        """add_to_dnc registers a phone so subsequent checks block it."""
        await checker.add_to_dnc(phone="+18001234567")
        assert "+18001234567" in checker._dnc_phones

    @pytest.mark.asyncio
    async def test_add_to_dnc_name(self, checker):
        """add_to_dnc registers a name so subsequent checks block it."""
        await checker.add_to_dnc(name="John Smith")
        assert "john smith" in checker._dnc_names

        # Verify that the DNC name actually blocks a lead
        lead = sample_lead(first_name="John", last_name="Smith")
        passed, issues = await checker.check_lead(lead)
        assert passed is False
