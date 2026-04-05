"""
Unit tests for leadgen.pipeline.normalizer -- Normalizer.

10 tests covering phone/email/name cleaning, zip validation,
name splitting, platform dispatch, and URL/city cleaning.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from leadgen.pipeline.normalizer import Normalizer
from tests.conftest import sample_raw_scrape


@pytest.fixture
def norm():
    return Normalizer()


# ---------------------------------------------------------------------------
# 1. clean_phone various formats
# ---------------------------------------------------------------------------

class TestCleanPhone:

    def test_clean_phone_formats(self, norm):
        """clean_phone normalizes various US phone formats to +1XXXXXXXXXX."""
        assert norm.clean_phone("(801) 555-1234") == "+18015551234"
        assert norm.clean_phone("801.555.1234") == "+18015551234"
        assert norm.clean_phone("801-555-1234") == "+18015551234"
        assert norm.clean_phone("1-801-555-1234") == "+18015551234"
        assert norm.clean_phone("+18015551234") == "+18015551234"
        assert norm.clean_phone("8015551234") == "+18015551234"

    def test_clean_phone_returns_none_for_invalid(self, norm):
        """Non-standard numbers return None."""
        assert norm.clean_phone("") is None
        assert norm.clean_phone("123") is None
        assert norm.clean_phone("555-1234") is None  # Only 7 digits -> 10 digit check fails


# ---------------------------------------------------------------------------
# 2. clean_email validation
# ---------------------------------------------------------------------------

class TestCleanEmail:

    def test_clean_email_valid(self, norm):
        """Valid emails are lowercased and stripped."""
        assert norm.clean_email("  Jane.Smith@Example.COM  ") == "jane.smith@example.com"

    def test_clean_email_returns_none_for_invalid(self, norm):
        """Invalid emails return None."""
        assert norm.clean_email("not-an-email") is None
        assert norm.clean_email("") is None
        assert norm.clean_email("missing@tld") is None


# ---------------------------------------------------------------------------
# 3. clean_name title case
# ---------------------------------------------------------------------------

class TestCleanName:

    def test_clean_name_title_case(self, norm):
        """Names are title-cased and stripped of non-alpha characters."""
        assert norm.clean_name("jane smith") == "Jane Smith"
        assert norm.clean_name("  JOHN   DOE  ") == "John Doe"
        assert norm.clean_name("mary-jane watson") == "Mary-Jane Watson"
        assert norm.clean_name("o'brien") == "O'Brien"


# ---------------------------------------------------------------------------
# 4. is_utah_zip valid/invalid
# ---------------------------------------------------------------------------

class TestIsUtahZip:

    def test_utah_zip_valid(self, norm):
        """Utah ZIPs in the 840xx-847xx range return True."""
        assert norm.is_utah_zip("84101") is True
        assert norm.is_utah_zip("84060") is True
        assert norm.is_utah_zip("84799") is True
        assert norm.is_utah_zip("84000") is True

    def test_utah_zip_invalid(self, norm):
        """Non-Utah ZIPs return False."""
        assert norm.is_utah_zip("90210") is False
        assert norm.is_utah_zip("10001") is False
        assert norm.is_utah_zip("") is False
        assert norm.is_utah_zip("abc") is False
        assert norm.is_utah_zip("84800") is False  # Just outside range


# ---------------------------------------------------------------------------
# 5. _split_name various formats
# ---------------------------------------------------------------------------

class TestSplitName:

    def test_split_name_formats(self):
        """_split_name handles single, two-part, and multi-part names."""
        assert Normalizer._split_name("Jane Smith") == ("Jane", "Smith")
        assert Normalizer._split_name("Jane") == ("Jane", "")
        assert Normalizer._split_name("") == ("", "")
        assert Normalizer._split_name("Mary Jane Watson") == ("Mary", "Jane Watson")
        assert Normalizer._split_name("  John   Doe  ") == ("John", "Doe")


# ---------------------------------------------------------------------------
# 6. normalize dispatches to correct platform
# ---------------------------------------------------------------------------

class TestNormalizeDispatch:

    @pytest.mark.asyncio
    async def test_normalize_dispatches_to_ksl(self, norm):
        """Platform 'ksl' uses _normalize_ksl under the hood."""
        raw = sample_raw_scrape()
        lead = await norm.normalize(raw)
        assert lead is not None
        assert lead.source_platform == "ksl"
        assert lead.first_name is not None

    @pytest.mark.asyncio
    async def test_normalize_dispatches_unknown_to_generic(self, norm):
        """Unknown platform falls back to _normalize_generic."""
        raw = {
            "platform": "some_unknown_platform",
            "raw_data": {
                "name": "Test User",
                "email": "test@example.com",
            },
        }
        lead = await norm.normalize(raw)
        assert lead is not None
        assert lead.source_platform == "some_unknown_platform"


# ---------------------------------------------------------------------------
# 7. normalize returns None for insufficient data
# ---------------------------------------------------------------------------

class TestNormalizeInsufficientData:

    @pytest.mark.asyncio
    async def test_normalize_returns_none_for_empty_data(self, norm):
        """Scrape with no identity fields returns None."""
        raw = {
            "platform": "ksl",
            "raw_data": {
                "description": "Some listing with no contact info",
            },
        }
        result = await norm.normalize(raw)
        assert result is None


# ---------------------------------------------------------------------------
# 8. _normalize_ksl maps fields correctly
# ---------------------------------------------------------------------------

class TestNormalizeKsl:

    @pytest.mark.asyncio
    async def test_normalize_ksl_maps_fields(self, norm):
        """KSL normalizer maps raw KSL fields to Lead model fields."""
        raw = sample_raw_scrape()
        lead = await norm.normalize(raw)
        assert lead is not None
        assert lead.first_name == "John"
        assert lead.last_name == "Doe"
        assert lead.email == "john.doe@example.com"
        assert lead.phone == "+18015559876"
        assert lead.current_role == "Experienced Sales Manager"
        assert lead.location_city == "Provo"
        assert lead.location_state == "UT"
        assert lead.fingerprint is not None  # compute_fingerprint was called


# ---------------------------------------------------------------------------
# 9. _clean_url strips tracking params
# ---------------------------------------------------------------------------

class TestCleanUrl:

    def test_clean_url_strips_tracking_params(self):
        """Tracking parameters (utm_*, fbclid, etc.) are removed from URLs."""
        url = "https://example.com/page?id=123&utm_source=google&utm_campaign=test&fbclid=abc"
        cleaned = Normalizer._clean_url(url)
        assert "utm_source" not in cleaned
        assert "utm_campaign" not in cleaned
        assert "fbclid" not in cleaned
        assert "id=123" in cleaned

    def test_clean_url_ensures_https(self):
        """HTTP URLs are upgraded to HTTPS."""
        assert Normalizer._clean_url("http://example.com").startswith("https://")
        assert Normalizer._clean_url("example.com").startswith("https://")


# ---------------------------------------------------------------------------
# 10. _clean_city aliases
# ---------------------------------------------------------------------------

class TestCleanCity:

    def test_clean_city_aliases(self):
        """Common Utah city abbreviations are expanded."""
        assert Normalizer._clean_city("slc") == "Salt Lake City"
        assert Normalizer._clean_city("wvc") == "West Valley City"
        assert Normalizer._clean_city("st george") == "St. George"
        assert Normalizer._clean_city("saint george") == "St. George"

    def test_clean_city_title_case_for_unknown(self):
        """Unknown city names are title-cased."""
        assert Normalizer._clean_city("provo") == "Provo"
        assert Normalizer._clean_city("OGDEN") == "Ogden"
