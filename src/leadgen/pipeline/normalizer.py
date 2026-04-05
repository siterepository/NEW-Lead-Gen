"""
Normalization stage: converts raw scrapes into typed Lead models.

Maps platform-specific fields, cleans data (names, phones, emails,
locations, URLs), and computes dedup fingerprints.
"""

from __future__ import annotations

import re
import logging
from typing import Optional
from urllib.parse import urlparse, urlencode, parse_qs

from leadgen.models.lead import Lead

logger = logging.getLogger(__name__)

# Utah ZIP code range boundaries
_UTAH_ZIP_MIN = 84000
_UTAH_ZIP_MAX = 84799

# Tracking query params to strip from URLs
_TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid", "ref", "ref_src", "ref_url",
    "s_kwcid", "msclkid", "twclid", "igshid", "si",
}


class Normalizer:
    """Converts raw scrape dicts from any platform into typed Lead models."""

    # Maps platform name -> normalizer method
    _PLATFORM_MAP = {
        "ksl": "_normalize_ksl",
        "craigslist": "_normalize_craigslist",
        "indeed": "_normalize_indeed",
        "linkedin": "_normalize_linkedin",
        "reddit": "_normalize_reddit",
        "facebook": "_normalize_facebook",
    }

    async def normalize(self, raw_scrape: dict) -> Optional[Lead]:
        """
        Take raw scrape data from any platform and produce a typed Lead.

        Returns None if the scrape has insufficient data to form a lead
        (e.g., no name and no email and no phone).
        """
        platform = raw_scrape.get("platform", "").lower().strip()

        # Pick the right platform normalizer
        method_name = self._PLATFORM_MAP.get(platform, "_normalize_generic")
        normalizer_fn = getattr(self, method_name)

        try:
            mapped: dict = normalizer_fn(raw_scrape.get("raw_data", raw_scrape))
        except Exception as exc:
            logger.warning("Normalization failed for platform=%s: %s", platform, exc)
            return None

        # Ensure source_platform is set
        mapped.setdefault("source_platform", platform or "unknown")

        # Clean individual fields
        mapped = self._clean_fields(mapped)

        # Must have at least a name, email, or phone to be useful
        has_identity = any([
            mapped.get("first_name"),
            mapped.get("last_name"),
            mapped.get("email"),
            mapped.get("phone"),
        ])
        if not has_identity:
            logger.debug("Skipping scrape with no identity fields: %s", raw_scrape.get("url", ""))
            return None

        try:
            lead = Lead(**mapped)
            lead.compute_fingerprint()
            return lead
        except Exception as exc:
            logger.warning("Failed to build Lead model: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Platform-specific normalizers
    # ------------------------------------------------------------------

    def _normalize_ksl(self, data: dict) -> dict:
        """Map KSL Classifieds / KSL Jobs fields to Lead model.

        Accepts field names produced by all three KSL agents
        (job_seekers, services_offered, business_for_sale) as well as
        legacy / alternative key names for robustness.
        """
        full_name = data.get("name") or data.get("seller_name") or ""
        first, last = self._split_name(full_name)
        return {
            "first_name": first,
            "last_name": last,
            "email": data.get("contact_email") or data.get("email"),
            "phone": data.get("contact_phone") or data.get("phone"),
            "current_role": data.get("title") or data.get("job_title"),
            "current_company": data.get("company"),
            "location_city": (
                data.get("location_city")
                or data.get("city")
                or data.get("location")
            ),
            "location_state": (
                data.get("location_state")
                or data.get("state")
                or "UT"
            ),
            "location_zip": data.get("zip") or data.get("zip_code"),
            "source_url": (
                data.get("source_url")
                or data.get("url")
                or data.get("listing_url")
            ),
            "source_post_text": data.get("description") or data.get("post_text"),
            "linkedin_url": data.get("linkedin_url"),
        }

    def _normalize_craigslist(self, data: dict) -> dict:
        """Map Craigslist fields to Lead model."""
        full_name = data.get("name") or data.get("poster_name") or ""
        first, last = self._split_name(full_name)
        return {
            "first_name": first,
            "last_name": last,
            "email": data.get("reply_email") or data.get("email"),
            "phone": data.get("phone"),
            "current_role": data.get("title") or data.get("job_title"),
            "location_city": data.get("city") or data.get("neighborhood"),
            "location_state": data.get("state", "UT"),
            "location_zip": data.get("zip"),
            "source_url": data.get("url") or data.get("post_url"),
            "source_post_text": data.get("body") or data.get("description"),
        }

    def _normalize_indeed(self, data: dict) -> dict:
        """Map Indeed fields to Lead model."""
        full_name = data.get("candidate_name") or data.get("name") or ""
        first, last = self._split_name(full_name)
        return {
            "first_name": first,
            "last_name": last,
            "email": data.get("email"),
            "phone": data.get("phone"),
            "current_role": data.get("job_title") or data.get("title") or data.get("headline"),
            "current_company": data.get("company") or data.get("current_employer"),
            "location_city": data.get("city") or data.get("location"),
            "location_state": data.get("state"),
            "location_zip": data.get("zip"),
            "source_url": data.get("url") or data.get("resume_url"),
            "source_post_text": data.get("summary") or data.get("description"),
            "education": data.get("education"),
            "career_history": data.get("work_history") or data.get("experience"),
        }

    def _normalize_linkedin(self, data: dict) -> dict:
        """Map LinkedIn fields to Lead model."""
        full_name = data.get("full_name") or data.get("name") or ""
        first = data.get("first_name") or ""
        last = data.get("last_name") or ""
        if not first and not last and full_name:
            first, last = self._split_name(full_name)
        return {
            "first_name": first,
            "last_name": last,
            "email": data.get("email"),
            "phone": data.get("phone"),
            "current_role": data.get("headline") or data.get("title"),
            "current_company": data.get("company") or data.get("current_company"),
            "location_city": data.get("city") or data.get("location"),
            "location_state": data.get("state"),
            "location_zip": data.get("zip"),
            "linkedin_url": data.get("profile_url") or data.get("linkedin_url") or data.get("url"),
            "source_url": data.get("profile_url") or data.get("url"),
            "source_post_text": data.get("about") or data.get("summary"),
            "education": data.get("education"),
            "career_history": data.get("experience") or data.get("positions"),
        }

    def _normalize_reddit(self, data: dict) -> dict:
        """Map Reddit fields to Lead model."""
        username = data.get("author") or data.get("username") or ""
        full_name = data.get("name") or ""
        first, last = self._split_name(full_name) if full_name else ("", "")
        return {
            "first_name": first or username,
            "last_name": last,
            "email": data.get("email"),
            "phone": data.get("phone"),
            "current_role": data.get("mentioned_role") or data.get("flair"),
            "location_city": data.get("city") or data.get("mentioned_location"),
            "location_state": data.get("state"),
            "location_zip": data.get("zip"),
            "source_url": data.get("permalink") or data.get("url"),
            "source_post_text": data.get("body") or data.get("selftext") or data.get("text"),
            "recruiting_signals": data.get("signals") or data.get("keywords"),
            "motivation_keywords": data.get("motivation_keywords"),
        }

    def _normalize_facebook(self, data: dict) -> dict:
        """Map Facebook Marketplace / Groups fields to Lead model."""
        full_name = data.get("seller_name") or data.get("name") or data.get("author") or ""
        first, last = self._split_name(full_name)
        return {
            "first_name": first,
            "last_name": last,
            "email": data.get("email"),
            "phone": data.get("phone") or data.get("contact_phone"),
            "current_role": data.get("job_title") or data.get("work"),
            "location_city": data.get("city") or data.get("location"),
            "location_state": data.get("state"),
            "location_zip": data.get("zip"),
            "source_url": data.get("post_url") or data.get("url"),
            "source_post_text": data.get("post_text") or data.get("message") or data.get("description"),
            "linkedin_url": data.get("linkedin_url"),
        }

    def _normalize_generic(self, data: dict) -> dict:
        """Fallback normalizer for unknown platforms."""
        full_name = data.get("name") or data.get("full_name") or ""
        first = data.get("first_name") or ""
        last = data.get("last_name") or ""
        if not first and not last and full_name:
            first, last = self._split_name(full_name)
        return {
            "first_name": first,
            "last_name": last,
            "email": data.get("email"),
            "phone": data.get("phone"),
            "current_role": data.get("title") or data.get("role") or data.get("job_title"),
            "current_company": data.get("company"),
            "location_city": data.get("city") or data.get("location"),
            "location_state": data.get("state"),
            "location_zip": data.get("zip") or data.get("zip_code"),
            "source_url": data.get("url"),
            "source_post_text": data.get("text") or data.get("description") or data.get("body"),
            "linkedin_url": data.get("linkedin_url"),
        }

    # ------------------------------------------------------------------
    # Cleaning helpers
    # ------------------------------------------------------------------

    def _clean_fields(self, mapped: dict) -> dict:
        """Apply per-field cleaning to the mapped dict."""
        if mapped.get("first_name"):
            mapped["first_name"] = self.clean_name(mapped["first_name"])
        if mapped.get("last_name"):
            mapped["last_name"] = self.clean_name(mapped["last_name"])
        if mapped.get("email"):
            mapped["email"] = self.clean_email(mapped["email"])
        if mapped.get("phone"):
            mapped["phone"] = self.clean_phone(mapped["phone"])
        if mapped.get("location_city"):
            mapped["location_city"] = self._clean_city(mapped["location_city"])
        if mapped.get("location_zip"):
            if not self.is_utah_zip(mapped["location_zip"]):
                # Keep the zip but don't discard the lead
                pass
        if mapped.get("linkedin_url"):
            mapped["linkedin_url"] = self._clean_url(mapped["linkedin_url"])
        if mapped.get("source_url"):
            mapped["source_url"] = self._clean_url(mapped["source_url"])

        # Strip None values so Pydantic defaults apply
        return {k: v for k, v in mapped.items() if v is not None and v != ""}

    def clean_phone(self, phone: str) -> Optional[str]:
        """
        Normalize phone to +1XXXXXXXXXX format.

        Handles formats like:
          (801) 555-1234, 801.555.1234, 801-555-1234,
          1-801-555-1234, +18015551234, 8015551234
        """
        if not phone:
            return None
        # Strip everything except digits
        digits = re.sub(r"[^\d]", "", phone)

        # Handle country code
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        elif len(digits) == 10:
            pass
        else:
            # Not a standard US number
            return None

        return f"+1{digits}"

    def clean_email(self, email: str) -> Optional[str]:
        """Validate and clean email: lowercase, strip whitespace."""
        if not email:
            return None
        email = email.strip().lower()
        # Basic format check
        if not re.match(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", email):
            return None
        return email

    def clean_name(self, name: str) -> str:
        """Title case, strip whitespace, remove non-alpha characters (keep spaces/hyphens)."""
        if not name:
            return ""
        # Remove characters that aren't letters, spaces, hyphens, or apostrophes
        name = re.sub(r"[^a-zA-Z\s\-']", "", name)
        name = " ".join(name.split())  # collapse multiple spaces
        return name.strip().title()

    def is_utah_zip(self, zip_code: str) -> bool:
        """Validate Utah ZIP codes (840xx through 847xx range)."""
        if not zip_code:
            return False
        # Take just the 5-digit prefix
        digits = re.sub(r"[^\d]", "", zip_code)[:5]
        if len(digits) != 5:
            return False
        try:
            code = int(digits)
            return _UTAH_ZIP_MIN <= code <= _UTAH_ZIP_MAX
        except ValueError:
            return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _split_name(full_name: str) -> tuple[str, str]:
        """Split 'First Last' into (first, last). Handles multi-part last names."""
        parts = full_name.strip().split()
        if len(parts) == 0:
            return ("", "")
        if len(parts) == 1:
            return (parts[0], "")
        return (parts[0], " ".join(parts[1:]))

    @staticmethod
    def _clean_city(city: str) -> str:
        """Normalize city name: title case, strip, collapse whitespace."""
        if not city:
            return ""
        city = " ".join(city.strip().split())
        # Normalize common Utah city abbreviations
        _CITY_ALIASES = {
            "slc": "Salt Lake City",
            "salt lake": "Salt Lake City",
            "west jordan": "West Jordan",
            "west valley": "West Valley City",
            "wvc": "West Valley City",
            "south jordan": "South Jordan",
            "sandy ut": "Sandy",
            "provo ut": "Provo",
            "ogden ut": "Ogden",
            "orem ut": "Orem",
            "st george": "St. George",
            "saint george": "St. George",
        }
        lookup = city.lower().rstrip(",").strip()
        if lookup in _CITY_ALIASES:
            return _CITY_ALIASES[lookup]
        return city.title()

    @staticmethod
    def _clean_url(url: str) -> str:
        """Ensure https scheme and strip tracking parameters."""
        if not url:
            return ""
        url = url.strip()

        # Ensure scheme
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        elif url.startswith("http://"):
            url = "https://" + url[7:]

        # Strip tracking params
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query, keep_blank_values=False)
            clean_params = {
                k: v for k, v in params.items()
                if k.lower() not in _TRACKING_PARAMS
            }
            clean_query = urlencode(clean_params, doseq=True) if clean_params else ""
            url = parsed._replace(query=clean_query).geturl()
        except Exception:
            pass

        return url
