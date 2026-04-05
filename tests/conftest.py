"""
Shared pytest fixtures for the NEW Lead Gen test suite.

Provides reusable factories and fixtures for Lead models, raw scrape data,
temporary databases, scoring engines, and normalizers.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone

import pytest
import pytest_asyncio

from leadgen.models.lead import Lead, RawScrape
from leadgen.scoring.engine import ScoringEngine
from leadgen.pipeline.normalizer import Normalizer
from leadgen.db.queue import JobQueue


# ---------------------------------------------------------------------------
# Factory helpers (not fixtures -- call these to get fresh instances)
# ---------------------------------------------------------------------------

def sample_lead(**overrides) -> Lead:
    """
    Create a Lead with sensible defaults for testing.

    Pass keyword arguments to override any field.
    """
    defaults = {
        "first_name": "Jane",
        "last_name": "Smith",
        "email": "jane.smith@example.com",
        "phone": "+18015551234",
        "linkedin_url": "https://linkedin.com/in/janesmith",
        "location_city": "Salt Lake City",
        "location_state": "UT",
        "location_zip": "84101",
        "current_role": "Insurance Agent",
        "current_company": "State Farm",
        "source_platform": "linkedin",
        "source_url": "https://linkedin.com/in/janesmith",
        "source_post_text": "Looking for new opportunities in financial services.",
    }
    defaults.update(overrides)
    return Lead(**defaults)


def sample_raw_scrape(**overrides) -> dict:
    """
    Create a raw KSL scrape data dict with sensible defaults for testing.

    Pass keyword arguments to override any field.
    """
    defaults = {
        "platform": "ksl",
        "url": "https://www.ksl.com/classifieds/listing/12345",
        "raw_data": {
            "name": "John Doe",
            "contact_email": "john.doe@example.com",
            "contact_phone": "(801) 555-9876",
            "title": "Experienced Sales Manager",
            "company": "Acme Corp",
            "location_city": "Provo",
            "location_state": "UT",
            "zip": "84601",
            "description": "Experienced sales professional looking for new opportunities.",
            "source_url": "https://www.ksl.com/classifieds/listing/12345",
        },
    }
    defaults.update(overrides)
    return defaults


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def tmp_db(tmp_path):
    """
    Create a temporary SQLite database with all tables initialized.

    Yields a JobQueue instance connected to the temp DB, then cleans up.
    """
    db_path = str(tmp_path / "test_leadgen.db")
    queue = JobQueue(db_path)
    await queue.init_db()
    yield queue
    await queue.close()


@pytest.fixture
def scoring_engine():
    """Return a ScoringEngine with default priority ZIPs."""
    return ScoringEngine(priority_zips={
        "84004", "84060", "84098", "84020", "84092", "84093",
        "84117", "84121", "84095", "84010", "84037", "84025",
        "84043",
    })


@pytest.fixture
def normalizer():
    """Return a Normalizer instance."""
    return Normalizer()
