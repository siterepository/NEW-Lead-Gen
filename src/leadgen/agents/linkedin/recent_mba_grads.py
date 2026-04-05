"""
LinkedIn/Apollo Agent -- Recent MBA Graduates

Searches Apollo for recent MBA graduates in the Utah area.  MBA
graduates understand business fundamentals, financial analysis,
and professional networking -- all directly applicable to NWM
financial advising.

Credit budget: max 5 credits per run.
"""

from __future__ import annotations

import logging
from typing import Any

from leadgen.agents.base import BaseAgent
from leadgen.agents.linkedin._apollo_base import ApolloLinkedInBase

logger = logging.getLogger(__name__)


class LinkedInMBAGradsAgent(ApolloLinkedInBase):
    """Find recent MBA graduates in Utah via Apollo People Search.

    Targets early-career MBA holders who may be evaluating multiple
    career paths, including financial advising.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="linkedin_recent_mba_grads",
            config=config,
            db=db,
        )

    def get_apollo_params(self) -> dict[str, Any]:
        """Apollo filters targeting recent MBA graduates in Utah.

        Uses a combination of education-related titles and
        entry/mid-level seniority to find recent graduates.
        """
        return {
            "person_titles": [
                "MBA Candidate",
                "MBA Graduate",
                "Recent MBA",
                "MBA Student",
                "Business Analyst",
                "Financial Analyst",
                "Associate",
                "Strategy Analyst",
                "Management Consultant",
                "Business Development Associate",
            ],
            "person_locations": ["Utah, United States"],
            "person_seniorities": ["entry", "senior"],
            "q_keywords": "MBA",
        }
