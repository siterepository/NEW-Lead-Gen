"""
LinkedIn/Apollo Agent -- Career Changers

Searches Apollo for people in Utah who recently changed jobs or have
titles suggesting they are in career transition.  These individuals are
prime NWM financial advisor recruiting targets because they are already
open to new professional paths.

Credit budget: max 5 credits per run.
"""

from __future__ import annotations

import logging
from typing import Any

from leadgen.agents.base import BaseAgent
from leadgen.agents.linkedin._apollo_base import ApolloLinkedInBase

logger = logging.getLogger(__name__)


class LinkedInCareerChangersAgent(ApolloLinkedInBase):
    """Find career changers in Utah via Apollo People Search.

    Targets people whose titles or keywords suggest recent job changes,
    career pivots, or "open to work" status.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="linkedin_career_changers",
            config=config,
            db=db,
        )

    def get_apollo_params(self) -> dict[str, Any]:
        """Apollo filters targeting career changers in Utah."""
        return {
            "person_titles": [
                "Career Transition",
                "Open to Opportunities",
                "Seeking New Opportunities",
                "In Transition",
                "Career Change",
                "Freelancer",
                "Consultant",
                "Independent Contractor",
                "Self-Employed",
                "Looking for Opportunities",
            ],
            "person_locations": ["Utah, United States"],
            "person_seniorities": ["senior", "manager", "director", "vp"],
        }
