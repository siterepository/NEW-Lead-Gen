"""
LinkedIn/Apollo Agent -- Teachers Leaving Education

Searches Apollo for teachers and educators in Utah who appear to be
transitioning out of education.  Former educators bring strong
communication, mentoring, and trust-building skills that are highly
valued in NWM financial advising.

Credit budget: max 5 credits per run.
"""

from __future__ import annotations

import logging
from typing import Any

from leadgen.agents.base import BaseAgent
from leadgen.agents.linkedin._apollo_base import ApolloLinkedInBase

logger = logging.getLogger(__name__)


class LinkedInTeachersAgent(ApolloLinkedInBase):
    """Find teachers/educators in Utah via Apollo People Search.

    Targets educators who may be exploring career changes,
    particularly those with leadership or coaching experience
    that translates well to financial advising.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="linkedin_teachers_leaving",
            config=config,
            db=db,
        )

    def get_apollo_params(self) -> dict[str, Any]:
        """Apollo filters targeting teachers and educators in Utah.

        We search broadly for educators, then the scoring pipeline
        can filter for those who show signs of career transition
        (short tenure, recent title changes, etc.).
        """
        return {
            "person_titles": [
                "Teacher",
                "High School Teacher",
                "Elementary Teacher",
                "Educator",
                "Adjunct Professor",
                "Instructor",
                "School Counselor",
                "Academic Advisor",
                "Department Head",
                "Athletic Director",
                "Coach",
                "Former Teacher",
                "Education Consultant",
            ],
            "person_locations": ["Utah, United States"],
            "person_seniorities": ["senior", "manager", "director"],
        }
