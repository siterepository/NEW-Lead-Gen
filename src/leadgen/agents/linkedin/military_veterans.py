"""
LinkedIn/Apollo Agent -- Military Veterans

Searches Apollo for military veterans in Utah who are transitioning
to civilian careers.  Veterans bring discipline, leadership, and
a service mindset that makes them outstanding NWM financial advisor
recruits.

Credit budget: max 5 credits per run.
"""

from __future__ import annotations

import logging
from typing import Any

from leadgen.agents.base import BaseAgent
from leadgen.agents.linkedin._apollo_base import ApolloLinkedInBase

logger = logging.getLogger(__name__)


class LinkedInVeteransAgent(ApolloLinkedInBase):
    """Find military veterans in Utah via Apollo People Search.

    Targets transitioning military personnel and recent veterans
    who bring leadership and discipline to financial advising.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="linkedin_military_veterans",
            config=config,
            db=db,
        )

    def get_apollo_params(self) -> dict[str, Any]:
        """Apollo filters targeting military veterans in Utah.

        Apollo doesn't have a direct "veteran" filter, so we search
        for common titles that indicate military background or
        transition status.
        """
        return {
            "person_titles": [
                "Veteran",
                "Military Veteran",
                "Transitioning Military",
                "Former Military",
                "Retired Military",
                "Army Veteran",
                "Navy Veteran",
                "Air Force Veteran",
                "Marine Veteran",
                "Military Officer",
                "NCO",
                "Squad Leader",
                "Platoon Leader",
                "Company Commander",
                "Military Transition",
            ],
            "person_locations": ["Utah, United States"],
        }
