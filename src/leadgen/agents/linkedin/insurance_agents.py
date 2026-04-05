"""
LinkedIn/Apollo Agent -- Insurance Agents

Searches Apollo for insurance agents in Utah.  Insurance agents
already hold financial licenses and understand needs-based selling,
making them excellent NWM financial advisor recruits who require
minimal additional licensing.

Credit budget: max 7 credits per run.
"""

from __future__ import annotations

import logging
from typing import Any

from leadgen.agents.base import BaseAgent
from leadgen.agents.linkedin._apollo_base import ApolloLinkedInBase

logger = logging.getLogger(__name__)


class LinkedInInsuranceAgent(ApolloLinkedInBase):
    """Find insurance agents in Utah via Apollo People Search.

    Targets licensed insurance professionals who can transition
    to NWM financial advising with minimal re-licensing.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="linkedin_insurance_agents",
            config=config,
            db=db,
        )

    def get_apollo_params(self) -> dict[str, Any]:
        """Apollo filters targeting insurance agents in Utah."""
        return {
            "person_titles": [
                "Insurance Agent",
                "Insurance Broker",
                "Licensed Insurance Agent",
                "Life Insurance Agent",
                "Health Insurance Agent",
                "Insurance Producer",
                "Insurance Advisor",
                "Insurance Sales Agent",
                "Insurance Consultant",
                "Property and Casualty Agent",
                "P&C Agent",
                "Insurance Account Manager",
            ],
            "person_locations": ["Utah, United States"],
        }
