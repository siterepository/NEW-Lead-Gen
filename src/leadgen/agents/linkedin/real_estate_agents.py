"""
LinkedIn/Apollo Agent -- Real Estate Agents

Searches Apollo for real estate agents and brokers in Utah.  Real
estate professionals are prime NWM financial advisor recruits because
they already hold client-facing licenses, understand commission-based
compensation, and have built local networks.

Credit budget: max 7 credits per run.
"""

from __future__ import annotations

import logging
from typing import Any

from leadgen.agents.base import BaseAgent
from leadgen.agents.linkedin._apollo_base import ApolloLinkedInBase

logger = logging.getLogger(__name__)


class LinkedInRealEstateAgent(ApolloLinkedInBase):
    """Find real estate agents/brokers in Utah via Apollo People Search.

    Targets licensed real estate professionals who already work on
    commission and have transferable client-management skills.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="linkedin_real_estate_agents",
            config=config,
            db=db,
        )

    def get_apollo_params(self) -> dict[str, Any]:
        """Apollo filters targeting real estate professionals in Utah."""
        return {
            "person_titles": [
                "Real Estate Agent",
                "Realtor",
                "Real Estate Broker",
                "Licensed Real Estate Agent",
                "Real Estate Associate",
                "Buyer's Agent",
                "Listing Agent",
                "Real Estate Consultant",
                "Real Estate Advisor",
                "Managing Broker",
            ],
            "person_locations": ["Utah, United States"],
            "q_organization_domains": [],  # No company filter -- cast wide net
        }
