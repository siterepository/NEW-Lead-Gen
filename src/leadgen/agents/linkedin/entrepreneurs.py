"""
LinkedIn/Apollo Agent -- Entrepreneurs

Searches Apollo for founders, co-founders, and business owners in
Utah.  Entrepreneurs already possess the self-starter mentality,
risk tolerance, and client-relationship skills that make them
natural NWM financial advisor recruits.

Credit budget: max 7 credits per run.
"""

from __future__ import annotations

import logging
from typing import Any

from leadgen.agents.base import BaseAgent
from leadgen.agents.linkedin._apollo_base import ApolloLinkedInBase

logger = logging.getLogger(__name__)


class LinkedInEntrepreneursAgent(ApolloLinkedInBase):
    """Find entrepreneurs and business owners in Utah via Apollo.

    Targets founders and small-business owners who demonstrate
    the drive and independence NWM looks for in financial advisors.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="linkedin_entrepreneurs",
            config=config,
            db=db,
        )

    def get_apollo_params(self) -> dict[str, Any]:
        """Apollo filters targeting entrepreneurs in Utah."""
        return {
            "person_titles": [
                "Founder",
                "Co-Founder",
                "Owner",
                "Business Owner",
                "CEO",
                "Managing Partner",
                "Principal",
                "Entrepreneur",
                "President",
                "Sole Proprietor",
                "Small Business Owner",
                "Startup Founder",
            ],
            "person_locations": ["Utah, United States"],
            "person_seniorities": ["founder", "owner", "c_suite"],
        }
