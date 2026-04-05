"""
LinkedIn/Apollo Agent -- Sales Professionals

Searches Apollo for sales managers, account executives, and business
development representatives in Utah.  Sales professionals already have
the relationship-building skills that translate directly to financial
advising at NWM.

Credit budget: max 7 credits per run.
"""

from __future__ import annotations

import logging
from typing import Any

from leadgen.agents.base import BaseAgent
from leadgen.agents.linkedin._apollo_base import ApolloLinkedInBase

logger = logging.getLogger(__name__)


class LinkedInSalesProsAgent(ApolloLinkedInBase):
    """Find sales professionals in Utah via Apollo People Search.

    Targets people in sales roles who have the interpersonal and
    closing skills that NWM values in financial advisor recruits.
    """

    def __init__(self, config: dict[str, Any], db: Any) -> None:
        super().__init__(
            name="linkedin_sales_professionals",
            config=config,
            db=db,
        )

    def get_apollo_params(self) -> dict[str, Any]:
        """Apollo filters targeting sales professionals in Utah."""
        return {
            "person_titles": [
                "Sales Manager",
                "Account Executive",
                "Business Development Representative",
                "BDR",
                "SDR",
                "Sales Director",
                "Regional Sales Manager",
                "Account Manager",
                "Sales Representative",
                "VP of Sales",
                "Sales Consultant",
                "Outside Sales",
                "Inside Sales Manager",
            ],
            "person_locations": ["Utah, United States"],
            "person_seniorities": ["senior", "manager", "director"],
        }
