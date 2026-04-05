"""KSL classifieds scraping agents."""

from leadgen.agents.ksl.job_seekers import KSLJobSeekersAgent
from leadgen.agents.ksl.services_offered import KSLServicesOfferedAgent
from leadgen.agents.ksl.business_for_sale import KSLBusinessForSaleAgent

__all__ = [
    "KSLJobSeekersAgent",
    "KSLServicesOfferedAgent",
    "KSLBusinessForSaleAgent",
]
