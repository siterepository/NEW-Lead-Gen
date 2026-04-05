"""LinkedIn/Apollo scraping agents.

All 8 agents use the Apollo.io People Search API (free tier, 60
credits/month shared) to find NWM financial advisor recruiting
prospects in Utah.  They inherit from BaseAgent but override
scrape() to call the API via httpx instead of Playwright.
"""

from leadgen.agents.linkedin.career_changers import LinkedInCareerChangersAgent
from leadgen.agents.linkedin.sales_professionals import LinkedInSalesProsAgent
from leadgen.agents.linkedin.real_estate_agents import LinkedInRealEstateAgent
from leadgen.agents.linkedin.insurance_agents import LinkedInInsuranceAgent
from leadgen.agents.linkedin.teachers_leaving import LinkedInTeachersAgent
from leadgen.agents.linkedin.military_veterans import LinkedInVeteransAgent
from leadgen.agents.linkedin.recent_mba_grads import LinkedInMBAGradsAgent
from leadgen.agents.linkedin.entrepreneurs import LinkedInEntrepreneursAgent

__all__ = [
    "LinkedInCareerChangersAgent",
    "LinkedInSalesProsAgent",
    "LinkedInRealEstateAgent",
    "LinkedInInsuranceAgent",
    "LinkedInTeachersAgent",
    "LinkedInVeteransAgent",
    "LinkedInMBAGradsAgent",
    "LinkedInEntrepreneursAgent",
]
