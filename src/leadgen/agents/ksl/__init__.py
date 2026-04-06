"""KSL classifieds scraping agents."""

from leadgen.agents.ksl.job_seekers import KSLJobSeekersAgent
from leadgen.agents.ksl.services_offered import KSLServicesOfferedAgent
from leadgen.agents.ksl.business_for_sale import KSLBusinessForSaleAgent
from leadgen.agents.ksl.resume_posts import KSLResumePostsAgent
from leadgen.agents.ksl.career_services import KSLCareerServicesAgent
from leadgen.agents.ksl.gig_workers import KSLGigWorkersAgent
from leadgen.agents.ksl.professional_services import KSLProfessionalServicesAgent
from leadgen.agents.ksl.coaching_consulting import KSLCoachingConsultingAgent

__all__ = [
    "KSLJobSeekersAgent",
    "KSLServicesOfferedAgent",
    "KSLBusinessForSaleAgent",
    "KSLResumePostsAgent",
    "KSLCareerServicesAgent",
    "KSLGigWorkersAgent",
    "KSLProfessionalServicesAgent",
    "KSLCoachingConsultingAgent",
]
