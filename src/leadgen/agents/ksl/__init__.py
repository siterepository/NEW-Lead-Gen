"""KSL classifieds scraping agents."""

from leadgen.agents.ksl.job_seekers import KSLJobSeekersAgent

# Other KSL agents use crawlee which requires browserforge fix.
# Lazy-import them to avoid blocking the working agents.
try:
    from leadgen.agents.ksl.services_offered import KSLServicesOfferedAgent
    from leadgen.agents.ksl.business_for_sale import KSLBusinessForSaleAgent
    from leadgen.agents.ksl.resume_posts import KSLResumePostsAgent
    from leadgen.agents.ksl.career_services import KSLCareerServicesAgent
    from leadgen.agents.ksl.gig_workers import KSLGigWorkersAgent
    from leadgen.agents.ksl.professional_services import KSLProfessionalServicesAgent
    from leadgen.agents.ksl.coaching_consulting import KSLCoachingConsultingAgent
except ImportError:
    KSLServicesOfferedAgent = None  # type: ignore
    KSLBusinessForSaleAgent = None  # type: ignore
    KSLResumePostsAgent = None  # type: ignore
    KSLCareerServicesAgent = None  # type: ignore
    KSLGigWorkersAgent = None  # type: ignore
    KSLProfessionalServicesAgent = None  # type: ignore
    KSLCoachingConsultingAgent = None  # type: ignore

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
