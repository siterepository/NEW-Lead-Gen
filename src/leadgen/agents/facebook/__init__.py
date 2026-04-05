"""Facebook group discovery agents.

These agents use Google search to find public Utah Facebook groups
relevant to NWM financial advisor recruiting.  They ONLY access
publicly visible content and do NOT log into Facebook.
"""

from leadgen.agents.facebook.utah_career_groups import FBUtahCareerGroupsAgent
from leadgen.agents.facebook.entrepreneur_groups import FBEntrepreneurGroupsAgent
from leadgen.agents.facebook.job_seeker_groups import FBJobSeekerGroupsAgent
from leadgen.agents.facebook.veteran_transition import FBVeteranTransitionAgent
from leadgen.agents.facebook.real_estate_groups import FBRealEstateGroupsAgent
from leadgen.agents.facebook.sales_pro_groups import FBSalesProGroupsAgent
from leadgen.agents.facebook.young_professional import FBYoungProfessionalAgent

__all__ = [
    "FBUtahCareerGroupsAgent",
    "FBEntrepreneurGroupsAgent",
    "FBJobSeekerGroupsAgent",
    "FBVeteranTransitionAgent",
    "FBRealEstateGroupsAgent",
    "FBSalesProGroupsAgent",
    "FBYoungProfessionalAgent",
]
