"""Craigslist scraping agents."""

from leadgen.agents.craigslist.slc_jobs_wanted import CraigslistSLCJobsWantedAgent
from leadgen.agents.craigslist.provo_jobs_wanted import CraigslistProvoJobsWantedAgent
from leadgen.agents.craigslist.slc_resumes import CraigslistSLCResumesAgent
from leadgen.agents.craigslist.slc_gigs import CraigslistSLCGigsAgent
from leadgen.agents.craigslist.slc_business import CraigslistSLCBusinessAgent

__all__ = [
    "CraigslistSLCJobsWantedAgent",
    "CraigslistProvoJobsWantedAgent",
    "CraigslistSLCResumesAgent",
    "CraigslistSLCGigsAgent",
    "CraigslistSLCBusinessAgent",
]
