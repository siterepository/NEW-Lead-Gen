"""Job board scraping agents."""

from leadgen.agents.job_boards.indeed_utah import IndeedUtahAgent
from leadgen.agents.job_boards.glassdoor_utah import GlassdoorUtahAgent
from leadgen.agents.job_boards.utah_workforce import UtahWorkforceAgent
from leadgen.agents.job_boards.deseret_news_jobs import DeseretNewsJobsAgent
from leadgen.agents.job_boards.ziprecruiter_utah import ZipRecruiterUtahAgent

__all__ = [
    "IndeedUtahAgent",
    "GlassdoorUtahAgent",
    "UtahWorkforceAgent",
    "DeseretNewsJobsAgent",
    "ZipRecruiterUtahAgent",
]
