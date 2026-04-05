"""
Recruiting-focused lead scoring engine.

Scores people on how strong of a NWM financial advisor recruit they would be
across five dimensions totalling 100 points:

  career_fit    (35)  Transferable skills for financial advising
  motivation    (25)  Signals that someone is actively seeking change
  people_skills (20)  Interpersonal / leadership indicators
  demographics  (10)  Location, age proxy, education
  data_quality  (10)  Completeness and freshness of contact data
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from leadgen.models.lead import Lead


class ScoringEngine:
    """Score a Lead on recruiting potential for NWM financial advising."""

    # ------------------------------------------------------------------
    # Keyword constants
    # ------------------------------------------------------------------

    CAREER_FIT_KEYWORDS: dict[str, tuple[list[str], int]] = {
        "sales": (
            [
                "sales", "business development", "account executive",
                "account manager", "sales manager", "sales rep",
                "sales representative", "bdr", "sdr", "closer",
                "revenue", "quota", "b2b sales", "b2c sales",
                "outside sales", "inside sales", "sales director",
                "sales associate", "territory manager",
            ],
            10,
        ),
        "entrepreneurial": (
            [
                "entrepreneur", "founder", "co-founder", "owner",
                "self-employed", "freelancer", "freelance",
                "independent contractor", "small business owner",
                "startup", "sole proprietor", "business owner",
                "consultant", "solopreneur",
            ],
            8,
        ),
        "real_estate": (
            [
                "real estate agent", "real estate broker", "realtor",
                "real estate", "realty", "property manager",
                "real estate associate", "listing agent", "buyer agent",
                "keller williams", "re/max", "coldwell banker",
                "century 21",
            ],
            7,
        ),
        "insurance": (
            [
                "insurance agent", "insurance broker", "insurance",
                "underwriter", "claims adjuster", "actuary",
                "state farm", "allstate", "farmers insurance",
                "insurance sales", "licensed agent", "p&c agent",
                "life insurance",
            ],
            7,
        ),
        "military": (
            [
                "veteran", "military", "army", "navy", "air force",
                "marine", "marines", "coast guard", "national guard",
                "transitioning military", "military veteran",
                "retired military", "usmc", "usaf", "service member",
                "mil-spouse",
            ],
            7,
        ),
        "teaching": (
            [
                "teacher", "professor", "instructor", "educator",
                "teaching", "coach", "coaching", "mentor", "mentoring",
                "tutor", "academic", "faculty", "adjunct",
                "school counselor", "trainer",
            ],
            6,
        ),
        "leadership": (
            [
                "manager", "director", "vp", "vice president",
                "team lead", "team leader", "supervisor", "head of",
                "chief", "executive", "management", "operations manager",
                "general manager", "regional manager",
            ],
            5,
        ),
        "athletics": (
            [
                "athlete", "athletics", "collegiate athlete",
                "sports", "competitive", "division 1", "d1",
                "varsity", "captain", "team captain", "ncaa",
                "marathon", "triathlon", "fitness", "personal trainer",
            ],
            4,
        ),
        "customer_facing": (
            [
                "retail manager", "hospitality", "restaurant manager",
                "front desk", "concierge", "customer service",
                "customer success", "barista", "bartender",
                "store manager", "assistant manager", "shift lead",
                "retail", "food service",
            ],
            3,
        ),
    }

    MOTIVATION_KEYWORDS: list[str] = [
        # Explicitly job seeking
        "looking for work", "open to opportunities", "job search",
        "seeking employment", "available immediately", "open to work",
        "actively looking", "seeking new role", "#opentowork",
        "job hunting", "between jobs",
        # Career change language
        "career change", "career pivot", "career transition",
        "new chapter", "fresh start", "reinvent myself",
        "switching careers", "exploring options", "new direction",
        "career shift",
        # Dissatisfaction signals
        "burned out", "burnout", "need a change", "underpaid",
        "undervalued", "toxic workplace", "hate my job",
        "no growth", "dead end", "stuck in a rut", "unfulfilled",
        "frustrated", "overworked", "no work life balance",
        "layoff", "laid off", "downsized", "restructured",
        # Entrepreneurial aspiration
        "passive income", "financial freedom", "be my own boss",
        "own boss", "side hustle", "side income", "wealth building",
        "multiple streams", "residual income", "time freedom",
        # Returning to workforce
        "returning to work", "back to work", "re-entering workforce",
        "career comeback", "stay at home", "gap in resume",
    ]

    PEOPLE_SKILLS_KEYWORDS: list[str] = [
        # Active networker
        "networking", "networker", "connector", "community builder",
        "relationship builder", "500+ connections",
        # Volunteer / community
        "volunteer", "community service", "nonprofit", "charity",
        "giving back", "philanthropy", "board member", "pta",
        "rotary", "lions club", "habitat for humanity",
        # Coaching / mentoring
        "coach", "coaching", "mentor", "mentoring", "mentored",
        "life coach", "executive coach",
        # Public speaking
        "public speaking", "speaker", "keynote", "toastmasters",
        "presenter", "panelist", "ted talk", "tedx",
        # Social media presence
        "influencer", "content creator", "blogger", "vlogger",
        "podcast", "podcaster", "thought leader", "brand ambassador",
        # Team leadership
        "team leader", "led a team", "managed a team", "built a team",
        "team captain", "led team", "leadership",
    ]

    # Default priority ZIPs - overridden by config/settings.toml if available
    PRIORITY_ZIPS: set[str] = {
        "84004", "84060", "84098", "84020", "84092", "84093",
        "84117", "84121", "84095", "84010", "84037", "84025",
        "84043",
    }

    def __init__(self, priority_zips: set[str] | None = None) -> None:
        """Initialize scoring engine. Optionally override priority ZIP codes."""
        if priority_zips is not None:
            self.PRIORITY_ZIPS = priority_zips
        else:
            # Try loading from dynaconf settings
            try:
                from dynaconf import settings
                zips = settings.get("PRIORITY_ZIPS", None)
                if zips:
                    self.PRIORITY_ZIPS = set(str(z) for z in zips)
            except Exception:
                pass  # Use class default

    # ------------------------------------------------------------------
    # Scoring methods
    # ------------------------------------------------------------------

    def score_career_fit(self, lead: Lead) -> int:
        """Score transferable-skill signals (max 35 points)."""
        text = self._build_career_text(lead)
        if not text:
            return 0

        awarded: set[str] = set()
        total = 0

        for category, (keywords, points) in self.CAREER_FIT_KEYWORDS.items():
            if category in awarded:
                continue
            for kw in keywords:
                if kw in text:
                    total += points
                    awarded.add(category)
                    break

        return min(total, 35)

    def score_motivation(self, lead: Lead) -> int:
        """Score signals that the person is seeking change (max 25 points)."""
        text = self._build_full_text(lead)
        if not text:
            return 0

        total = 0

        # Explicitly job seeking (+10)
        job_seeking = [
            "looking for work", "open to opportunities", "job search",
            "seeking employment", "available immediately", "open to work",
            "actively looking", "seeking new role", "#opentowork",
            "job hunting", "between jobs",
        ]
        if any(kw in text for kw in job_seeking):
            total += 10

        # Career change language (+8)
        career_change = [
            "career change", "career pivot", "career transition",
            "new chapter", "fresh start", "reinvent myself",
            "switching careers", "exploring options", "new direction",
            "career shift",
        ]
        if any(kw in text for kw in career_change):
            total += 8

        # Recently unemployed / laid off (+7)
        unemployed = [
            "laid off", "layoff", "downsized", "restructured",
            "recently unemployed", "lost my job",
        ]
        if any(kw in text for kw in unemployed):
            total += 7

        # Dissatisfaction signals (+6)
        dissatisfaction = [
            "burned out", "burnout", "need a change", "underpaid",
            "undervalued", "toxic workplace", "hate my job",
            "no growth", "dead end", "stuck in a rut", "unfulfilled",
            "frustrated", "overworked", "no work life balance",
        ]
        if any(kw in text for kw in dissatisfaction):
            total += 6

        # Entrepreneurial aspiration (+5)
        entrepreneurial = [
            "passive income", "financial freedom", "be my own boss",
            "own boss", "side hustle", "side income", "wealth building",
            "multiple streams", "residual income", "time freedom",
        ]
        if any(kw in text for kw in entrepreneurial):
            total += 5

        # Returning to workforce (+5)
        returning = [
            "returning to work", "back to work", "re-entering workforce",
            "career comeback", "stay at home", "gap in resume",
        ]
        if any(kw in text for kw in returning):
            total += 5

        # Freshness boost based on first_seen
        total += self._freshness_bonus(lead)

        return min(total, 25)

    def score_people_skills(self, lead: Lead) -> int:
        """Score interpersonal and leadership indicators (max 20 points)."""
        text = self._build_full_text(lead)
        if not text:
            return 0

        total = 0

        # Active networker (+6)
        networker = [
            "networking", "networker", "connector", "community builder",
            "relationship builder", "500+ connections",
        ]
        if any(kw in text for kw in networker):
            total += 6

        # Volunteer / community service (+5)
        volunteer = [
            "volunteer", "community service", "nonprofit", "charity",
            "giving back", "philanthropy", "board member", "pta",
            "rotary", "lions club", "habitat for humanity",
        ]
        if any(kw in text for kw in volunteer):
            total += 5

        # Coaching / mentoring (+5)
        coaching = [
            "coach", "coaching", "mentor", "mentoring", "mentored",
            "life coach", "executive coach",
        ]
        if any(kw in text for kw in coaching):
            total += 5

        # Public speaking (+4)
        speaking = [
            "public speaking", "speaker", "keynote", "toastmasters",
            "presenter", "panelist", "ted talk", "tedx",
        ]
        if any(kw in text for kw in speaking):
            total += 4

        # Strong social media presence (+3)
        social = [
            "influencer", "content creator", "blogger", "vlogger",
            "podcast", "podcaster", "thought leader", "brand ambassador",
        ]
        if any(kw in text for kw in social):
            total += 3

        # Team leadership (+3)
        team_lead = [
            "team leader", "led a team", "managed a team", "built a team",
            "team captain", "led team", "leadership",
        ]
        if any(kw in text for kw in team_lead):
            total += 3

        return min(total, 20)

    def score_demographics(self, lead: Lead) -> int:
        """Score location and education signals (max 10 points)."""
        total = 0

        # Age 25-55 proxy (+3)
        # We cannot directly determine age, but if career_history or education
        # hints at graduation years we try to estimate.
        estimated_age = self._estimate_age(lead)
        if estimated_age is not None and 25 <= estimated_age <= 55:
            total += 3

        # Utah resident (+3)
        state = (lead.location_state or "").strip().lower()
        if state in ("utah", "ut"):
            total += 3

        # College educated (+2)
        education = (lead.education or "").lower()
        edu_keywords = [
            "bachelor", "master", "mba", "phd", "degree", "university",
            "college", "b.s.", "b.a.", "m.s.", "m.a.", "associate",
            "doctorate",
        ]
        if any(kw in education for kw in edu_keywords):
            total += 2

        # Priority ZIP code (+2)
        zipcode = (lead.location_zip or "").strip()
        if zipcode and zipcode[:5] in self.PRIORITY_ZIPS:
            total += 2

        return min(total, 10)

    def score_data_quality(self, lead: Lead) -> int:
        """Score completeness and freshness of contact data (max 10 points)."""
        total = 0

        # Email available and verified (+3)
        if lead.email and "@" in lead.email:
            total += 3

        # Phone available (+2)
        if lead.phone and len(lead.phone.strip()) >= 7:
            total += 2

        # LinkedIn URL available (+2)
        if lead.linkedin_url and "linkedin.com" in lead.linkedin_url.lower():
            total += 2

        # Multiple sources (+2)
        if lead.sources_count >= 2:
            total += 2

        # Recent activity < 30 days (+1)
        if lead.last_seen:
            days_old = (datetime.now(timezone.utc) - lead.last_seen).days
            if days_old < 30:
                total += 1

        return min(total, 10)

    # ------------------------------------------------------------------
    # Main scoring entry point
    # ------------------------------------------------------------------

    def score_lead(self, lead: Lead) -> Lead:
        """
        Run the full scoring pipeline on a lead.

        Computes all five dimension scores, sums them to a total,
        assigns a tier (A/B/C/D), and returns the updated Lead.
        """
        lead.score_career_fit = self.score_career_fit(lead)
        lead.score_motivation = self.score_motivation(lead)
        lead.score_people_skills = self.score_people_skills(lead)
        lead.score_demographics = self.score_demographics(lead)
        lead.score_data_quality = self.score_data_quality(lead)

        lead.total_score = (
            lead.score_career_fit
            + lead.score_motivation
            + lead.score_people_skills
            + lead.score_demographics
            + lead.score_data_quality
        )

        # Tier assignment
        if lead.total_score >= 75:
            lead.tier = "A"
        elif lead.total_score >= 50:
            lead.tier = "B"
        elif lead.total_score >= 25:
            lead.tier = "C"
        else:
            lead.tier = "D"

        lead.updated_at = datetime.now(timezone.utc)
        return lead

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_career_text(self, lead: Lead) -> str:
        """Combine career-relevant fields into a single lowercase string."""
        parts: list[str] = []
        if lead.current_role:
            parts.append(lead.current_role)
        if lead.current_company:
            parts.append(lead.current_company)
        if lead.career_history:
            parts.extend(lead.career_history)
        if lead.source_post_text:
            parts.append(lead.source_post_text)
        if lead.recruiting_signals:
            parts.extend(lead.recruiting_signals)
        return " ".join(parts).lower()

    def _build_full_text(self, lead: Lead) -> str:
        """Combine all text fields into a single lowercase string for scanning."""
        parts: list[str] = []
        if lead.current_role:
            parts.append(lead.current_role)
        if lead.current_company:
            parts.append(lead.current_company)
        if lead.career_history:
            parts.extend(lead.career_history)
        if lead.source_post_text:
            parts.append(lead.source_post_text)
        if lead.recruiting_signals:
            parts.extend(lead.recruiting_signals)
        if lead.motivation_keywords:
            parts.extend(lead.motivation_keywords)
        if lead.education:
            parts.append(lead.education)
        return " ".join(parts).lower()

    def _freshness_bonus(self, lead: Lead) -> int:
        """Award bonus points based on how recently the lead was seen."""
        if not lead.first_seen:
            return 0
        days_old = (datetime.now(timezone.utc) - lead.first_seen).days
        if days_old < 7:
            return 5
        elif days_old < 14:
            return 3
        elif days_old < 30:
            return 1
        return 0

    def _estimate_age(self, lead: Lead) -> Optional[int]:
        """
        Attempt to estimate age from education graduation year.

        Returns None if no signal is found.
        """
        text = ""
        if lead.education:
            text += lead.education
        if lead.career_history:
            text += " ".join(lead.career_history)

        # Look for 4-digit years that could be graduation years
        years = re.findall(r"\b(19[5-9]\d|20[0-2]\d)\b", text)
        if not years:
            return None

        # Assume the earliest year is roughly graduation; age ~ now - grad + 22
        earliest = min(int(y) for y in years)
        estimated_age = datetime.now(timezone.utc).year - earliest + 22
        if 18 <= estimated_age <= 90:
            return estimated_age
        return None
