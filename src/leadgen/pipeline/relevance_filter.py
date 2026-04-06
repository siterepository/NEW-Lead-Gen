"""
Pre-pipeline relevance filter for scraped items.

KEY RULE: We want PEOPLE actively seeking work or unhappy in their current
role -- NOT companies posting job listings. We target:
  - Door-to-door sales reps looking for a change
  - Sales people expressing grievances with their employer
  - Entrepreneurs whose business is struggling or for sale
  - Financial / insurance / real estate pros seeking new opportunities
  - Anyone actively posting "looking for work" or "need a change"

We REJECT:
  - Companies posting "we're hiring" / "now hiring" / "join our team"
  - Generic job listings (these are employers, not prospects)
  - Vehicles, appliances, pets, trades
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)


def _has(keyword: str, text: str) -> bool:
    """Word-boundary match (case-insensitive)."""
    return bool(re.search(r'\b' + re.escape(keyword) + r'\b', text, re.IGNORECASE))


class RelevanceFilter:
    """Filter that ONLY passes people seeking work / expressing dissatisfaction.

    Rejects company job postings and irrelevant listings.
    """

    # ----------------------------------------------------------------
    # COMPANY/HIRING SIGNALS -- instant reject (these are employers, not leads)
    # ----------------------------------------------------------------
    HIRING_SIGNALS: list[str] = [
        # Direct hiring language
        "we are hiring", "we're hiring", "now hiring", "currently hiring",
        "hiring immediately", "hiring now", "help wanted",
        "join our team", "join our growing team", "join us",
        "apply now", "apply today", "apply within",
        "seeking candidates", "looking for candidates",
        "accepting applications", "taking applications",
        "open position", "open positions", "position available",
        "positions available", "job opening", "job openings",
        "immediate opening", "immediate openings",
        # Company-side recruiting language
        "we offer", "we provide", "company offers",
        "competitive salary", "competitive pay", "competitive wages",
        "benefits include", "benefits package", "full benefits",
        "401k", "health insurance provided", "PTO",
        "equal opportunity employer", "EOE",
        "background check required", "drug test required",
        "must pass background", "pre-employment",
        # Job requirement language (company posting, not person seeking)
        "must have experience", "must have a valid",
        "requirements include", "qualifications include",
        "minimum qualifications", "preferred qualifications",
        "years of experience required", "degree required",
        "send resume to", "email resume", "fax resume",
    ]

    # ----------------------------------------------------------------
    # PERSON SEEKING / DISSATISFIED -- these are our target leads (2 pts each)
    # ----------------------------------------------------------------
    PERSON_SEEKING: list[str] = [
        # Actively looking
        "looking for work", "looking for a job", "looking for a new",
        "looking for opportunity", "looking for opportunities",
        "seeking employment", "seeking work", "seeking a position",
        "seeking new opportunities", "open to opportunities",
        "available for hire", "available immediately",
        "hire me", "need a job", "need work", "need a change",
        "in search of", "on the job market",
        "between jobs", "recently laid off", "recently let go",
        "unemployed", "job hunting", "job searching",
        # Dissatisfaction / grievances
        "hate my job", "hate this job", "tired of my job",
        "burned out", "burnt out", "burnout",
        "underpaid", "overworked", "undervalued", "unappreciated",
        "toxic workplace", "toxic boss", "toxic management",
        "no growth", "no advancement", "dead end job", "dead-end",
        "ready for a change", "need a change", "time for a change",
        "done with", "fed up", "frustrated with my",
        "leaving my job", "quitting my job", "quit my job",
        "thinking about leaving", "considering leaving",
        "door to door is killing me", "tired of door to door",
        "tired of cold calling", "tired of knocking doors",
        # Career transition
        "career change", "career pivot", "career transition",
        "new chapter", "fresh start", "starting over",
        "changing careers", "switching careers",
        "want to do something different", "ready to move on",
        # Entrepreneurial distress (business failing/selling)
        "selling my business", "selling my", "closing my business",
        "business is struggling", "business for sale",
        "need to sell", "shutting down",
        "ready for something new", "ready for a new",
    ]

    # ----------------------------------------------------------------
    # TARGET INDUSTRY -- person is in our target industry (1 pt each)
    # ----------------------------------------------------------------
    TARGET_INDUSTRY: list[str] = [
        # Door to door / direct sales
        "door to door", "d2d", "door-to-door",
        "pest control", "solar sales", "alarm sales", "vivint",
        "aptive", "summit solar",
        # Sales
        "sales rep", "sales experience", "sales background",
        "commission only", "commission based", "1099",
        "outside sales", "inside sales", "cold calling",
        "B2B sales", "B2C sales", "direct sales",
        "MLM", "network marketing", "multi-level",
        # Insurance
        "life insurance", "insurance agent", "insurance sales",
        "insurance broker", "health insurance", "P&C",
        # Financial
        "financial advisor", "financial planner", "financial services",
        "wealth management", "investment", "mortgage",
        # Real estate
        "real estate agent", "realtor", "real estate",
        "property management", "broker",
        # Entrepreneur
        "entrepreneur", "business owner", "small business",
        "startup", "founder", "self employed", "freelance",
    ]

    # ----------------------------------------------------------------
    # HARD REJECT -- not a person, not relevant
    # ----------------------------------------------------------------
    REJECT: list[str] = [
        # Vehicles
        "Honda", "Toyota", "Ford", "Chevrolet", "Dodge", "Jeep", "BMW",
        "Polaris", "Kawasaki", "Yamaha", "Triumph", "Harley", "Can-Am", "KTM",
        "motorcycle", "ATV", "UTV",
        # Auto/Parts
        "auto parts", "transmission", "bumper", "exhaust", "CDL",
        # Food workers
        "line cook", "dishwasher", "busser", "barista", "server wanted",
        # Trades
        "electrician", "plumber", "welder", "HVAC", "roofer", "carpenter",
        "landscaping crew", "lawn care technician",
        # Pets
        "puppy", "puppies", "kitten", "AKC",
        # Recreation
        "paddleboard", "kayak", "golf cart",
        # Random stuff
        "canning jar", "turf supply", "mower",
    ]

    def is_relevant(self, title: str, description: str = "") -> tuple[bool, float, str]:
        """Check if this is a PERSON seeking work (not a company hiring).

        Returns:
            (is_relevant: bool, confidence: float 0-1, reason: str)
        """
        text = f"{title} {description}"

        # STEP 1: Reject companies posting jobs
        for signal in self.HIRING_SIGNALS:
            if _has(signal, text):
                return (False, 0.95, f"Company hiring post: '{signal}'")

        # STEP 2: Reject non-professional items
        for rej in self.REJECT:
            if _has(rej, text):
                return (False, 0.9, f"Rejected: '{rej}'")

        # STEP 3: Score for person seeking work (2 pts each)
        score = 0
        matches = []
        for kw in self.PERSON_SEEKING:
            if _has(kw, text):
                score += 2
                matches.append(kw)

        # STEP 4: Score for target industry (1 pt each)
        for kw in self.TARGET_INDUSTRY:
            if _has(kw, text):
                score += 1
                if kw not in matches:
                    matches.append(kw)

        # STEP 5: Threshold
        if score >= 4:
            return (True, 0.95, f"Strong prospect: {', '.join(matches[:3])}")
        elif score >= 3:
            return (True, 0.85, f"Good prospect: {', '.join(matches[:3])}")
        elif score >= 2:
            return (True, 0.70, f"Possible prospect: {', '.join(matches[:2])}")
        elif score >= 1:
            # Single industry match without seeking signal - weak
            return (False, 0.4, f"Industry match but no seeking signal: {', '.join(matches[:1])}")
        else:
            return (False, 0.2, "No relevant signals")

    def filter_batch(self, items: list[dict]) -> tuple[list[dict], list[dict]]:
        """Filter a batch of items. Returns (relevant, rejected)."""
        relevant: list[dict] = []
        rejected: list[dict] = []

        for item in items:
            title = item.get("title", "")
            desc = item.get("description", item.get("source_post_text", ""))
            is_rel, conf, reason = self.is_relevant(title, desc)
            item["_relevance_score"] = conf
            item["_relevance_reason"] = reason
            if is_rel:
                relevant.append(item)
            else:
                rejected.append(item)

        logger.debug(
            "Relevance filter: %d relevant, %d rejected out of %d",
            len(relevant), len(rejected), len(relevant) + len(rejected),
        )
        return relevant, rejected
