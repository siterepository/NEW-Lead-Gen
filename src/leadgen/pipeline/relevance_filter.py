"""
Pre-pipeline relevance filter for scraped items.

Runs BEFORE items enter the normalize -> dedupe -> score pipeline.
Uses word-boundary matching to identify business/professional content
relevant to NWM recruiting. Items that don't pass get dropped.
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)


def _word_match(keyword: str, text: str) -> bool:
    """Check if keyword appears as a whole word/phrase in text (not substring)."""
    return bool(re.search(r'\b' + re.escape(keyword) + r'\b', text, re.IGNORECASE))


class RelevanceFilter:
    """Keyword-based relevance gate for raw scraped items."""

    # HIGH-VALUE keywords (2 points each) - these are strong NWM recruit signals
    HIGH_VALUE: list[str] = [
        # Entrepreneur/Business Owner
        "entrepreneur", "business owner", "small business owner", "franchise owner",
        "startup founder", "co-founder", "business for sale", "franchise",
        "business available", "business opportunity", "turnkey",
        # Sales Professionals
        "sales manager", "sales director", "account executive", "sales rep",
        "business development", "sales leader", "sales executive",
        "outside sales", "inside sales", "sales representative",
        "sales position", "sales role", "earn commission",
        # Real Estate
        "real estate agent", "realtor", "real estate broker", "listing agent",
        "property manager", "real estate",
        # Insurance
        "insurance agent", "insurance broker", "life insurance", "insurance producer",
        "insurance sales",
        # Financial
        "financial advisor", "financial planner", "wealth manager",
        "investment advisor", "CPA", "financial consultant", "financial services",
        # Executive/Leadership
        "vice president", "chief executive", "chief operating",
        "general manager", "regional manager", "district manager",
        # Coaching/Consulting
        "business coach", "executive coach", "sales coach", "business consultant",
        "management consultant",
        # Recruiting-friendly phrases in job posts
        "unlimited income", "unlimited earning", "be your own boss",
        "own boss", "high commission", "uncapped commission",
        "six figure", "six-figure", "remote sales",
    ]

    # MEDIUM-VALUE keywords (1 point each) - relevant but need context
    MEDIUM_VALUE: list[str] = [
        "sales", "commission", "revenue", "closing",
        "financial", "insurance", "broker", "mortgage",
        "advisor", "consulting", "coaching", "executive",
        "director", "leadership", "manager",
        "income potential", "self employed", "independent",
        "business", "profitable", "established",
    ]

    # REJECT keywords - instant disqualify
    REJECT: list[str] = [
        # Vehicles/brands
        "Honda", "Toyota", "Ford", "Chevrolet", "Dodge", "Jeep", "BMW",
        "Polaris", "Kawasaki", "Yamaha", "Triumph", "Harley", "Can-Am", "KTM",
        "motorcycle", "ATV", "UTV", "snowmobile",
        # Auto/Parts
        "auto parts", "transmission", "bumper", "exhaust", "CDL",
        # Food/Restaurant (not the business, just line workers)
        "line cook", "dishwasher", "busser", "barista",
        # Trades (not target market)
        "electrician", "plumber", "welder", "HVAC", "roofer", "carpenter",
        "landscaping crew", "lawn care technician",
        # Pets
        "puppy", "puppies", "kitten", "AKC",
        # Recreation
        "paddleboard", "kayak", "golf cart",
        # Random
        "canning jar", "turf supply", "mower",
    ]

    def is_relevant(self, title: str, description: str = "") -> tuple[bool, float, str]:
        """Check if an item is relevant to NWM recruiting.

        Uses word-boundary matching to avoid false positives like
        'COO' matching inside 'Coordinator' or 'Cook'.

        Returns:
            (is_relevant: bool, confidence: float 0-1, reason: str)
        """
        text = f"{title} {description}"

        # Check reject keywords first (word-boundary match)
        for rej in self.REJECT:
            if _word_match(rej, text):
                return (False, 0.9, f"Rejected: '{rej}'")

        # Score with high-value keywords (2 points each)
        score = 0
        matches = []
        for kw in self.HIGH_VALUE:
            if _word_match(kw, text):
                score += 2
                matches.append(kw)

        # Score with medium-value keywords (1 point each)
        for kw in self.MEDIUM_VALUE:
            if _word_match(kw, text):
                score += 1
                if kw not in matches:
                    matches.append(kw)

        if score >= 3:
            return (True, 0.95, f"Strong: {', '.join(matches[:3])}")
        elif score >= 2:
            return (True, 0.80, f"Good: {', '.join(matches[:3])}")
        elif score >= 1:
            return (True, 0.60, f"Match: {', '.join(matches[:2])}")
        else:
            return (False, 0.3, "No relevant keywords")

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
