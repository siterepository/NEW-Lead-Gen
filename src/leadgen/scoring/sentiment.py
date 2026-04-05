"""
NLP sentiment analyzer for recruiting lead intelligence.

Uses TextBlob for polarity / subjectivity analysis and custom keyword
matching to detect motivation signals and recruiting-relevant language
in source texts (posts, bios, comments).
"""

from __future__ import annotations

from textblob import TextBlob


class SentimentAnalyzer:
    """Analyze text for sentiment, motivation, and recruiting signals."""

    # ------------------------------------------------------------------
    # Keyword categories for signal extraction
    # ------------------------------------------------------------------

    SIGNAL_KEYWORDS: dict[str, list[str]] = {
        "career_change": [
            "career change", "new opportunity", "pivot", "transition",
            "career pivot", "career transition", "switching careers",
            "new chapter", "fresh start", "reinvent myself",
            "new direction", "career shift", "exploring options",
            "next chapter", "crossroads", "starting over",
            "change of pace", "new path", "different direction",
        ],
        "entrepreneurial": [
            "start a business", "entrepreneur", "side hustle",
            "own boss", "be my own boss", "startup", "founder",
            "self-employed", "freelance", "solopreneur",
            "business owner", "independent", "passive income stream",
            "build something", "launch a company", "co-founder",
            "sole proprietor", "contractor",
        ],
        "financial_interest": [
            "financial freedom", "wealth building", "investing",
            "retirement", "financial independence", "fire movement",
            "multiple income streams", "residual income",
            "passive income", "financial literacy", "wealth management",
            "compound interest", "portfolio", "assets",
            "net worth", "financial planning", "401k",
            "stock market", "real estate investing",
        ],
        "help_others": [
            "help people", "make a difference", "coaching",
            "mentoring", "give back", "community impact",
            "change lives", "empower others", "serve others",
            "volunteer", "nonprofit", "philanthropy",
            "teach others", "support families", "help families",
            "make an impact", "meaningful work",
        ],
        "dissatisfaction": [
            "hate my job", "burned out", "underpaid",
            "no growth", "dead end", "stuck", "frustrated",
            "toxic workplace", "overworked", "undervalued",
            "unfulfilled", "miserable", "dreading work",
            "no work life balance", "soul crushing",
            "going nowhere", "wasting my potential",
            "not appreciated", "burnout", "exhausted",
            "need a change", "stuck in a rut",
        ],
    }

    # Flattened list for quick motivation detection
    _MOTIVATION_PHRASES: list[str] = [
        # Job seeking
        "looking for work", "open to opportunities", "job search",
        "seeking employment", "available immediately", "open to work",
        "actively looking", "#opentowork", "job hunting",
        "between jobs", "seeking new role",
        # Career change
        "career change", "career pivot", "career transition",
        "new chapter", "fresh start", "switching careers",
        "new direction", "career shift",
        # Dissatisfaction
        "burned out", "burnout", "need a change", "underpaid",
        "hate my job", "no growth", "dead end", "stuck in a rut",
        "frustrated", "toxic workplace", "overworked",
        # Aspiration
        "passive income", "financial freedom", "be my own boss",
        "own boss", "side hustle", "wealth building",
        # Returning
        "returning to work", "back to work", "re-entering workforce",
        "career comeback",
        # Laid off
        "laid off", "downsized", "restructured", "lost my job",
    ]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def analyze(self, text: str) -> dict:
        """
        Run full sentiment analysis on a block of text.

        Returns:
            dict with keys:
                polarity      (float -1..1)  negative to positive
                subjectivity  (float  0..1)  objective to subjective
                motivation_score (float 0..1) composite motivation estimate
        """
        if not text or not text.strip():
            return {"polarity": 0.0, "subjectivity": 0.0, "motivation_score": 0.0}

        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        subjectivity = blob.sentiment.subjectivity
        motivation = self._compute_motivation_score(text, polarity)

        return {
            "polarity": round(polarity, 4),
            "subjectivity": round(subjectivity, 4),
            "motivation_score": round(motivation, 4),
        }

    def detect_motivation(self, text: str) -> dict:
        """
        Detect motivation signals in text using sentiment + keyword matching.

        High-motivation pattern: negative sentiment about current situation
        combined with positive language about growth / change.

        Returns:
            dict with keys:
                score          (float 0..1)
                keywords_found (list[str])
                sentiment      (str: 'positive', 'negative', 'neutral')
        """
        if not text or not text.strip():
            return {"score": 0.0, "keywords_found": [], "sentiment": "neutral"}

        text_lower = text.lower()
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity

        # Classify overall sentiment
        if polarity > 0.1:
            sentiment_label = "positive"
        elif polarity < -0.1:
            sentiment_label = "negative"
        else:
            sentiment_label = "neutral"

        # Find matching motivation keywords
        keywords_found: list[str] = []
        for phrase in self._MOTIVATION_PHRASES:
            if phrase in text_lower and phrase not in keywords_found:
                keywords_found.append(phrase)

        # Compute motivation score
        motivation = self._compute_motivation_score(text, polarity)

        return {
            "score": round(motivation, 4),
            "keywords_found": keywords_found,
            "sentiment": sentiment_label,
        }

    def extract_recruiting_signals(self, text: str) -> list[str]:
        """
        Scan text for all recruiting-relevant keywords across every
        signal category.

        Returns:
            Deduplicated list of matched signal phrases.
        """
        if not text or not text.strip():
            return []

        text_lower = text.lower()
        matched: list[str] = []

        for category, keywords in self.SIGNAL_KEYWORDS.items():
            for kw in keywords:
                if kw in text_lower and kw not in matched:
                    matched.append(kw)

        return matched

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _compute_motivation_score(self, text: str, polarity: float) -> float:
        """
        Compute a 0-1 motivation score from keyword density and sentiment.

        Logic:
        - Base score from keyword match ratio (how many motivation phrases hit)
        - Boost when negative sentiment about current situation appears
          alongside positive language about growth / change
        - Clamp to [0, 1]
        """
        text_lower = text.lower()

        # Count keyword hits
        hits = sum(1 for phrase in self._MOTIVATION_PHRASES if phrase in text_lower)
        # Normalize: hitting 5+ phrases is a strong signal
        keyword_ratio = min(hits / 5.0, 1.0)

        # Check for the high-motivation pattern:
        # negative about *current* situation + positive about *future*
        has_dissatisfaction = any(
            kw in text_lower for kw in self.SIGNAL_KEYWORDS["dissatisfaction"]
        )
        has_aspiration = any(
            kw in text_lower
            for kw in (
                self.SIGNAL_KEYWORDS["entrepreneurial"]
                + self.SIGNAL_KEYWORDS["financial_interest"]
                + self.SIGNAL_KEYWORDS["career_change"]
            )
        )

        # Sentiment-context boost
        context_boost = 0.0
        if has_dissatisfaction and has_aspiration:
            # Classic high-motivation pattern
            context_boost = 0.3
        elif has_dissatisfaction and polarity < -0.1:
            # Strong negative sentiment with dissatisfaction keywords
            context_boost = 0.15
        elif has_aspiration and polarity > 0.1:
            # Positive outlook with aspiration language
            context_boost = 0.1

        score = keyword_ratio * 0.7 + context_boost
        return max(0.0, min(score, 1.0))
