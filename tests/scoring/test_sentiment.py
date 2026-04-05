"""
Unit tests for leadgen.scoring.sentiment -- SentimentAnalyzer.

5 tests covering analyze(), detect_motivation(), extract_recruiting_signals(),
and empty-text edge case.
"""

from __future__ import annotations

import pytest

from leadgen.scoring.sentiment import SentimentAnalyzer


@pytest.fixture
def analyzer():
    return SentimentAnalyzer()


# ---------------------------------------------------------------------------
# 1. analyze() returns correct structure
# ---------------------------------------------------------------------------

class TestAnalyze:

    def test_analyze_returns_correct_structure(self, analyzer):
        """analyze() returns dict with polarity, subjectivity, motivation_score."""
        result = analyzer.analyze("I hate my job and I want financial freedom")
        assert "polarity" in result
        assert "subjectivity" in result
        assert "motivation_score" in result
        assert isinstance(result["polarity"], float)
        assert isinstance(result["subjectivity"], float)
        assert isinstance(result["motivation_score"], float)
        assert -1.0 <= result["polarity"] <= 1.0
        assert 0.0 <= result["subjectivity"] <= 1.0
        assert 0.0 <= result["motivation_score"] <= 1.0


# ---------------------------------------------------------------------------
# 2-3. detect_motivation()
# ---------------------------------------------------------------------------

class TestDetectMotivation:

    def test_detect_motivation_finds_keywords(self, analyzer):
        """detect_motivation identifies motivation phrases in text."""
        text = "I am actively looking for work. Open to opportunities. Career change ahead."
        result = analyzer.detect_motivation(text)
        assert result["score"] > 0
        assert len(result["keywords_found"]) > 0
        assert "actively looking" in result["keywords_found"] or \
               "looking for work" in result["keywords_found"]
        assert result["sentiment"] in {"positive", "negative", "neutral"}

    def test_detect_motivation_negative_plus_growth_is_high(self, analyzer):
        """Negative sentiment about current job + growth language yields high score."""
        text = (
            "I hate my job. Burned out, underpaid, stuck in a rut. "
            "I want financial freedom, to be my own boss, and start a business. "
            "Career change is what I need. Ready for a fresh start."
        )
        result = analyzer.detect_motivation(text)
        assert result["score"] >= 0.5  # High motivation pattern
        assert len(result["keywords_found"]) >= 3


# ---------------------------------------------------------------------------
# 4. extract_recruiting_signals()
# ---------------------------------------------------------------------------

class TestExtractRecruitingSignals:

    def test_extract_recruiting_signals_finds_signals(self, analyzer):
        """extract_recruiting_signals picks up keywords from all categories."""
        text = (
            "I'm considering a career change and thinking about starting a business. "
            "I want financial freedom and to help people make a difference. "
            "Frustrated with my dead end job."
        )
        signals = analyzer.extract_recruiting_signals(text)
        assert isinstance(signals, list)
        assert len(signals) > 0
        # Should find at least one from career_change, entrepreneurial,
        # financial_interest, help_others, dissatisfaction
        signal_text = " ".join(signals).lower()
        assert any(kw in signal_text for kw in ["career change", "start a business",
                                                  "financial freedom", "help people",
                                                  "frustrated", "dead end"])


# ---------------------------------------------------------------------------
# 5. Empty text returns zeros
# ---------------------------------------------------------------------------

class TestEmptyText:

    def test_empty_text_returns_zeros(self, analyzer):
        """All methods handle empty/blank text gracefully."""
        # analyze
        result = analyzer.analyze("")
        assert result["polarity"] == 0.0
        assert result["subjectivity"] == 0.0
        assert result["motivation_score"] == 0.0

        # detect_motivation
        result = analyzer.detect_motivation("")
        assert result["score"] == 0.0
        assert result["keywords_found"] == []
        assert result["sentiment"] == "neutral"

        # extract_recruiting_signals
        signals = analyzer.extract_recruiting_signals("")
        assert signals == []

        # Also test None-like whitespace
        result = analyzer.analyze("   ")
        assert result["polarity"] == 0.0
