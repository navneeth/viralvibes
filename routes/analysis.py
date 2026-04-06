"""
routes/analysis.py — Analysis page content.

The Analysis page is the primary entry point for playlist analysis.
It hosts the AnalysisFormCard (compact mode) and communicates the value of
the tool before and after a user submits a playlist.

Design follows the /lists and /creators conventions:
  - Compact page header with theme tokens (text-foreground / text-muted-foreground)
  - Container(cls=ContainerT.xl) wrapper — consistent with the creators page
  - FeaturePill reused from components.buttons (no bespoke pill HTML)
  - No hardcoded gray colours — all resolved via MonsterUI CSS vars
"""

from fasthtml.common import *
from monsterui.all import *

from components.buttons import FeaturePill
from components.cards import AnalysisFormCard


# ---------------------------------------------------------------------------
# Page header — compact, aligned with /lists and /creators patterns
# ---------------------------------------------------------------------------


def _page_header() -> Div:
    """Compact page header: eyebrow label → H1 → subtitle → feature pills."""
    pills = [
        ("chart-bar", "Engagement Metrics"),
        ("trending-up", "Viral Patterns"),
        ("users", "Audience Insights"),
        ("zap", "Instant Results"),
    ]
    return Div(
        P(
            "YouTube Playlist Intelligence",
            cls="text-xs font-semibold text-muted-foreground uppercase tracking-widest",
        ),
        H1(
            "Analyze Any YouTube Playlist",
            cls="text-2xl sm:text-3xl font-bold text-foreground mt-1",
        ),
        P(
            "Paste a playlist URL and get deep insights into views, engagement, "
            "controversy scores, and viral patterns — in seconds.",
            cls="text-muted-foreground mt-2 text-sm sm:text-base max-w-2xl",
        ),
        Div(
            *[FeaturePill(icon, label) for icon, label in pills],
            cls="flex flex-wrap gap-2 mt-4",
        ),
        cls="mb-6 pt-6",
    )


# ---------------------------------------------------------------------------
# "What you'll discover" cards (below the form)
# ---------------------------------------------------------------------------


def _insight_cards() -> Div:
    """Three-column card grid explaining what the analysis surfaces."""
    cards = [
        (
            "bar-chart-2",
            "Performance Metrics",
            "Views, likes, comments, and dislikes broken down per video so you can spot your best content.",
        ),
        (
            "trending-up",
            "Engagement & Virality",
            "Engagement rate and controversy score reveal which videos break through — and why.",
        ),
        (
            "clock",
            "Duration Analysis",
            "Average video length and watch-time patterns help you find the optimal content format.",
        ),
    ]
    return Div(
        H2(
            "What you'll discover",
            cls="text-xl sm:text-2xl font-bold text-foreground mb-2",
        ),
        P(
            "Every analysis produces a full interactive dashboard with these insights.",
            cls="text-muted-foreground mb-8 text-sm sm:text-base",
        ),
        Div(
            *[
                Card(
                    Div(
                        UkIcon(icon, cls="w-8 h-8 text-red-500 mb-4"),
                        H3(title, cls="text-lg font-semibold text-foreground mb-2"),
                        P(description, cls="text-muted-foreground text-sm leading-relaxed"),
                        cls="p-6",
                    ),
                    cls="border rounded-2xl shadow-sm hover:shadow-md transition-shadow",
                )
                for icon, title, description in cards
            ],
            cls="grid grid-cols-1 md:grid-cols-3 gap-6",
        ),
        cls="pt-8 pb-16",
    )


# ---------------------------------------------------------------------------
# Public page builder
# ---------------------------------------------------------------------------


def analysis_page_content() -> FT:
    """
    Full body content for the /analysis route (without nav/title wrapper).
    Composed of: page header → analysis form (compact) → insight cards.
    Uses Container(cls=ContainerT.xl) to match /creators and /lists layout.
    """
    return Container(
        _page_header(),
        AnalysisFormCard(compact=True),
        _insight_cards(),
        cls=ContainerT.xl,
    )
