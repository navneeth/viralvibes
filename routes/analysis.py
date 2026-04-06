"""
routes/analysis.py — Analysis page content.

The Analysis page is the primary entry point for playlist analysis.
It hosts the AnalysisFormCard and communicates the value of the tool
before and after a user submits a playlist.
"""

from fasthtml.common import *
from monsterui.all import *

from components.cards import AnalysisFormCard


# ---------------------------------------------------------------------------
# Hero
# ---------------------------------------------------------------------------


def _hero() -> Section:
    """Full-width hero at the top of the analysis page."""
    pills = [
        ("chart-bar", "Engagement Metrics"),
        ("trending-up", "Viral Patterns"),
        ("users", "Audience Insights"),
        ("zap", "Instant Results"),
    ]
    return Section(
        Div(
            # Eye-brow label
            Div(
                UkIcon("youtube", cls="w-4 h-4 text-red-500 mr-2"),
                Span("YouTube Playlist Intelligence", cls="text-sm font-medium text-red-600"),
                cls="inline-flex items-center bg-red-50 border border-red-100 rounded-full px-4 py-1 mb-6",
            ),
            H1(
                "Analyze Any YouTube Playlist",
                cls="text-5xl font-bold text-gray-900 leading-tight mb-4",
            ),
            P(
                "Paste a playlist URL and get deep insights into views, engagement, "
                "controversy scores, and viral patterns — in seconds.",
                cls="text-xl text-gray-500 max-w-2xl mx-auto mb-8",
            ),
            # Feature pills
            Div(
                *[
                    Div(
                        UkIcon(icon, cls="w-4 h-4 mr-2 text-red-500"),
                        Span(label, cls="text-sm font-medium text-gray-700"),
                        cls="inline-flex items-center bg-white border border-gray-200 rounded-full px-4 py-2 shadow-sm",
                    )
                    for icon, label in pills
                ],
                cls="flex flex-wrap justify-center gap-3",
            ),
            cls="text-center py-16 px-4 max-w-3xl mx-auto",
        ),
        cls="bg-gradient-to-b from-gray-50 to-white border-b border-gray-100",
    )


# ---------------------------------------------------------------------------
# "What you'll discover" cards (below the form)
# ---------------------------------------------------------------------------


def _insight_cards() -> Section:
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

    return Section(
        Div(
            H2(
                "What you'll discover",
                cls="text-2xl font-bold text-gray-900 text-center mb-2",
            ),
            P(
                "Every analysis produces a full interactive dashboard with these insights.",
                cls="text-gray-500 text-center mb-10",
            ),
            Div(
                *[
                    Card(
                        Div(
                            UkIcon(icon, cls="w-8 h-8 text-red-500 mb-4"),
                            H3(title, cls="text-lg font-semibold text-gray-900 mb-2"),
                            P(description, cls="text-gray-500 text-sm leading-relaxed"),
                            cls="p-6",
                        ),
                        cls="border border-gray-200 rounded-2xl shadow-sm hover:shadow-md transition-shadow",
                    )
                    for icon, title, description in cards
                ],
                cls="grid grid-cols-1 md:grid-cols-3 gap-6",
            ),
            cls="max-w-5xl mx-auto px-4 py-16",
        ),
    )


# ---------------------------------------------------------------------------
# Public page builder
# ---------------------------------------------------------------------------


def analysis_page_content() -> Div:
    """
    Full body content for the /analysis route (without nav/title wrapper).
    Composed of: hero → analysis form → insight cards.
    """
    return Div(
        _hero(),
        Div(
            AnalysisFormCard(),
            cls="max-w-3xl mx-auto px-4 -mt-6",
        ),
        _insight_cards(),
    )
