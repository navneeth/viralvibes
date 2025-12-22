# routes/homepage.py
from fasthtml.common import *
from monsterui.all import *

from constants import FLEX_COL, SECTION_BASE
from ui_components import benefit, section_header, section_wrapper


def how_it_works_section():
    """Step-by-step workflow section"""
    steps_msg = [
        (
            "1️⃣ Submit Playlist URL",
            "Paste your YouTube playlist link into the analysis form.",
        ),
        (
            "2️⃣ Preview",
            "See the playlist title, channel name, and thumbnail instantly.",
        ),
        (
            "3️⃣ Deep Analysis",
            "We crunch video stats—views, likes, dislikes, comments, engagement, and controversy.",
        ),
        (
            "4️⃣  Results Dashboard",
            "Get a detailed table and dashboard with trends and viral signals.",
        ),
    ]
    return section_wrapper(
        (
            Div(
                section_header(
                    "HOW IT WORKS",
                    "Analyze any YouTube playlist in seconds.",
                    "ViralVibes guides you through a simple, step-by-step workflow to decode YouTube trends and performance.",
                ),
                cls="max-w-3xl w-full mx-auto flex-col items-center text-center gap-6 mb-8 lg:mb-8",
            ),
            Div(
                *[benefit(title, content) for title, content in steps_msg],
                cls=f"{FLEX_COL} w-full lg:flex-row gap-4 items-center lg:gap-8 max-w-7xl mx-auto justify-center",
            ),
        ),
        bg_color="red-700",
        flex=False,
    )


def features_section():
    """Modern features grid using MonsterUI Card patterns."""
    feature_items = [
        ("bolt", "Real-Time Analytics", "Live data processing—no waiting for reports."),
        (
            "chart-bar",
            "Viral Pattern Detection",
            "Spot engagement spikes and controversy signals.",
        ),
        ("users", "Creator Insights", "Understand audience behavior beyond raw views."),
        ("zap", "Instant Results", "Cached analysis for lightning-fast reloads."),
        ("lock", "Privacy First", "No data stored—your playlists stay private."),
        ("download", "CSV/JSON Exports", "Download full data for offline analysis."),
    ]

    cards = [
        Card(
            Div(
                UkIcon(icon, cls="w-10 h-10 text-red-600 mb-4"),
                H4(title, cls="text-lg font-semibold text-gray-900 mb-2"),
                P(desc, cls="text-sm text-gray-600"),
                cls="flex flex-col items-center text-center h-full",
            ),
            cls=(CardT.hover, "p-6 transition-all duration-300"),
        )
        for icon, title, desc in feature_items
    ]

    return Section(
        Container(
            H2("Key Features", cls="text-3xl font-bold text-center mb-4"),
            P(
                "Everything you need to decode YouTube virality",
                cls="text-center text-gray-600 mb-12",
            ),
            Grid(
                *cards,
                cols="1 md:2 lg:3",
                gap=8,
                cls="max-w-7xl mx-auto",
            ),
        ),
        cls="py-16 bg-gray-50",
        id="features-section",
    )
