"""
About page — mission, product overview, and company story.
"""

from fasthtml.common import *
from monsterui.all import *


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _about_page(title: str, *sections) -> Div:
    """Wraps an about page in a consistent container."""
    return Div(
        # Page header
        Div(
            H1(title, cls="text-3xl font-bold text-foreground mb-2"),
            P(
                "Learn more about ViralVibes and our mission to democratize creator intelligence.",
                cls="text-sm text-muted-foreground",
            ),
            cls="mb-10 pb-8 border-b border-border",
        ),
        # Sections
        Div(*sections, cls="space-y-12"),
        cls="max-w-3xl mx-auto px-4 py-16",
    )


def _section(heading: str, *paragraphs) -> Div:
    return Div(
        *([H2(heading, cls="text-xl font-semibold text-foreground mb-3")] if heading else []),
        *[P(text, cls="text-muted-foreground leading-relaxed mb-3") for text in paragraphs],
    )


def _feature_grid(items: list[tuple[str, str]]) -> Div:
    """Creates a grid of feature items (title, description)."""
    return Div(
        *[
            Div(
                H3(title, cls="text-lg font-semibold text-foreground mb-2"),
                P(desc, cls="text-muted-foreground leading-relaxed"),
                cls="p-4 border border-border rounded-lg",
            )
            for title, desc in items
        ],
        cls="grid grid-cols-1 md:grid-cols-2 gap-6",
    )


# ---------------------------------------------------------------------------
# About Page Content
# ---------------------------------------------------------------------------


def about_page_content() -> Div:
    return _about_page(
        "About ViralVibes",
        _section(
            "Our Mission",
            "We believe creator selection shouldn't be a guessing game. Brands spend thousands on creators "
            "that don't deliver, while the best creators are hard to discover. ViralVibes reverses this "
            "by giving agencies and brands data-driven insights to find, evaluate, and shortlist creators with confidence.",
            "We're building the intelligence layer for creator marketing — turning public YouTube data into "
            "actionable insights that save time and improve campaign ROAS.",
        ),
        _section(
            "The Problem We Solve",
            "Creator research is broken. It's time-consuming, manual, and often relies on outdated metrics like subscriber count. "
            "Agencies spend days building spreadsheets. Brands overpay creators with inflated engagement. The best creators are invisible.",
            "ViralVibes eliminates this friction by analyzing 150+ countries of public YouTube data in real time, "
            "revealing who's truly viral and whose audience actually converts.",
        ),
        _section(
            "How ViralVibes Works",
            "Start with curated, ranked creator lists filtered by niche, country, and audience quality. "
            "Review detailed creator profiles without needing channel access. Paste any playlist URL for instant viral analysis. "
            "Export ranked shortlists for your campaigns. All data is public, analyzed in seconds, and actionable.",
        ),
        _section(
            "What Makes Us Different",
        ),
        _feature_grid(
            [
                (
                    "Real Engagement Metrics",
                    "We analyze view-to-like ratios, comment density, and consistency — not just vanity metrics. "
                    "See which audiences are actually engaged.",
                ),
                (
                    "Growth Velocity",
                    "Who is rising now vs. plateauing? Reach out before their rates double — or before they disappear.",
                ),
                (
                    "ROAS-Correlated Signals",
                    "Our metrics correlate with purchase intent and campaign performance, not just impressions.",
                ),
                (
                    "Any Public Channel",
                    "No partnerships required. Analyze any creator's public data across 150+ countries instantly.",
                ),
                (
                    "Agency Workflow",
                    "CSV/JSON exports, bulk analysis, and campaign shortlists designed for how agencies actually work.",
                ),
                (
                    "Privacy-First",
                    "All analysis is on public data. No scraping, no API abuse, no cookie tracking.",
                ),
            ]
        ),
        _section(
            "Who Uses ViralVibes",
            "Marketing agencies building creator rosters. Brand managers finding influencers for campaigns. "
            "Creator networks benchmarking their talent. Anyone who needs to understand YouTube virality fast.",
        ),
        _section(
            "Technology",
            "ViralVibes is built with Python backend, real-time YouTube data processing, and a privacy-first frontend. "
            "We process public data only, with no cookies or cross-site tracking. Infrastructure is hosted on Cloud Infrastructure "
            "with EU data residency for GDPR compliance.",
        ),
        _section(
            "What's Next",
            "We're expanding analysis to TikTok, Instagram, and emerging platforms. We're adding predictive virality scoring, "
            "audience overlap detection, and campaign performance tracking. Our goal is to become the standard intelligence layer "
            "for creator marketing.",
        ),
    )
