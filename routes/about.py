"""
About page — mission, product overview, and company story.
"""

from fasthtml.common import *
from monsterui.all import *

from components.page_layout import (
    FeatureGrid,
    PageSection,
    PullQuote,
    StaticPage,
)


# ---------------------------------------------------------------------------
# About Page Content
# ---------------------------------------------------------------------------


def about_page_content() -> Div:
    return StaticPage(
        "Creator intelligence, built for decisions.",
        # Lede — oversized first paragraph carries the mission.
        PageSection(
            "Our Mission",
            "We believe creator selection shouldn't be a guessing game. Brands spend thousands "
            "on creators that don't deliver, while the best creators are hard to discover. "
            "ViralVibes reverses this by giving agencies and brands data-driven insights to find, "
            "evaluate, and shortlist creators with confidence.",
            "We're building the intelligence layer for creator marketing — turning public YouTube "
            "data into actionable insights that save time and improve campaign ROAS.",
            variant="lead",
            eyebrow="01 — Mission",
        ),
        # Asymmetric split — heading anchored left, narrative right.
        PageSection(
            "The Problem We Solve",
            "Creator research is broken. It's time-consuming, manual, and often relies on outdated "
            "metrics like subscriber count. Agencies spend days building spreadsheets. Brands overpay "
            "creators with inflated engagement. The best creators are invisible.",
            "ViralVibes eliminates this friction by analyzing 150+ countries of public YouTube data "
            "in real time, revealing who's truly viral and whose audience actually converts.",
            variant="split",
            eyebrow="02 — Problem",
        ),
        # Pull-quote — strong editorial breath between sections.
        PullQuote(
            "Subscriber counts lie. Engagement quality doesn't.",
            attribution="The ViralVibes thesis",
        ),
        # Default rhythm.
        PageSection(
            "How ViralVibes Works",
            "Start with curated, ranked creator lists filtered by niche, country, and audience "
            "quality. Review detailed creator profiles without needing channel access. Paste any "
            "playlist URL for instant viral analysis. Export ranked shortlists for your campaigns. "
            "All data is public, analyzed in seconds, and actionable.",
            eyebrow="03 — Workflow",
        ),
        # Centered spotlight introduces the feature grid.
        PageSection(
            "What Makes Us Different",
            "Six pillars that separate signal from vanity.",
            variant="centered",
            eyebrow="04 — Differentiators",
        ),
        FeatureGrid(
            [
                (
                    "Real Engagement Metrics",
                    "We analyze view-to-like ratios, comment density, and consistency — not just "
                    "vanity metrics. See which audiences are actually engaged.",
                ),
                (
                    "Growth Velocity",
                    "Who is rising now vs. plateauing? Reach out before their rates double — or "
                    "before they disappear.",
                ),
                (
                    "ROAS-Correlated Signals",
                    "Our metrics correlate with purchase intent and campaign performance, not just "
                    "impressions.",
                ),
                (
                    "Any Public Channel",
                    "No partnerships required. Analyze any creator's public data across 150+ "
                    "countries instantly.",
                ),
                (
                    "Agency Workflow",
                    "CSV/JSON exports, bulk analysis, and campaign shortlists designed for how "
                    "agencies actually work.",
                ),
                (
                    "Privacy-First",
                    "All analysis is on public data. No scraping, no API abuse, no cookie tracking.",
                ),
            ]
        ),
        # Accent — sets audience apart from the surrounding flow.
        PageSection(
            "Who Uses ViralVibes",
            "Marketing agencies building creator rosters. Brand managers finding influencers for "
            "campaigns. Creator networks benchmarking their talent. Anyone who needs to understand "
            "YouTube virality fast.",
            variant="accent",
            eyebrow="05 — Audience",
        ),
        # Split again to keep the rhythm syncopated.
        PageSection(
            "Technology",
            "ViralVibes is built with a Python backend, real-time YouTube data processing, and a "
            "privacy-first frontend. We process public data only, with no cookies or cross-site "
            "tracking. Infrastructure is hosted on cloud infrastructure with EU data residency for "
            "GDPR compliance.",
            variant="split",
            eyebrow="06 — Stack",
        ),
        # Numbered close — large faded "07" anchors the final beat.
        PageSection(
            "What's Next",
            "We're expanding analysis to TikTok, Instagram, and emerging platforms. We're adding "
            "predictive virality scoring, audience overlap detection, and campaign performance "
            "tracking. Our goal is to become the standard intelligence layer for creator marketing.",
            variant="numbered",
            number="07",
            eyebrow="Roadmap",
        ),
        subtitle=(
            "Learn more about ViralVibes and our mission to democratize creator intelligence."
        ),
        eyebrow="About",
    )
