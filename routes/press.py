"""
Press Kit page — public route for journalists, partners, and analysts.

Renders a one-glance overview of ViralVibes: company description, live
coverage stats (creator count, country coverage, top categories), and a
press contact. Stats are queried at request time with graceful fallbacks
so a DB hiccup degrades to a static-content page instead of a 500.
"""

import logging

from fasthtml.common import *
from monsterui.all import *

from components.page_layout import (
    FeatureGrid,
    InfoCard,
    LinkCard,
    PageSection,
    StaticPage,
)
from constants import CONTACT_EMAIL
from db_lists import (
    get_top_categories_with_counts,
    get_top_countries_with_counts,
)
from utils.formatting import format_number

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Live stats — graceful fallbacks so the page is never broken by a DB blip.
# ---------------------------------------------------------------------------


def _safe_hero_stats() -> dict:
    """Return creator hero stats, or a conservative fallback on any failure."""
    try:
        # Imported lazily so a Supabase import-time error can't kill the page.
        from db import get_creator_hero_stats

        return get_creator_hero_stats() or {}
    except Exception:
        logger.warning("press: get_creator_hero_stats failed", exc_info=True)
        return {}


def _safe_top_categories(limit: int = 6) -> list[tuple[str, int]]:
    try:
        return get_top_categories_with_counts(limit=limit) or []
    except Exception:
        logger.warning("press: get_top_categories_with_counts failed", exc_info=True)
        return []


def _safe_top_countries(limit: int = 6) -> list[tuple[str, int]]:
    try:
        return get_top_countries_with_counts(limit=limit) or []
    except Exception:
        logger.warning("press: get_top_countries_with_counts failed", exc_info=True)
        return []


def _format_count(n: int | float | None) -> str:
    """Press-page count formatter — ``format_number`` with an em-dash for missing data.

    Wraps the shared ``utils.formatting.format_number`` so the press page
    matches numbers shown elsewhere (e.g. nav, hero) instead of inventing
    its own "807K" vs "807.0K" rounding rules.
    """
    if not n:
        return "—"
    return format_number(n)


# ---------------------------------------------------------------------------
# Page content
# ---------------------------------------------------------------------------


def press_page_content() -> Div:
    stats = _safe_hero_stats()
    top_categories = _safe_top_categories(limit=6)
    top_countries = _safe_top_countries(limit=6)

    total_creators = _format_count(stats.get("total_creators"))
    total_countries = stats.get("total_countries") or 0
    total_languages = stats.get("total_languages") or 0

    # Headline stat grid — three numbers a reporter can lift verbatim.
    headline_stats = FeatureGrid(
        [
            (
                f"{total_creators} creators tracked",
                "YouTube channels in the ViralVibes index, refreshed on a rolling basis.",
            ),
            (
                f"{total_countries}+ countries",
                "Geographies represented in the creator dataset, normalised for fair "
                "cross-market comparison.",
            ),
            (
                f"{total_languages}+ languages",
                "Spoken-content languages covered, enabling discovery beyond English-first "
                "creator lists.",
            ),
        ],
        cols=3,
    )

    # Top categories — only render if we have data, so the page never shows
    # an empty grid.
    category_items = [
        (label or "—", f"{_format_count(count)} creators") for label, count in top_categories
    ]
    category_block = (
        FeatureGrid(category_items, cols=3)
        if category_items
        else P(
            "Category breakdown is temporarily unavailable.",
            cls="text-muted-foreground text-sm",
        )
    )

    # Top countries rendered as a compact comma-joined sentence — feels more
    # editorial than another grid in the same page.
    country_sentence = (
        ", ".join(f"{code.upper()} ({_format_count(count)})" for code, count in top_countries)
        if top_countries
        else "Country breakdown is temporarily unavailable."
    )

    return StaticPage(
        "Press Kit",
        # Lede — the one-paragraph pitch a journalist can quote.
        PageSection(
            "About ViralVibes",
            "ViralVibes is a creator intelligence platform for brands and agencies running "
            "YouTube campaigns. Instead of picking creators by subscriber count, customers get "
            "ranked lists built on real engagement signals, growth velocity, and consistency — "
            "the inputs that actually predict whether a campaign will convert. Anyone can also "
            "analyze a public playlist for free, with no account required.",
            variant="lead",
            eyebrow="01 — Company",
        ),
        # Headline stats — the numbers a reporter wants up top.
        PageSection(
            "By the numbers",
            "Live coverage figures from the ViralVibes index.",
            variant="centered",
            eyebrow="02 — Scale",
        ),
        headline_stats,
        # Top categories block.
        PageSection(
            "Where the coverage is deepest",
            "The largest categories in our index by tracked-creator count. Numbers update as "
            "the index grows.",
            variant="split",
            eyebrow="03 — Categories",
        ),
        category_block,
        # Top countries — short editorial line, not a grid.
        PageSection(
            "Top markets",
            country_sentence,
            variant="accent",
            eyebrow="04 — Geography",
        ),
        # Founding thesis — quotable one-liner.
        PageSection(
            "The thesis",
            "Subscriber counts lie. Engagement quality doesn't. ViralVibes was built to give "
            "marketers a defensible answer to the question 'will this creator actually convert?' "
            "— before the budget is spent, not after.",
            variant="lead",
            eyebrow="05 — Editorial",
        ),
        # Press contact + assets.
        PageSection(
            "Press contact",
            "For interviews, custom data pulls, or product questions, please reach out below. "
            "We typically respond within one business day.",
            variant="centered",
            eyebrow="06 — Contact",
        ),
        Div(
            LinkCard(
                "Email the team",
                "Press, analyst, and partnership enquiries.",
                CONTACT_EMAIL,
                f"mailto:{CONTACT_EMAIL}?subject=Press%20enquiry%20—%20ViralVibes",
                variant="accent",
            ),
            InfoCard(
                "Product screenshots",
                P(
                    "High-resolution screenshots and brand assets are available on request — "
                    "email the address above and we will share a download link.",
                    cls="text-muted-foreground leading-relaxed text-sm",
                ),
                variant="tinted",
            ),
            cls="grid grid-cols-1 md:grid-cols-2 gap-5",
        ),
    )
