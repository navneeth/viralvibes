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

from __future__ import annotations

from fasthtml.common import *
from monsterui.all import *

from components.buttons import FeaturePill
from components.cards import AnalysisFormCard
from constants import KNOWN_PLAYLISTS
from db import get_user_dashboards
from utils import format_date_relative, format_number


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
                        UkIcon(icon, cls="w-8 h-8 text-primary mb-4"),
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
# Sample playlists showcase — static, zero DB cost, shown to all visitors
# ---------------------------------------------------------------------------

# Icon to show alongside each playlist; derived from keywords in the title.
_PLAYLIST_ICONS: list[tuple[str, str]] = [
    ("music", "music"),
    ("film", "movie"),
    ("trending-up", "viral"),
    ("gamepad-2", "game"),  # may fall back to default
    ("trophy", "nfl"),
    ("trophy", "sport"),
    ("mic-2", "remix"),
    ("mic-2", "song"),
]
_DEFAULT_ICON = "play-square"


def _playlist_icon(title: str) -> str:
    title_lower = title.lower()
    for icon, keyword in _PLAYLIST_ICONS:
        if keyword in title_lower:
            return icon
    return _DEFAULT_ICON


def _sample_row(playlist: dict) -> Div:
    """Single action row for one sample playlist."""
    title = playlist.get("title", "Untitled Playlist")
    url = playlist.get("url", "")
    video_count = playlist.get("video_count", 0)
    icon = _playlist_icon(title)

    # JS: fill the input and scroll the form into view.
    # Defensive: null-check elements before accessing to avoid errors if template changes.
    _js = (
        "const urlInput = document.getElementById('playlist_url'); "
        f"if (urlInput) urlInput.value = {url!r}; "
        "const form = document.getElementById('analysis-form'); "
        "if (form) form.scrollIntoView({behavior:'smooth',block:'start'});"
    )

    return Div(
        # Icon
        UkIcon(icon, cls="w-5 h-5 text-muted-foreground flex-none"),
        # Title
        Span(title, cls="flex-1 text-sm font-medium text-foreground truncate"),
        # Video count badge
        Span(
            f"{video_count:,} videos",
            cls="hidden sm:inline-flex text-xs text-muted-foreground whitespace-nowrap",
        ),
        # Analyze button
        Button(
            "Analyze →",
            type="button",
            cls="text-xs font-medium text-primary hover:underline bg-transparent border-0 cursor-pointer p-0 whitespace-nowrap",
        ),
        cls=(
            "flex items-center gap-3 px-3 py-2.5 -mx-3 rounded-lg "
            "hover:bg-muted/50 transition-colors "
            "border-b border-border last:border-b-0 cursor-pointer"
        ),
        onclick=_js,
        role="button",
        tabindex="0",
    )


def _sample_playlists() -> Div | None:
    """Sample playlist rows for all visitors. Returns None if list is empty."""
    if not KNOWN_PLAYLISTS:
        return None

    return Div(
        # Section header
        Div(
            Div(
                UkIcon("sparkles", cls="w-4 h-4 text-muted-foreground"),
                H2(
                    "Popular playlists to try",
                    cls="text-base font-semibold text-foreground",
                ),
                cls="flex items-center gap-2",
            ),
            P(
                "Click any row to prefill the form above.",
                cls="text-xs text-muted-foreground",
            ),
            cls="flex items-center justify-between mb-2",
        ),
        # Rows
        Div(
            *[_sample_row(p) for p in KNOWN_PLAYLISTS],
            cls="px-3",
        ),
        cls="border border-border rounded-2xl p-4 mb-8",
    )


# ---------------------------------------------------------------------------
# Recent analyses — compact list rows, logged-in users only
# ---------------------------------------------------------------------------

_MAX_RECENT = 5


def _recent_row(dashboard: dict) -> A:
    """Single compact row linking to a saved dashboard."""
    dashboard_id = dashboard.get("dashboard_id", "")
    title = dashboard.get("title") or "Untitled Playlist"
    channel_name = dashboard.get("channel_name") or "Unknown Channel"
    thumbnail = dashboard.get("channel_thumbnail") or "/static/favicon.jpeg"
    view_count = dashboard.get("view_count") or 0
    video_count = dashboard.get("video_count") or 0
    processed_on = dashboard.get("processed_on")

    return A(
        # Thumbnail chip
        Img(
            src=thumbnail,
            alt=title,
            cls="w-10 h-10 rounded-md object-cover flex-none bg-muted",
            onerror="this.src='/static/favicon.jpeg'",
        ),
        # Title + channel
        Div(
            P(title, cls="text-sm font-medium text-foreground truncate leading-tight"),
            P(channel_name, cls="text-xs text-muted-foreground truncate"),
            cls="flex-1 min-w-0",
        ),
        # Stats (hidden on xs, visible sm+)
        Div(
            Span(format_number(view_count), cls="font-medium text-foreground text-xs"),
            Span(" views", cls="text-muted-foreground text-xs"),
            cls="hidden sm:flex items-baseline gap-0.5 whitespace-nowrap",
        ),
        Div(
            Span(str(video_count), cls="font-medium text-foreground text-xs"),
            Span(" videos", cls="text-muted-foreground text-xs"),
            cls="hidden md:flex items-baseline gap-0.5 whitespace-nowrap",
        ),
        # Date
        Span(
            format_date_relative(processed_on),
            cls="text-xs text-muted-foreground whitespace-nowrap",
        ),
        # Chevron
        UkIcon(
            "chevron-right",
            cls="w-4 h-4 text-muted-foreground flex-none group-hover:text-foreground transition-colors",
        ),
        href=f"/d/{dashboard_id}",
        cls=(
            "flex items-center gap-3 px-3 py-3 -mx-3 rounded-lg "
            "hover:bg-muted/50 transition-colors group no-underline "
            "border-b border-border last:border-b-0"
        ),
    )


def _recent_analyses(user_id: str) -> Div | None:
    """Compact recent-analyses list for the logged-in user.

    Returns None when the user has no dashboards yet so the caller
    can omit the section entirely.
    """
    dashboards = [
        d
        for d in get_user_dashboards(user_id, sort="recent")[:_MAX_RECENT]
        if d.get("dashboard_id")
    ]
    if not dashboards:
        return None

    return Div(
        # Section header
        Div(
            Div(
                UkIcon("history", cls="w-4 h-4 text-muted-foreground"),
                H2(
                    "Your recent analyses",
                    cls="text-base font-semibold text-foreground",
                ),
                cls="flex items-center gap-2",
            ),
            A(
                "View all",
                UkIcon("arrow-right", cls="w-3 h-3"),
                href="/me/dashboards",
                cls="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors no-underline",
            ),
            cls="flex items-center justify-between mb-2",
        ),
        # Rows
        Div(
            *[_recent_row(d) for d in dashboards],
            cls="px-3",
        ),
        cls="border border-border rounded-2xl p-4 mb-12",
    )


# ---------------------------------------------------------------------------
# Public page builder
# ---------------------------------------------------------------------------


def analysis_page_content(user_id: str | None = None) -> Div:
    """
    Full body content for the /analysis route (without nav/title wrapper).
    Composed of: page header → analysis form (compact) → insight cards
                 → recent analyses (logged-in users only).
    Returns a bare Div — the caller (route handler) provides the Container.

    Args:
        user_id: Session user_id, or None for anonymous visitors.
    """
    recent = _recent_analyses(user_id) if user_id else None
    samples = _sample_playlists()
    return Div(
        _page_header(),
        AnalysisFormCard(compact=True),
        _insight_cards(),
        *([] if samples is None else [samples]),
        *([] if recent is None else [recent]),
    )
