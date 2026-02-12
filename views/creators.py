"""
Creator Intelligence Dashboard - Analytics-first design for YouTube creators
Focused on what matters: Growth, Revenue, Engagement, Quality

Data collected by worker:
- current_subscribers, current_view_count, current_video_count (from YouTube API)
- engagement_score (calculated from recent video comments/likes)
- quality_grade (A+/A/B+/B/C based on engagement + subscriber size)
- country_code, channel_name, channel_thumbnail_url
- last_updated_at, last_synced_at (for freshness indicator)
- 30-day deltas (subscribers_change_30d, views_change_30d)

Creator perspective (Jimmy Donaldson style):
1. Growth trajectory (trending up/down?) - MOST IMPORTANT
2. Revenue potential (monthly earnings)
3. Engagement quality (audience size vs views ratio)
4. Video consistency (how many videos, posting frequency)
5. Ranking/position (competitive benchmark)
6. Quality assessment (why are they ranked this way?)
"""

import logging
from datetime import datetime
from urllib.parse import urlencode

from fasthtml.common import *
from monsterui.all import *

from utils import format_date_relative, format_number

logger = logging.getLogger(__name__)


# ============================================================================
# SHARED HELPERS
# ============================================================================


def _get_value(obj, key, default=0):
    """Safely get value from dict or Supabase object. Returns default if None."""
    if isinstance(obj, dict):
        value = obj.get(key, default)
    else:
        value = getattr(obj, key, default)
    return value if value is not None else default


# ============================================================================
# MAIN PAGE FUNCTION
# ============================================================================


def render_creators_page(
    creators: list[dict],
    sort: str = "subscribers",
    search: str = "",
    grade_filter: str = "all",
    stats: dict = None,
) -> Div:
    """
    Analytics-first creator discovery dashboard.
    Optimized for what creators care about: growth, revenue, engagement.
    Args:
        creators: List of creator dicts with stats and ranking
        sort: Sort criteria (subscribers, views, videos, engagement, quality)
        search: Search query for filtering by name
        grade_filter: Quality grade filter (all, A+, A, B+, B, C)
        stats: Aggregate statistics dict from backend
    """
    # Count creators by grade for filter badges
    grade_counts = _count_by_grade(creators)

    # Use provided stats or calculate from creators
    if stats is None:
        stats = {
            "total_subscribers": sum(
                _get_value(c, "current_subscribers", 0) for c in creators
            ),
            "total_views": sum(
                _get_value(c, "current_view_count", 0) for c in creators
            ),
            "avg_engagement": (
                sum(_get_value(c, "engagement_score", 0) for c in creators)
                / len(creators)
                if creators
                else 0
            ),
            "total_revenue": int(
                sum(
                    (_get_value(c, "current_view_count", 0) * 4) / 1000
                    for c in creators
                )
            ),
        }

    return Container(
        # Hero section with real stats
        _render_hero(len(creators), stats),
        # Filter controls (sticky bar)
        _render_filter_bar(search, sort, grade_filter, grade_counts),
        # Creators grid or empty state
        (
            _render_creators_grid(creators)
            if creators
            else _render_empty_state(search, grade_filter)
        ),
        cls=ContainerT.xl,
    )


def _render_hero(creator_count: int, stats: dict) -> Div:
    """Hero section with real statistics - statement design."""
    return Div(
        Div(
            H1(
                "Creator Intelligence",
                cls="text-5xl font-bold text-gray-900 tracking-tight",
            ),
            P(
                "Analytics for creators who want to grow.",
                cls="text-lg text-gray-600 mt-2",
            ),
            cls="mb-8",
        ),
        # Metric Strip - evenly spaced statement
        Div(
            Div(
                P(
                    "Creators Analyzed",
                    cls="text-xs font-semibold text-gray-500 uppercase tracking-wider",
                ),
                H2(
                    format_number(creator_count),
                    cls="text-4xl font-bold text-gray-900 mt-2",
                ),
                cls="text-center",
            ),
            Div(
                P(
                    "Total Subscribers",
                    cls="text-xs font-semibold text-gray-500 uppercase tracking-wider",
                ),
                H2(
                    format_number(stats.get("total_subscribers", 0)),
                    cls="text-4xl font-bold text-blue-600 mt-2",
                ),
                cls="text-center",
            ),
            Div(
                P(
                    "Avg Engagement",
                    cls="text-xs font-semibold text-gray-500 uppercase tracking-wider",
                ),
                H2(
                    f"{stats.get('avg_engagement', 0):.1f}%",
                    cls="text-4xl font-bold text-emerald-600 mt-2",
                ),
                cls="text-center",
            ),
            Div(
                P(
                    "Est. Monthly Revenue",
                    cls="text-xs font-semibold text-gray-500 uppercase tracking-wider",
                ),
                H2(
                    f"${format_number(stats.get('total_revenue', 0))}",
                    cls="text-4xl font-bold text-amber-600 mt-2",
                ),
                cls="text-center",
            ),
            cls="grid grid-cols-4 gap-8 py-8 border-t border-b border-gray-200",
        ),
        cls="bg-white rounded-lg border border-gray-200 p-8 mb-8",
    )


def _render_filter_bar(
    search: str, sort: str, grade_filter: str, grade_counts: dict
) -> Div:
    """Sticky filter bar with search, sort, grade pills."""

    grade_options = [
        ("all", "All", "🎯"),
        ("A+", "Elite", "👑"),
        ("A", "Star", "⭐"),
        ("B+", "Rising", "📈"),
        ("B", "Good", "💎"),
        ("C", "New", "🔍"),
    ]

    # Search form
    search_form = Form(
        Input(
            type="search",
            name="search",
            placeholder="Search creators...",
            value=search,
            cls="w-full px-4 py-2.5 rounded-lg border border-gray-300 focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-100",
            autofocus=bool(search),
        ),
        Input(type="hidden", name="sort", value=sort),
        Input(type="hidden", name="grade", value=grade_filter),
        method="GET",
        action="/creators",
        cls="flex-1",
    )

    # Sort select
    sort_form = Form(
        Select(
            Option(
                "📊 Most Subscribers",
                value="subscribers",
                selected=(sort == "subscribers"),
            ),
            Option("👀 Most Views", value="views", selected=(sort == "views")),
            Option(
                "🔥 Best Engagement",
                value="engagement",
                selected=(sort == "engagement"),
            ),
            Option("⭐ Quality Score", value="quality", selected=(sort == "quality")),
            Option("🆕 Recently Updated", value="recent", selected=(sort == "recent")),
            name="sort",
            cls="h-10 px-4 rounded-lg border border-gray-300 font-medium",
            onchange="this.form.submit()",
        ),
        Input(type="hidden", name="search", value=search),
        Input(type="hidden", name="grade", value=grade_filter),
        method="GET",
        action="/creators",
    )

    # Grade pills
    grade_pills = Div(
        *[
            A(
                f"{emoji} {label} ({grade_counts.get(val, 0)})",
                href=f"/creators?{urlencode({'sort': sort, 'search': search, 'grade': val})}",
                cls=(
                    "px-4 py-2 rounded-lg transition-all inline-block no-underline text-sm font-medium "
                    + (
                        "bg-blue-600 text-white shadow-md"
                        if grade_filter == val
                        else "bg-white border border-gray-200 hover:bg-gray-50 text-gray-700"
                    )
                ),
            )
            for val, label, emoji in grade_options
        ],
        cls="flex gap-2 flex-wrap",
    )

    # Container
    return Div(
        Div(cls="flex gap-4 mb-4")(search_form, sort_form),
        grade_pills,
        cls="sticky top-4 z-10 bg-white/95 backdrop-blur-sm p-6 rounded-lg border border-gray-200 shadow-sm space-y-4",
    )


def _render_creators_grid(creators: list[dict]) -> Grid:
    """Grid of creator cards using MonsterUI Grid."""
    return Grid(
        *[_render_creator_card(creator) for creator in creators],
        cols_xl=3,
        cols_lg=2,
        cols_md=1,
        cols_sm=1,
        gap=6,
    )


def _render_creator_card(creator: dict) -> Div:
    """
    Creator card - clean, data-driven design.

    Layout:
    [Thumbnail + Rank] | [Name + Badge]
    ────────────────────────────────────
    SUBSCRIBERS | VIEWS (large 2-col metrics)
    ────────────────────────────────────
    AVG/VID | VIDEOS | ENGAGEMENT | REVENUE (small 4-col metrics)
    ────────────────────────────────────
    30-Day Trend bar
    ────────────────────────────────────
    Updated · Analyze → (footer)
    """

    # Extract all data
    channel_id = _get_value(creator, "channel_id", "N/A")
    channel_name = _get_value(creator, "channel_name", "Unknown")
    channel_url = f"https://youtube.com/@{channel_id}"
    quality_grade = _get_value(creator, "quality_grade", "C")
    rank = _get_value(creator, "_rank", "—")
    thumbnail_url = (
        _get_value(creator, "channel_thumbnail_url")
        or _get_value(creator, "thumbnail_url")
        or "https://via.placeholder.com/64x64?text=No+Image"
    )

    # Ensure all numeric fields are actually numeric
    current_subs = int(_get_value(creator, "current_subscribers", 0) or 0)
    current_views = int(_get_value(creator, "current_view_count", 0) or 0)
    current_videos = int(_get_value(creator, "current_video_count", 0) or 0)
    subs_change = int(_get_value(creator, "subscribers_change_30d", 0) or 0)
    views_change = int(_get_value(creator, "views_change_30d", 0) or 0)
    engagement_score = float(_get_value(creator, "engagement_score", 0) or 0)

    # Calculations
    avg_views_per_video = (
        int(current_views / current_videos) if current_videos > 0 else 0
    )
    estimated_revenue = int((current_views * 4) / 1000)
    growth_rate = (subs_change / current_subs * 100) if current_subs > 0 else 0
    last_updated = _get_value(creator, "last_updated_at", "")

    # Grade colors and interpretation - muted/subtle
    grade_colors = {
        "A+": "bg-purple-200 text-purple-900",
        "A": "bg-blue-200 text-blue-900",
        "B+": "bg-cyan-200 text-cyan-900",
        "B": "bg-gray-200 text-gray-900",
        "C": "bg-gray-200 text-gray-700",
    }

    # Grade interpretation (signal)
    grade_signals = {
        "A+": ("Elite Performer", "⭐"),
        "A": ("Strong Creator", "✓"),
        "B+": ("Rising Star", "📈"),
        "B": ("Established", "●"),
        "C": ("New Creator", "○"),
    }

    grade_bg = grade_colors.get(quality_grade, "bg-gray-200 text-gray-700")
    grade_signal, grade_icon = grade_signals.get(quality_grade, ("Unrated", "?"))

    # Growth direction indicator
    growth_trend = "↑" if growth_rate > 1 else ("↓" if growth_rate < -1 else "→")
    growth_color = (
        "text-green-600"
        if growth_rate > 1
        else ("text-red-600" if growth_rate < -1 else "text-gray-500")
    )

    # Growth signal (interpretation) - MOVE THIS BEFORE return Div
    if growth_rate > 2:
        growth_signal = ("Growing", "↑", "bg-emerald-100 text-emerald-700")
    elif growth_rate < -2:
        growth_signal = ("Declining", "↓", "bg-red-100 text-red-700")
    else:
        growth_signal = ("Stable", "→", "bg-slate-100 text-slate-700")

    return Div(
        # Header: Thumbnail + Name + Grade
        Div(
            # Thumbnail with rank badge overlay
            Div(
                Img(
                    src=thumbnail_url,
                    alt=channel_name,
                    cls="w-16 h-16 rounded-lg object-cover",
                ),
                # Subtle rank badge
                Div(
                    f"#{rank}",
                    cls="absolute -top-2 -right-2 bg-gray-900 text-white text-xs font-bold w-7 h-7 rounded-full flex items-center justify-center",
                ),
                cls="relative",
            ),
            # Channel info
            Div(
                Div(
                    H3(channel_name, cls="font-semibold text-gray-900 truncate mb-0.5"),
                    P(
                        f"{format_number(current_subs)} subscribers · {current_videos} videos",
                        cls="text-xs text-gray-600",
                    ),
                    cls="flex-1",
                ),
                # Quality grade badge (top right) with interpretation
                Div(
                    Div(
                        P(grade_icon, cls="text-lg"),
                        P(quality_grade, cls="text-xs font-bold"),
                        cls="flex flex-col items-center",
                    ),
                    Div(
                        P(grade_signal, cls="text-xs font-semibold text-right"),
                        cls="text-right",
                    ),
                    cls=f"px-3 py-2 rounded-lg {grade_bg} flex gap-2",
                ),
                cls="flex justify-between items-start gap-3 flex-1",
            ),
            cls="flex gap-3 mb-4 pb-4 border-b border-gray-100",
        ),
        # PRIMARY METRICS (2-column: Subs + Views)
        Div(
            # Subscribers
            Div(
                P(
                    "SUBSCRIBERS",
                    cls="text-xs font-semibold text-gray-600 uppercase tracking-wide",
                ),
                H2(
                    format_number(current_subs),
                    cls="text-3xl font-bold text-blue-600 mt-1",
                ),
                P(
                    f"{'+' if subs_change > 0 else ''}{format_number(subs_change)} (30d)",
                    cls="text-xs text-gray-600 mt-1",
                ),
                cls="bg-blue-50 rounded-lg p-3 text-center",
            ),
            # Views
            Div(
                P(
                    "VIEWS",
                    cls="text-xs font-semibold text-gray-600 uppercase tracking-wide",
                ),
                H2(
                    format_number(current_views),
                    cls="text-3xl font-bold text-purple-600 mt-1",
                ),
                P(
                    f"{'+' if views_change > 0 else ''}{format_number(views_change)} (30d)",
                    cls="text-xs text-gray-600 mt-1",
                ),
                cls="bg-purple-50 rounded-lg p-3 text-center",
            ),
            cls="grid grid-cols-2 gap-3 mb-4",
        ),
        # SECONDARY METRICS (4-column)
        Div(
            Div(
                P("AVG", cls="text-xs font-semibold text-gray-600 uppercase"),
                P(
                    f"{format_number(avg_views_per_video)}",
                    cls="text-lg font-bold text-gray-900 mt-1",
                ),
                P("per video", cls="text-xs text-gray-500"),
                cls="bg-gray-50 rounded-lg p-3 text-center",
            ),
            Div(
                P("VIDEOS", cls="text-xs font-semibold text-gray-600 uppercase"),
                P(
                    format_number(current_videos),
                    cls="text-lg font-bold text-gray-900 mt-1",
                ),
                P("published", cls="text-xs text-gray-500"),
                cls="bg-gray-50 rounded-lg p-3 text-center",
            ),
            Div(
                P("ENGAGEMENT", cls="text-xs font-semibold text-gray-600 uppercase"),
                Div(
                    P(
                        f"{engagement_score:.1f}%",
                        cls="text-lg font-bold text-gray-900 mt-1",
                    ),
                    cls="flex items-end gap-2",
                ),
                P(
                    "on videos" if engagement_score > 0 else "no engagement",
                    cls="text-xs text-gray-500 mt-1",
                ),
                cls="bg-gray-50 rounded-lg p-3 text-center",
            ),
            Div(
                P(
                    "REVENUE",
                    cls="text-xs font-semibold text-green-700 uppercase font-bold",
                ),
                P(
                    f"${format_number(estimated_revenue)}",
                    cls="text-lg font-bold text-green-600 mt-1",
                ),
                P("/month", cls="text-xs text-green-600"),
                cls="bg-green-50 rounded-lg p-3 text-center",
            ),
            cls="grid grid-cols-4 gap-3 mb-4",
        ),
        # GROWTH TREND
        Div(
            Div(
                P("30-DAY TREND", cls="text-xs font-semibold text-gray-600"),
                Div(
                    P(
                        f"{growth_signal[1]} {growth_rate:+.1f}%",
                        cls=f"text-sm font-bold text-gray-900",
                    ),
                    Span(
                        growth_signal[0],
                        cls=f"px-2 py-1 text-xs font-semibold rounded-full {growth_signal[2]}",
                    ),
                    cls="flex items-center gap-2",
                ),
                cls="flex justify-between items-center mb-3",
            ),
            # Simple growth bar
            Div(
                Div(
                    cls=(
                        "h-2 bg-green-500 rounded-full"
                        if growth_rate >= 0
                        else "h-2 bg-red-500 rounded-full"
                    ),
                    style=f"width: {min(100, max(0, abs(growth_rate) * 5))}%",
                ),
                cls="w-full h-2 bg-gray-200 rounded-full overflow-hidden",
            ),
            cls=(
                "bg-green-50 rounded-lg p-3 mb-4"
                if growth_rate >= 0
                else "bg-red-50 rounded-lg p-3 mb-4"
            ),
        ),
        # FOOTER: Timestamp + Action
        Div(
            Div(
                Span("🕐", cls="mr-1.5"),
                P(format_date_relative(last_updated), cls="text-xs text-gray-500"),
                cls="flex items-center",
            ),
            A(
                "View Channel →",
                href=channel_url,
                target="_blank",
                rel="noopener noreferrer",
                cls="text-xs font-semibold text-blue-600 hover:text-blue-700 no-underline",
            ),
            cls="flex justify-between items-center pt-3 border-t border-gray-100 text-sm",
        ),
        cls="bg-white rounded-lg border border-gray-200 p-4 hover:shadow-md hover:scale-[1.02] transition-all duration-300 cursor-pointer",
    )


def _render_empty_state(search: str, grade_filter: str) -> Div:
    """Empty state when no creators found."""
    if search or grade_filter != "all":
        return Card(
            Div(
                Span("🔍", cls="text-6xl block text-center mb-4"),
                H2("No creators found", cls="text-center text-2xl font-bold mb-2"),
                P(
                    "Try adjusting your filters or search terms",
                    cls="text-center text-gray-600 mb-6",
                ),
                Div(
                    A(Button("Clear Filters", cls=ButtonT.secondary), href="/creators"),
                    cls="flex justify-center",
                ),
                cls="space-y-4 p-12",
            ),
            cls="bg-gray-50 max-w-md mx-auto",
        )
    else:
        return Card(
            Div(
                Span("🚀", cls="text-6xl block text-center mb-4"),
                H2(
                    "No creators discovered yet",
                    cls="text-center text-2xl font-bold mb-2",
                ),
                P(
                    "Analyze YouTube playlists to automatically discover and track creators.",
                    cls="text-center text-gray-600 mb-6",
                ),
                Div(
                    A(
                        Button("📊 Analyze Your First Playlist", cls=ButtonT.primary),
                        href="/#analyze-section",
                    ),
                    cls="flex justify-center",
                ),
                cls="space-y-4 p-12",
            ),
            cls="bg-white max-w-md mx-auto border border-gray-200",
        )


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def _count_by_grade(creators: list[dict]) -> dict:
    """Count creators by quality grade for filter pills."""
    counts = {"all": len(creators), "A+": 0, "A": 0, "B+": 0, "B": 0, "C": 0}
    for creator in creators:
        grade = _get_value(creator, "quality_grade", "C")
        if grade in counts:
            counts[grade] += 1
    return counts


def _estimate_monthly_revenue(current_views: int, views_30d: int = 0) -> float:
    """
    Estimate monthly revenue based on YouTube CPM model.

    Uses a baseline CPM of $4 per 1000 views. If 30-day view data is available,
    uses that for direct estimation. Otherwise, assumes current_views represents
    lifetime views and extrapolates monthly average.

    Args:
        current_views: Lifetime or total view count
        views_30d: Optional 30-day view count for direct calculation

    Returns:
        Estimated monthly revenue in USD
    """
    if views_30d > 0:
        # Direct calculation from 30-day data
        return (views_30d * 4) / 1000
    else:
        # Fallback: assume current_views is lifetime, extrapolate monthly average
        # This is a rough estimate and should ideally use historical data
        return (current_views * 4) / 1000 / 30 if current_views > 0 else 0
