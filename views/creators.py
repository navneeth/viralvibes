"""
Creator Intelligence Dashboard - discover and explore top YouTube creators.
Simplified version with tested patterns from MonsterUI examples

Key features:
- Analytics-first design competing with SocialBlade
- Reuses MonsterUI components (Grid, Card, Select, etc.)
- 3-tier information hierarchy
- Growth metrics inline + visual indicators
- Revenue estimation prominent
- Color-coded metrics for fast scanning
"""

import logging
from datetime import datetime
from urllib.parse import urlencode

from fasthtml.common import *
from monsterui.all import *

from utils import format_date_relative, format_number

logger = logging.getLogger(__name__)


# ============================================================================
# MAIN PAGE FUNCTION
# ============================================================================


def render_creators_page(
    creators: list[dict],
    sort: str = "subscribers",
    search: str = "",
    grade_filter: str = "all",
) -> Div:
    """
    Analytics-first creator discovery dashboard.
    Competes with SocialBlade through superior UX and insights.

    Args:
        creators: List of creator dicts with stats
        sort: Sort criteria (subscribers, views, videos, engagement, quality)
        search: Search query for filtering by name
        grade_filter: Quality grade filter (all, A+, A, B+, B, C)
    """
    # Count creators by grade for filter badges
    grade_counts = _count_by_grade(creators)

    return Container(
        # Hero section with value prop
        _render_hero(len(creators)),
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


def _render_hero(creator_count: int) -> Div:
    """Hero section with quick stats."""
    return Div(
        H1(
            "Creator Intelligence Platform",
            cls="text-5xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-600 via-purple-600 to-pink-600 text-center",
        ),
        P(
            "Track growth, estimate earnings, discover rising creators",
            cls="text-gray-600 text-center mt-2 mb-8",
        ),
        # Quick stats - using simple grid
        Div(
            Div(
                H2(
                    format_number(creator_count), cls="text-3xl font-bold text-blue-600"
                ),
                P("Creators Tracked", cls="text-sm text-gray-600 mt-1"),
                cls="text-center",
            ),
            Div(
                H2("Real-time", cls="text-3xl font-bold text-purple-600"),
                P("Data", cls="text-sm text-gray-600 mt-1"),
                cls="text-center",
            ),
            Div(
                H2("A.I. Ranked", cls="text-3xl font-bold text-pink-600"),
                P("System", cls="text-sm text-gray-600 mt-1"),
                cls="text-center",
            ),
            cls="grid grid-cols-3 gap-6 max-w-2xl mx-auto mb-8",
        ),
        cls="bg-gradient-to-br from-blue-50 via-purple-50 to-pink-50 rounded-2xl border border-gray-200 p-8 mb-8 text-center",
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
            Option(
                "📈 Fastest Growing",
                value="growth_rate",
                selected=(sort == "growth_rate"),
            ),
            Option(
                "💰 Revenue Potential",
                value="revenue_potential",
                selected=(sort == "revenue_potential"),
            ),
            Option("🆕 Recently Updated", value="recent", selected=(sort == "recent")),
            name="sort",
            cls="h-10 px-4 rounded-lg border border-gray-300",
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
                        "bg-gradient-to-r from-blue-600 to-blue-700 text-white shadow-lg"
                        if grade_filter == val
                        else "bg-white border border-gray-200 hover:border-gray-300 text-gray-700 hover:shadow-md"
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
        cls="sticky top-4 z-10 bg-white/80 backdrop-blur-sm p-6 rounded-2xl border border-gray-200 shadow-sm space-y-4",
    )


def _render_creators_grid(creators: list[dict]) -> Container:
    """Grid of creator cards using MonsterUI Grid."""
    return Container(
        Grid(
            *[_render_creator_card(creator) for creator in creators],
            cols_xl=3,
            cols_lg=2,
            cols_md=1,
            cols_sm=1,
            gap=6,
        ),
        cls=ContainerT.xl,
    )


def _render_creator_card(creator: dict) -> Card:
    """
    Creator card with 3-tier information hierarchy.
    Tier 1: Subs + Views (large, colored)
    Tier 2: Growth, Revenue, Engagement (medium)
    Tier 3: Updated, Action (small, footer)
    """

    # Extract basic info
    channel_id = creator.get("channel_id", "N/A")
    channel_name = creator.get("channel_name", "Unknown")
    channel_thumbnail = creator.get("channel_thumbnail_url", "/static/favicon.jpeg")
    channel_url = f"https://youtube.com/channel/{channel_id}"
    quality_grade = creator.get("quality_grade", "C")
    last_updated = creator.get("last_updated", datetime.now())

    # Extract metrics
    current_subs = creator.get("current_subscribers", 0) or 0
    current_views = creator.get("current_view_count", 0) or 0
    current_videos = creator.get("current_video_count", 0) or 0
    engagement_score = creator.get("engagement_score", 0) or 0

    # Growth indicators
    subs_change = creator.get("subscriber_change_30d", 0) or 0
    views_change = creator.get("views_change_30d", 0) or 0

    # Calculated metrics
    avg_views_per_video = (
        current_views / max(1, current_videos) if current_videos > 0 else 0
    )
    growth_rate = (
        (subs_change / max(1, current_subs - subs_change)) * 100
        if subs_change > 0
        else 0
    )
    estimated_revenue = (current_views * 0.004) / 30 if current_views > 0 else 0

    # Tier styling
    tier_styles = {
        "A+": ("bg-gradient-to-r from-yellow-400 to-orange-500 text-white", "👑 Elite"),
        "A": ("bg-gradient-to-r from-green-400 to-emerald-500 text-white", "⭐ Star"),
        "B+": ("bg-gradient-to-r from-blue-400 to-cyan-500 text-white", "📈 Rising"),
        "B": ("bg-gradient-to-r from-purple-400 to-pink-500 text-white", "💎 Good"),
        "C": ("bg-gradient-to-r from-gray-400 to-slate-500 text-white", "🔍 New"),
    }
    tier_gradient, tier_label = tier_styles.get(quality_grade, tier_styles["C"])

    # Growth color indicator
    growth_color = (
        "text-green-600"
        if growth_rate > 5
        else ("text-yellow-600" if growth_rate > 2 else "text-gray-600")
    )

    return Card(
        # HEADER: Avatar + Name + Tier Badge
        Div(
            Div(
                Img(
                    src=channel_thumbnail,
                    alt=channel_name,
                    cls="w-16 h-16 rounded-xl object-cover border-2 border-white shadow-md",
                    onerror="this.src='/static/favicon.jpeg'",
                ),
                Div(
                    H3(
                        channel_name, cls="text-lg font-bold text-gray-900 line-clamp-2"
                    ),
                    P(channel_id, cls="text-xs text-gray-500 font-mono truncate"),
                    cls="flex-1 min-w-0",
                ),
                cls="flex gap-3 items-start mb-4",
            ),
            Div(
                tier_label,
                cls=f"{tier_gradient} px-3 py-2 rounded-lg text-sm font-bold text-center",
            ),
            cls="flex justify-between items-start pb-4 border-b border-gray-100",
        ),
        # TIER 1: PRIMARY METRICS (Large, colored backgrounds)
        Div(
            # Subscribers
            Div(
                P(
                    "Subscribers",
                    cls="text-xs font-semibold text-gray-600 uppercase tracking-wide",
                ),
                H2(
                    format_number(current_subs),
                    cls="text-2xl font-bold text-blue-600 mt-1",
                ),
                P(
                    (
                        f"+{format_number(subs_change)} (30d)"
                        if subs_change > 0
                        else f"{format_number(subs_change)} (30d)"
                    ),
                    cls=f"text-xs font-medium {growth_color} mt-1",
                ),
                cls="bg-blue-50 border border-blue-100 rounded-lg p-3",
            ),
            # Views
            Div(
                P(
                    "Views",
                    cls="text-xs font-semibold text-gray-600 uppercase tracking-wide",
                ),
                H2(
                    format_number(current_views),
                    cls="text-2xl font-bold text-purple-600 mt-1",
                ),
                P(
                    (
                        f"+{format_number(views_change)} (30d)"
                        if views_change > 0
                        else f"{format_number(views_change)} (30d)"
                    ),
                    cls="text-xs font-medium text-gray-600 mt-1",
                ),
                cls="bg-purple-50 border border-purple-100 rounded-lg p-3",
            ),
            cls="grid grid-cols-2 gap-3 mb-4",
        ),
        # TIER 2: SECONDARY METRICS (Quality indicators)
        Div(
            Div(
                P(
                    "Avg Views/Video",
                    cls="text-xs font-semibold text-gray-600 uppercase",
                ),
                P(
                    format_number(int(avg_views_per_video)),
                    cls="text-lg font-bold text-gray-900 mt-1",
                ),
                cls="bg-gray-50 border border-gray-200 rounded-lg p-3 text-center",
            ),
            Div(
                P("Videos", cls="text-xs font-semibold text-gray-600 uppercase"),
                P(
                    format_number(current_videos),
                    cls="text-lg font-bold text-gray-900 mt-1",
                ),
                cls="bg-gray-50 border border-gray-200 rounded-lg p-3 text-center",
            ),
            Div(
                P("Engagement", cls="text-xs font-semibold text-gray-600 uppercase"),
                P(
                    f"{engagement_score:.1f}%",
                    cls="text-lg font-bold text-gray-900 mt-1",
                ),
                cls="bg-gray-50 border border-gray-200 rounded-lg p-3 text-center",
            ),
            Div(
                P("Revenue Est.", cls="text-xs font-semibold text-gray-600 uppercase"),
                P(
                    f"${format_number(int(estimated_revenue))}/mo",
                    cls="text-lg font-bold text-green-600 mt-1",
                ),
                cls="bg-green-50 border border-green-200 rounded-lg p-3 text-center",
            ),
            cls="grid grid-cols-4 gap-3 mb-4",
        ),
        # Growth indicator bar
        Div(
            Div(
                P("30-Day Growth", cls="text-xs font-semibold text-gray-600"),
                P(
                    f"{growth_rate:.1f}% growth" if growth_rate > 0 else "Stable",
                    cls=f"text-xs font-bold {growth_color}",
                ),
                cls="flex justify-between items-center mb-2",
            ),
            Div(
                Div(
                    cls=f"h-1.5 bg-gradient-to-r from-green-400 to-emerald-600 transition-all duration-500 rounded-full",
                    style=f"width: {min(100, max(0, growth_rate * 5))}%",
                ),
                cls="w-full h-1.5 bg-gray-200 rounded-full overflow-hidden",
            ),
            cls="bg-green-50 border border-green-100 rounded-lg p-3 mb-4",
        ),
        # TIER 3: FOOTER (Metadata + Action)
        Div(
            Div(
                UkIcon("clock", cls="w-3 h-3 text-gray-400"),
                P(
                    format_date_relative(last_updated),
                    cls="text-xs text-gray-500 font-medium",
                ),
                cls="flex items-center gap-1.5",
            ),
            A(
                "Analyze →",
                href=channel_url,
                target="_blank",
                rel="noopener noreferrer",
                cls="text-xs font-bold text-blue-600 hover:text-blue-700 px-3 py-1.5 bg-blue-50 rounded-lg hover:bg-blue-100 no-underline transition-colors inline-block",
            ),
            cls="flex justify-between items-center pt-4 border-t border-gray-100",
        ),
        cls="bg-white rounded-xl border border-gray-200 shadow-sm hover:shadow-lg hover:border-gray-300 transition-all duration-200",
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
                    "Analyze YouTube playlists to automatically discover and track creators. "
                    "We'll build your creator database in real-time.",
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
            cls="bg-gradient-to-br from-blue-50 via-purple-50 to-pink-50 border-blue-200 max-w-md mx-auto",
        )


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def _count_by_grade(creators: list[dict]) -> dict:
    """Count creators by quality grade for filter pills."""
    counts = {"all": len(creators), "A+": 0, "A": 0, "B+": 0, "B": 0, "C": 0}
    for creator in creators:
        grade = creator.get("quality_grade", "C")
        if grade in counts:
            counts[grade] += 1
    return counts
