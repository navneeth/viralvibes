"""
Creator Intelligence Dashboard - discover and explore top YouTube creators.
Enhanced version with clickable thumbnails, ranking badges, and real stats.

Key features:
- Clickable thumbnail images linking to YouTube channels
- Ranking badges showing position based on sort
- Real aggregate stats in hero section
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
    stats: dict = None,
) -> Div:
    """
    Analytics-first creator discovery dashboard.
    Competes with SocialBlade through superior UX and insights.

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

        def get_value(obj, key, default=0):
            """Safely get value from dict or object. Returns default if None."""
            if isinstance(obj, dict):
                value = obj.get(key, default)
            else:
                value = getattr(obj, key, default)
            return value if value is not None else default

        stats = {
            "total_subscribers": sum(
                get_value(c, "current_subscribers", 0) for c in creators
            ),
            "total_views": sum(get_value(c, "current_view_count", 0) for c in creators),
            "avg_engagement": (
                sum(get_value(c, "engagement_score", 0) for c in creators)
                / len(creators)
                if creators
                else 0
            ),
            "total_revenue": int(
                sum(
                    (get_value(c, "current_view_count", 0) * 4) / 1000 for c in creators
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
    """Hero section with real statistics."""
    return Div(
        H1(
            "Creator Intelligence Platform",
            cls="text-5xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-600 via-purple-600 to-pink-600 text-center",
        ),
        P(
            "Track growth, estimate earnings, discover rising creators",
            cls="text-gray-600 text-center mt-2 mb-8",
        ),
        # Real stats grid
        Div(
            Div(
                H2(
                    format_number(creator_count), cls="text-3xl font-bold text-blue-600"
                ),
                P("Creators Tracked", cls="text-sm text-gray-600 mt-1"),
                cls="text-center",
            ),
            Div(
                H2(
                    format_number(stats.get("total_subscribers", 0)),
                    cls="text-3xl font-bold text-purple-600",
                ),
                P("Total Subscribers", cls="text-sm text-gray-600 mt-1"),
                cls="text-center",
            ),
            Div(
                H2(
                    f"{stats.get('avg_engagement', 0):.1f}%",
                    cls="text-3xl font-bold text-pink-600",
                ),
                P("Avg Engagement", cls="text-sm text-gray-600 mt-1"),
                cls="text-center",
            ),
            Div(
                H2(
                    f"${format_number(stats.get('total_revenue', 0))}",
                    cls="text-3xl font-bold text-green-600",
                ),
                P("Est. Monthly Revenue", cls="text-sm text-gray-600 mt-1"),
                cls="text-center",
            ),
            cls="grid grid-cols-4 gap-6 max-w-4xl mx-auto mb-8",
        ),
        cls="bg-gradient-to-br from-blue-50 via-purple-50 to-pink-50 rounded-2xl border border-gray-200 p-8 mb-8 text-center",
        data_reveal=True,
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

    # Sort select with visual indicator
    sort_labels = {
        "subscribers": "📊 Most Subscribers",
        "views": "👀 Most Views",
        "engagement": "🔥 Best Engagement",
        "quality": "⭐ Quality Score",
        "recent": "🆕 Recently Updated",
    }

    sort_form = Form(
        Select(
            *[
                Option(
                    sort_labels.get(s, s),
                    value=s,
                    selected=(sort == s),
                )
                for s in ["subscribers", "views", "engagement", "quality", "recent"]
            ],
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
    Creator card with 3-tier information hierarchy.
    Tier 1: Thumbnail (clickable) + Rank Badge + Quality Grade
    Tier 2: Subs + Views (large, colored)
    Tier 3: Growth, Revenue, Engagement (medium)
    Tier 4: Footer (metadata + action)
    """

    # Helper to safely get values from dict or object
    def get_value(obj, key, default=0):
        """Safely get value from dict or Supabase object. Returns default if None."""
        if isinstance(obj, dict):
            value = obj.get(key, default)
        else:
            value = getattr(obj, key, default)

        # If value is None, return default instead
        return value if value is not None else default

    # Extract basic info
    channel_id = get_value(creator, "channel_id", "N/A")
    channel_name = get_value(creator, "channel_name", "Unknown")
    channel_url = f"https://youtube.com/@{channel_id}"
    quality_grade = get_value(creator, "quality_grade", "C")
    rank = get_value(creator, "_rank", "—")

    # Thumbnail URL - adjust based on your data structure
    thumbnail_url = (
        get_value(creator, "channel_thumbnail_url")
        or get_value(creator, "thumbnail_url")
        or "https://via.placeholder.com/300x200?text=No+Image"
    )

    # Extract metrics - ensure all are numeric
    current_subs = int(get_value(creator, "current_subscribers", 0) or 0)
    current_views = int(get_value(creator, "current_view_count", 0) or 0)
    current_videos = int(get_value(creator, "current_video_count", 0) or 0)
    subs_change = int(get_value(creator, "subscribers_change_30d", 0) or 0)
    views_change = int(get_value(creator, "views_change_30d", 0) or 0)
    engagement_score = float(get_value(creator, "engagement_score", 0) or 0)
    growth_rate = (subs_change / current_subs * 100) if current_subs > 0 else 0
    avg_views_per_video = current_views / current_videos if current_videos > 0 else 0
    estimated_revenue = (current_views * 4) / 1000
    last_updated = get_value(creator, "last_updated_at", "")

    # Color coding for growth
    if growth_rate > 5:
        growth_color = "text-green-600 font-bold"
    elif growth_rate > 0:
        growth_color = "text-green-500"
    elif growth_rate < -2:
        growth_color = "text-red-600 font-bold"
    else:
        growth_color = "text-gray-500"

    # Grade badge colors
    grade_colors = {
        "A+": "bg-gradient-to-r from-purple-600 to-pink-600 text-white",
        "A": "bg-blue-600 text-white",
        "B+": "bg-cyan-600 text-white",
        "B": "bg-blue-500 text-white",
        "C": "bg-gray-400 text-white",
    }
    grade_bg = grade_colors.get(quality_grade, "bg-gray-400 text-white")

    return Div(
        # Header with thumbnail, rank, and grade
        Div(
            # Clickable Thumbnail
            A(
                Img(
                    src=thumbnail_url,
                    alt=channel_name,
                    cls="w-full h-48 object-cover rounded-lg hover:brightness-90 transition-all duration-200",
                ),
                href=channel_url,
                target="_blank",
                rel="noopener noreferrer",
                cls="block relative group overflow-hidden rounded-lg",
            ),
            cls="relative mb-4",
        ),
        # Rank and Grade badges
        Div(
            # Rank badge (top-left)
            Div(
                Div(
                    f"#{rank}",
                    cls="text-sm font-bold text-white",
                ),
                cls="absolute top-3 left-3 bg-gray-900/80 backdrop-blur rounded-full w-10 h-10 flex items-center justify-center text-lg font-bold shadow-lg",
            ),
            # Grade badge (top-right)
            Div(
                quality_grade,
                cls=f"absolute top-3 right-3 {grade_bg} px-3 py-1.5 rounded-full text-sm font-bold shadow-lg",
            ),
            # Channel name overlay (on hover)
            Div(
                channel_name,
                cls="absolute bottom-0 left-0 right-0 bg-gradient-to-t from-black to-transparent text-white p-3 text-sm font-semibold opacity-0 group-hover:opacity-100 transition-opacity duration-200",
            ),
            cls="absolute inset-0 pointer-events-none group-hover:pointer-events-auto",
        ),
        # Channel info section
        Div(
            H3(
                channel_name,
                cls="text-lg font-bold text-gray-900 truncate",
            ),
            Div(
                Span(
                    f"{format_number(current_subs)} subscribers",
                    cls="text-xs text-gray-600 font-medium",
                ),
                cls="mt-1",
            ),
            cls="mb-4 pb-4 border-b border-gray-100",
        ),
        # TIER 1: PRIMARY METRICS (Subs + Views)
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
                P("30-Day Trend", cls="text-xs font-semibold text-gray-600"),
                P(
                    (
                        f"+{growth_rate:.1f}% growth"
                        if growth_rate > 0
                        else (
                            f"{growth_rate:.1f}% decline"
                            if growth_rate < 0
                            else "Stable"
                        )
                    ),
                    cls=f"text-xs font-bold {growth_color}",
                ),
                cls="flex justify-between items-center mb-2",
            ),
            Div(
                Div(
                    cls=(
                        "h-1.5 bg-gradient-to-r from-green-400 to-emerald-600 transition-all duration-500 rounded-full"
                        if growth_rate >= 0
                        else "h-1.5 bg-gradient-to-r from-red-400 to-red-600 transition-all duration-500 rounded-full"
                    ),
                    style=f"width: {min(100, max(0, abs(growth_rate) * 5))}%",
                ),
                cls="w-full h-1.5 bg-gray-200 rounded-full overflow-hidden",
            ),
            cls=(
                "bg-green-50 border border-green-100 rounded-lg p-3 mb-4"
                if growth_rate >= 0
                else "bg-red-50 border border-red-100 rounded-lg p-3 mb-4"
            ),
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
                "View Channel →",
                href=channel_url,
                target="_blank",
                rel="noopener noreferrer",
                cls="text-xs font-bold text-blue-600 hover:text-blue-700 px-3 py-1.5 bg-blue-50 rounded-lg hover:bg-blue-100 no-underline transition-colors inline-block",
            ),
            cls="flex justify-between items-center pt-4 border-t border-gray-100",
        ),
        cls="bg-white rounded-xl border border-gray-200 shadow-sm hover:shadow-lg hover:border-gray-300 transition-all duration-200 p-4 group",
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

    def get_value(obj, key, default=None):
        """Safely get value from dict or object. Returns default if None."""
        if isinstance(obj, dict):
            value = obj.get(key, default)
        else:
            value = getattr(obj, key, default)
        return value if value is not None else default

    counts = {"all": len(creators), "A+": 0, "A": 0, "B+": 0, "B": 0, "C": 0}
    for creator in creators:
        grade = get_value(creator, "quality_grade", "C")
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
