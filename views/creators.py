"""
Creators page - discover and explore top YouTube creators.
"""

import logging
from datetime import datetime

from fasthtml.common import *
from monsterui.all import *

from utils import format_number, format_date_relative

logger = logging.getLogger(__name__)


def render_creators_page(
    creators: list[dict],
    sort: str = "subscribers",
    search: str = "",
    grade_filter: str = "all",
) -> Div:
    """
    Render the Creators discovery page with interactive filtering.

    Args:
        creators: List of creator dicts with stats
        sort: Sort criteria (subscribers, views, videos, engagement, quality)
        search: Search query for filtering by name
        grade_filter: Quality grade filter (all, A+, A, B+, B, C)
    """

    # Count creators by grade for filter badges
    grade_counts = _count_by_grade(creators)

    return Container(
        # Page header
        Div(
            H1("🌟 Top Creators", cls="text-4xl font-bold mb-2"),
            P(
                f"Discover {len(creators)} YouTube creators with verified stats",
                cls="text-gray-600 text-lg",
            ),
            cls="mb-8 mt-8",
        ),
        # Filter & Sort Controls
        render_filter_controls(
            search=search,
            sort=sort,
            grade_filter=grade_filter,
            grade_counts=grade_counts,
        ),
        # Stats Overview Cards
        render_stats_overview(creators) if creators else None,
        # Creators Grid or Empty State
        (
            render_creators_grid(creators)
            if creators
            else render_empty_state(search, grade_filter)
        ),
        cls=ContainerT.xl,
    )


def _count_by_grade(creators: list[dict]) -> dict:
    """Count creators by quality grade."""
    counts = {"all": len(creators), "A+": 0, "A": 0, "B+": 0, "B": 0, "C": 0}
    for creator in creators:
        grade = creator.get("quality_grade", "C")
        if grade in counts:
            counts[grade] += 1
    return counts


def render_filter_controls(
    search: str,
    sort: str,
    grade_filter: str,
    grade_counts: dict,
) -> Div:
    """Filter and sort controls."""

    grade_options = [
        ("all", "All Grades", "🎯"),
        ("A+", "A+ Tier", "⭐"),
        ("A", "A Tier", "🌟"),
        ("B+", "B+ Tier", "💎"),
        ("B", "B Tier", "📈"),
        ("C", "C Tier", "📊"),
    ]

    return Div(
        # Search bar
        Form(
            Div(
                Input(
                    type="search",
                    name="search",
                    placeholder="Search creators by name...",
                    value=search,
                    cls="input input-bordered flex-1",
                    autofocus=bool(search),
                ),
                # Hidden fields to preserve filters
                Input(type="hidden", name="sort", value=sort),
                Input(type="hidden", name="grade", value=grade_filter),
                cls="flex gap-4 items-center",
            ),
            method="GET",
            action="/creators",
            cls="mb-4",
        ),
        # Grade filter pills
        Div(
            *[
                A(
                    Div(
                        Span(emoji, cls="text-lg mr-2"),
                        Span(
                            label,
                            cls="font-medium",
                        ),
                        Span(
                            f"({grade_counts.get(grade_val, 0)})",
                            cls="ml-1 text-xs opacity-75",
                        ),
                        cls=(
                            "px-4 py-2 rounded-full transition-all duration-200 flex items-center gap-1 "
                            + (
                                "bg-blue-600 text-white shadow-lg scale-105"
                                if grade_filter == grade_val
                                else "bg-gray-100 text-gray-700 hover:bg-gray-200"
                            )
                        ),
                    ),
                    href=f"/creators?sort={sort}&search={search}&grade={grade_val}",
                    cls="no-underline",
                )
                for grade_val, label, emoji in grade_options
            ],
            cls="flex gap-2 mb-6 overflow-x-auto pb-2 flex-wrap",
        ),
        # Sort dropdown
        Form(
            Div(
                Label("Sort by:", cls="text-sm font-medium text-gray-700 mr-2"),
                Select(
                    Option(
                        "📊 Most Subscribers",
                        value="subscribers",
                        selected=(sort == "subscribers"),
                    ),
                    Option("👀 Most Views", value="views", selected=(sort == "views")),
                    Option(
                        "🎬 Most Videos", value="videos", selected=(sort == "videos")
                    ),
                    Option(
                        "🔥 Best Engagement",
                        value="engagement",
                        selected=(sort == "engagement"),
                    ),
                    Option(
                        "⭐ Quality Grade",
                        value="quality",
                        selected=(sort == "quality"),
                    ),
                    Option(
                        "🆕 Recently Updated",
                        value="recent",
                        selected=(sort == "recent"),
                    ),
                    name="sort",
                    cls="select select-bordered w-64",
                    onchange="this.form.submit()",
                ),
                # Hidden fields to preserve filters
                Input(type="hidden", name="search", value=search),
                Input(type="hidden", name="grade", value=grade_filter),
                cls="flex items-center gap-2",
            ),
            method="GET",
            action="/creators",
        ),
        cls="mb-8",
    )


def render_stats_overview(creators: list[dict]) -> Div:
    """Show aggregate stats across all creators."""

    total_subs = sum(c.get("current_subscribers", 0) or 0 for c in creators)
    total_views = sum(c.get("current_view_count", 0) or 0 for c in creators)
    total_videos = sum(c.get("current_video_count", 0) or 0 for c in creators)
    avg_engagement = sum(c.get("engagement_score", 0) or 0 for c in creators) / max(
        1, len(creators)
    )

    return Div(
        # Stats cards
        Div(
            _stat_card(
                "👥 Total Subscribers",
                format_number(total_subs),
                "from-blue-400 to-blue-600",
            ),
            _stat_card(
                "👀 Total Views",
                format_number(total_views),
                "from-purple-400 to-purple-600",
            ),
            _stat_card(
                "🎬 Total Videos",
                format_number(total_videos),
                "from-green-400 to-green-600",
            ),
            _stat_card(
                "🔥 Avg Engagement",
                f"{avg_engagement:.2f}%",
                "from-orange-400 to-orange-600",
            ),
            cls="grid grid-cols-2 md:grid-cols-4 gap-4",
        ),
        cls="mb-8",
    )


def _stat_card(label: str, value: str, gradient: str) -> Card:
    """Individual stat card."""
    return Card(
        Div(
            P(label, cls="text-sm text-gray-600 mb-1"),
            P(value, cls="text-2xl font-bold text-gray-900"),
            cls=f"p-4 bg-gradient-to-br {gradient} bg-opacity-10 rounded-lg",
        ),
        cls="border-0 shadow-sm",
    )


def render_creators_grid(creators: list[dict]) -> Div:
    """
    Render responsive grid of creator cards.

    Grid layout:
    - 1 column on mobile
    - 2 columns on tablet
    - 3 columns on desktop
    - 4 columns on xl screens
    """

    return Grid(
        *[render_creator_card(c) for c in creators],
        cols=1,
        cols_md=2,
        cols_lg=3,
        cols_xl=4,
        gap=6,
        cls="mb-12",
    )


def render_creator_card(creator: dict) -> Div:
    """
    Render a single creator card with stats and quality indicators.

    Design:
    - Large creator avatar (circular)
    - Channel name and ID
    - Key metrics (subscribers, views, videos)
    - Engagement score with visual gauge
    - Quality grade badge
    - Last updated timestamp
    """

    # Extract data
    channel_id = creator.get("channel_id", "")
    channel_name = creator.get("channel_name", "Unknown Creator")
    channel_url = creator.get("channel_url", "#")
    channel_thumbnail = creator.get("channel_thumbnail_url", "/static/favicon.jpeg")

    current_subs = creator.get("current_subscribers", 0) or 0
    current_views = creator.get("current_view_count", 0) or 0
    current_videos = creator.get("current_video_count", 0) or 0
    engagement_score = creator.get("engagement_score", 0) or 0
    quality_grade = creator.get("quality_grade", "C")
    last_updated = creator.get("last_updated_at") or creator.get("last_synced_at")

    # Calculate derived metrics
    avg_views_per_video = (current_views / current_videos) if current_videos > 0 else 0

    # Format numbers
    subs_formatted = format_number(current_subs)
    views_formatted = format_number(current_views)
    videos_formatted = format_number(current_videos)

    # Quality grade styling
    grade_styles = {
        "A+": "bg-gradient-to-r from-yellow-400 to-yellow-500 text-white",
        "A": "bg-gradient-to-r from-green-400 to-green-500 text-white",
        "B+": "bg-gradient-to-r from-blue-400 to-blue-500 text-white",
        "B": "bg-gradient-to-r from-purple-400 to-purple-500 text-white",
        "C": "bg-gradient-to-r from-gray-400 to-gray-500 text-white",
    }
    grade_style = grade_styles.get(quality_grade, grade_styles["C"])

    # Engagement gauge color
    engagement_color = (
        "from-green-400 to-green-600"
        if engagement_score > 5
        else (
            "from-yellow-400 to-yellow-600"
            if engagement_score > 2
            else "from-gray-300 to-gray-500"
        )
    )

    return Card(
        Div(
            # Header with avatar and grade badge
            Div(
                # Avatar
                Div(
                    Img(
                        src=channel_thumbnail,
                        alt=f"{channel_name} avatar",
                        cls="w-20 h-20 rounded-full object-cover border-4 border-white shadow-lg",
                        onerror="this.src='/static/favicon.jpeg'",
                    ),
                    cls="relative",
                ),
                # Quality grade badge
                Div(
                    Span(quality_grade, cls="font-bold text-sm px-3 py-1"),
                    cls=f"{grade_style} rounded-full shadow-lg",
                ),
                cls="flex items-center justify-between mb-4",
            ),
            # Channel name
            H3(
                channel_name,
                cls="text-lg font-bold mb-1 line-clamp-2 text-gray-900 hover:text-blue-600 transition-colors",
                title=channel_name,
            ),
            # Channel ID
            P(
                channel_id,
                cls="text-xs text-gray-500 mb-4 font-mono truncate",
                title=channel_id,
            ),
            # Stats grid
            Div(
                # Subscribers
                Div(
                    UkIcon("users", cls="w-4 h-4 text-blue-600"),
                    Div(
                        Span(subs_formatted, cls="font-bold text-gray-900 text-sm"),
                        Span(" subs", cls="text-xs text-gray-500"),
                        cls="flex flex-col",
                    ),
                    cls="flex items-center gap-2",
                ),
                # Views
                Div(
                    UkIcon("eye", cls="w-4 h-4 text-purple-600"),
                    Div(
                        Span(views_formatted, cls="font-bold text-gray-900 text-sm"),
                        Span(" views", cls="text-xs text-gray-500"),
                        cls="flex flex-col",
                    ),
                    cls="flex items-center gap-2",
                ),
                # Videos
                Div(
                    UkIcon("film", cls="w-4 h-4 text-green-600"),
                    Div(
                        Span(videos_formatted, cls="font-bold text-gray-900 text-sm"),
                        Span(" videos", cls="text-xs text-gray-500"),
                        cls="flex flex-col",
                    ),
                    cls="flex items-center gap-2",
                ),
                cls="grid grid-cols-3 gap-3 mb-4 pb-4 border-b border-gray-200",
            ),
            # Engagement section
            Div(
                Div(
                    Span("🔥 Engagement", cls="text-xs text-gray-600 font-medium"),
                    Span(
                        f"{engagement_score:.2f}%",
                        cls="text-sm font-bold text-gray-900",
                    ),
                    cls="flex justify-between items-center mb-2",
                ),
                # Engagement gauge
                Div(
                    Div(
                        cls=f"h-2 bg-gradient-to-r {engagement_color} rounded-full transition-all duration-500",
                        style=f"width: {min(100, engagement_score * 10)}%",
                    ),
                    cls="w-full h-2 bg-gray-200 rounded-full mb-3",
                ),
                # Avg views per video
                Div(
                    UkIcon("trending-up", cls="w-3 h-3 text-blue-600"),
                    Span(
                        f"{format_number(int(avg_views_per_video))} avg views/video",
                        cls="text-xs text-gray-600",
                    ),
                    cls="flex items-center gap-1",
                ),
                cls="mb-4",
            ),
            # Footer
            Div(
                Div(
                    UkIcon("clock", cls="w-3 h-3 text-gray-400"),
                    Span(
                        format_date_relative(last_updated),
                        cls="text-xs text-gray-500",
                    ),
                    cls="flex items-center gap-1",
                ),
                A(
                    "View Channel →",
                    href=channel_url,
                    target="_blank",
                    rel="noopener noreferrer",
                    cls="text-xs text-blue-600 hover:text-blue-700 font-medium no-underline",
                ),
                cls="flex justify-between items-center pt-3 border-t border-gray-200",
            ),
            cls="p-6",
        ),
        cls="hover:shadow-xl transition-all duration-200 bg-white h-full",
    )


def render_empty_state(search: str, grade_filter: str) -> Div:
    """Empty state when no creators found."""

    if search or grade_filter != "all":
        # Filtered results are empty
        return Card(
            Div(
                Div(Span("🔍", cls="text-6xl mb-4"), cls="flex justify-center"),
                H2("No creators found", cls="text-2xl font-bold mb-3 text-center"),
                P(
                    "Try adjusting your filters or search terms",
                    cls="text-gray-600 mb-6 text-center",
                ),
                Div(
                    A(
                        Button("Clear Filters", cls=ButtonT.secondary),
                        href="/creators",
                    ),
                    cls="flex justify-center",
                ),
                cls="p-12 text-center",
            ),
            cls="bg-gray-50",
        )
    else:
        # No creators in database yet
        return Card(
            Div(
                Div(Span("🌟", cls="text-6xl mb-4"), cls="flex justify-center"),
                H2("No creators yet", cls="text-2xl font-bold mb-3 text-center"),
                P(
                    "Creators are discovered automatically when you analyze playlists. Start analyzing to build your creator database!",
                    cls="text-gray-600 mb-6 text-center max-w-md mx-auto",
                ),
                Div(
                    A(
                        Button(
                            "🚀 Analyze a Playlist",
                            cls=ButtonT.primary + " text-lg px-6 py-3",
                        ),
                        href="/#analyze-section",
                    ),
                    cls="flex justify-center",
                ),
                cls="p-12 text-center",
            ),
            cls="bg-gradient-to-br from-blue-50 to-purple-50",
        )
