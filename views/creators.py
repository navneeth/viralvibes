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

from utils.creator_metrics import (
    calculate_avg_views_per_video,
    calculate_growth_rate,
    estimate_monthly_revenue,
    format_channel_age,
    get_grade_info,
    get_growth_signal,
    get_sync_status_badge,
)
from utils import format_date_relative, format_number, safe_get_value

logger = logging.getLogger(__name__)


def _filter_valid_creators(creators: list[dict]) -> list[dict]:
    """
    Filter out creators with incomplete data.

    Only shows creators that have:
    - A channel_name (successfully resolved)
    - At least 1 subscriber (data has been synced)

    This prevents showing empty "Sync Pending" cards.
    """

    def get_val(obj, key, default=None):
        if isinstance(obj, dict):
            v = obj.get(key, default)
        else:
            v = getattr(obj, key, default)
        return v if v is not None else default

    valid = []
    for creator in creators:
        channel_name = get_val(creator, "channel_name")
        subs = get_val(creator, "current_subscribers", 0)

        if channel_name and subs > 0:
            valid.append(creator)

    return valid


# ============================================================================
# MAIN PAGE FUNCTION
# ============================================================================


def render_creators_page(
    creators: list[dict],
    sort: str = "subscribers",
    search: str = "",
    grade_filter: str = "all",
    language_filter: str = "all",
    activity_filter: str = "all",
    age_filter: str = "all",
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
    creators = _filter_valid_creators(creators)

    # Use provided stats or calculate from creators
    if stats is None:
        stats = {
            "total_subscribers": sum(
                safe_get_value(c, "current_subscribers", 0) for c in creators
            ),
            "total_views": sum(
                safe_get_value(c, "current_view_count", 0) for c in creators
            ),
            "avg_engagement": (
                sum(safe_get_value(c, "engagement_score", 0) for c in creators)
                / len(creators)
                if creators
                else 0
            ),
            "total_revenue": int(
                sum(
                    (safe_get_value(c, "current_view_count", 0) * 4) / 1000
                    for c in creators
                )
            ),
        }

    return Container(
        # Hero section with real stats
        _render_hero(len(creators), stats),
        # Filter controls (sticky bar)
        _render_filter_bar(
            search=search,
            sort=sort,
            grade_filter=grade_filter,
            language_filter=language_filter,
            activity_filter=activity_filter,
            age_filter=age_filter,
            grade_counts=grade_counts,
        ),
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
    search: str,
    sort: str,
    grade_filter: str,
    grade_counts: dict,
    language_filter: str = "all",
    activity_filter: str = "all",
    age_filter: str = "all",
) -> Div:
    """Sticky filter bar with search, sort, grade pills."""

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
        Input(type="hidden", name="language", value=language_filter),
        Input(type="hidden", name="activity", value=activity_filter),
        Input(type="hidden", name="age", value=age_filter),
        method="GET",
        action="/creators",
        cls="flex-1",
    )

    # Sort dropdown options with emojis for visual appeal
    sort_options = [
        ("subscribers", "📊 Most Subscribers"),
        ("views", "👀 Most Views"),
        ("engagement", "🔥 Best Engagement"),
        ("quality", "⭐ Quality Score"),
        ("recent", "🆕 Recently Updated"),
        ("consistency", "📈 Most Consistent"),  # NEW
        ("newest_channel", "🎉 Newest Channels"),  # NEW
        ("oldest_channel", "👑 Oldest Channels"),  # NEW
    ]
    # Grade pills with counts and emojis
    grade_options = [
        ("all", "All", "🎯"),
        ("A+", "Elite", "👑"),
        ("A", "Star", "⭐"),
        ("B+", "Rising", "📈"),
        ("B", "Good", "💎"),
        ("C", "New", "🔍"),
    ]

    # Sort select
    sort_form = Form(
        Select(
            *[
                Option(label, value=val, selected=(sort == val))
                for val, label in sort_options
            ],
            name="sort",
            cls="h-10 px-4 rounded-lg border border-gray-300 font-medium",
            onchange="this.form.submit()",
        ),
        Input(type="hidden", name="search", value=search),
        Input(type="hidden", name="grade", value=grade_filter),
        Input(type="hidden", name="language", value=language_filter),
        Input(type="hidden", name="activity", value=activity_filter),
        Input(type="hidden", name="age", value=age_filter),
        method="GET",
        action="/creators",
    )

    # Grade pills
    grade_pills = Div(
        *[
            A(
                f"{emoji} {label} ({grade_counts.get(val, 0)})",
                href=f"/creators?{urlencode({'sort': sort, 'search': search, 'grade': val, 'language': language_filter, 'activity': activity_filter, 'age': age_filter})}",
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

    # Language filter pills
    language_options = [
        ("all", "All Languages", "🌍"),
        ("en", "English", "🇺🇸"),
        ("ja", "日本語", "🇯🇵"),
        ("es", "Español", "🇪🇸"),
        ("ko", "한국어", "🇰🇷"),
        ("zh", "中文", "🇨🇳"),
    ]

    language_pills = Div(
        P("Language:", cls="text-sm font-semibold text-gray-700 mb-2"),
        Div(
            *[
                A(
                    f"{emoji} {label}",
                    href=f"/creators?{urlencode({'sort': sort, 'search': search, 'grade': grade_filter, 'language': val, 'activity': activity_filter, 'age': age_filter})}",
                    cls=(
                        "px-3 py-1.5 rounded-lg transition-all inline-block no-underline text-sm font-medium "
                        + (
                            "bg-blue-100 text-blue-700 border border-blue-300"
                            if language_filter == val
                            else "bg-white border border-gray-200 hover:bg-gray-50 text-gray-700"
                        )
                    ),
                )
                for val, label, emoji in language_options
            ],
            cls="flex gap-2 flex-wrap",
        ),
        cls="mb-4",
    )

    # Activity filter pills
    activity_options = [
        ("all", "All Activity", "📊"),
        ("active", "Very Active (>5/mo)", "🔥"),
        ("dormant", "Dormant (<1/mo)", "⚠️"),
    ]

    activity_pills = Div(
        P("Activity:", cls="text-sm font-semibold text-gray-700 mb-2"),
        Div(
            *[
                A(
                    f"{emoji} {label}",
                    href=f"/creators?{urlencode({'sort': sort, 'search': search, 'grade': grade_filter, 'language': language_filter, 'activity': val, 'age': age_filter})}",
                    cls=(
                        "px-3 py-1.5 rounded-lg transition-all inline-block no-underline text-sm font-medium "
                        + (
                            "bg-green-100 text-green-700 border border-green-300"
                            if activity_filter == val
                            else "bg-white border border-gray-200 hover:bg-gray-50 text-gray-700"
                        )
                    ),
                )
                for val, label, emoji in activity_options
            ],
            cls="flex gap-2 flex-wrap",
        ),
        cls="mb-4",
    )

    # Channel age filter pills
    age_options = [
        ("all", "All Ages", "📅"),
        ("new", "New (<1 year)", "🆕"),
        ("established", "Established (1-10 yr)", "🏆"),
        ("veteran", "Veteran (10+ yr)", "👑"),
    ]

    age_pills = Div(
        P("Channel Age:", cls="text-sm font-semibold text-gray-700 mb-2"),
        Div(
            *[
                A(
                    f"{emoji} {label}",
                    href=f"/creators?{urlencode({'sort': sort, 'search': search, 'grade': grade_filter, 'language': language_filter, 'activity': activity_filter, 'age': val})}",
                    cls=(
                        "px-3 py-1.5 rounded-lg transition-all inline-block no-underline text-sm font-medium "
                        + (
                            "bg-purple-100 text-purple-700 border border-purple-300"
                            if age_filter == val
                            else "bg-white border border-gray-200 hover:bg-gray-50 text-gray-700"
                        )
                    ),
                )
                for val, label, emoji in age_options
            ],
            cls="flex gap-2 flex-wrap",
        ),
        cls="mb-4",
    )

    # Container for all filters with sticky positioning
    return Div(
        search_form,
        sort_form,
        Div(
            P("Quality:", cls="text-sm font-semibold text-gray-700 mb-2"),
            grade_pills,
            cls="mb-4",
        ),
        language_pills,
        activity_pills,
        age_pills,
        cls="sticky top-0 bg-white border-b border-gray-200 p-4 shadow-sm z-10 space-y-4",
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


# =============================================================================
# CREATOR CARD SECTION BUILDERS
# =============================================================================


def _build_card_header(
    thumbnail_url: str,
    channel_name: str,
    current_subs: int,
    current_videos: int,
    rank: str,
    grade_icon: str,
    grade_label: str,
    grade_bg: str,
    quality_grade: str,
    channel_age_days: int,
) -> Div:
    """Build card header section with avatar, name, rank, and grade badges."""
    return Div(
        # Thumbnail with rank badge overlay
        Div(
            Img(
                src=thumbnail_url,
                alt=channel_name,
                cls="w-16 h-16 rounded-lg object-cover",
            ),
            # Rank badge
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
            # Quality grade badge
            Div(
                Div(
                    P(grade_icon, cls="text-lg"),
                    P(quality_grade, cls="text-xs font-bold"),
                    cls="flex flex-col items-center",
                ),
                Div(
                    P(grade_label, cls="text-xs font-semibold text-right"),
                    cls="text-right",
                ),
                cls=f"px-3 py-2 rounded-lg {grade_bg} flex gap-2",
            ),
            # Channel age badge
            (
                Div(
                    (
                        "👑 Veteran"
                        if channel_age_days > 3650
                        else (
                            "🏆 Established"
                            if channel_age_days > 1825
                            else ("📈 Growing" if channel_age_days > 365 else "🆕 New")
                        )
                    ),
                    cls="text-xs font-semibold px-2.5 py-1 rounded-md "
                    "bg-purple-100 text-purple-700 whitespace-nowrap",
                )
                if channel_age_days
                else None
            ),
            cls="flex justify-between items-start gap-3 flex-1",
        ),
        cls="flex gap-3 mb-4 pb-4 border-b border-gray-100",
    )


def _build_primary_metrics(
    current_subs: int, subs_change: int, current_views: int, views_change: int
) -> Div:
    """Build primary metrics section (subscribers and views)."""
    return Div(
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
    )


def _build_performance_metrics(
    avg_views_per_video: int,
    current_videos: int,
    engagement_score: float,
    estimated_revenue: int,
) -> Div:
    """Build performance metrics grid (4-column)."""
    return Div(
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
            P(
                f"{engagement_score:.1f}%",
                cls="text-lg font-bold text-gray-900 mt-1",
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
    )


def _build_growth_trend(
    growth_rate: float, growth_signal_text: str, growth_emoji: str, growth_style: str
) -> Div:
    """Build growth trend indicator section."""
    return Div(
        Div(
            P("30-DAY TREND", cls="text-xs font-semibold text-gray-600"),
            Div(
                P(
                    f"{growth_emoji} {growth_rate:+.1f}%",
                    cls=f"text-sm font-bold text-gray-900",
                ),
                Span(
                    growth_signal_text,
                    cls=f"px-2 py-1 text-xs font-semibold rounded-full border {growth_style}",
                ),
                cls="flex items-center gap-2",
            ),
            cls="flex justify-between items-center mb-3",
        ),
        # Growth bar
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
    )


def _build_card_footer(last_updated: str, channel_url: str) -> Div:
    """Build card footer with timestamp and CTA link."""
    return Div(
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
    channel_id = safe_get_value(creator, "channel_id", "N/A")
    channel_name = safe_get_value(creator, "channel_name", "Unknown")
    # Preserve existing channel_url if present, otherwise construct from channel_id
    channel_url = (
        safe_get_value(creator, "channel_url")
        or f"https://youtube.com/channel/{channel_id}"
    )
    quality_grade = safe_get_value(creator, "quality_grade", "C")
    rank = safe_get_value(creator, "_rank", "—")
    thumbnail_url = (
        safe_get_value(creator, "channel_thumbnail_url")
        or safe_get_value(creator, "thumbnail_url")
        or "https://via.placeholder.com/64x64?text=No+Image"
    )
    channel_age_days = safe_get_value(creator, "channel_age_days", 0)

    # Numeric fields
    current_subs = int(safe_get_value(creator, "current_subscribers", 0) or 0)
    current_views = int(safe_get_value(creator, "current_view_count", 0) or 0)
    current_videos = int(safe_get_value(creator, "current_video_count", 0) or 0)
    subs_change = int(safe_get_value(creator, "subscribers_change_30d", 0) or 0)
    views_change = int(safe_get_value(creator, "views_change_30d", 0) or 0)
    engagement_score = float(safe_get_value(creator, "engagement_score", 0) or 0)
    last_updated = safe_get_value(creator, "last_updated_at", "")

    # === CALCULATIONS ===
    avg_views_per_video = calculate_avg_views_per_video(current_views, current_videos)
    estimated_revenue = estimate_monthly_revenue(current_views)
    growth_rate = calculate_growth_rate(subs_change, current_subs)

    # === STATUS & STYLING ===
    sync_status = safe_get_value(creator, "sync_status", "pending")
    sync_badge_info = get_sync_status_badge(sync_status)
    card_border = f"border-l-4 border-amber-400" if sync_status != "synced" else ""

    grade_icon, grade_label, grade_bg = get_grade_info(quality_grade)
    growth_signal_text, growth_emoji, growth_style = get_growth_signal(growth_rate)

    # === METADATA SECTION (optional) ===
    custom_url = safe_get_value(creator, "custom_url", "")
    language = safe_get_value(creator, "default_language", "")
    keywords = safe_get_value(creator, "keywords", "")
    monthly_uploads = safe_get_value(creator, "monthly_uploads", 0)

    metadata_section = (
        Div(
            # Custom URL
            (
                Div(
                    Span(
                        f"@{custom_url}",
                        cls="text-sm font-semibold text-blue-600 truncate",
                    ),
                    cls="mb-2",
                )
                if custom_url
                else None
            ),
            # Language + Activity
            Div(
                (
                    Span(
                        f"{get_language_emoji(language)} {get_language_name(language)}",
                        cls="text-xs text-gray-600 font-medium",
                    )
                    if language
                    else None
                ),
                (
                    Span(
                        get_activity_badge(monthly_uploads),
                        cls="text-xs text-gray-600 font-medium ml-2 pl-2 border-l border-gray-300",
                    )
                    if monthly_uploads
                    else None
                ),
                cls="flex items-center gap-2 text-xs text-gray-600 mb-2",
            ),
            # Keywords
            (
                P(keywords, cls="text-xs text-gray-500 italic line-clamp-1")
                if keywords
                else None
            ),
            cls="mb-3 pb-3 border-b border-gray-100 text-xs",
        )
        if (custom_url or language or keywords or monthly_uploads)
        else None
    )

    # === COMPOSE CARD ===
    return Div(
        # Sync status badge (if not synced)
        (
            Div(
                sync_badge_info[0],
                cls=f"text-xs font-semibold px-3 py-1 rounded-t-lg {sync_badge_info[2]}",
            )
            if sync_badge_info
            else None
        ),
        # Header section
        _build_card_header(
            thumbnail_url,
            channel_name,
            current_subs,
            current_videos,
            rank,
            grade_icon,
            grade_label,
            grade_bg,
            quality_grade,
            channel_age_days,
        ),
        # Metadata (optional)
        metadata_section,
        # Primary metrics
        _build_primary_metrics(current_subs, subs_change, current_views, views_change),
        # Performance metrics
        _build_performance_metrics(
            avg_views_per_video, current_videos, engagement_score, estimated_revenue
        ),
        # Growth trend
        _build_growth_trend(
            growth_rate, growth_signal_text, growth_emoji, growth_style
        ),
        # Footer
        _build_card_footer(last_updated, channel_url),
        cls=f"bg-white rounded-lg border border-gray-200 p-4 hover:shadow-md hover:scale-[1.02] transition-all duration-300 cursor-pointer {card_border}",
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
        grade = safe_get_value(creator, "quality_grade", "C")
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


def get_language_emoji(language_code: str) -> str:
    """Get emoji for language code."""
    language_emojis = {
        "en": "🇺🇸",
        "ja": "🇯🇵",
        "es": "🇪🇸",
        "ko": "🇰🇷",
        "zh": "🇨🇳",
        "ru": "🇷🇺",
        "fr": "🇫🇷",
        "de": "🇩🇪",
        "pt": "🇵🇹",
        "it": "🇮🇹",
    }
    return language_emojis.get(language_code, "🌍")


def get_language_name(language_code: str) -> str:
    """Get full language name from code."""
    language_names = {
        "en": "English",
        "ja": "日本語",
        "es": "Español",
        "ko": "한국어",
        "zh": "中文",
        "ru": "Русский",
        "fr": "Français",
        "de": "Deutsch",
        "pt": "Português",
        "it": "Italiano",
    }
    return language_names.get(language_code, language_code)


def get_activity_badge(monthly_uploads: Optional[float]) -> Optional[str]:
    """Get activity badge based on monthly uploads."""
    if not monthly_uploads:
        return None

    if monthly_uploads > 10:
        return "🔥 Very Active"
    elif monthly_uploads > 5:
        return "📈 Active"
    elif monthly_uploads > 2:
        return "📊 Regular"
    else:
        return "⚠️ Inactive"
