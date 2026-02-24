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

from __future__ import annotations

import logging
from urllib.parse import urlencode

from fasthtml.common import *
from monsterui.all import *

from utils import format_date_relative, format_number, safe_get_value
from utils.creator_metrics import (
    calculate_avg_views_per_video,
    calculate_growth_rate,
    calculate_views_per_subscriber,
    estimate_monthly_revenue,
    format_channel_age,
    get_activity_badge,
    get_activity_title,
    get_age_emoji,
    get_age_title,
    get_country_flag,
    get_grade_info,
    get_growth_signal,
    get_language_emoji,
    get_language_name,
    get_sync_status_badge,
)

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
    page: int = 1,
    per_page: int = 50,
    total_count: int = 0,
    total_pages: int = 1,
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

    # Check if any filters are active
    has_active_filters = (
        search
        or grade_filter != "all"
        or language_filter != "all"
        or activity_filter != "all"
        or age_filter != "all"
    )

    return Container(
        # Hero section with real stats from DB
        _render_hero(
            stats=stats,
            filtered_count=len(creators),
            has_filters=has_active_filters,
        ),
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
            Div(
                _render_creators_grid(creators),
                # Pagination controls
                _render_pagination(
                    page=page,
                    total_pages=total_pages,
                    search=search,
                    sort=sort,
                    grade_filter=grade_filter,
                    language_filter=language_filter,
                    activity_filter=activity_filter,
                    age_filter=age_filter,
                    per_page=per_page,
                    total_count=total_count,
                ),
            )
            if creators
            else _render_empty_state(search, grade_filter)
        ),
        cls=ContainerT.xl,
    )


def _render_hero(
    stats: dict, filtered_count: int = 0, has_filters: bool = False
) -> Div:
    """
    Hero section with marketing-relevant statistics from database.

    Smart display:
    - No filters: Shows total creators from DB (e.g., "500 creators")
    - With filters: Shows filtered count + total (e.g., "45 of 500 creators")

    All numbers come from DB state (via stats dict), NOT from filtered results.
    Designed for agencies looking to identify collaboration opportunities.
    """
    total_creators = stats.get("total_creators", 0)

    return Div(
        Div(
            H1(
                "Creator Intelligence",
                cls="text-5xl font-bold text-gray-900 tracking-tight",
            ),
            P(
                "Discover high-performing creators for brand collaborations.",
                cls="text-lg text-gray-600 mt-2",
            ),
            cls="mb-8",
        ),
        # Metric Strip - marketing-focused metrics from DB
        Div(
            # Total/Filtered creators - smart display based on filter state
            Div(
                P(
                    "Filtered Results" if has_filters else "Total Creators",
                    cls="text-xs font-semibold text-gray-500 uppercase tracking-wider",
                ),
                H2(
                    (
                        f"{format_number(filtered_count)} of {format_number(total_creators)}"
                        if has_filters
                        else format_number(total_creators)
                    ),
                    cls="text-4xl font-bold text-gray-900 mt-2",
                ),
                P(
                    "matching filters" if has_filters else "In database",
                    cls="text-xs text-gray-600 mt-1",
                ),
                cls="text-center",
            ),
            # Premium creators (A+/A grade)
            Div(
                P(
                    "Premium Creators",
                    cls="text-xs font-semibold text-gray-500 uppercase tracking-wider",
                ),
                H2(
                    format_number(stats.get("premium_creators", 0)),
                    cls="text-4xl font-bold text-purple-600 mt-2",
                ),
                P(
                    "A+ / A grade",
                    cls="text-xs text-gray-600 mt-1",
                ),
                cls="text-center",
            ),
            # Growing creators (trending)
            Div(
                P(
                    "Growing Creators",
                    cls="text-xs font-semibold text-gray-500 uppercase tracking-wider",
                ),
                H2(
                    format_number(stats.get("growing_creators", 0)),
                    cls="text-4xl font-bold text-green-600 mt-2",
                ),
                P(
                    "Positive momentum",
                    cls="text-xs text-gray-600 mt-1",
                ),
                cls="text-center",
            ),
            # Average engagement (quality indicator)
            Div(
                P(
                    "Avg Engagement",
                    cls="text-xs font-semibold text-gray-500 uppercase tracking-wider",
                ),
                H2(
                    (
                        "–"
                        if not stats.get("has_engagement_data", False)
                        else f"{stats.get('avg_engagement', 0):.1f}%"
                    ),
                    cls="text-4xl font-bold text-blue-600 mt-2",
                ),
                P(
                    (
                        "No data"
                        if not stats.get("has_engagement_data", False)
                        else "Audience quality"
                    ),
                    cls="text-xs text-gray-600 mt-1",
                ),
                cls="text-center",
            ),
            cls="grid grid-cols-2 md:grid-cols-4 gap-6 md:gap-8 py-8 border-t border-b border-gray-200",
        ),
        cls="bg-white rounded-lg border border-gray-200 p-6 md:p-8 mb-8",
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
    """
    Clean horizontal card-based filter bar.

    Shows search + sort on top line, then 4 filter cards below.
    All filters visible at once, no accordion clicks needed.

    Space: ~150px, all filters visible
    Clicks: 0 (vs 1-4 with accordion)
    Design: Modern, card-based, professional
    """

    # ═══════════════════════════════════════════════════════════════
    # 1. SEARCH FORM
    # ═══════════════════════════════════════════════════════════════
    search_form = Form(
        Input(
            type="search",
            name="search",
            placeholder="Search creators by name or @handle...",
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

    # ═══════════════════════════════════════════════════════════════
    # 2. SORT DROPDOWN
    # ═══════════════════════════════════════════════════════════════
    sort_options = [
        ("subscribers", "📊 Most Subscribers"),
        ("views", "👀 Most Views"),
        ("engagement", "🔥 Best Engagement"),
        ("quality", "⭐ Quality Score"),
        ("recent", "🆕 Recently Updated"),
        ("consistency", "📈 Most Consistent"),
        ("newest_channel", "🎉 Newest Channels"),
        ("oldest_channel", "👑 Oldest Channels"),
    ]

    sort_form = Form(
        Div(
            Label("Sort:", cls="text-sm font-semibold text-gray-700 whitespace-nowrap"),
            Select(
                *[
                    Option(label, value=val, selected=(sort == val))
                    for val, label in sort_options
                ],
                name="sort",
                cls="h-10 px-3 rounded-lg border border-gray-300 font-medium",
                onchange="this.form.submit()",
            ),
            cls="flex gap-2 items-center flex-1",
        ),
        Input(type="hidden", name="search", value=search),
        Input(type="hidden", name="grade", value=grade_filter),
        Input(type="hidden", name="language", value=language_filter),
        Input(type="hidden", name="activity", value=activity_filter),
        Input(type="hidden", name="age", value=age_filter),
        method="GET",
        action="/creators",
    )

    # ═══════════════════════════════════════════════════════════════
    # 3. QUALITY GRADE PILLS
    # ═══════════════════════════════════════════════════════════════
    grade_options = [
        ("all", "All", "🎯"),
        ("A+", "Elite", "👑"),
        ("A", "Star", "⭐"),
        ("B+", "Rising", "📈"),
        ("B", "Good", "💎"),
        ("C", "New", "🔍"),
    ]

    grade_pills = Div(
        *[
            A(
                f"{emoji} {label}",
                href=f"/creators?{urlencode({'sort': sort, 'search': search, 'grade': val, 'language': language_filter, 'activity': activity_filter, 'age': age_filter})}",
                cls=(
                    "px-2.5 py-1 rounded-md transition-all inline-block no-underline text-xs font-medium "
                    + (
                        "bg-blue-600 text-white shadow-sm"
                        if grade_filter == val
                        else "bg-white border border-gray-200 hover:bg-gray-50 text-gray-700"
                    )
                ),
            )
            for val, label, emoji in grade_options
        ],
        cls="flex gap-1.5 flex-wrap",
    )

    # ═══════════════════════════════════════════════════════════════
    # 4. LANGUAGE FILTER PILLS
    # ═══════════════════════════════════════════════════════════════
    language_options = [
        ("all", "All", "🌍"),
        ("en", "English", "🇺🇸"),
        ("ja", "日本語", "🇯🇵"),
        ("es", "Español", "🇪🇸"),
        ("ko", "Korean", "🇰🇷"),
        ("zh", "Chinese", "🇨🇳"),
    ]

    language_pills = Div(
        *[
            A(
                f"{emoji} {label}",
                href=f"/creators?{urlencode({'sort': sort, 'search': search, 'grade': grade_filter, 'language': val, 'activity': activity_filter, 'age': age_filter})}",
                cls=(
                    "px-2.5 py-1 rounded-md transition-all inline-block no-underline text-xs font-medium "
                    + (
                        "bg-blue-100 text-blue-700 border border-blue-300"
                        if language_filter == val
                        else "bg-white border border-gray-200 hover:bg-gray-50 text-gray-700"
                    )
                ),
            )
            for val, label, emoji in language_options
        ],
        cls="flex gap-1.5 flex-wrap",
    )

    # ═══════════════════════════════════════════════════════════════
    # 5. ACTIVITY FILTER PILLS
    # ═══════════════════════════════════════════════════════════════
    activity_options = [
        ("all", "All", "📊"),
        ("active", "Active (>5/mo)", "🔥"),
        ("dormant", "Dormant (<1/mo)", "⚠️"),
    ]

    activity_pills = Div(
        *[
            A(
                f"{emoji} {label}",
                href=f"/creators?{urlencode({'sort': sort, 'search': search, 'grade': grade_filter, 'language': language_filter, 'activity': val, 'age': age_filter})}",
                cls=(
                    "px-2.5 py-1 rounded-md transition-all inline-block no-underline text-xs font-medium "
                    + (
                        "bg-green-100 text-green-700 border border-green-300"
                        if activity_filter == val
                        else "bg-white border border-gray-200 hover:bg-gray-50 text-gray-700"
                    )
                ),
            )
            for val, label, emoji in activity_options
        ],
        cls="flex gap-1.5 flex-wrap",
    )

    # ═══════════════════════════════════════════════════════════════
    # 6. CHANNEL AGE FILTER PILLS
    # ═══════════════════════════════════════════════════════════════
    age_options = [
        ("all", "All", "📅"),
        ("new", "0–1 yr", "🆕"),
        ("established", "1–10 yrs", "🏆"),
        ("veteran", "10+ yrs", "👑"),
    ]

    age_pills = Div(
        *[
            A(
                f"{emoji} {label}",
                href=f"/creators?{urlencode({'sort': sort, 'search': search, 'grade': grade_filter, 'language': language_filter, 'activity': activity_filter, 'age': val})}",
                cls=(
                    "px-2.5 py-1 rounded-md transition-all inline-block no-underline text-xs font-medium "
                    + (
                        "bg-purple-100 text-purple-700 border border-purple-300"
                        if age_filter == val
                        else "bg-white border border-gray-200 hover:bg-gray-50 text-gray-700"
                    )
                ),
            )
            for val, label, emoji in age_options
        ],
        cls="flex gap-1.5 flex-wrap",
    )

    # ═══════════════════════════════════════════════════════════════
    # 7. COUNT ACTIVE FILTERS
    # ═══════════════════════════════════════════════════════════════
    active_filters = sum(
        f != "all" for f in (grade_filter, language_filter, activity_filter, age_filter)
    )

    # ═══════════════════════════════════════════════════════════════
    # 8. BUILD FLOATING FILTER BUTTON
    # ═══════════════════════════════════════════════════════════════
    filter_button = A(
        # Icon and text
        Div(
            # Filter icon (using emoji for consistency)
            Span("🔍", cls="text-xl md:text-2xl", aria_hidden="true"),
            # Keep label available to screen readers on small screens, visible from sm and up
            Span(
                "Filters", cls="text-xs md:text-sm font-semibold sr-only sm:not-sr-only"
            ),
            cls="flex items-center gap-2",
        ),
        # Active count badge (only show if filters are active)
        (
            Span(
                str(active_filters),
                cls="absolute -top-1 -right-1 md:-top-1.5 md:-right-1.5 bg-red-500 text-white text-[10px] md:text-xs font-bold w-4 h-4 md:w-5 md:h-5 rounded-full flex items-center justify-center",
                aria_label=f"{active_filters} active filters",
            )
            if active_filters > 0
            else None
        ),
        href="#filter-modal",
        uk_toggle=True,
        aria_label="Open filters menu",
        cls="fixed bottom-4 right-4 md:bottom-6 md:right-6 lg:bottom-8 lg:right-8 z-[999] bg-purple-600 hover:bg-purple-700 active:bg-purple-800 text-white rounded-full px-4 py-3 md:px-5 md:py-3.5 shadow-2xl hover:shadow-purple-500/50 transition-all hover:scale-105 active:scale-95 no-underline",
    )

    # ═══════════════════════════════════════════════════════════════
    # 9. BUILD FILTER MODAL WITH ACCORDION
    # ═══════════════════════════════════════════════════════════════

    # Reset filters link
    reset_link = (
        A(
            "Reset All Filters",
            href=f"/creators?{urlencode({'sort': sort, 'search': search})}",
            cls="text-sm font-medium text-purple-600 hover:text-purple-700 hover:underline",
        )
        if active_filters > 0
        else None
    )

    filter_modal = Div(
        Div(
            Div(
                # Header
                Div(
                    Div(
                        H3(
                            "Filter Creators",
                            cls="text-xl font-bold text-gray-900 mb-1",
                        ),
                        P(
                            f"{grade_counts.get('all', 0)} creators available",
                            cls="text-sm text-gray-600",
                        ),
                        cls="flex-1",
                    ),
                    # Close button
                    Button(
                        Span("✕", cls="text-xl"),
                        cls="uk-modal-close-default p-2 hover:bg-gray-100 rounded-lg transition-colors",
                        type="button",
                    ),
                    cls="flex items-start justify-between mb-4 pb-4 border-b border-gray-100",
                ),
                # Reset link
                (
                    Div(
                        reset_link,
                        cls="mb-4",
                    )
                    if reset_link
                    else None
                ),
                # Accordion with filters
                Accordion(
                    AccordionItem(
                        "Quality Grade",
                        grade_pills,
                        open=(grade_filter != "all"),
                    ),
                    AccordionItem(
                        "Language",
                        language_pills,
                        open=(language_filter != "all"),
                    ),
                    AccordionItem(
                        "Activity Level",
                        activity_pills,
                        open=(activity_filter != "all"),
                    ),
                    AccordionItem(
                        "Channel Age",
                        age_pills,
                        open=(age_filter != "all"),
                    ),
                    multiple=True,
                    collapsible=True,
                    cls="space-y-2",
                ),
                cls="uk-modal-body bg-white rounded-t-3xl md:rounded-2xl p-6 max-h-[85vh] overflow-y-auto",
            ),
            cls="uk-modal-dialog uk-margin-auto-vertical",
        ),
        id="filter-modal",
        uk_modal="bg-close: true; esc-close: true;",
        cls="uk-modal",
    )

    # ═══════════════════════════════════════════════════════════════
    # 10. RETURN CLEAN TOP BAR + FLOATING BUTTON + MODAL
    # ═══════════════════════════════════════════════════════════════
    return Div(
        # Compact top bar: Search + Sort only
        Div(
            search_form,
            sort_form,
            cls="flex gap-3",
        ),
        # Floating filter button (fixed position)
        filter_button,
        # Filter modal (hidden until toggled)
        filter_modal,
        cls="sticky top-0 bg-white border-b border-gray-200 p-4 shadow-sm z-30",
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
    channel_url: str,
    custom_url: str,
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
    # Normalise handle — strip leading @ and re-add for consistency
    handle_display = f"@{custom_url.lstrip('@')}" if custom_url else None

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
                # Channel name as a link to the channel
                A(
                    H3(
                        channel_name,
                        cls="font-semibold text-gray-900 truncate mb-0.5 hover:text-blue-600 transition-colors",
                    ),
                    href=channel_url,
                    target="_blank",
                    rel="noopener noreferrer",
                    cls="no-underline",
                ),
                # @handle in small muted text
                (
                    P(handle_display, cls="text-xs text-gray-400 font-medium mb-0.5")
                    if handle_display
                    else None
                ),
                P(
                    f"{format_number(current_subs)} subscribers · {current_videos} videos",
                    cls="text-xs text-gray-600",
                ),
                cls="flex-1",
            ),
            # Quality grade badge — hidden for grade C ("New"), which just means unscored
            (
                Div(
                    Div(
                        P(grade_icon, cls="text-lg"),
                        cls="flex flex-col items-center",
                    ),
                    Div(
                        P(grade_label, cls="text-xs font-semibold text-right"),
                        cls="text-right",
                    ),
                    cls=f"px-3 py-2 rounded-lg {grade_bg} flex gap-2",
                )
                if quality_grade and quality_grade != "C"
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
    estimated_revenue: int,
    views_per_sub: float,
) -> Div:
    """Build performance metrics grid."""
    return Div(
        Div(
            P("AVG VIEWS", cls="text-xs font-semibold text-gray-600 uppercase"),
            P(
                format_number(avg_views_per_video),
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
            P("VIEWS/SUB", cls="text-xs font-semibold text-indigo-700 uppercase"),
            P(
                f"{views_per_sub:.1f}x",
                cls="text-lg font-bold text-indigo-600 mt-1",
            ),
            P("audience reach", cls="text-xs text-indigo-500"),
            cls="bg-indigo-50 rounded-lg p-3 text-center",
        ),
        Div(
            P(
                "EST. REVENUE",
                cls="text-xs font-semibold text-green-700 uppercase font-bold",
            ),
            P(
                f"${format_number(estimated_revenue)}",
                cls="text-lg font-bold text-green-600 mt-1",
            ),
            P("/month est.", cls="text-xs text-green-600"),
            cls="bg-green-50 rounded-lg p-3 text-center",
        ),
        cls="grid grid-cols-2 gap-3 mb-4",
    )


def _build_growth_trend(
    growth_rate: float, growth_label: str, growth_style: str
) -> Div:
    """Build growth trend indicator section."""
    return Div(
        Div(
            P("30-DAY TREND", cls="text-xs font-semibold text-gray-600"),
            Div(
                P(
                    f"{growth_rate:+.1f}%",
                    cls="text-sm font-bold text-gray-900",
                ),
                Span(
                    growth_label,
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


def _render_bio(bio: str | None, max_chars: int = 130) -> P | None:
    """Render a truncated bio paragraph, or None if no bio is available."""
    if not bio:
        return None
    text = bio[:max_chars].rstrip() + "…" if len(bio) > max_chars else bio
    return P(text, cls="text-xs text-gray-500 leading-relaxed mb-4 line-clamp-2")


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


def _build_info_strip(
    language: str,
    country_code: str,
    channel_age_days: int,
    monthly_uploads: float,
    custom_url: str = "",
) -> Div | None:
    """Build clean emoji/icon strip showing key channel info."""
    # Build icon list
    icons = []

    # Country flag
    country_flag = get_country_flag(country_code)
    if country_flag:
        icons.append(
            Span(
                country_flag,
                title=f"Country: {country_code.upper()}",
                cls="text-lg",
            )
        )

    # Language
    if language:
        icons.append(
            Span(
                get_language_emoji(language),
                title=f"Language: {get_language_name(language)}",
                cls="text-lg",
            )
        )

    # Channel age — show emoji + human-readable text (e.g. "🆕 8mo", "👑 12y")
    if channel_age_days:
        icons.append(
            Span(
                f"{get_age_emoji(channel_age_days)} {format_channel_age(channel_age_days)}",
                title=f"Channel age: {get_age_title(channel_age_days)}",
                cls="text-xs font-medium text-gray-600",
            )
        )

    # Activity level — use get_activity_badge for labelled text (e.g. "🔥 Very Active")
    activity_badge = get_activity_badge(monthly_uploads)
    if activity_badge:
        icons.append(
            Span(
                activity_badge,
                title=f"Upload frequency: {get_activity_title(monthly_uploads)}",
                cls="text-xs font-medium text-gray-600",
            )
        )
    # Note: custom_url checkmark removed — @handle is now shown in the card header

    if not icons:
        return None

    return Div(
        *icons,
        cls="flex items-center justify-center gap-3 py-2 bg-gray-50 rounded-lg",
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
    views_per_sub = calculate_views_per_subscriber(current_views, current_subs)

    # === STATUS & STYLING ===
    sync_status = safe_get_value(creator, "sync_status", "pending")
    sync_badge_info = get_sync_status_badge(sync_status)
    card_border = f"border-l-4 border-amber-400" if sync_status != "synced" else ""

    grade_icon, grade_label, grade_bg = get_grade_info(quality_grade)
    growth_label, growth_style = get_growth_signal(growth_rate)

    # === INFO STRIP DATA ===
    custom_url = safe_get_value(creator, "custom_url", "")
    language = safe_get_value(creator, "default_language", "")
    country_code = safe_get_value(creator, "country_code", "")
    monthly_uploads = safe_get_value(creator, "monthly_uploads", 0)
    keywords = safe_get_value(creator, "keywords", "")

    info_strip = _build_info_strip(
        language, country_code, channel_age_days, monthly_uploads, custom_url
    )

    # === COMPOSE CARD ===
    return Div(
        # Sync status badge (if not synced)
        (
            Div(
                f"{sync_badge_info[0]} {sync_badge_info[1]}",
                cls=f"text-xs font-semibold px-3 py-1 rounded-t-lg {sync_badge_info[2]}",
            )
            if sync_badge_info
            else None
        ),
        # Header section
        _build_card_header(
            thumbnail_url,
            channel_name,
            channel_url,
            custom_url,
            current_subs,
            current_videos,
            rank,
            grade_icon,
            grade_label,
            grade_bg,
            quality_grade,
            channel_age_days,
        ),
        # Bio — shown when present, truncated to keep cards uniform
        _render_bio(
            safe_get_value(creator, "bio")
            or safe_get_value(creator, "channel_description")
        ),
        # Primary metrics
        _build_primary_metrics(current_subs, subs_change, current_views, views_change),
        # Performance metrics
        _build_performance_metrics(
            avg_views_per_video, current_videos, estimated_revenue, views_per_sub
        ),
        # Growth trend
        _build_growth_trend(growth_rate, growth_label, growth_style),
        # Keywords (if available)
        (
            Div(
                P(
                    keywords,
                    cls="text-xs text-gray-500 italic line-clamp-1 text-center",
                ),
                cls="mb-2",
            )
            if keywords
            else None
        ),
        # Info strip at bottom (clean emoji display)
        info_strip,
        # Footer
        _build_card_footer(last_updated, channel_url),
        cls=f"bg-white rounded-lg border border-gray-200 p-4 hover:shadow-md hover:scale-[1.02] transition-all duration-300 cursor-pointer {card_border}",
    )


def _render_pagination(
    page: int,
    total_pages: int,
    search: str,
    sort: str,
    grade_filter: str,
    language_filter: str,
    activity_filter: str,
    age_filter: str,
    per_page: int,
    total_count: int,
) -> Div:
    """
    Render pagination controls with smart page button display.

    Pattern inspired by FastHTML best practices:
    - Shows first, last, and current pages
    - Uses ellipsis (...) for skipped ranges
    - Highlights current page
    - Preserves all filter state in URLs
    """
    if total_pages <= 1:
        return Div()  # No pagination needed

    # Build base query params
    base_params = {
        "search": search,
        "sort": sort,
        "grade": grade_filter,
        "language": language_filter,
        "activity": activity_filter,
        "age": age_filter,
        "per_page": str(per_page),
    }

    def page_link(page_num: int, label: str = None, is_current: bool = False):
        """Generate a page link button."""
        if label is None:
            label = str(page_num)

        params = {**base_params, "page": str(page_num)}
        url = f"/creators?{urlencode(params)}"

        if is_current:
            return Span(
                label,
                cls="px-3 py-2 bg-purple-600 text-white font-semibold rounded-lg cursor-default",
            )
        else:
            return A(
                label,
                href=url,
                cls="px-3 py-2 bg-white border border-gray-300 text-gray-700 font-medium rounded-lg hover:bg-gray-50 transition-colors no-underline",
            )

    def ellipsis():
        """Render ellipsis for skipped pages."""
        return Span("...", cls="px-2 text-gray-400")

    # Smart page button display logic (like the FastHTML example)
    buttons = []

    if total_pages <= 7:
        # Show all pages if 7 or fewer
        buttons = [
            page_link(p, is_current=(p == page)) for p in range(1, total_pages + 1)
        ]
    elif page <= 3:
        # Near start: [1] [2] [3] [4] ... [last]
        buttons = [page_link(p, is_current=(p == page)) for p in range(1, 5)]
        buttons.append(ellipsis())
        buttons.append(page_link(total_pages))
    elif page >= total_pages - 2:
        # Near end: [1] ... [last-3] [last-2] [last-1] [last]
        buttons.append(page_link(1))
        buttons.append(ellipsis())
        buttons.extend(
            [
                page_link(p, is_current=(p == page))
                for p in range(total_pages - 3, total_pages + 1)
            ]
        )
    else:
        # Middle: [1] ... [current-1] [current] [current+1] ... [last]
        buttons.append(page_link(1))
        buttons.append(ellipsis())
        buttons.extend(
            [page_link(p, is_current=(p == page)) for p in range(page - 1, page + 2)]
        )
        buttons.append(ellipsis())
        buttons.append(page_link(total_pages))

    # Previous/Next buttons
    prev_params = {**base_params, "page": str(page - 1)}
    next_params = {**base_params, "page": str(page + 1)}

    prev_button = (
        A(
            "← Previous",
            href=f"/creators?{urlencode(prev_params)}",
            cls="px-4 py-2 bg-white border border-gray-300 text-gray-700 font-medium rounded-lg hover:bg-gray-50 transition-colors no-underline",
        )
        if page > 1
        else Span(
            "← Previous",
            cls="px-4 py-2 bg-gray-100 text-gray-400 font-medium rounded-lg cursor-not-allowed",
        )
    )

    next_button = (
        A(
            "Next →",
            href=f"/creators?{urlencode(next_params)}",
            cls="px-4 py-2 bg-white border border-gray-300 text-gray-700 font-medium rounded-lg hover:bg-gray-50 transition-colors no-underline",
        )
        if page < total_pages
        else Span(
            "Next →",
            cls="px-4 py-2 bg-gray-100 text-gray-400 font-medium rounded-lg cursor-not-allowed",
        )
    )

    # Results info
    start_result = (page - 1) * per_page + 1
    end_result = min(page * per_page, total_count)

    return Div(
        # Results summary
        Div(
            P(
                f"Showing {start_result:,}–{end_result:,} of {total_count:,} creators",
                cls="text-sm text-gray-600",
            ),
            cls="text-center mb-4",
        ),
        # Pagination controls
        Div(
            prev_button,
            Div(*buttons, cls="flex gap-1"),
            next_button,
            cls="flex items-center justify-center gap-4 flex-wrap",
        ),
        cls="py-8",
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


# NOTE: Helper functions moved to utils/creator_metrics.py for better organization
# - get_language_emoji, get_language_name, get_activity_badge
# - estimate_monthly_revenue (replaces _estimate_monthly_revenue)
