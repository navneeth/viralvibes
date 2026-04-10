"""
My Dashboards page - user's playlist analysis history.
"""

import json
import logging
from datetime import datetime

from fasthtml.common import *
from monsterui.all import *

from components.tables import Badge
from utils import format_date_relative, format_date_simple, format_number

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Billing section
# ---------------------------------------------------------------------------

_PLAN_BADGE_CLS = {
    "free": "bg-gray-100 text-gray-700",
    "pro": "bg-red-100 text-red-700",
    "agency": "bg-purple-100 text-purple-700",
}

_STATUS_BADGE_CLS = {
    "active": "bg-green-100 text-green-700",
    "trialing": "bg-blue-100 text-blue-700",
    "past_due": "bg-yellow-100 text-yellow-800",
    "canceled": "bg-gray-100 text-gray-500",
    "inactive": "bg-gray-100 text-gray-500",
}


def render_billing_section(plan_info: dict) -> Div:
    """
    Billing summary card — shown at the top of /me/dashboards.

    plan_info keys (from db.get_user_plan):
        plan, interval, status, current_period_end
    """
    plan = plan_info.get("plan", "free")
    status = plan_info.get("status", "inactive")
    interval = plan_info.get("interval")  # 'month' | 'year' | None
    period_end = plan_info.get("current_period_end")  # ISO string | None

    plan_label = plan.capitalize()
    status_label = status.replace("_", " ").capitalize()

    plan_badge_cls = _PLAN_BADGE_CLS.get(plan, "bg-gray-100 text-gray-700")
    status_badge_cls = _STATUS_BADGE_CLS.get(status, "bg-gray-100 text-gray-500")

    # Renewal / expiry line
    if period_end and status in ("active", "trialing", "past_due"):
        try:
            dt = datetime.fromisoformat(period_end.replace("Z", "+00:00"))
            verb = "Trial ends" if status == "trialing" else "Renews"
            period_line = f"{verb} {dt.strftime('%b %-d, %Y')}"
        except (ValueError, AttributeError):
            period_line = None
    elif status == "canceled" and period_end:
        try:
            dt = datetime.fromisoformat(period_end.replace("Z", "+00:00"))
            period_line = f"Access until {dt.strftime('%b %-d, %Y')}"
        except (ValueError, AttributeError):
            period_line = None
    else:
        period_line = None

    interval_label = {"month": "Monthly", "year": "Annual"}.get(interval, "") if interval else ""

    # CTA buttons
    if plan == "free":
        cta = A(
            UkIcon("arrow-up-circle", cls="w-4 h-4 mr-1.5"),
            Span("Upgrade to Pro"),
            href="/pricing",
            cls="inline-flex items-center px-4 py-2 rounded-lg bg-red-500 hover:bg-red-600 "
            "text-white text-sm font-semibold transition-colors",
        )
    else:
        cta = A(
            UkIcon("settings", cls="w-4 h-4 mr-1.5"),
            Span("Manage billing"),
            href="/billing/portal",
            cls="inline-flex items-center px-4 py-2 rounded-lg border border-border "
            "text-sm font-medium hover:bg-muted transition-colors",
        )

    return Div(
        Div(
            # Left: plan info
            Div(
                Span(
                    "Your plan",
                    cls="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2 block",
                ),
                Div(
                    Span(
                        plan_label,
                        cls=f"text-sm font-bold px-2.5 py-0.5 rounded-full {plan_badge_cls}",
                    ),
                    *(
                        [Span(interval_label, cls="text-xs text-muted-foreground ml-2")]
                        if interval_label
                        else []
                    ),
                    Span(
                        status_label,
                        cls=f"text-xs px-2 py-0.5 rounded-full ml-2 {status_badge_cls}",
                    ),
                    cls="flex items-center flex-wrap gap-1",
                ),
                *(
                    [P(period_line, cls="text-xs text-muted-foreground mt-1.5")]
                    if period_line
                    else []
                ),
            ),
            # Right: CTA
            cta,
            cls="flex items-center justify-between gap-4 flex-wrap",
        ),
        cls="mb-8 p-4 rounded-xl border border-border bg-muted/40",
    )


def extract_engagement_metrics(summary_stats: dict | None) -> dict:
    """
    Extract engagement metrics from summary_stats JSON.

    Returns:
        {
            'avg_likes': float,
            'avg_comments': float,
            'engagement_rate': float,  # (likes+comments)/views * 100
        }
    """
    if not summary_stats:
        return {"avg_likes": 0, "avg_comments": 0, "engagement_rate": 0}

    try:
        if isinstance(summary_stats, str):
            summary_stats = json.loads(summary_stats)

        total_likes = summary_stats.get("total_likes", 0) or 0
        total_comments = summary_stats.get("total_comments", 0) or 0
        total_views = max(1, summary_stats.get("total_views", 0))

        avg_likes = total_likes / max(1, summary_stats.get("video_count", 1))
        avg_comments = total_comments / max(1, summary_stats.get("video_count", 1))
        engagement_rate = (total_likes + total_comments) / total_views * 100

        return {
            "avg_likes": avg_likes,
            "avg_comments": avg_comments,
            "engagement_rate": engagement_rate,
        }
    except (json.JSONDecodeError, TypeError, KeyError, ZeroDivisionError) as e:
        logger.warning(
            f"Failed to extract engagement metrics from summary_stats: {type(e).__name__}: {e}"
        )
        return {"avg_likes": 0, "avg_comments": 0, "engagement_rate": 0}


def render_my_dashboards_page(
    dashboards: list[dict],
    user_name: str,
    search: str = "",
    sort: str = "recent",
    plan_info: dict | None = None,
) -> Div:
    """
    Render the My Dashboards page with grid layout.

    Uses MonsterUI Grid component for responsive layout.
    """
    _plan_info = plan_info or {
        "plan": "free",
        "status": "inactive",
        "interval": None,
        "current_period_end": None,
    }

    return Container(
        # Page header
        Div(
            H1(f"👋 Welcome back, {user_name}!", cls="text-4xl font-bold mb-2"),
            P(
                f"You have {len(dashboards)} dashboard{'s' if len(dashboards) != 1 else ''}",
                cls="text-gray-600 text-lg",
            ),
            cls="mb-8 mt-8",
        ),
        # Billing summary
        render_billing_section(_plan_info),
        # Search & Filter Bar
        render_search_filter_bar(search=search, sort=sort),
        # Dashboard Grid or Empty State
        (render_dashboard_grid(dashboards) if dashboards else render_empty_state(search)),
        cls=ContainerT.xl,
    )


def render_search_filter_bar(search: str = "", sort: str = "recent") -> Form:
    """Search and sort controls using MonsterUI form components."""

    return Form(
        Div(
            # Search input
            Input(
                type="search",
                name="search",
                placeholder="Search playlists by name or channel...",
                value=search,
                cls="input input-bordered flex-1",
                autofocus=bool(search),  # Focus if there's an active search
            ),
            # Sort dropdown
            Select(
                Option("Most Recent", value="recent", selected=(sort == "recent")),
                Option("Most Views", value="views", selected=(sort == "views")),
                Option("Most Videos", value="videos", selected=(sort == "videos")),
                Option("Alphabetical", value="title", selected=(sort == "title")),
                name="sort",
                cls="select select-bordered w-48",
                onchange="this.form.submit()",
            ),
            cls="flex gap-4 items-center mb-8",
        ),
        method="GET",
        action="/me/dashboards",
    )


def render_dashboard_grid(dashboards: list[dict]) -> Div:
    """
    Render responsive grid of dashboard cards.

    Uses MonsterUI Grid component:
    - 1 column on mobile
    - 2 columns on tablet
    - 3 columns on desktop
    """

    return Grid(
        *[render_dashboard_card(d) for d in dashboards],
        cols=1,  # Mobile: 1 column
        cols_md=2,  # Tablet: 2 columns
        cols_lg=3,  # Desktop: 3 columns
        gap=6,  # 1.5rem gap
        cls="mb-12",
    )


def get_engagement_gradient(rate: float) -> str:
    """Get gradient color based on engagement rate."""
    if rate > 10:
        return "from-green-400 to-green-600"
    elif rate > 5:
        return "from-yellow-400 to-yellow-600"
    else:
        return "from-gray-300 to-gray-500"


def render_dashboard_card(dashboard: dict) -> A:
    """
    Render a single dashboard card with proper aspect ratio, creator thumbnail,
    and rich metrics.

    Design:
    - 16:9 aspect ratio thumbnail (respects playlist image proportions)
    - Creator avatar overlay (bottom-left corner) - prominently featured
    - Rich metrics with visual hierarchy
    - Smooth hover effects and transitions
    """

    # Extract data with safe defaults
    dashboard_id = dashboard.get("dashboard_id", "")
    title = dashboard.get("title", "Untitled Playlist")
    channel_name = dashboard.get("channel_name", "Unknown Channel")

    channel_thumbnail = dashboard.get("channel_thumbnail", "/static/favicon.jpeg")
    video_count = dashboard.get("video_count", 0) or 0
    view_count = dashboard.get("view_count", 0) or 0
    processed_on = dashboard.get("processed_on")

    # Additional metrics (if available in your data structure)
    engagement_score = dashboard.get("engagement_score", 0)
    avg_views_per_video = dashboard.get("avg_views_per_video", 0)

    summary_stats = dashboard.get("summary_stats")
    engagement_metrics = extract_engagement_metrics(summary_stats)

    # Format date
    date_str = format_date_relative(processed_on)

    # Format large numbers using existing utility
    views_formatted = format_number(view_count)

    # Calculate derived metrics
    avg_views_per_video = (view_count / video_count) if video_count > 0 else 0
    engagement_rate = engagement_metrics["engagement_rate"]
    avg_likes = engagement_metrics["avg_likes"]

    # Determine engagement level for badge styling
    engagement_level = "low"  # gray
    if engagement_rate > 5:
        engagement_level = "medium"  # yellow
    if engagement_rate > 10:
        engagement_level = "high"  # green

    engagement_colors = {
        "low": "bg-gray-100 text-gray-700",
        "medium": "bg-yellow-100 text-yellow-700",
        "high": "bg-green-100 text-green-700",
    }

    # ===== RENDER CARD =====
    return A(
        Card(
            # Thumbnail with 16:9 aspect ratio (REUSE EXISTING)
            Div(
                Div(
                    Img(
                        src=channel_thumbnail,
                        alt=f"{title} playlist thumbnail",
                        cls="w-full h-full object-cover",
                        onerror="this.src='/static/favicon.jpeg'",
                    ),
                    cls="absolute inset-0 bg-gray-300",
                ),
                # Creator avatar overlay (bottom-left)
                Div(
                    Img(
                        src=channel_thumbnail,
                        alt=f"{channel_name} channel avatar",
                        cls="w-12 h-12 rounded-full border-2 border-white shadow-lg object-cover",
                        onerror="this.src='/static/favicon.jpeg'",
                    ),
                    # Scale on hover for premium feel
                    cls="absolute bottom-3 left-3 z-10 group-hover:scale-110 transition-transform duration-200",
                ),
                # Dark overlay on hover
                Div(
                    cls="absolute inset-0 bg-gradient-to-t from-black/50 via-black/10 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-200",
                ),
                cls="relative w-full overflow-hidden bg-gray-300 aspect-video rounded-t-lg group",
            ),
            # Content section
            Div(
                # Title
                H3(
                    title,
                    cls="text-sm font-semibold mb-1 line-clamp-2 text-gray-900 group-hover:text-blue-600 transition-colors duration-200",
                    title=title,
                ),
                # Channel name
                Div(
                    UkIcon("tv", cls="w-3 h-3 text-gray-400 mr-1"),
                    Span(
                        channel_name,
                        cls="text-xs text-gray-700 font-medium truncate group-hover:text-gray-900",
                        title=channel_name,
                    ),
                    cls="flex items-center gap-0 mb-2 min-h-[1.25rem]",
                ),
                # ✨ NEW: Engagement badges (reuse Badge component from cards.py)
                Div(
                    Badge(
                        f"📊 {engagement_rate:.1f}% engagement",
                        cls=engagement_colors[engagement_level],
                    ),
                    Badge(
                        f"❤️ {format_number(int(engagement_metrics['avg_likes']))} avg likes",
                        cls="bg-red-50 text-red-700",
                    ),
                    cls="flex flex-wrap gap-2 mb-2",
                ),
                # Primary metrics row
                Div(
                    Div(
                        UkIcon("film", cls="w-3 h-3 mr-0.5"),
                        Span(
                            f"{video_count:,}",
                            cls="font-semibold text-gray-900 text-xs",
                        ),
                        Span(" videos", cls="text-gray-500 text-xs"),
                        cls="flex items-center gap-0.5",
                    ),
                    Span("•", cls="text-gray-300 text-xs mx-1"),
                    Div(
                        UkIcon("eye", cls="w-3 h-3 mr-0.5"),
                        Span(views_formatted, cls="font-semibold text-gray-900 text-xs"),
                        Span(" views", cls="text-gray-500 text-xs"),
                        cls="flex items-center gap-0.5",
                    ),
                    cls="flex items-center gap-1 mb-2 text-xs flex-wrap",
                ),
                # Secondary metrics
                Div(
                    UkIcon("trending-up", cls="w-3 h-3 mr-0.5 text-blue-600"),
                    Span(
                        f"{format_number(int(avg_views_per_video))} avg/video",
                        cls="text-xs text-gray-700 font-medium",
                    ),
                    cls="flex items-center gap-1 mb-2",
                ),
                # Engagement gauge
                Div(
                    Div(
                        cls=f"h-1 bg-gradient-to-r {get_engagement_gradient(engagement_rate)}",
                        style=f"width: {min(100, engagement_rate * 10)}%",
                    ),
                    cls="w-full h-2 bg-gray-200 rounded-full mb-3",
                ),
                # Footer with date
                Div(
                    UkIcon("calendar", cls="w-3 h-3 mr-1"),
                    Span(date_str, cls="text-xs text-gray-500"),
                    cls="flex items-center pt-2 border-t border-gray-200 gap-1",
                ),
                cls="p-4 flex flex-col flex-1 justify-between",
            ),
            cls="flex flex-col h-full hover:shadow-xl transition-all duration-200 bg-white overflow-hidden",
        ),
        href=f"/d/{dashboard_id}",
        cls="block group no-underline h-full",
        title=f"View dashboard: {title}",
    )


def render_empty_state(search: str = "") -> Div:
    """
    Empty state component when no dashboards found.

    Shows different messages for:
    - No search results
    - New user (no dashboards yet)
    """

    if search:
        # Search returned no results
        return Card(
            Div(
                # Icon
                Div(Span("🔍", cls="text-6xl mb-4"), cls="flex justify-center"),
                # Heading
                H2("No playlists found", cls="text-2xl font-bold mb-3 text-center"),
                # Message
                P(
                    f"No playlists matching '{search}'",
                    cls="text-gray-600 mb-6 text-center",
                ),
                # Actions
                Div(
                    A(
                        Button("Clear Search", cls=ButtonT.secondary),
                        href="/me/dashboards",
                    ),
                    cls="flex justify-center",
                ),
                cls="p-12 text-center",
            ),
            cls="bg-gray-50",
        )

    else:
        # User has no dashboards yet
        return Card(
            Div(
                # Icon
                Div(Span("📊", cls="text-6xl mb-4"), cls="flex justify-center"),
                # Heading
                H2("No dashboards yet", cls="text-2xl font-bold mb-3 text-center"),
                # Message
                P(
                    "You haven't analyzed any playlists yet. Get started by analyzing your first playlist!",
                    cls="text-gray-600 mb-6 text-center max-w-md mx-auto",
                ),
                # CTA
                Div(
                    A(
                        Button(
                            "🚀 Analyze Your First Playlist",
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
