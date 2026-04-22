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
            period_line = f"{verb} {dt.strftime('%b %d, %Y').replace(' 0', ' ')}"
        except (ValueError, AttributeError):
            period_line = None
    elif status == "canceled" and period_end:
        try:
            dt = datetime.fromisoformat(period_end.replace("Z", "+00:00"))
            period_line = f"Access until {dt.strftime('%b %d, %Y').replace(' 0', ' ')}"
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
    fav_creators: list[dict] | None = None,
) -> Div:
    """
    Product-driven user home.

    Sections (top → bottom):
      1. Header  — greeting + plan badge
      2. Creators / Watchlist Pulse  — saved creators ranked by growth (Pro/Agency)
      3. Playlist Analysis  — the core job-to-be-done, search + grid
      4. Lists  — discovery shortcuts to public lists
      5. Campaigns  — stub, coming soon
    """
    _plan_info = plan_info or {
        "plan": "free",
        "status": "inactive",
        "interval": None,
        "current_period_end": None,
    }
    plan = _plan_info.get("plan", "free")
    _fav_creators = fav_creators or []

    return Container(
        # ── 1. Header ──────────────────────────────────────────────────────
        _section_header(user_name, len(dashboards), _plan_info),
        # ── 2. Watchlist Pulse ─────────────────────────────────────────────
        render_watchlist_pulse(_fav_creators, plan),
        # ── 3. Playlist Analysis ───────────────────────────────────────────
        _section_analysis(dashboards, search, sort),
        # ── 4. Lists ───────────────────────────────────────────────────────
        _section_lists(plan),
        # ── 5. Campaigns ───────────────────────────────────────────────────
        _section_campaigns(plan),
        cls=ContainerT.xl,
    )


# ---------------------------------------------------------------------------
# Section helpers
# ---------------------------------------------------------------------------


def _section_header(user_name: str, dashboard_count: int, plan_info: dict) -> Div:
    plan = plan_info.get("plan", "free")
    plan_cls = {
        "free": "bg-gray-100 text-gray-600",
        "pro": "bg-red-100 text-red-700",
        "agency": "bg-purple-100 text-purple-700",
    }.get(plan, "bg-gray-100 text-gray-600")

    return Div(
        Div(
            H1(
                f"👋 Welcome back, {user_name}",
                cls="text-3xl font-bold text-gray-900",
            ),
            Span(
                plan.capitalize(),
                cls=f"text-xs font-bold px-2.5 py-1 rounded-full {plan_cls} ml-3 align-middle",
            ),
            cls="flex items-center mb-1",
        ),
        P(
            f"{dashboard_count} playlist{'s' if dashboard_count != 1 else ''} analyzed",
            cls="text-gray-500 text-sm",
        ),
        cls="mb-8 mt-8",
    )


def _section_label(icon: str, title: str, href: str | None = None) -> Div:
    """Consistent section heading with optional 'View all' link."""
    return Div(
        Div(
            Span(icon, cls="mr-2 text-base"),
            Span(title, cls="font-semibold text-gray-800 text-sm uppercase tracking-wide"),
            cls="flex items-center",
        ),
        *(
            [A("View all →", href=href, cls="text-xs text-red-500 hover:underline font-medium")]
            if href
            else []
        ),
        cls="flex items-center justify-between mb-3",
    )


def _section_analysis(dashboards: list[dict], search: str, sort: str) -> Div:
    """Playlist analysis section — search bar + grid or empty state."""
    return Div(
        _section_label("📊", "Playlist Analysis", href="/analysis"),
        render_search_filter_bar(search=search, sort=sort),
        render_dashboard_grid(dashboards) if dashboards else render_empty_state(search),
        cls="mb-10",
    )


def _section_lists(plan: str) -> Div:
    """Quick-access tiles for the public Lists product."""
    tiles = [
        ("🏆", "Top Rated", "/lists/top-rated"),
        ("🚀", "Rising Stars", "/lists/rising-stars"),
        ("⚡", "Most Active", "/lists/most-active"),
        ("🌍", "By Country", "/lists"),
        ("🎮", "By Category", "/lists"),
    ]

    def _tile(emoji, label, href):
        return A(
            Div(
                Span(emoji, cls="text-2xl mb-1 block"),
                Span(label, cls="text-xs font-medium text-gray-700 text-center leading-tight"),
                cls="flex flex-col items-center justify-center p-3",
            ),
            href=href,
            cls=(
                "rounded-xl border border-border bg-white hover:border-gray-300 "
                "hover:shadow-sm transition-all duration-150 no-underline"
            ),
        )

    return Div(
        _section_label("📋", "Lists", href="/lists"),
        Div(
            *[_tile(e, l, h) for e, l, h in tiles],
            cls="grid grid-cols-5 gap-3",
        ),
        cls="mb-10",
    )


def _section_campaigns(plan: str) -> Div:
    """Campaigns stub — gated behind agency plan, coming soon for others."""
    is_agency = plan == "agency"

    if is_agency:
        body = Div(
            P(
                "Manage outreach campaigns across your saved creators.",
                cls="text-sm text-gray-500 mb-3",
            ),
            A(
                Button("+ New Campaign", cls=ButtonT.primary),
                href="/campaigns/new",
            ),
            cls="py-2",
        )
    else:
        body = Div(
            P(
                "Run outreach campaigns across your saved creators — schedule, track, and report in one place.",
                cls="text-sm text-gray-500 mb-3 max-w-lg",
            ),
            Div(
                Span(
                    "Coming soon · Agency plan",
                    cls="text-xs font-semibold text-purple-700 bg-purple-100 px-3 py-1 rounded-full",
                ),
                A(
                    "Learn more →",
                    href="/pricing",
                    cls="text-xs text-red-500 hover:underline font-medium ml-4",
                ),
                cls="flex items-center gap-2 flex-wrap",
            ),
            cls="py-2",
        )

    return Div(
        _section_label("📣", "Campaigns"),
        Div(
            body,
            cls="p-4 rounded-xl border border-dashed border-gray-200 bg-gray-50/60",
        ),
        cls="mb-10",
    )


# ---------------------------------------------------------------------------
# Watchlist Pulse
# ---------------------------------------------------------------------------

_GRADE_CLS = {
    "A+": "bg-emerald-100 text-emerald-800",
    "A": "bg-green-100 text-green-700",
    "B+": "bg-blue-100 text-blue-800",
    "B": "bg-blue-50 text-blue-700",
    "C": "bg-yellow-100 text-yellow-800",
    "D": "bg-red-100 text-red-700",
}


def _flag(country_code: str | None) -> str:
    if not country_code or len(country_code) != 2:
        return ""
    try:
        a, b = country_code.upper()
        return chr(0x1F1E6 + ord(a) - 65) + chr(0x1F1E6 + ord(b) - 65)
    except Exception:
        return ""


def _growth_cell(delta: int | None) -> Span:
    if delta is None:
        return Span("─  n/a", cls="text-gray-400 text-sm tabular-nums")
    if delta > 0:
        return Span(f"▲ +{delta:,}", cls="text-emerald-600 font-semibold text-sm tabular-nums")
    if delta < 0:
        return Span(f"▼ {delta:,}", cls="text-red-500 font-semibold text-sm tabular-nums")
    return Span("─  0", cls="text-gray-400 text-sm tabular-nums")


def _pulse_row(creator: dict) -> Div:
    thumb = creator.get("channel_thumbnail_url") or ""
    name = creator.get("channel_name") or "Unknown"
    delta = creator.get("subscribers_change_30d")
    grade = creator.get("quality_grade") or ""
    code = creator.get("country_code") or ""
    cat = creator.get("category") or ""

    avatar = (
        Img(src=thumb, alt=name, cls="w-9 h-9 rounded-full object-cover flex-shrink-0")
        if thumb
        else Div(
            Span(name[:1].upper(), cls="text-xs font-bold text-gray-500"),
            cls="w-9 h-9 rounded-full bg-gray-200 flex items-center justify-center flex-shrink-0",
        )
    )
    grade_cls = _GRADE_CLS.get(grade, "bg-gray-100 text-gray-600")

    return Div(
        avatar,
        Span(name, cls="flex-1 font-medium text-sm truncate min-w-0"),
        _growth_cell(delta),
        *(
            [Span(grade, cls=f"text-xs font-bold px-2 py-0.5 rounded-full {grade_cls}")]
            if grade
            else []
        ),
        *(
            [Span(f"{_flag(code)} {code}", cls="text-xs text-gray-400 w-10 text-center")]
            if code
            else []
        ),
        *(
            [Span(cat, cls="text-xs text-gray-400 hidden sm:block truncate max-w-[5rem]")]
            if cat
            else []
        ),
        cls="flex items-center gap-3 py-2 border-b border-gray-100 last:border-0",
    )


def render_watchlist_pulse(creators: list[dict], plan: str) -> Div:
    """
    Watchlist Pulse — saved creators ranked by 30-day subscriber growth.
    Gated to Pro / Agency plans.
    """
    label = _section_label("❤️", "Saved Creators", href="/me/favourites")

    if plan not in ("pro", "agency"):
        return Div(
            label,
            Div(
                Div(
                    Span("🔒", cls="text-xl mr-3"),
                    Div(
                        P(
                            "Track subscriber growth for your saved creators.",
                            cls="text-sm text-gray-600",
                        ),
                        P("Available on Pro and Agency plans.", cls="text-xs text-gray-400 mt-0.5"),
                    ),
                    A(
                        UkIcon("arrow-up-circle", cls="w-4 h-4 mr-1"),
                        Span("Upgrade"),
                        href="/pricing",
                        cls=(
                            "ml-auto inline-flex items-center px-3 py-1.5 rounded-lg "
                            "bg-red-500 hover:bg-red-600 text-white text-sm font-semibold "
                            "transition-colors flex-shrink-0"
                        ),
                    ),
                    cls="flex items-center gap-3",
                ),
                cls="p-4 rounded-xl border border-dashed border-red-200 bg-red-50/40",
            ),
            cls="mb-10",
        )

    if not creators:
        body = Div(
            P("No saved creators yet.", cls="text-sm text-gray-500 mb-2"),
            A(
                "Browse creators →",
                href="/creators",
                cls="text-sm text-red-500 hover:underline font-medium",
            ),
            cls="py-3 text-center",
        )
        return Div(
            label,
            Div(body, cls="p-4 rounded-xl border border-border bg-muted/30"),
            cls="mb-10",
        )

    rows = [_pulse_row(c) for c in creators[:8]]
    footer_link = (
        Div(
            A(
                f"View all {len(creators)} saved creators →",
                href="/me/favourites",
                cls="text-xs text-red-500 hover:underline font-medium",
            ),
            cls="pt-2 text-right",
        )
        if len(creators) > 8
        else None
    )

    return Div(
        label,
        Div(
            Div(*rows),
            *(([footer_link]) if footer_link else []),
            cls="p-4 rounded-xl border border-border bg-white",
        ),
        cls="mb-10",
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
