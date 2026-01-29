"""
My Dashboards page - user's playlist analysis history.
"""

from datetime import datetime

from fasthtml.common import *
from monsterui.all import *

from utils import format_number  # ✅ Reuse existing utility


def render_my_dashboards_page(
    dashboards: list[dict],
    user_name: str,
    search: str = "",
    sort: str = "recent",
) -> Div:
    """
    Render the My Dashboards page with grid layout.

    Uses MonsterUI Grid component for responsive layout.
    """

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
        # Search & Filter Bar
        render_search_filter_bar(search=search, sort=sort),
        # Dashboard Grid or Empty State
        (
            render_dashboard_grid(dashboards)
            if dashboards
            else render_empty_state(search)
        ),
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


def render_dashboard_card(dashboard: dict) -> A:
    """
    Render a single dashboard card with thumbnail and stats.

    Uses MonsterUI Card component for consistent styling.
    """

    # Extract data with safe defaults
    dashboard_id = dashboard.get("dashboard_id", "")
    title = dashboard.get("title", "Untitled Playlist")
    channel_name = dashboard.get("channel_name", "Unknown Channel")
    channel_thumbnail = dashboard.get("channel_thumbnail", "/static/favicon.jpeg")
    video_count = dashboard.get("video_count", 0) or 0
    view_count = dashboard.get("view_count", 0) or 0
    processed_on = dashboard.get("processed_on")

    # Format date
    date_str = format_date(processed_on)

    # Format large numbers using existing utility
    views_formatted = format_number(view_count)  # ✅ Reuse utils.format_number()

    return A(
        Card(
            # Thumbnail
            Div(
                Img(
                    src=channel_thumbnail,
                    alt=f"{channel_name} thumbnail",
                    cls="w-full h-48 object-cover",
                    onerror="this.src='/static/favicon.jpeg'",  # Fallback image
                ),
                cls="overflow-hidden rounded-t-lg bg-gray-100",
            ),
            # Content
            Div(
                # Title (truncate after 2 lines)
                H3(
                    title,
                    cls="text-lg font-semibold mb-2 line-clamp-2 min-h-[3.5rem]",
                    title=title,  # Show full title on hover
                ),
                # Channel name
                P(
                    "📺 ",
                    Span(channel_name, cls="text-gray-600"),
                    cls="text-sm mb-3 truncate",
                ),
                # Stats row
                Div(
                    # Video count
                    Div(
                        Span("📹", cls="mr-1"),
                        Span(f"{video_count:,}", cls="font-medium text-gray-700"),
                        Span(" videos", cls="text-gray-500 text-xs ml-1"),
                        cls="flex items-center text-sm",
                    ),
                    # View count (formatted with K/M/B)
                    Div(
                        Span("👁️", cls="mr-1"),
                        Span(views_formatted, cls="font-medium text-gray-700"),
                        Span(" views", cls="text-gray-500 text-xs ml-1"),
                        cls="flex items-center text-sm",
                    ),
                    cls="flex gap-4 mb-3",
                ),
                # Date badge
                Div(
                    Span(f"📅 {date_str}", cls="text-xs text-gray-500"),
                    cls="flex items-center",
                ),
                cls="p-4",
            ),
            cls="hover:shadow-xl transition-shadow duration-200 cursor-pointer",
        ),
        href=f"/d/{dashboard_id}",
        cls="block group no-underline",
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


def format_date(date_str: str | None) -> str:
    """
    Format ISO datetime to human-readable date.

    Examples:
        "2026-01-29T10:30:00Z" → "Jan 29, 2026"
        "2026-01-29" → "Jan 29, 2026"
        None → "Recently"
    """
    if not date_str:
        return "Recently"

    try:
        # Handle both datetime and date strings
        if isinstance(date_str, str):
            # Remove timezone info for parsing
            clean_date = date_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean_date)
        else:
            dt = date_str

        return dt.strftime("%b %d, %Y")
    except Exception:
        return "Recently"
