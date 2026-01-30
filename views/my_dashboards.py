"""
My Dashboards page - user's playlist analysis history.
"""

from datetime import datetime

from fasthtml.common import *
from monsterui.all import *

from utils import format_number


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
    playlist_thumbnail = dashboard.get("playlist_thumbnail", "/static/favicon.jpeg")
    channel_thumbnail = dashboard.get("channel_thumbnail", "/static/favicon.jpeg")
    video_count = dashboard.get("video_count", 0) or 0
    view_count = dashboard.get("view_count", 0) or 0
    processed_on = dashboard.get("processed_on")

    # Additional metrics (if available in your data structure)
    engagement_score = dashboard.get("engagement_score", 0)
    avg_views_per_video = dashboard.get("avg_views_per_video", 0)

    # Format date
    date_str = format_date(processed_on)

    # Format large numbers using existing utility
    views_formatted = format_number(view_count)

    # Calculate average views per video if not provided
    if not avg_views_per_video and video_count > 0:
        avg_views_per_video = view_count / video_count

    return A(
        Card(
            # Thumbnail container with 16:9 aspect ratio preservation
            Div(
                # Background playlist image (16:9 aspect ratio)
                Div(
                    Img(
                        src=channel_thumbnail,
                        alt=f"{title} playlist thumbnail",
                        cls="w-full h-full object-cover",
                        onerror="this.src='/static/favicon.jpeg'",
                    ),
                    cls="absolute inset-0 bg-gray-300",
                ),
                # Creator thumbnail badge - PROMINENT overlay at bottom-left
                Div(
                    Img(
                        src=channel_thumbnail,
                        alt=f"{channel_name} channel avatar",
                        cls="w-12 h-12 rounded-full border-2 border-white shadow-lg object-cover",
                        onerror="this.src='/static/favicon.jpeg'",
                    ),
                    cls="absolute bottom-3 left-3 z-10 group-hover:scale-110 transition-transform duration-200",
                ),
                # Dark gradient overlay on hover (for visual feedback)
                Div(
                    cls="absolute inset-0 bg-gradient-to-t from-black/50 via-black/10 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-200",
                ),
                cls="relative w-full overflow-hidden bg-gray-300 aspect-video rounded-t-lg group",
            ),
            # Content section with flexbox to fill remaining space
            Div(
                # Playlist title (2 lines max, proper visual hierarchy)
                H3(
                    title,
                    cls="text-sm font-semibold mb-1 line-clamp-2 text-gray-900 group-hover:text-blue-600 transition-colors duration-200",
                    title=title,  # Show full title on hover tooltip
                ),
                # Channel name with icon - emphasize creator
                Div(
                    Span("📺", cls="text-gray-400 text-xs mr-1"),
                    Span(
                        channel_name,
                        cls="text-xs text-gray-700 font-medium truncate group-hover:text-gray-900",
                        title=channel_name,
                    ),
                    cls="flex items-center gap-0 mb-2 min-h-[1.25rem]",
                ),
                # Primary metrics: Video count + Total views
                Div(
                    Div(
                        Span("📹", cls="text-xs mr-0.5"),
                        Span(
                            f"{video_count:,}",
                            cls="font-semibold text-gray-900 text-xs",
                        ),
                        Span(" videos", cls="text-gray-500 text-xs"),
                        cls="flex items-center gap-0.5",
                    ),
                    Span("•", cls="text-gray-300 text-xs mx-1"),
                    Div(
                        Span("👁️", cls="text-xs mr-0.5"),
                        Span(
                            views_formatted, cls="font-semibold text-gray-900 text-xs"
                        ),
                        Span(" total", cls="text-gray-500 text-xs"),
                        cls="flex items-center gap-0.5",
                    ),
                    cls="flex items-center gap-1 mb-2 text-xs flex-wrap",
                ),
                # Secondary metrics: Average views and engagement
                (
                    Div(
                        Div(
                            Span("📊", cls="text-xs mr-0.5"),
                            Span(
                                f"{format_number(int(avg_views_per_video))}",
                                cls="font-semibold text-gray-700 text-xs",
                            ),
                            Span(" avg/video", cls="text-gray-500 text-xs"),
                            cls="flex items-center gap-0.5",
                        ),
                        (
                            (
                                Span("•", cls="text-gray-300 text-xs mx-1"),
                                Div(
                                    Span("⚡", cls="text-xs mr-0.5"),
                                    Span(
                                        f"{int(engagement_score)}%",
                                        cls="font-semibold text-amber-600 text-xs",
                                    ),
                                    Span(" engagement", cls="text-gray-500 text-xs"),
                                    cls="flex items-center gap-0.5",
                                ),
                            )
                            if engagement_score
                            else None
                        ),
                        cls="flex items-center gap-1 text-xs mb-2",
                    )
                    if avg_views_per_video
                    else None
                ),
                # Footer: Date analyzed with visual separator
                Div(
                    Span(f"📅 {date_str}", cls="text-xs text-gray-500"),
                    cls="flex items-center pt-2 border-t border-gray-200",
                ),
                cls="p-4 flex flex-col flex-1 justify-between",
            ),
            cls="flex flex-col h-full hover:shadow-xl transition-all duration-200 bg-white overflow-hidden",
        ),
        href=f"/d/{dashboard_id}",
        cls="block group no-underline h-full",
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
