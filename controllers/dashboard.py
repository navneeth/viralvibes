"""
Dashboard controller - handles dashboard viewing and user playlists.

Following FastHTML controller pattern:
- Pure business logic functions
- No route decorators (routes defined in main.py)
- Returns FT components or Response objects
"""

import logging
from typing import Union, Optional

from fasthtml.common import *
from starlette.responses import Response, RedirectResponse
from monsterui.all import *

from db import (
    supabase_client,  # ✅ Global client
    record_dashboard_event,
    get_dashboard_event_counts,
)
from utils import load_df_from_json

logger = logging.getLogger(__name__)


# ============================================================================
# Error Helpers
# ============================================================================


def _error_response(title: str, message: str) -> Div:
    """Standardized error response component."""
    return Div(
        H2(title, cls="text-2xl font-bold text-gray-900 mb-2"),
        P(message, cls="text-gray-600"),
        cls="max-w-xl mx-auto mt-24 text-center p-6 bg-red-50 rounded-lg border border-red-200",
    )


# ============================================================================
# Controller Functions
# ============================================================================


def view_dashboard_controller(
    dashboard_id: str, sess: dict, sort_by: str = "Views", order: str = "desc"
) -> Union[Div, Response]:
    """
    Controller for viewing a public dashboard.

    ✅ Pure function - no route decorator
    ✅ Takes all inputs as parameters
    ✅ Returns FT component or Response

    Args:
        dashboard_id: Dashboard ID to view
        sess: Session dict (for optional personalization)
        sort_by: Column to sort by (default: Views)
        order: Sort order (default: desc)

    Returns:
        Dashboard HTML or error response
    """
    # Get user_id for personalization (optional - works for anonymous too)
    user_id = sess.get("user_id") if sess else None

    logger.info(
        f"Loading dashboard {dashboard_id} (user={user_id or 'anonymous'}, sort={sort_by}, order={order})"
    )

    # Get Supabase client
    if not supabase_client:
        logger.error("Supabase client not initialized")
        return _error_response(
            "Service Unavailable",
            "Dashboard service is temporarily unavailable. Please try again later.",
        )

    # Fetch playlist data
    try:
        logger.debug(f"Querying playlist_stats for dashboard_id={dashboard_id}")
        resp = (
            supabase_client.table("playlist_stats")
            .select("*")
            .eq("dashboard_id", dashboard_id)
            .limit(1)
            .execute()
        )

        if not resp.data or len(resp.data) == 0:
            logger.warning(f"Dashboard not found: {dashboard_id}")
            return _error_response(
                "Dashboard Not Found",
                "This playlist dashboard does not exist. Try analyzing a new playlist.",
            )

        playlist_row = resp.data[0]
        playlist_url = playlist_row.get("playlist_url")
        logger.debug(f"Found playlist: {playlist_url}")

    except Exception as e:
        logger.exception(f"Database query failed for dashboard {dashboard_id}: {e}")
        return _error_response(
            "Database Error", "Failed to load dashboard. Please try again later."
        )

    # Validate required fields
    required_fields = ["df_json", "playlist_url"]
    missing_fields = [f for f in required_fields if f not in playlist_row]

    if missing_fields:
        logger.error(f"Missing required fields: {missing_fields}")
        return _error_response(
            "Invalid Data", "Playlist data is corrupted. Please analyze again."
        )

    # Record view event
    try:
        record_dashboard_event(
            supabase_client,
            dashboard_id=dashboard_id,
            event_type="view",
        )
        logger.debug(f"Recorded view event for {dashboard_id}")
    except Exception as e:
        logger.warning(f"Failed to record event for {dashboard_id} (non-critical): {e}")

    # Load and deserialize DataFrame
    try:
        df = load_df_from_json(playlist_row["df_json"])
        logger.debug(f"Loaded DataFrame: {len(df)} rows")
    except KeyError as e:
        logger.error(f"Missing JSON field in playlist_row: {e}")
        return _error_response(
            "Invalid Data", "Playlist data is corrupted. Please contact support."
        )
    except Exception as e:
        logger.exception(f"Failed to deserialize DataFrame for {dashboard_id}: {e}")
        return _error_response(
            "Data Error", "Failed to parse playlist data. Please try again later."
        )

    # Fetch interest metrics (analytics)
    interest = {}
    try:
        # ✅ CORRECTED: Pass global client
        interest = get_dashboard_event_counts(supabase_client, dashboard_id)
        logger.debug(f"Event counts: {interest}")
    except Exception as e:
        logger.warning(
            f"Failed to get event counts for {dashboard_id} (non-critical): {e}"
        )

    # Render dashboard
    try:
        from views.dashboard import render_dashboard

        logger.debug(f"Rendering dashboard for {dashboard_id}")
        dashboard_html = render_dashboard(
            df=df,
            playlist_stats=playlist_row,
            mode="public",
            dashboard_id=dashboard_id,
            interest=interest,
            current_user_id=user_id,
            sort_by=sort_by,
            order=order,
        )

        # Build response with cache headers
        response = Response(content=str(dashboard_html), media_type="text/html")

        # Cache for 1 hour (data updates infrequently)
        etag = f'"{dashboard_id}"'
        response.headers["Cache-Control"] = "public, max-age=3600"
        response.headers["ETag"] = etag
        response.headers["Vary"] = "Accept-Encoding"

        logger.info(f"Successfully rendered public dashboard: {dashboard_id}")
        return response

    except Exception as e:
        logger.exception(f"Failed to render dashboard for {dashboard_id}: {e}")
        return _error_response(
            "Render Error", "Failed to display dashboard. Please refresh the page."
        )


def list_user_dashboards_controller(sess: dict, oauth, req) -> Union[Div, Response]:
    """
    Controller for listing user's personal dashboards.

    ✅ Pure function - no route decorator
    ✅ Handles auth check internally
    ✅ Returns FT component or redirect

    Args:
        sess: Session dict
        oauth: OAuth instance (for NavComponent)
        req: Request object (for NavComponent)

    Returns:
        Dashboard list HTML or redirect to login
    """
    # Check authentication
    user_id = sess.get("user_id") if sess else None

    if not user_id:
        logger.info("Unauthorized access to /me/dashboards - redirecting to login")
        return RedirectResponse("/login?return_to=/me/dashboards", status_code=303)

    logger.info(f"Loading personal dashboards for user {user_id}")

    # Get Supabase client
    if not supabase_client:
        logger.error("Supabase client not initialized")
        # Import NavComponent here to avoid circular import
        from components import NavComponent

        return Titled(
            "My Dashboards",
            NavComponent(oauth, req, sess),
            Container(
                Alert(
                    P("Service unavailable. Please try again later."), cls=AlertT.error
                ),
                cls="p-6",
            ),
        )

    # Fetch user's analyzed playlists
    try:
        logger.debug(f"Querying user playlists for user_id={user_id}")
        resp = (
            supabase_client.table("playlist_stats")
            .select("dashboard_id, playlist_url, title, created_at, view_count")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .execute()
        )

        playlists = resp.data or []
        logger.info(f"Found {len(playlists)} playlists for user {user_id}")

    except Exception as e:
        logger.exception(f"Failed to fetch user playlists for {user_id}: {e}")
        from components import NavComponent

        return Titled(
            "My Dashboards",
            NavComponent(oauth, req, sess),
            Container(
                Alert(
                    P("Failed to load your dashboards. Please try again."),
                    cls=AlertT.error,
                ),
                cls="p-6",
            ),
        )

    # Import NavComponent (avoid circular import at module level)
    from components import NavComponent

    # Empty state
    if not playlists:
        return Titled(
            "My Dashboards",
            NavComponent(oauth, req, sess),
            Container(
                Div(
                    H2(
                        "No Dashboards Yet", cls="text-2xl font-bold text-gray-900 mb-4"
                    ),
                    P(
                        "You haven't analyzed any playlists yet. Start by analyzing your first playlist!",
                        cls="text-gray-600 mb-6",
                    ),
                    A(
                        Button("Analyze Your First Playlist", cls=ButtonT.primary),
                        href="/#analyze-section",
                    ),
                    cls="text-center p-12 bg-gray-50 rounded-lg max-w-xl mx-auto mt-12",
                )
            ),
        )

    # Build playlist cards
    playlist_cards = []
    for pl in playlists:
        card = Div(
            A(
                Div(
                    H3(
                        pl.get("title", "Untitled Playlist"),
                        cls="text-lg font-semibold text-gray-900 mb-2",
                    ),
                    P(
                        f"{pl.get('view_count', 0):,} views",
                        cls="text-sm text-gray-600",
                    ),
                    cls="p-4",
                ),
                href=f"/d/{pl['dashboard_id']}",
                cls="block rounded-lg border border-gray-200 hover:shadow-lg transition-shadow bg-white",
            )
        )
        playlist_cards.append(card)

    return Titled(
        "My Dashboards",
        NavComponent(oauth, req, sess),
        Container(
            Div(
                H1("My Dashboards", cls="text-3xl font-bold text-gray-900 mb-2"),
                P(
                    f"You've analyzed {len(playlists)} playlist{'s' if len(playlists) != 1 else ''}",
                    cls="text-gray-600 mb-8",
                ),
                Grid(*playlist_cards, cols_md=2, cols_lg=3, gap=6),
                cls="py-12",
            )
        ),
    )
