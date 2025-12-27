"""
routes/dashboard.py - Persistent dashboard page route

Performance-optimized with indexed dashboard_id lookup.

Dependencies:
    - db.get_supabase(): Returns initialized Supabase client
    - db.record_dashboard_event(): Logs view/click events
    - db.get_dashboard_event_counts(): Retrieves analytics
    - utils.load_df_from_json(): Deserializes cached DataFrame
    - views.dashboard.render_dashboard(): Renders HTML dashboard

Data Flow:
    1. dashboard_id (URL param) → indexed lookup in playlist_stats
    2. Load cached DataFrame from df_json column
    3. Record analytics event (view)
    4. Fetch event counts (interest metrics)
    5. Render persistent dashboard view
"""

import logging
from typing import Union

from fasthtml.common import *
from starlette.responses import Response

from db import (
    get_dashboard_event_counts,
    get_supabase,
    record_dashboard_event,
)
from utils import load_df_from_json
from views.dashboard import render_dashboard

logger = logging.getLogger(__name__)


def _error_response(title: str, message: str, status_code: int = 200) -> Div:
    """Render standardized error response."""
    return Div(
        H2(title, cls="text-2xl font-bold text-gray-900 mb-2"),
        P(message, cls="text-gray-600"),
        cls="max-w-xl mx-auto mt-24 text-center p-6 bg-red-50 rounded-lg border border-red-200",
    )


@rt("/d/{dashboard_id}")
def dashboard_view(dashboard_id: str) -> Union[Div, Response]:
    """
    Render persistent playlist dashboard.

    Args:
        dashboard_id: Dashboard ID from playlist_stats table (indexed column)

    Returns:
        Rendered dashboard HTML or error message

    Flow:
        1. Query playlist_stats by dashboard_id (fast indexed lookup)
        2. Validate data integrity
        3. Record analytics event (non-blocking)
        4. Load and render dashboard with interest metrics
    """
    logger.info(f"Loading dashboard: {dashboard_id}")

    # --- 1️⃣ Initialize Supabase ---
    try:
        supabase = get_supabase()
        if not supabase:
            logger.error("Supabase client not initialized")
            return _error_response(
                "Service Unavailable",
                "Dashboard service is temporarily unavailable. Please try again later.",
            )
    except Exception as e:
        logger.exception(f"Failed to initialize Supabase: {e}")
        return _error_response(
            "Service Error",
            "Unable to load dashboard. Please try again later.",
        )

    # --- 2️⃣ Fetch playlist by dashboard_id (FAST indexed query) ---
    try:
        logger.debug(f"Querying playlist_stats for dashboard_id={dashboard_id}")
        resp = (
            supabase.table("playlist_stats")
            .select("*")
            .eq("dashboard_id", dashboard_id)
            .execute()
        )

        if not resp.data or len(resp.data) == 0:
            logger.warning(f"Dashboard not found: {dashboard_id}")
            return _error_response(
                "Dashboard Not Found",
                "This playlist dashboard does not exist.",
            )

        playlist_row = resp.data[0]
        logger.debug(f"Found playlist: playlist_url={playlist_row.get('playlist_url')}")

    except Exception as e:
        logger.exception(f"Database query failed for dashboard {dashboard_id}: {e}")
        return _error_response(
            "Database Error",
            "Failed to load dashboard. Please try again later.",
        )

    # --- 3️⃣ Validate data integrity ---
    try:
        # Check required fields
        required_fields = ["df_json", "playlist_url"]
        missing_fields = [f for f in required_fields if f not in playlist_row]

        if missing_fields:
            logger.error(f"Missing required fields in playlist_row: {missing_fields}")
            return _error_response(
                "Invalid Data",
                "Playlist data is corrupted. Please contact support.",
            )

    except Exception as e:
        logger.exception(f"Data validation failed for {dashboard_id}: {e}")
        return _error_response(
            "Validation Error",
            "Failed to validate playlist data. Please try again.",
        )

    # --- 4️⃣ Record analytics event (non-critical, don't fail if it breaks) ---
    try:
        record_dashboard_event(
            supabase,
            dashboard_id=dashboard_id,
            event_type="view",
        )
        logger.debug(f"Recorded view event for {dashboard_id}")
    except Exception as e:
        logger.warning(f"Failed to record event for {dashboard_id} (non-critical): {e}")
        # Continue - analytics isn't critical to page load

    # --- 5️⃣ Load and deserialize DataFrame ---
    try:
        logger.debug(f"Deserializing DataFrame for {dashboard_id}")
        df = load_df_from_json(playlist_row["df_json"])
        logger.debug(f"DataFrame loaded: {len(df)} rows")
    except KeyError as e:
        logger.error(f"Missing JSON field in playlist_row: {e}")
        return _error_response(
            "Invalid Data",
            "Playlist data is corrupted. Please contact support.",
        )
    except Exception as e:
        logger.exception(f"Failed to deserialize DataFrame for {dashboard_id}: {e}")
        return _error_response(
            "Data Error",
            "Failed to parse playlist data. Please try again later.",
        )

    # --- 6️⃣ Fetch interest metrics (analytics) ---
    interest = {}
    try:
        logger.debug(f"Fetching event counts for {dashboard_id}")
        interest = get_dashboard_event_counts(supabase, dashboard_id)
        logger.debug(f"Event counts: {interest}")
    except Exception as e:
        logger.warning(
            f"Failed to get event counts for {dashboard_id} (non-critical): {e}"
        )
        # Fallback to empty dict - dashboard still works without analytics

    # --- 7️⃣ Render dashboard ---
    try:
        logger.debug(f"Rendering dashboard for {dashboard_id}")
        dashboard_html = render_dashboard(
            df=df,
            playlist_stats=playlist_row,
            mode="persistent",
            dashboard_id=dashboard_id,
            interest=interest,
        )

        # Build response with cache headers
        response = Response(content=str(dashboard_html), media_type="text/html")

        # Cache for 1 hour (data updates infrequently)
        # Use ETag to support conditional requests
        etag = f'"{dashboard_id}"'
        response.headers["Cache-Control"] = "public, max-age=3600"
        response.headers["ETag"] = etag
        response.headers["Vary"] = "Accept-Encoding"

        logger.info(f"Successfully rendered dashboard: {dashboard_id}")
        return response

    except Exception as e:
        logger.exception(f"Failed to render dashboard for {dashboard_id}: {e}")
        return _error_response(
            "Render Error",
            "Failed to display dashboard. Please refresh or contact support.",
        )
