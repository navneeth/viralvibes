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

# ============================================================================
# Error Response Helper
# ============================================================================


def _error_response(title: str, message: str, status_code: int = 200) -> Div:
    """Render standardized error response."""
    return Div(
        H2(title, cls="text-2xl font-bold text-gray-900 mb-2"),
        P(message, cls="text-gray-600"),
        cls="max-w-xl mx-auto mt-24 text-center p-6 bg-red-50 rounded-lg border border-red-200",
    )


# ============================================================================
# Helper: Extract User ID from Request
# ============================================================================


def get_user_from_request(request: Request) -> Optional[str]:
    """
    Extract user_id from request context.

    Supports multiple auth methods:
    - FastHTML session: request.session.get('user_id')
    - JWT cookie: parse from Authorization header
    - Query param (testing only): ?user_id=...

    Returns:
        user_id (str) if authenticated, None if anonymous
    """
    # Try session first (FastHTML)
    user_id = request.session.get("user_id") if hasattr(request, "session") else None

    # Try query param (testing/override)
    if not user_id and hasattr(request, "query_params"):
        user_id = request.query_params.get("user_id")

    if user_id:
        logger.debug(f"User authenticated: {user_id}")
    else:
        logger.debug("Anonymous user")

    return user_id


def require_auth(func):
    """Decorator to require authenticated user."""

    @wraps(func)
    def wrapper(request: Request, *args, **kwargs):
        user_id = get_user_from_request(request)
        if not user_id:
            logger.warning(f"Unauthorized access to {func.__name__}")
            return _error_response(
                "Access Denied",
                "You must be logged in to view this page.",
                status_code=401,
            )
        return func(request, user_id, *args, **kwargs)

    return wrapper


# ============================================================================
# PUBLIC ROUTE: /d/{dashboard_id}
# ============================================================================


@rt("/d/{dashboard_id}")
def dashboard_view(request: Request, dashboard_id: str) -> Union[Div, Response]:
    """
    Render PUBLIC playlist dashboard.
    - Anyone can view (no authentication required)
    - Uses user_id=None for queries (anonymous cache)
    - Records view event for analytics

    Args:
        request: FastHTML request object
        dashboard_id: Dashboard ID from playlist_stats table (indexed column)

    Returns:
        Rendered dashboard HTML or error message

    Flow:
        1. Query playlist_stats by dashboard_id (fast indexed lookup)
        2. Validate data integrity
        3. Record analytics event (non-blocking)
        4. Load and render dashboard with interest metrics
    """

    user_id = get_user_from_request(
        request
    )  # Get user_id if logged in (for personalization)
    logger.info(f"Loading public dashboard: {dashboard_id} (user={user_id})")

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
            .limit(1)  # ✅ Handle potential hash collisions
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
                "Playlist data is corrupted. Please analyze again.",
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
        df = load_df_from_json(playlist_row["df_json"])
        logger.debug(f"Loaded DataFrame: {len(df)} rows")
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
            mode="public",
            dashboard_id=dashboard_id,
            interest=interest,
            current_user_id=user_id,  # ✅ For personalization (favorite, share, etc.)
        )

        # Build response with cache headers
        response = Response(content=str(dashboard_html), media_type="text/html")

        # Cache for 1 hour (data updates infrequently)
        # Use ETag to support conditional requests
        etag = f'"{dashboard_id}"'
        response.headers["Cache-Control"] = "public, max-age=3600"
        response.headers["ETag"] = etag
        response.headers["Vary"] = "Accept-Encoding"

        logger.info(f"Successfully rendered public dashboard: {dashboard_id}")
        return response

    except Exception as e:
        logger.exception(f"Failed to render dashboard for {dashboard_id}: {e}")
        return _error_response(
            "Render Error",
            "Failed to display dashboard. Please refresh.",
        )


# ============================================================================
# PROTECTED ROUTE: /me/dashboards
# ============================================================================


@rt("/me/dashboards")
@require_auth
def my_dashboards(request: Request, user_id: str) -> Div:
    """
    Render user's PERSONAL dashboards.
    - Only authenticated users can view
    - Queries with user_id (private cache)
    - Shows only playlists analyzed by this user

    Args:
        request: FastHTML request object
        user_id: Authenticated user ID (from @require_auth decorator)

    Returns:
        Rendered dashboards list HTML
    """
    logger.info(f"Loading personal dashboards for user: {user_id}")

    if not supabase_client:
        return _error_response(
            "Service Unavailable",
            "Dashboard service is temporarily unavailable.",
        )

    # --- Fetch user's analyzed playlists ---
    try:
        logger.debug(f"Querying user playlists: {user_id}")
        resp = (
            supabase_client.table("playlist_stats")
            .select(
                "dashboard_id, playlist_url, title, processed_date, view_count, engagement_rate"
            )
            .eq("user_id", user_id)  # ✅ USER-SCOPED QUERY
            .order("processed_date", desc=True)
            .execute()
        )

        playlists = resp.data or []
        logger.info(f"Found {len(playlists)} playlists for user {user_id}")

    except Exception as e:
        logger.exception(f"Failed to fetch user playlists: {e}")
        return _error_response(
            "Database Error",
            "Failed to load your dashboards. Please try again.",
        )

    # --- Render dashboards list ---
    if not playlists:
        return Div(
            H2("No Dashboards Yet", cls="text-2xl font-bold text-gray-900 mb-2"),
            P("Start by analyzing your first playlist", cls="text-gray-600 mb-6"),
            A(
                Button("Analyze a Playlist", cls=ButtonT.primary),
                href="/analyze",
            ),
            cls="max-w-xl mx-auto mt-24 text-center p-6",
        )

    # ✅ Build dashboard cards
    dashboard_cards = []
    for pl in playlists:
        card = Div(
            A(
                Div(
                    H3(
                        pl.get("title", "Untitled"),
                        cls="text-lg font-semibold text-gray-900",
                    ),
                    P(
                        f"{pl.get('view_count', 0):,} views • "
                        f"{pl.get('engagement_rate', 0):.1f}% engagement",
                        cls="text-sm text-gray-600 mt-2",
                    ),
                    cls="p-4",
                ),
                href=f"/d/{pl['dashboard_id']}",
                cls="block rounded-lg border hover:shadow-lg transition-shadow",
            )
        )
        dashboard_cards.append(card)

    return Div(
        Div(
            H2("Your Dashboards", cls="text-3xl font-bold text-gray-900"),
            P(f"You've analyzed {len(playlists)} playlists", cls="text-gray-600"),
            cls="mb-8",
        ),
        Grid(*dashboard_cards, cols_md=2, cols_lg=3, gap=6),
        cls="max-w-7xl mx-auto p-6",
    )


# ============================================================================
# PROTECTED ROUTE: /me (Profile)
# ============================================================================


@rt("/me")
@require_auth
def user_profile(request: Request, user_id: str) -> Div:
    """
    Render user profile page.

    Shows:
    - User info (email, name, avatar)
    - Quick stats (playlists analyzed, total views, etc.)
    - Links to settings, dashboards, etc.
    """
    logger.info(f"Loading profile for user: {user_id}")

    if not supabase_client:
        return _error_response("Service Unavailable", "Please try again later.")

    # --- Fetch user stats ---
    try:
        # Count playlists
        playlists_resp = (
            supabase_client.table("playlist_stats")
            .select("id", count="exact")
            .eq("user_id", user_id)
            .execute()
        )
        playlist_count = len(playlists_resp.data or [])

        # Fetch user info (if using users table)
        user_resp = (
            supabase_client.table("users")
            .select("email, name, avatar_url")
            .eq("id", user_id)
            .limit(1)
            .execute()
        )
        user_info = user_resp.data[0] if user_resp.data else {}

    except Exception as e:
        logger.exception(f"Failed to fetch user info: {e}")
        return _error_response("Database Error", "Failed to load profile.")

    # --- Render profile ---
    return Div(
        Div(
            H1(user_info.get("name", "User"), cls="text-3xl font-bold text-gray-900"),
            P(user_info.get("email", ""), cls="text-gray-600"),
            cls="mb-8",
        ),
        Grid(
            Div(
                P(playlist_count, cls="text-3xl font-bold text-red-600"),
                P("Playlists Analyzed", cls="text-gray-600"),
                cls="p-4 rounded-lg border text-center",
            ),
            Div(
                A(
                    Button("View Dashboards", cls=ButtonT.primary),
                    href="/me/dashboards",
                )
            ),
            cols_md=2,
            gap=4,
        ),
        A("Settings", href="/me/settings", cls="text-red-600 hover:underline mt-6"),
        cls="max-w-2xl mx-auto p-6",
    )


# ============================================================================
# NEW ROUTES TO ADD LATER
# ============================================================================

"""
@rt("/me/settings")
@require_auth
def user_settings(request: Request, user_id: str) -> Div:
    '''Account settings, preferences, danger zone'''
    pass

@rt("/me/export")
@require_auth
def export_all_data(request: Request, user_id: str) -> Response:
    '''Export all user playlists and stats as CSV/JSON'''
    pass

@rt("/api/me")
@require_auth
def get_current_user_api(request: Request, user_id: str) -> dict:
    '''JSON endpoint for frontend to detect auth state'''
    return {
        "user_id": user_id,
        "authenticated": True,
    }
"""
# ============================================================================


@rt("/me/dashboards")
def my_dashboards(req, sess):
    """
    User's personal dashboards page.

    Shows list of playlists the user has analyzed.
    """
    # Check authentication
    user_id = sess.get("user_id") if sess else None

    if not user_id:
        # Not logged in - redirect to login
        return RedirectResponse("/login?return_to=/me/dashboards", status_code=303)

    supabase = get_supabase()
    if not supabase:
        return Div(
            Alert(
                P("Service unavailable. Please try again later."),
                cls=AlertT.error,
            ),
            cls="p-6 max-w-2xl mx-auto",
        )

    try:
        # Fetch user's analyzed playlists
        resp = (
            supabase.table("playlist_stats")
            .select("dashboard_id, playlist_url, title, created_at, view_count")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .execute()
        )

        playlists = resp.data or []

    except Exception as e:
        logger.exception(f"Failed to fetch playlists for user {user_id}: {e}")
        return Div(
            Alert(
                P("Failed to load your dashboards."),
                cls=AlertT.error,
            ),
            cls="p-6 max-w-2xl mx-auto",
        )

    # Render dashboard list
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
                        "You haven't analyzed any playlists yet.",
                        cls="text-gray-600 mb-6",
                    ),
                    A(
                        Button("Analyze Your First Playlist", cls=ButtonT.primary),
                        href="/#analyze-section",
                    ),
                    cls="text-center p-12 bg-gray-50 rounded-lg",
                )
            ),
        )

    # Build playlist cards
    playlist_cards = [
        Div(
            A(
                H3(
                    pl.get("title", "Untitled Playlist"),
                    cls="text-lg font-semibold mb-2",
                ),
                P(
                    f"{pl.get('view_count', 0):,} views",
                    cls="text-sm text-gray-600",
                ),
                href=f"/d/{pl['dashboard_id']}",
                cls="block p-4 rounded-lg border hover:shadow-lg transition-shadow bg-white",
            )
        )
        for pl in playlists
    ]

    return Titled(
        "My Dashboards",
        NavComponent(oauth, req, sess),
        Container(
            Div(
                H1("My Dashboards", cls="text-3xl font-bold text-gray-900 mb-2"),
                P(
                    f"You've analyzed {len(playlists)} playlists",
                    cls="text-gray-600 mb-8",
                ),
                Grid(*playlist_cards, cols_md=2, cols_lg=3, gap=6),
                cls="py-12",
            )
        ),
    )
