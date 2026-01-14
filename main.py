"""
Main entry point for the ViralVibes web app.
Modernized with Tailwind-inspired design and MonsterUI components.
Includes Google OAuth with token revocation support.
"""

import logging
import os
import re
from datetime import datetime
from typing import Optional
from urllib.parse import quote_plus

from dotenv import load_dotenv
from fasthtml.common import *
from fasthtml.common import RedirectResponse, Response
from fasthtml.core import HtmxHeaders
from fasthtml.oauth import GoogleAppClient, OAuth
from monsterui.all import *
from starlette.responses import StreamingResponse

from auth.auth_service import ViralVibesAuth, init_google_oauth
from auth.token_revocation import clear_auth_session, revoke_google_token
from components import (
    AnalysisFormCard,
    AnalyticsDashboardSection,
    BenefitsCard,
    ExploreGridSection,
    FeaturesCard,
    HeaderCard,
    HomepageAccordion,
    NavComponent,
    NewsletterCard,
    SectionDivider,
    StepProgress,
    faq_section,
    features_section,
    footer,
    hero_section,
    how_it_works_section,
)
from constants import (
    PLAYLIST_STATS_TABLE,
    PLAYLIST_STEPS_CONFIG,
    SECTION_BASE,
    SIGNUPS_TABLE,
)
from controllers.auth_routes import (
    build_login_page,
    build_logout_response,
    require_auth,
)
from controllers.job_progress import job_progress_controller
from controllers.preview import preview_playlist_controller, get_playlist_preview_info
from db import (
    get_cached_playlist_stats,
    get_estimated_stats,
    get_job_progress,
    get_playlist_job_status,
    init_supabase,
    resolve_playlist_url_from_dashboard_id,
    setup_logging,
    submit_playlist_job,
    supabase_client,
    upsert_playlist_stats,
)
from services.playlist_loader import load_cached_or_stub
from utils import compute_dashboard_id
from validators import YoutubePlaylist, YoutubePlaylistValidator
from views.dashboard import render_full_dashboard
from views.table import DISPLAY_HEADERS, get_sort_col, render_playlist_table

# Get logger instance
logger = logging.getLogger(__name__)

# ============================================================================
# STEP 1: Load environment variables FIRST
# ============================================================================
load_dotenv()


# Initialize application components
def init_app_services(app):
    """Initialize application services.

    This function should be called at application startup.
    It sets up logging and initializes the Supabase client.

    Returns:
        {
            'supabase_client': client,
            'oauth': oauth,
            'google_client': google_client
        }
    """
    # 1. Configure logging
    setup_logging()

    # Check if TESTING env var is set (GitHub Actions will set this)
    is_testing = os.getenv("TESTING") == "1"
    if is_testing:
        logger.info("🧪 Test mode detected - Supabase & OAuth disabled")
        return {
            "supabase_client": None,
            "oauth": None,
            "google_client": None,
        }

    # 2. Initialize Supabase client ONCE
    try:
        client = init_supabase()
        if client is not None:
            # Update the global supabase_client variable
            global supabase_client
            supabase_client = client
            logger.info("✅ Supabase integration enabled successfully")
        else:
            logger.warning("⚠️  Running without Supabase integration")
    except Exception as e:
        logger.error(f"❌ Unexpected error during Supabase initialization: {str(e)}")
        # Continue running without Supabase
        supabase_client = None

    # 3. Initialize OAuth
    google_client, oauth = init_google_oauth(app, supabase_client)

    return {
        "supabase_client": supabase_client,
        "oauth": oauth,
        "google_client": google_client,
    }


# ============================================================================
# STEP 3: Create FastHTML app instance with headers
# ============================================================================
# --- App Initialization ---
# Get frankenui and tailwind headers via CDN using Theme.blue.headers()
# Choose a theme color (blue, green, red, etc)
hdrs = Theme.red.headers(apex_charts=True)

app, rt = fast_app(
    hdrs=hdrs,
    title="ViralVibes - YouTube Trends, Decoded",
    static_dir="static",
    favicon="/static/favicon.ico",
    apple_touch_icon="/static/favicon.jpeg",
    meta=[
        {"charset": "UTF-8"},
        {"name": "viewport", "content": "width=device-width, initial-scale=1.0"},
        {
            "name": "description",
            "content": "Analyze YouTube playlists instantly – discover engagement, reach, and controversy.",
        },
    ],
    head=Head(
        # ✅ Add Google Fonts
        Link(
            rel="stylesheet",
            href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@600;700&display=swap",
        )
    ),
)


# Set the favicon
app.favicon = "/static/favicon.ico"

# ============================================================================
# STEP 4: Initialize OAuth with the app instance
# ============================================================================
# Initialize the application before any DB usage

services = init_app_services(app)
supabase_client = services["supabase_client"]
oauth = services["oauth"]

# =============================================================================
# Routes
# =============================================================================


@rt
def index(req, sess):
    """Homepage - public route"""

    def _Section(*c, **kwargs):
        return Section(*c, cls=f"{SECTION_BASE} space-y-3 my-48", **kwargs)

    return Titled(
        "ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            Container(
                hero_section(),
                SectionDivider(),
                _Section(HeaderCard(), id="home-section"),
                SectionDivider(),
                _Section(features_section(), id="features-section"),
                _Section(how_it_works_section(), id="how-it-works-section"),
                SectionDivider(),
                _Section(AnalysisFormCard(), id="analyze-section"),
                # grid-first Explore, then accordion for details
                # SectionDivider(),
                # _Section(ExploreGridSection(), id="explore-grid"),
                SectionDivider(),
                _Section(HomepageAccordion(), id="explore-section"),
                SectionDivider(),
                _Section(faq_section(), id="faq-section"),
                footer(),
                cls=(ContainerT.xl, "uk-container-expand"),
            ),
        ),
    )


@rt("/login")
def login(req, sess):
    """Login page - public route"""
    # If user manually visited /login (no intended_url), clear any stored URL
    # so they get redirected to homepage after login
    if not sess.get("intended_url"):
        sess.pop("intended_url", None)

    return Titled(
        "ViralVibes - Login",
        Container(
            NavComponent(oauth, req, sess),
            build_login_page(oauth, req),
        ),
    )


@rt("/logout")
def logout():
    """Standard logout - clears session and redirects"""
    return build_logout_response()


@rt("/revoke")
def revoke(req, sess):
    """
    Revoke Google OAuth token and logout.

    This endpoint:
    1. Revokes the access token at Google (full disconnect)
    2. Clears all session data
    3. Redirects to homepage

    Useful for testing OAuth flow and full user disconnection.
    """
    access_token = sess.get("access_token")
    user_email = sess.get("user_email", "unknown")

    # Attempt token revocation
    if access_token:
        revoke_success = revoke_google_token(access_token)
        logger.info(
            f"Token revocation for {user_email}: {'successful' if revoke_success else 'failed'}"
        )
    else:
        logger.warning(f"No access token found for revocation (user: {user_email})")

    # Clear all session data
    clear_auth_session(sess)

    logger.info(f"User {user_email} revoked access and logged out")

    # Redirect to homepage
    return RedirectResponse("/", status_code=303)


@rt("/avatar/{user_id}")
def get_avatar(user_id: str):
    """
    Serve user avatar from Supabase storage.

    Avatars are stored as blobs in Supabase storage at:
    avatars/{user_id}/avatar.jpg

    Args:
        user_id: User ID from users table

    Returns:
        Image response or 404
    """
    if not supabase_client:
        return Response(status_code=404)

    try:
        # Download avatar from Supabase storage
        avatar_path = f"avatars/{user_id}/avatar.jpg"
        response = supabase_client.storage.from_("users").download(avatar_path)

        if response:
            return Response(
                content=response,
                media_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=3600"},  # Cache for 1 hour
            )
        else:
            logger.warning(f"No avatar found for user {user_id}")
            return Response(status_code=404)

    except Exception as e:
        logger.warning(f"Failed to fetch avatar for user {user_id}: {e}")
        return Response(status_code=404)


@rt("/validate/url", methods=["POST"])
def validate_url(playlist: YoutubePlaylist, req, sess):
    """Validate playlist URL - requires authentication"""
    if not (sess and sess.get("auth")):
        sess["intended_url"] = str(req.url.path)
        return Alert(
            P("Please log in to analyze playlists."),
            cls=AlertT.warning,
        )

    errors = YoutubePlaylistValidator.validate(playlist)
    if errors:
        return Div(
            Ul(*[Li(e, cls="text-red-600 list-disc") for e in errors]),
            cls="text-red-100 bg-red-50 p-4 border border-red-300 rounded",
        )
    return Script(
        "htmx.ajax('POST', '/validate/preview', {target: '#preview-box', values: {playlist_url: '%s'}});"
        % playlist.playlist_url
    )


@rt("/validate/preview", methods=["POST"])
def preview_playlist(playlist_url: str, req, sess):
    """Preview playlist - No auth check allow public previews"""

    return preview_playlist_controller(playlist_url)


def update_meter(meter_id: str, value: int = None, max_value: int = None):
    """Emit a <script> tag to update the progress meter."""
    if max_value is not None:
        yield f"<script>var el=document.getElementById('{meter_id}'); if(el){{ el.max={max_value}; }}</script>"
    if value is not None:
        yield f"<script>var el=document.getElementById('{meter_id}'); if(el) el.value={value};</script>"


@rt("/d/{dashboard_id}", methods=["GET"])
def dashboard_page(
    dashboard_id: str, req, sess, sort_by: str = "Views", order: str = "desc"
):
    """View saved dashboard - PROTECTED route"""
    # Store intended URL before redirecting to login
    auth = sess.get("auth") if sess else None

    if os.getenv("TESTING") != "1" and not auth:
        sess["intended_url"] = str(req.url.path)
        return RedirectResponse("/login", status_code=303)

    # 1. Resolve dashboard_id → playlist_url
    playlist_url = resolve_playlist_url_from_dashboard_id(dashboard_id)

    if not playlist_url:
        return Alert(
            P("Dashboard not found."),
            cls=AlertT.error,
        )

    # 2. Load data from Supabase (same as validate/full)
    data = load_cached_or_stub(playlist_url, 1)

    df = data["df"]
    summary_stats = data["summary_stats"]

    playlist_name = data["playlist_name"]
    channel_name = data["channel_name"]
    channel_thumbnail = data["channel_thumbnail"]
    cached_stats = data.get("cached_stats")

    # 3. Sorting (identical to validate/full)
    sortable_map = {
        h: get_sort_col(h) for h in DISPLAY_HEADERS if get_sort_col(h) in df.columns
    }

    # Validate sort parameter
    if sort_by not in sortable_map:
        logger.warning(f"Invalid sort column '{sort_by}', defaulting to 'Views'")
        sort_by = "Views"

    valid_sort = sort_by
    valid_order = order if order in ("asc", "desc") else "desc"

    if valid_sort in sortable_map:
        df = df.sort(
            sortable_map[valid_sort],
            descending=(valid_order == "desc"),
        )

    def next_order(col):
        return "asc" if (col == valid_sort and valid_order == "desc") else "desc"

    # 4. Render dashboard (persistent mode)
    return Titled(
        f"{playlist_name} - ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            render_full_dashboard(
                df=df,
                summary_stats=summary_stats,
                playlist_name=playlist_name,
                channel_name=channel_name,
                channel_thumbnail=channel_thumbnail,
                playlist_url=playlist_url,
                valid_sort=valid_sort,
                valid_order=valid_order,
                next_order=next_order,
                cached_stats=cached_stats,
                mode="persistent",
                dashboard_id=dashboard_id,
            ),
        ),
    )


@rt("/validate/full", methods=["POST", "GET"])
def validate_full(
    htmx: HtmxHeaders,
    playlist_url: str,
    req,
    sess,
    meter_id: str = "fetch-progress-meter",
    meter_max: Optional[int] = None,
    sort_by: str = "Views",
    order: str = "desc",
):
    """Full playlist analysis - PROTECTED route"""
    # Store intended URL before returning error
    if not (sess and sess.get("auth")):
        sess["intended_url"] = str(req.url.path)
        return Alert(
            P("Please log in to analyze playlists."),
            A("Log in", href="/login", cls=f"{ButtonT.primary}"),
            cls=AlertT.warning,
        )

    # --- Check if this is an HTMX sort request BEFORE streaming ---
    if htmx.target == "playlist-table-container":
        # For sort requests, just load data and return the table
        data = load_cached_or_stub(playlist_url, meter_max or 1)
        df = data["df"]
        summary_stats = data["summary_stats"]

        # --- Normalize sort_by ---
        sortable_map = {
            h: get_sort_col(h) for h in DISPLAY_HEADERS if get_sort_col(h) in df.columns
        }
        valid_sort = sort_by if sort_by in sortable_map else "Views"
        valid_order = order.lower() if order.lower() in ("asc", "desc") else "desc"

        # --- Apply sorting ---
        if valid_sort in sortable_map:
            sort_col = sortable_map[valid_sort]
            df = df.sort(sort_col, descending=(valid_order == "desc"))

        # --- Build next_order function ---
        def next_order(col):
            return "asc" if (col == valid_sort and valid_order == "desc") else "desc"

        # Return only the table, no streaming
        table_html = render_playlist_table(
            df=df,
            summary_stats=summary_stats,
            playlist_url=playlist_url,
            valid_sort=valid_sort,
            valid_order=valid_order,
            next_order=next_order,
        )
        return table_html

    # --- For initial full page load, use streaming ---
    def stream():
        try:
            # --- 1) INIT: set the meter max (from preview) and reset value ---
            initial_max = meter_max or 1
            yield f"<script>var el=document.getElementById('{meter_id}'); if(el){{ el.max={initial_max}; el.value=0; }}</script>"

            # --- 2) Try cache first ---
            # ---       Use helper to load cached or stub stats ---
            data = load_cached_or_stub(playlist_url, initial_max)

            df = data["df"]
            playlist_name = data["playlist_name"]
            channel_name = data["channel_name"]
            channel_thumbnail = data["channel_thumbnail"]
            summary_stats = data["summary_stats"]
            total = data["total"]
            cached_stats = data["cached_stats"]

            # Update meter to correct max
            yield from update_meter(meter_id, max_value=max(total, 1))

            # If cached, tick meter quickly to completion
            if data["cached"]:
                for i in range(1, (total or 1) + 1):
                    yield from update_meter(meter_id, value=i)

            # --- 4) Sorting ---
            # ---    Ensure raw numeric columns exist for reliable sorting ---

            # --- 5) Build header-to-df column mapping (robust to differences like 'Views' vs 'View Count') ---
            # Use yt_service display headers, but map them to the actual df columns used in table
            svc_headers = (
                # yt_service.get_display_headers()
                DISPLAY_HEADERS  # e.g., ["Rank","Title","Views","Likes",...]
            )
            # Map display header → actual column in DF (raw for sorting, formatted for display)

            # Build sortable_map: header → raw numeric column
            sortable_map = {
                h: get_sort_col(h)
                for h in DISPLAY_HEADERS
                if get_sort_col(h) in df.columns
            }

            # --- 6) Normalize sort_by ---
            valid_sort = sort_by if sort_by in sortable_map else "Views"
            valid_order = order.lower() if order.lower() in ("asc", "desc") else "desc"

            # --- 7) Apply sorting ---
            if valid_sort in sortable_map:
                sort_col = sortable_map[valid_sort]
                df = df.sort(sort_col, descending=(valid_order == "desc"))

            # --- 8) Build THEAD with working arrows ---
            def next_order(col):
                return (
                    "asc" if (col == valid_sort and valid_order == "desc") else "desc"
                )

            # --- 9) Final render: steps + header side-by-side, then table, then plots ---
            # --- inside a target container for HTMX swaps ---

            # --- 9.5) Redirect to persistent dashboard (canonical URL) ---
            dashboard_id = compute_dashboard_id(playlist_url)
            final_html = str(
                render_full_dashboard(
                    df=df,
                    summary_stats=summary_stats,
                    playlist_name=playlist_name,
                    channel_name=channel_name,
                    channel_thumbnail=channel_thumbnail,
                    playlist_url=playlist_url,
                    valid_sort=valid_sort,
                    valid_order=valid_order,
                    next_order=next_order,
                    cached_stats=cached_stats,
                    mode="embedded",
                    dashboard_id=dashboard_id,
                )
            )

            yield final_html

            # # Optional escape hatch for debugging
            # skip_redirect = htmx and htmx.boosted is False and False
            # # (you can later wire ?session=1 if you want)

            # # 🔁 Instead of rendering, redirect

            # yield f"""
            # <script>
            #     window.location.href = "/d/{dashboard_id}";
            # </script>
            # """
            # return

        except Exception as e:
            logger.exception("Deep analysis failed")
            yield str(Alert(P("Failed to fetch playlist data."), cls=AlertT.error))

    return StreamingResponse(stream(), media_type="text/html")


# Alternative approach: Progressive step updates
@rt("/update-steps/<int:step>")
def update_steps_progressive(step: int):
    """Progressively update steps to show completion"""
    response = StepProgress(step)

    # If not the last step, trigger the next update
    if step < len(PLAYLIST_STEPS_CONFIG) - 1:
        response = Div(
            response,
            Script(
                f"""
                setTimeout(() => {{
                    htmx.ajax('GET', '/update-steps/{step + 1}', {{target: '#playlist-steps'}});
                }}, 800);
            """
            ),
        )

    return response


@rt("/newsletter", methods=["POST"])
def newsletter(email: str, req, sess):  # ✅ Add sess (public route)
    """Newsletter signup - PUBLIC route"""
    # No auth check - allow public signups
    # Normalize email input by trimming whitespace and lowercasing
    email = email.strip().lower()

    # Comprehensive email validation using regex
    email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    if not re.match(email_regex, email):
        return Div(
            P("Please enter a valid email address."),
            cls="text-red-600 bg-red-50 p-4 border border-red-300 rounded",
        )

    # Check if Supabase client is available
    if supabase_client is None:
        logger.warning("Supabase client not available for newsletter signup")
        return Div(
            "Newsletter signup is temporarily unavailable. Please try again later.",
            style="color: orange",
        )

    # Send to Supabase
    payload = {"email": email, "created_at": datetime.utcnow().isoformat()}
    try:
        logger.info(f"Attempting to insert newsletter signup for: {email}")

        # Insert data using Supabase client
        data = supabase_client.table(SIGNUPS_TABLE).insert(payload).execute()

        # Check if we have data in the response
        if data.data:
            logger.info(f"Successfully added newsletter signup for: {email}")
            return Div("Thanks for signing up! 🎉", style="color: green")
        else:
            logger.warning(f"No data returned from Supabase for: {email}")
            return Div(
                "Unable to process your signup. Please try again later.",
                style="color: orange",
            )

    except Exception as e:
        logger.exception(f"Newsletter signup failed for {email}")
        return Div(
            "We're having trouble processing your signup. Please try again later.",
            style="color: orange",
        )


@rt("/submit-job", methods=["POST"])
def submit_job(playlist_url: str, req, sess):
    """Submit job - PROTECTED route"""
    auth = sess.get("auth") if sess else None

    # Use existing require_auth - it will skip in tests
    auth_error = require_auth(auth)
    if auth_error:
        sess["intended_url"] = str(req.url.path)
        return auth_error  # Returns the Alert

    submit_playlist_job(playlist_url)
    # Return HTMX polling instruction
    # return Div(
    #     P("Analyzing playlist... This might take a moment."),
    #     Div(
    #         Loading(
    #             id="loading-bar",
    #             cls=(LoadingT.bars, LoadingT.lg),
    #             style="margin-top:1rem; color:#393e6e;",
    #         ),
    #     ),
    #     hx_get=f"/check-job-status?playlist_url={quote_plus(playlist_url)}",
    #     hx_trigger="every 3s",
    #     hx_swap="outerHTML",
    # )
    # Instead of polling instruction, show the full engagement screen
    return Div(
        hx_get=f"/job-progress?playlist_url={quote_plus(playlist_url)}",
        hx_trigger="load, every 2s",
        hx_swap="outerHTML",
        id="preview-box",
        children=[
            P("Analyzing playlist... This might take a moment."),
            Span(
                cls="loading loading-bars loading-large",
                id="loading-bar",
                style="margin-top:1rem; color:#393e6e;",
            ),
        ],
    )


@rt("/check-job-status", methods=["GET"])
def check_job_status(playlist_url: str, req, sess):
    """Protected route
    Checks the status of a playlist analysis job and updates the UI accordingly.
    This endpoint is designed to be polled by HTMX.
    """
    auth = sess.get("auth") if sess else None

    # Use existing require_auth
    auth_error = require_auth(auth)
    if auth_error:
        return auth_error

    job_status = get_playlist_job_status(playlist_url)

    # Check for both "complete" and "done" status
    if job_status in ["complete", "done"]:
        logger.info(f"Job for {playlist_url} is complete. Loading full analysis.")
        return Script(
            "htmx.ajax('POST', '/validate/full', "
            "{target: '#preview-box', values: {playlist_url: '%s'}});" % playlist_url
        )

    elif job_status == "failed":
        logger.error(f"Job for {playlist_url} failed.")
        return Div(
            Alert(
                P("Playlist analysis failed. Please try again or check the URL."),
                cls=AlertT.error,
            )
        )

    elif job_status == "blocked":
        logger.warning(f"Job for {playlist_url} was blocked by YouTube.")
        return Div(
            Alert(
                P(
                    "YouTube blocked this analysis due to bot protection. "
                    "Try again later or provide authentication."
                ),
                cls=AlertT.warning,
            )
        )

    else:  # 'pending', 'processing', or None (if job somehow disappeared)
        logger.info(
            f"Job for {playlist_url} status: {job_status or 'Not found'}. Continuing to poll."
        )
        return Div(
            P("Analysis in progress... Please wait."),
            Div(
                Loading(
                    id="loading-bar",
                    cls=(LoadingT.bars, LoadingT.lg),
                    style="margin-top:1rem; color:#393e6e;",
                ),
            ),
            # Continue polling this endpoint
            hx_get=f"/check-job-status?playlist_url={quote_plus(playlist_url)}",
            hx_trigger="every 3s",  # Poll every 3 seconds
            hx_swap="outerHTML",  # Replace the entire div with the new response
        )


@rt("/job-progress", methods=["GET"])
def get_job_progress_data(playlist_url: str, req, sess):
    """Protected route - gets job progress"""
    auth = sess.get("auth") if sess else None

    # Use existing require_auth
    auth_error = require_auth(auth)
    if auth_error:
        sess["intended_url"] = str(req.url.path)
        return auth_error

    return job_progress_controller(playlist_url)


# ============================================================================
# Simplified analysis routes - no worker imports!
# ============================================================================


@rt("/analyze", methods=["POST"])
def analyze_playlist(playlist_url: str, req, sess):
    """Single-step analysis with cache-first strategy."""

    # 1. Validate URL format
    playlist = YoutubePlaylist(playlist_url=playlist_url)
    errors = YoutubePlaylistValidator.validate(playlist)

    if errors:
        return Alert(
            Ul(*[Li(e, cls="text-red-600") for e in errors]),
            cls=AlertT.error,
            id="results",
        )

    # 2. Check cache FIRST (fast path)
    cached = get_cached_playlist_stats(playlist_url)

    if cached and cached.get("df") is not None:
        # ✅ Cache hit - instant return!
        return Div(
            # Success message
            Alert(
                P("✓ Loaded from cache (instant results!)"),
                cls=AlertT.success + " mb-4",
            ),
            # Render dashboard
            render_full_dashboard(
                df=cached["df"],
                summary_stats=cached.get("summary_stats", {}),
                playlist_name=cached.get("playlist_name", "Playlist"),
                channel_name=cached.get("channel_name", "Channel"),
                channel_thumbnail=cached.get("channel_thumbnail", ""),
                playlist_url=playlist_url,
                mode="embedded",
                dashboard_id=compute_dashboard_id(playlist_url),
            ),
            id="results",
        )

    # 3. Cache miss - get preview info
    preview = get_playlist_preview_info(playlist_url)

    if not preview:
        return Alert(
            P("Could not load playlist. Please check the URL."),
            cls=AlertT.error,
            id="results",
        )

    # 4. Submit job (backend handles it - we just track status)
    submit_playlist_job(playlist_url)  # ✅ No import from worker!

    # 5. Return preview + auto-polling UI
    from components.cards import PlaylistPreviewCard

    return Div(
        # Show preview card while processing
        PlaylistPreviewCard(
            playlist_name=preview.get("playlist_name", "Unknown"),
            channel_name=preview.get("channel_name", "Unknown"),
            channel_thumbnail=preview.get("channel_thumbnail", ""),
            playlist_url=playlist_url,
            playlist_length=preview.get("video_count"),
            show_refresh=False,
            meter_id="analysis-progress",
        ),
        # Status message
        Div(
            P(
                "🔄 Analyzing playlist in background...",
                cls="text-center text-blue-600 font-medium mt-6",
            ),
            Progress(
                value=0,
                max=100,
                id="analysis-progress",
                cls="w-full mt-2",
            ),
            cls="mb-6",
        ),
        # Auto-polling component (checks job status)
        Div(
            hx_get=f"/analyze/status?playlist_url={quote_plus(playlist_url)}",
            hx_trigger="load, every 2s",
            hx_swap="outerHTML",
            id="status-poller",
        ),
        id="results",
    )


@rt("/analyze/status")
def analyze_status(playlist_url: str):
    """Poll job status - uses existing DB functions only."""

    status = get_playlist_job_status(playlist_url)  # ✅ DB function, not worker!

    if status == "complete":
        # Job done - trigger results render
        return Script(
            f"""
            // Stop polling
            const poller = document.getElementById('status-poller');
            if (poller) poller.remove();

            // Load results from cache
            htmx.ajax('GET', '/analyze/results?playlist_url={quote_plus(playlist_url)}', {{
                target: '#results',
                swap: 'innerHTML'
            }});
            """
        )

    elif status == "failed":
        return Alert(
            P("❌ Analysis failed. Please try again."),
            Button(
                "Try Again",
                onclick="location.reload()",
                cls="mt-4 px-4 py-2 bg-blue-600 text-white rounded",
            ),
            cls=AlertT.error,
            id="status-poller",
        )

    elif status == "blocked":
        return Alert(
            P("⚠️ YouTube blocked this request. Please try again in a few minutes."),
            cls=AlertT.warning,
            id="status-poller",
        )

    else:
        # Still processing - keep polling
        return Div(
            P(
                f"⏳ Status: {status}...",
                cls="text-center text-gray-600",
            ),
            # Keep polling
            hx_get=f"/analyze/status?playlist_url={quote_plus(playlist_url)}",
            hx_trigger="every 2s",
            hx_swap="outerHTML",
            id="status-poller",
        )


@rt("/analyze/results")
def analyze_results(playlist_url: str):
    """Render cached results (called after job completes)."""

    cached = get_cached_playlist_stats(playlist_url)

    if not cached or not cached.get("df"):
        return Alert(
            P("Results not ready yet. Please wait..."),
            cls=AlertT.warning,
        )

    return Div(
        # Success banner
        Alert(
            P("✅ Analysis complete!"),
            cls=AlertT.success + " mb-4",
        ),
        # Dashboard
        render_full_dashboard(
            df=cached["df"],
            summary_stats=cached.get("summary_stats", {}),
            playlist_name=cached.get("playlist_name", "Playlist"),
            channel_name=cached.get("channel_name", "Channel"),
            channel_thumbnail=cached.get("channel_thumbnail", ""),
            playlist_url=playlist_url,
            mode="embedded",
            dashboard_id=compute_dashboard_id(playlist_url),
        ),
    )


# ============================================================================
# Run the app
# ============================================================================

serve()
