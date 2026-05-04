"""
Main entry point for the ViralVibes web app.
Modernized with Tailwind-inspired design and MonsterUI components.
Includes Google OAuth with token revocation support.
"""

import csv
import io
import json
import logging
import mimetypes
import os
import re
import time as _time_module
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

from dotenv import load_dotenv
from fasthtml.common import *
from fasthtml.common import RedirectResponse, Response
from fasthtml.core import HtmxHeaders
from monsterui.all import *
from starlette.responses import Response as StarletteResponse, StreamingResponse

from auth.auth_service import (
    AUTH_SKIP_ROUTE_PATTERNS,
    ViralVibesAuth,
    init_google_oauth,
)
from auth.token_revocation import clear_auth_session, revoke_google_token
from components import (
    AnalyticsDashboardSection,
    BenefitsCard,
    BottomCTASection,
    CoreValuePropsSection,
    ExploreGridSection,
    FeaturesCard,
    HeaderCard,
    HomepageAccordion,
    ListsFeatureShowcase,
    NavComponent,
    NewsletterCard,
    SectionDivider,
    StepProgress,
    TopAlertBar,
    engagement_slider_section,
    faq_section,
    features_section,
    footer,
    hero_section,
    how_it_works_section,
)
from components.modals import ExportModal, ShareModal
from constants import (
    JobStatus,
    PLAYLIST_STATS_TABLE,
    PLAYLIST_STEPS_CONFIG,
    SECTION_BASE,
    SIGNUPS_TABLE,
)
from controllers.auth_routes import (
    build_auth_redirect_page,
    build_login_page,
    build_logout_response,
    build_onetap_login_page,
    normalize_intended_url,
    require_auth,
)

from controllers.job_progress import job_progress_controller
from controllers.preview import preview_playlist_controller
from db import (
    get_cached_playlist_stats,
    get_estimated_stats,
    get_favourite_creators_with_stats,
    get_job_progress,
    get_playlist_job_status,
    get_playlist_preview_info,
    get_user_dashboards,
    get_user_favourite_creators,
    get_user_favourite_list_keys,
    get_user_favourite_lists,
    add_favourite_list,
    remove_favourite_list,
    get_user_plan,
    init_supabase,
    resolve_playlist_url_from_dashboard_id,
    setup_logging,
    submit_playlist_job,
    supabase_client,
    upsert_playlist_stats,
    add_creator_by_handle,
)
from services.playlist_loader import load_cached_or_stub, load_dashboard_by_id
from utils import compute_dashboard_id, get_columns, get_language_name, sort_dataframe
from validators import YoutubePlaylist, YoutubePlaylistValidator
from views.dashboard import render_full_dashboard
from views.favourites import render_favourites_page
from views.my_dashboards import (
    render_my_dashboards_page,
    render_dashboard_page_partial,
)
from views.lists import _list_heart_btn
from views.table import DISPLAY_HEADERS, get_sort_col, render_playlist_table
from routes.analysis import analysis_page_content
from routes.creators import (
    blueprint_route,
    compare_creators_route,
    creator_add_status_route,
    creator_profile_route,
    creator_request_route,
    creators_route,
    toggle_favourite_route,
)
from routes.legal import privacy_page_content, terms_page_content
from routes.pricing import pricing_page_content
from routes.stripe_webhooks import stripe_webhook
from routes.stripe_checkout import (
    billing_checkout,
    billing_checkout_begin,
    billing_success_content,
    billing_portal,
)
from services.plan_gate import gate_plan
from services.sitemap import build_sitemap_xml as _sitemap_build_xml
from routes.lists import (
    categories_explorer_route,
    countries_explorer_route,
    languages_explorer_route,
    category_detail_more_route,
    category_detail_route,
    country_detail_more_route,
    country_detail_route,
    language_detail_more_route,
    language_detail_route,
    lists_more_categories_route,
    lists_more_countries_route,
    lists_more_languages_route,
    lists_route,
)
from routes.admin import admin_get, admin_jobs_fragment, admin_rescue_quota_jobs
from views.lists import _unslugify

# Get logger instance
logger = logging.getLogger(__name__)

# ============================================================================
# Safety Constants
# ============================================================================
# Allow only simple, safe identifiers for user_id used in storage paths
# Prevents path traversal and other injection attacks
SAFE_USER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

# Supported image formats for avatars
# Only these extensions are tried to prevent arbitrary file access
AVATAR_FORMATS = [".jpg", ".jpeg", ".png", ".webp"]

# Whether the app is running under the test suite (set by GitHub Actions / pytest)
IS_TESTING = os.getenv("TESTING") == "1"


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

# Add custom CSS and JS to headers
hdrs += (
    Link(rel="stylesheet", href="/css/main.css"),
    Script(src="/js/index.js"),
    Script(src="/js/reveal.js"),
)

# Add Vercel Web Analytics
# Script initializes the analytics queue before the main script loads
hdrs += (
    Script(
        "window.va = window.va || function () { (window.vaq = window.vaq || []).push(arguments); };"
    ),
    Script(src="https://cdn.vercel-insights.com/v1/script.js", defer=True),
)

# Add Vercel Speed Insights
# Using ES module to inject Speed Insights tracking
hdrs += (
    Script(
        """
        import { injectSpeedInsights } from 'https://cdn.jsdelivr.net/npm/@vercel/speed-insights@1/+esm';
        injectSpeedInsights();
        """,
        type="module",
    ),
)

# Auth beforeware — canonical FastHTML pattern (adv_app.py, quickstart docs).
# skip uses re.search(), so regex patterns like r'/static/.*' cover all children.
_login_redir = RedirectResponse("/login", status_code=303)


def _auth_before(req, sess):
    req.scope["auth"] = sess.get("auth", None)


_bware = Beforeware(
    _auth_before,
    skip=AUTH_SKIP_ROUTE_PATTERNS,
)

_app, rt = fast_app(
    hdrs=hdrs,
    before=_bware,
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
app = _app  # Vercel will now find 'app' as a FastHTML object, not a tuple


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

    # Ensure all components are wrapped safely
    sections = [
        hero_section() or Div(),  # Full-screen hero (wraps HeaderCard)
        SectionDivider(),
        engagement_slider_section() or Div(),
        CoreValuePropsSection(),  # 4-box value props section
        ListsFeatureShowcase(),  # NEW: Lists feature showcase with screenshot
        SectionDivider(),
        _Section(features_section() or Div(), id="features-section"),
        _Section(how_it_works_section() or Div(), id="how-it-works-section"),
        SectionDivider(),
        _Section(faq_section() or Div(), id="faq-section"),
        BottomCTASection(),
        footer() or Div(),
    ]

    # Filter out any None/False values
    sections = [s for s in sections if s is not None and s is not False]

    return Titled(
        "ViralVibes",
        Container(
            TopAlertBar(),
            NavComponent(oauth, req, sess),
            Container(
                *sections,
                cls=f"{ContainerT.xl} uk-container-expand",
            ),
        ),
    )


@rt("/login")
def login(req, sess):
    """
    PRIMARY LOGIN ENDPOINT - UNIFIED AUTH UI

    This now uses build_auth_redirect_page() which handles:
    ✅ Navbar display
    ✅ Switches between old/new UI via USE_NEW_LOGIN_UI env var
    ✅ Single source of truth for login page

    Default: Modern One-Tap Material Design UI
    Fallback: Simple button (set USE_NEW_LOGIN_UI=false)
    """
    # Normalize session: if user manually visited /login (no intended_url),
    # clear any stale URLs so they get redirected to homepage after login
    normalize_intended_url(sess)

    # Consume any contextual message set by a pre-auth redirect (e.g. checkout)
    subheadline = sess.pop("login_context", None)

    return build_auth_redirect_page(oauth, req, sess, return_url="/", subheadline=subheadline)


@rt("/login/onetap")
def login_onetap_test(req, sess):
    """
    DEPRECATED TEST ROUTE - Use /login/new instead

    Legacy parallel test route. Now handled by the unified helper.
    Kept for backward compatibility during migration.

    Redirects to /login with new UI enabled.
    """
    # Normalize session: clear stale intended_url if this was a manual visit
    normalize_intended_url(sess)

    # Use unified builder with force new UI (respects sess['intended_url'])
    return build_auth_redirect_page(oauth, req, sess, return_url="/", use_new_ui=True)


@rt("/login/new")
def login_new_ui(req, sess):
    """
    STEP 3: A/B TEST ROUTE - FORCE NEW ONE-TAP UI

    This explicitly forces the new Material Design 3 UI regardless
    of the USE_NEW_LOGIN_UI env var. Useful for A/B testing or
    demonstrating the new UI.

    After Step 2 testing, this route can be deprecated as /login
    will use the new UI by default.

    Access: http://localhost:5001/login/new
    """
    # Normalize session: clear stale intended_url if this was a manual visit
    normalize_intended_url(sess)

    # Use unified builder but FORCE new UI (respects sess['intended_url'])
    return build_auth_redirect_page(oauth, req, sess, return_url="/", use_new_ui=True)


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
    user_id = sess.get("user_id")
    user_email = sess.get("user_email", "unknown")

    access_token = None

    # Fetch access_token from secure backend storage
    if user_id and supabase_client:
        try:
            result = (
                supabase_client.table("auth_providers")
                .select("access_token")
                .eq("user_id", user_id)
                .eq("provider", "google")
                .single()
                .execute()
            )
            if result.data:
                access_token = result.data.get("access_token")
        except Exception as e:
            logger.warning(f"Failed to fetch access_token for revocation: {e}")

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

    # Validate user_id to prevent path traversal and injection attacks
    if not SAFE_USER_ID_RE.match(user_id):
        logger.warning(f"Invalid user_id format: {user_id}")
        return Response("Invalid user id", status_code=400)

    try:
        # Try multiple image formats; first successful download wins
        response = None
        media_type = None
        avatar_path = None

        for ext in AVATAR_FORMATS:
            avatar_path = f"avatars/{user_id}/avatar{ext}"
            try:
                response = supabase_client.storage.from_("users").download(avatar_path)
                if response:
                    # Derive MIME type from file extension
                    guessed_type, _ = mimetypes.guess_type(avatar_path)
                    media_type = guessed_type or "application/octet-stream"
                    logger.debug(f"Avatar found: {avatar_path} (type: {media_type})")
                    break
            except Exception:
                # This format doesn't exist, try the next one
                continue

        if response:
            return Response(
                content=response,
                media_type=media_type,
                headers={"Cache-Control": "public, max-age=3600"},  # Cache for 1 hour
            )
        else:
            logger.warning(
                f"No avatar found for user {user_id} (tried: {', '.join(AVATAR_FORMATS)})"
            )
            return Response(status_code=404)

    except Exception as e:
        logger.error(f"Failed to fetch avatar for user {user_id}: {e}")
        return Response(status_code=500)


@rt("/validate/url", methods=["POST"])
def validate_url(playlist: YoutubePlaylist, req, sess):
    """Validate playlist URL - requires authentication"""

    # Validate URL FIRST (before storing or processing)
    errors = YoutubePlaylistValidator.validate(playlist)
    if errors:
        return Div(
            Ul(*[Li(e, cls="text-red-600 list-disc") for e in errors]),
            cls="text-red-100 bg-red-50 p-4 border border-red-300 rounded",
        )

    # Check auth AFTER validation (only store valid URLs)
    if not (sess and sess.get("auth")):
        # Store the VALIDATED playlist URL they want to analyze
        sess["intended_playlist_url"] = playlist.playlist_url
        # Store intended URL for post-login redirect
        sess["intended_url"] = "/me/dashboards"

        # Redirect to clean login page (avoids navbar duplication)
        return RedirectResponse("/login", status_code=303)

    # Authenticated user - proceed to preview
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
def dashboard_page(dashboard_id: str, req, sess, sort_by: str = "Views", order: str = "desc"):
    """View saved dashboard - PROTECTED route"""

    # Store intended URL before redirecting to login
    # Extract user_id from session FIRST
    user_id = sess.get("user_id") if sess else None
    auth = sess.get("auth") if sess else None

    if not IS_TESTING and not auth:
        sess["intended_url"] = str(req.url.path)
        return RedirectResponse("/login", status_code=303)

    # 1. Load dashboard via service layer (handles all data deserialization)
    #    Resolve dashboard_id → playlist data
    data = load_dashboard_by_id(dashboard_id, user_id=user_id)

    if not data:
        return Alert(
            P("Dashboard not found."),
            cls=AlertT.error,
        )

    # 2. Extract clean data from service response
    df = data["df"]
    playlist_url = data["playlist_url"]
    playlist_name = data["playlist_name"]
    channel_name = data["channel_name"]
    channel_thumbnail = data["channel_thumbnail"]
    summary_stats = data["summary_stats"]
    cached_stats = data["cached_stats"]

    # 3. Sorting (identical to validate/full)
    df_columns = get_columns(df)
    sortable_map = {h: get_sort_col(h) for h in DISPLAY_HEADERS if get_sort_col(h) in df_columns}

    # Validate sort parameter
    if sort_by not in sortable_map:
        logger.warning(f"Invalid sort column '{sort_by}', defaulting to 'Views'")
        sort_by = "Views"

    valid_sort = sort_by
    valid_order = order if order in ("asc", "desc") else "desc"

    if valid_sort in sortable_map:
        sort_col = sortable_map[valid_sort]
        df = sort_dataframe(df, sort_col, descending=(valid_order == "desc"))

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

    # Extract user_id from session
    user_id = sess.get("user_id") if sess else None

    # Check auth
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
        df_columns = get_columns(df)
        sortable_map = {
            h: get_sort_col(h) for h in DISPLAY_HEADERS if get_sort_col(h) in df_columns
        }
        valid_sort = sort_by if sort_by in sortable_map else "Views"
        valid_order = order.lower() if order.lower() in ("asc", "desc") else "desc"

        # --- Apply sorting ---
        if valid_sort in sortable_map:
            sort_col = sortable_map[valid_sort]
            df = sort_dataframe(df, sort_col, descending=(valid_order == "desc"))

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
            data = load_cached_or_stub(playlist_url, initial_max, user_id=user_id)

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
            df_columns = get_columns(df)
            sortable_map = {
                h: get_sort_col(h) for h in DISPLAY_HEADERS if get_sort_col(h) in df_columns
            }

            # --- 6) Normalize sort_by ---
            valid_sort = sort_by if sort_by in sortable_map else "Views"
            valid_order = order.lower() if order.lower() in ("asc", "desc") else "desc"

            # --- 7) Apply sorting ---
            if valid_sort in sortable_map:
                sort_col = sortable_map[valid_sort]
                df = sort_dataframe(df, sort_col, descending=(valid_order == "desc"))

            # --- 8) Build THEAD with working arrows ---
            def next_order(col):
                return "asc" if (col == valid_sort and valid_order == "desc") else "desc"

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

    # Extract user_id from session
    user_id = sess.get("user_id") if sess else None
    auth = sess.get("auth") if sess else None

    # Use existing require_auth - it will skip in tests
    auth_error = require_auth(auth)
    if auth_error:
        sess["intended_url"] = str(req.url.path)
        return auth_error  # Returns the Alert

    # Pass user_id to submit_playlist_job
    logger.info(f"Submitting job for {playlist_url} (user_id={user_id})")
    submit_playlist_job(playlist_url, user_id=user_id)

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
    if job_status in JobStatus.SUCCESS:
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
                Span(
                    cls=f"{LoadingT.bars} {LoadingT.lg}",
                    id="loading-bar",
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


@rt("/modal/share/{dashboard_id}")
def get_share_modal(dashboard_id: str, req, sess):
    """Show share modal for dashboard."""

    # Resolve dashboard info
    playlist_url = resolve_playlist_url_from_dashboard_id(dashboard_id)

    if not playlist_url:
        return Alert(P("Dashboard not found."), cls=AlertT.error)

    # Get playlist name from cache
    cached = get_cached_playlist_stats(playlist_url)
    playlist_name = cached.get("title", "YouTube Playlist") if cached else "YouTube Playlist"

    # Build full URL
    dashboard_url = f"{req.base_url}d/{dashboard_id}"

    return ShareModal(
        dashboard_url=str(dashboard_url),
        playlist_name=playlist_name,
        modal_id="share-modal",
    )


@rt("/modal/export/{dashboard_id}")
def get_export_modal(dashboard_id: str, req, sess):
    """Show export modal for dashboard."""

    # Resolve dashboard info
    playlist_url = resolve_playlist_url_from_dashboard_id(dashboard_id)

    if not playlist_url:
        return Alert(P("Dashboard not found."), cls=AlertT.error)

    # Get playlist name from cache
    cached = get_cached_playlist_stats(playlist_url)
    playlist_name = cached.get("title", "YouTube Playlist") if cached else "YouTube Playlist"

    return ExportModal(
        dashboard_id=dashboard_id, playlist_name=playlist_name, modal_id="export-modal"
    )


@rt("/export/{dashboard_id}/csv")
def export_csv(dashboard_id: str, req, sess):
    """Export dashboard data as CSV — requires Pro plan."""

    user_id = sess.get("user_id") if sess else None

    # Auth check
    if not (sess and sess.get("auth")):
        return RedirectResponse("/login", status_code=303)

    # Plan gate: CSV export is a Pro+ feature
    blocked = gate_plan(user_id, required="pro", redirect_url=f"/export/{dashboard_id}/csv")
    if blocked:
        return blocked

    # Get data
    playlist_url = resolve_playlist_url_from_dashboard_id(dashboard_id, user_id=user_id)
    if not playlist_url:
        return Response("Dashboard not found", status_code=404)

    # Pass user_id
    data = load_cached_or_stub(playlist_url, 1, user_id=user_id)
    df = data["df"]

    # Convert to CSV
    csv_content = df.write_csv()

    # Return as download
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=viralvibes-{dashboard_id}.csv"},
    )


@rt("/export/{dashboard_id}/json")
def export_json(dashboard_id: str, req, sess):
    """Export dashboard data as JSON — requires Pro plan."""

    # Extract user_id
    user_id = sess.get("user_id") if sess else None

    # Auth check
    if not (sess and sess.get("auth")):
        return RedirectResponse("/login", status_code=303)

    # Plan gate: JSON export is a Pro+ feature
    blocked = gate_plan(user_id, required="pro", redirect_url=f"/export/{dashboard_id}/json")
    if blocked:
        return blocked

    # Get data
    playlist_url = resolve_playlist_url_from_dashboard_id(dashboard_id, user_id=user_id)
    if not playlist_url:
        return Response("Dashboard not found", status_code=404)

    # Pass user_id
    data = load_cached_or_stub(playlist_url, 1, user_id=user_id)
    df = data["df"]
    summary_stats = data["summary_stats"]

    # Convert to JSON
    export_data = {
        "dashboard_id": dashboard_id,
        "playlist_name": data["playlist_name"],
        "channel_name": data["channel_name"],
        "summary_stats": summary_stats,
        "videos": df.to_dicts(),
    }

    json_content = json.dumps(export_data, indent=2)

    # Return as download
    return Response(
        content=json_content,
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename=viralvibes-{dashboard_id}.json"},
    )


@rt("/analysis")
def analysis(req, sess):
    """Playlist analysis page — PUBLIC route"""
    user_id = sess.get("user_id") if sess else None
    return Titled(
        "Analyze a Playlist - ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            analysis_page_content(user_id=user_id),
            cls=ContainerT.xl,
        ),
    )


@rt("/creators")
def creators(req, sess):
    """Creators discovery page - PUBLIC route with filtering and sorting"""

    # Call the route handler
    page_content = creators_route(
        req,
        is_authenticated=bool(sess.get("auth")),
        user_id=sess.get("user_id") if sess else None,
    )

    # Pass through redirects (e.g. out-of-range page) without wrapping in the page template
    if isinstance(page_content, RedirectResponse):
        return page_content

    # Render with navigation
    return Titled(
        "Top Creators - YouTube",
        Container(
            NavComponent(oauth, req, sess),
            page_content,
        ),
    )


@rt("/creators/request")
async def creators_request(req, sess):
    """POST /creators/request — HTMX endpoint to queue a creator add request."""
    return await creator_request_route(req, sess)


@rt("/creators/add-status")
async def creators_add_status(req, sess):
    """GET /creators/add-status — HTMX polling endpoint for creator add job status."""
    return await creator_add_status_route(req, sess)


@rt("/lists")
def lists(req, sess):
    """Creator Lists page - curated, pre-filtered creator rankings"""

    # Call the route handler
    page_content = lists_route(req)

    # Render with navigation
    return Titled(
        "Creator Lists - YouTube",
        Container(
            NavComponent(oauth, req, sess),
            page_content,
        ),
    )


@rt("/lists/more-countries")
def lists_more_countries(req, sess):
    """HTMX partial — next batch of country group cards for the By Country tab."""
    return lists_more_countries_route(req)


@rt("/lists/more-categories")
def lists_more_categories(req, sess):
    """HTMX partial — next batch of category group cards for the By Category tab."""
    return lists_more_categories_route(req)


@rt("/lists/more-languages")
def lists_more_languages(req, sess):
    """HTMX partial — next batch of language group cards for the By Language tab."""
    return lists_more_languages_route(req)


@rt("/lists/country/{country_code}")
def lists_country_detail(req, sess, country_code: str):
    """Detailed creator rankings for a specific country."""
    page_content = country_detail_route(req, country_code)
    return Titled(
        f"{country_code} Creators - YouTube",
        Container(
            NavComponent(oauth, req, sess),
            page_content,
        ),
    )


@rt("/lists/country/{country_code}/more")
def lists_country_more(req, sess, country_code: str):
    """HTMX partial — load more creators for a specific country."""
    # Pass country_code from path parameter to ensure correct data
    req.country_code = country_code.upper()
    return country_detail_more_route(req)


@rt("/lists/categories")
def lists_categories_explorer(req, sess):
    """Visual bar-chart explorer of all content categories."""
    page_content = categories_explorer_route()
    return Titled(
        "Category Explorer - ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            page_content,
        ),
    )


@rt("/lists/countries")
def lists_countries_explorer(req, sess):
    """Visual bar-chart explorer of all countries with creator statistics."""
    page_content = countries_explorer_route()
    return Titled(
        "Country Explorer - ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            page_content,
        ),
    )


@rt("/lists/languages")
def lists_languages_explorer(req, sess):
    """Visual bar-chart explorer of all content languages."""
    page_content = languages_explorer_route()
    return Titled(
        "Language Explorer - ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            page_content,
        ),
    )


@rt("/lists/category/{category_slug}")
def lists_category_detail(req, sess, category_slug: str):
    """Detailed creator rankings for a specific topic category."""
    display_name = _unslugify(category_slug).title()
    page_content = category_detail_route(req, category_slug)
    return Titled(
        f"{display_name} Creators - YouTube",
        Container(
            NavComponent(oauth, req, sess),
            page_content,
        ),
    )


@rt("/lists/category/{category_slug}/more")
def lists_category_more(req, sess, category_slug: str):
    """HTMX partial — load more creators for a specific category."""
    req.category_slug = category_slug.lower()
    return category_detail_more_route(req)


@rt("/lists/language/{language_code}")
def lists_language_detail(req, sess, language_code: str):
    """Detailed creator rankings for a specific content language."""
    language_name = get_language_name(language_code.lower())
    page_content = language_detail_route(req, language_code)
    return Titled(
        f"{language_name} Creators - YouTube",
        Container(
            NavComponent(oauth, req, sess),
            page_content,
        ),
    )


@rt("/creator/{creator_id}")
def creator_profile(req, sess, creator_id: str):
    """Full creator profile page — /creator/{uuid}

    Call chain:
        main.py  @rt("/creator/{creator_id}")
          └─ creator_profile_route(req, creator_id, user_id)  ← routes/creators.py
               ├─ get_creator_stats(creator_id)                ← db.py
               ├─ is_creator_favourited(user_id, creator_id)   ← db.py (if logged in)
               ├─ 404 Div if not found
               └─ render_creator_profile_page(creator, ...)    ← views/creators.py
    """
    page_content = creator_profile_route(
        req, creator_id, user_id=sess.get("user_id") if sess else None
    )
    # Best-effort: extract channel name from the content for the title.
    # Fall back to a generic title if the creator was not found.
    channel_name = req.query_params.get("name", "Creator Profile")
    return Titled(
        f"{channel_name} - ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            page_content,
        ),
    )


@rt("/creator/{creator_id}/favourite", methods=["POST"])
def creator_favourite(req, sess, creator_id: str):
    """
    HTMX endpoint — toggle favourite state for a creator.

    POST /creator/{uuid}/favourite

    Call chain:
        main.py  @rt("/creator/{creator_id}/favourite")
          └─ toggle_favourite_route(req, sess, creator_id)  ← routes/creators.py
               ├─ is_creator_favourited(user_id, creator_id) ← db.py
               ├─ add_favourite_creator / remove_favourite_creator ← db.py
               └─ render_favourite_button(creator_id, is_favourited) ← views/creators.py

    Returns an HTMX partial (the updated FavouriteButton) for in-place swap.
    Requires authentication — returns 401 if not logged in.
    """
    return toggle_favourite_route(req, sess, creator_id)


@rt("/creator/{creator_id}/blueprint")
def creator_blueprint(req, sess, creator_id: str):
    """
    GET /creator/{uuid}/blueprint — Growth Blueprint page.

    Call chain:
        main.py  @rt("/creator/{creator_id}/blueprint")
          └─ blueprint_route(req, creator_id, user_id)  ← routes/creators.py
               ├─ get_creator_stats(creator_id)          ← db.py
               ├─ get_category_peer_benchmarks(category) ← db.py
               ├─ signals_from_row(row, ...)             ← utils/blueprint.py
               ├─ score_all_actions(signals)             ← utils/blueprint.py
               └─ render_blueprint_page(...)             ← views/blueprint.py
    """
    channel_name = req.query_params.get("name", "Growth Blueprint")
    page_content = blueprint_route(req, creator_id)
    return Titled(
        f"{channel_name} — Growth Blueprint — ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            page_content,
        ),
    )


@rt("/compare")
def compare_creators(req, sess):
    """GET /compare?a=<uuid>&b=<uuid> — side-by-side creator comparison."""
    user_id = sess.get("user_id") if sess else None
    page_content = compare_creators_route(req, user_id=user_id)
    return Titled(
        "Creator Comparison — ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            page_content,
        ),
    )


@rt("/lists/language/{language_code}/more")
def lists_language_more(req, sess, language_code: str):
    """HTMX partial — load more creators for a specific language."""
    req.language_code = language_code.lower()
    return language_detail_more_route(req)


@rt("/creators/add")
async def add_creator(req, sess):
    """POST /creators/add - Add creator by handle to database

    PROTECTED: Requires authentication to prevent abuse/quota spam.
    Authenticated users can submit creators for discovery.
    """

    # Require authentication - prevents unauthenticated spam
    auth = sess.get("auth") if sess else None
    user_id = sess.get("user_id") if sess else None

    if not auth:
        logger.warning("[AddCreator] Unauthorized submission attempt")
        return Response("Authentication required", status_code=401)

    if not user_id:
        logger.warning("[AddCreator] User auth present but no user_id")
        return Response("User identification failed", status_code=401)

    try:
        # Get form data
        form = await req.form()
        handle = form.get("handle")
        channel_id = form.get("channel_id")
        channel_name = form.get("channel_name")
        custom_url = form.get("custom_url")
        thumbnail = form.get("thumbnail")

        logger.info(f"[AddCreator] User {user_id} adding creator: {handle} (ID: {channel_id})")

        # Add to database and queue for sync
        creator_id = add_creator_by_handle(
            handle=handle,
            channel_id=channel_id,
            channel_name=channel_name,
            custom_url=custom_url,
            channel_thumbnail_url=thumbnail,
        )

        if creator_id:
            logger.info(
                f"[AddCreator] Successfully added creator {handle} (UUID: {creator_id}) by user {user_id}"
            )
            # Redirect to creators page to show the new creator
            # Use search to filter to just this creator
            return RedirectResponse(
                f"/creators?search={quote_plus(handle)}",
                status_code=303,
            )
        else:
            logger.error(f"[AddCreator] Failed to add creator {handle}")
            return RedirectResponse("/creators", status_code=303)

    except Exception as e:
        logger.exception(f"[AddCreator] Error adding creator: {e}")
        return RedirectResponse("/creators", status_code=303)


@rt("/me/dashboards")
def my_dashboards(
    req, sess, htmx: HtmxHeaders, search: str = "", sort: str = "recent", page: int = 1
):
    """User's personal dashboards page - PROTECTED route"""

    _PAGE_SIZE = 12

    # Extract user info
    user_id = sess.get("user_id") if sess else None
    auth = sess.get("auth") if sess else None
    user_name = sess.get("user_name", "User") if sess else "User"

    # Auth check
    if not auth or not user_id:
        sess["intended_url"] = str(req.url.path)
        return RedirectResponse("/login", status_code=303)

    # Check if user just logged in to analyze a playlist
    intended_playlist_url = sess.get("intended_playlist_url")
    if intended_playlist_url:
        sess.pop("intended_playlist_url", None)  # Remove after using
        logger.info(f"Submitting job for {intended_playlist_url} after login")
        submit_playlist_job(intended_playlist_url, user_id=user_id)
        dashboard_id = compute_dashboard_id(intended_playlist_url)
        return RedirectResponse(f"/d/{dashboard_id}", status_code=303)

    # Fetch all matching dashboards (search/sort applied in DB), paginate in Python
    all_dashboards = get_user_dashboards(user_id, search=search, sort=sort)
    offset = (page - 1) * _PAGE_SIZE
    page_items = all_dashboards[offset : offset + _PAGE_SIZE]
    has_more = len(all_dashboards) > offset + _PAGE_SIZE

    # HTMX load-more: return only new rows + OOB button replacement
    if htmx and page > 1:
        return render_dashboard_page_partial(page_items, has_more, page, search, sort)

    plan_info = get_user_plan(user_id)
    plan = plan_info.get("plan", "free")
    fav_creators = get_favourite_creators_with_stats(user_id) if plan in ("pro", "agency") else []
    fav_lists = get_user_favourite_lists(user_id)

    return Titled(
        f"{user_name}'s Dashboards - YouTube",
        Container(
            NavComponent(oauth, req, sess),
            render_my_dashboards_page(
                dashboards=page_items,
                user_name=user_name,
                search=search,
                sort=sort,
                plan_info=plan_info,
                fav_creators=fav_creators,
                fav_lists=fav_lists,
                has_more=has_more,
                page=page,
            ),
        ),
    )


@rt("/me/favourite-list", methods=["POST"])
def toggle_favourite_list(req, sess, list_key: str = "", list_label: str = "", list_url: str = ""):
    """
    POST /me/favourite-list — toggle a curated list bookmark.

    Returns the replacement _list_heart_btn form so HTMX can swap it in place.
    Requires authentication; redirects to /login if not logged in.
    """
    user_id = sess.get("user_id") if sess else None
    auth = sess.get("auth") if sess else None

    if not auth or not user_id:
        return RedirectResponse("/login", status_code=303)

    # Validate inputs (list_key is further validated inside add_favourite_list)
    if not list_key:
        return Response(status_code=400)
    # Clamp label/url lengths as a safety measure
    list_label = list_label.strip()[:100]
    list_url = list_url.strip()[:200]

    # Determine current state and toggle
    fav_keys = get_user_favourite_list_keys(user_id)
    is_currently_fav = list_key in fav_keys

    if is_currently_fav:
        remove_favourite_list(user_id, list_key)
        new_fav = False
    else:
        ok = add_favourite_list(user_id, list_key, list_label, list_url)
        new_fav = ok  # False if validation failed

    return _list_heart_btn(list_key, list_label, list_url, new_fav, authenticated=True)


@rt("/me/favourites/export.csv")
def me_favourites_export(req, sess):
    """Export the user's saved creators as a CSV file."""
    user_id = sess.get("user_id") if sess else None
    if not user_id:
        return RedirectResponse("/login", status_code=303)

    creators = get_user_favourite_creators(user_id)

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        [
            "channel_name",
            "channel_id",
            "channel_url",
            "custom_url",
            "current_subscribers",
            "current_view_count",
            "current_video_count",
            "primary_category",
            "country_code",
            "language",
            "quality_grade",
            "engagement_score",
            "sub_growth_30d",
            "view_growth_30d",
            "last_synced_at",
        ]
    )
    for c in creators:
        channel_id = c.get("channel_id") or ""
        channel_url = c.get("channel_url") or (
            f"https://www.youtube.com/channel/{channel_id}" if channel_id else ""
        )
        writer.writerow(
            [
                c.get("channel_name") or "",
                channel_id,
                channel_url,
                c.get("custom_url") or "",
                c.get("current_subscribers") or 0,
                c.get("current_view_count") or 0,
                c.get("current_video_count") or 0,
                c.get("primary_category") or "",
                c.get("country_code") or "",
                c.get("language") or "",
                c.get("quality_grade") or "",
                c.get("engagement_score") or "",
                c.get("sub_growth_30d") or "",
                c.get("view_growth_30d") or "",
                c.get("last_synced_at") or "",
            ]
        )

    return StarletteResponse(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="saved-creators.csv"'},
    )


@rt("/me/favourites")
def me_favourites(req, sess):
    """User's bookmarked creators page — PROTECTED route.

    Call chain:
        main.py  @rt("/me/favourites")
          └─ get_user_favourite_creators(user_id)  ← db.py
          └─ render_favourites_page(creators, user_name)  ← views/favourites.py
    """
    user_id = sess.get("user_id") if sess else None
    auth = sess.get("auth") if sess else None
    user_name = sess.get("user_name", "User") if sess else "User"

    if not auth or not user_id:
        # In test mode, require_auth is skipped; fall back to a sentinel id
        if IS_TESTING:
            user_id = user_id or "test-user-id"
        else:
            sess["intended_url"] = "/me/favourites"
            return RedirectResponse("/login", status_code=303)

    creators = get_user_favourite_creators(user_id)

    return Titled(
        "Saved Creators - ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            render_favourites_page(creators=creators, user_name=user_name),
        ),
    )


@rt("/billing/checkout", methods=["POST"])
async def checkout(req, sess):
    """Create Stripe Checkout session — redirects to Stripe-hosted page."""
    return await billing_checkout(req, sess)


@rt("/billing/checkout/begin")
async def checkout_begin(req, sess, plan: str = "pro", interval: str = "year"):
    """Resume checkout after login — redirected here via intended_url."""
    return await billing_checkout_begin(req, sess, plan=plan, interval=interval)


@rt("/billing/success")
def billing_success(req, sess, session_id: str = ""):
    """Post-checkout landing page — verifies from DB that subscription is active."""
    content = billing_success_content(req, sess, session_id)
    if isinstance(content, Response):
        return content
    return Titled(
        "Subscription Active — ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            content,
            cls=ContainerT.xl,
        ),
    )


@rt("/billing/portal")
def portal(req, sess):
    """Open Stripe Customer Portal for self-service billing management."""
    return billing_portal(req, sess)


@rt("/webhook", methods=["POST"])
async def webhook(req):
    """Stripe webhook endpoint — no auth, no session, raw body."""
    return await stripe_webhook(req)


@rt("/pricing")
def pricing(req, sess):
    """Pricing page — public route."""
    error = req.query_params.get("error", "")
    is_authenticated = bool(sess.get("auth"))
    return Titled(
        "Pricing - ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            pricing_page_content(error=error, is_authenticated=is_authenticated),
            footer(),
            cls=ContainerT.xl,
        ),
    )


@rt("/terms")
def terms(req, sess):
    """Terms of Service — public route."""
    return Titled(
        "Terms of Service - ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            terms_page_content(),
            cls=ContainerT.xl,
        ),
    )


@rt("/privacy")
def privacy(req, sess):
    """Privacy Policy — public route."""
    return Titled(
        "Privacy Policy - ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            privacy_page_content(),
            cls=ContainerT.xl,
        ),
    )


# ============================================================================
# Admin Dashboard
# ============================================================================


@rt("/admin")
def admin(req, sess):
    """Admin dashboard — protected by OAuth user ID or ADMIN_TOKEN."""
    content = admin_get(req, sess)
    # admin_get returns a Response (401) or FT content — only wrap FT in the layout
    if isinstance(content, Response):
        return content
    return Titled(
        "Admin — ViralVibes",
        Container(
            NavComponent(oauth, req, sess),
            content,
        ),
    )


@rt("/admin/jobs")
def admin_jobs(req, sess):
    """Admin jobs fragment — HTMX endpoint for job table refresh."""
    return admin_jobs_fragment(req, sess)


@rt("/admin/rescue-quota-jobs", methods=["POST"])
def admin_rescue_quota(req, sess):
    """Reset all quota-failed jobs back to pending."""
    return admin_rescue_quota_jobs(req, sess)


# ============================================================================
# Sitemap — live route with 24-hour in-process cache
# ============================================================================

_SITEMAP_BASE_URL = "https://viralvibes.fyi"
_SITEMAP_CACHE: dict = {"xml": None, "generated_at": None}
_SITEMAP_CACHE_TTL = 86_400  # 24 hours

_SITEMAP_STATIC_ROUTES = [
    ("/", "daily", "1.0"),
    ("/analyze", "weekly", "0.8"),
    ("/creators", "daily", "0.8"),
    ("/lists", "weekly", "0.7"),
    ("/lists/categories", "weekly", "0.6"),
    ("/lists/countries", "weekly", "0.6"),
    ("/lists/languages", "weekly", "0.6"),
    ("/pricing", "monthly", "0.5"),
    ("/terms", "monthly", "0.3"),
    ("/privacy", "monthly", "0.3"),
]


def _build_sitemap_xml() -> str:
    """Query Supabase for synced creators and return a sitemap XML string."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    urlset = _ET.Element("urlset", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")

    # Static routes
    for path, changefreq, priority in _SITEMAP_STATIC_ROUTES:
        url_el = _ET.SubElement(urlset, "url")
        _ET.SubElement(url_el, "loc").text = _urljoin(_SITEMAP_BASE_URL, path)
        _ET.SubElement(url_el, "lastmod").text = today
        _ET.SubElement(url_el, "changefreq").text = changefreq
        _ET.SubElement(url_el, "priority").text = priority

    # Dynamic creator pages
    try:
        resp = (
            supabase_client.table("creators")
            .select("id, last_updated_at")
            .eq("sync_status", "synced")
            .execute()
        )
        creators = resp.data or []
    except Exception:
        logger.warning("sitemap: could not fetch creators from Supabase", exc_info=True)
        creators = []

    for creator in creators:
        creator_id = creator.get("id")
        if not creator_id:
            continue
        raw_lastmod = creator.get("last_updated_at") or ""
        lastmod = raw_lastmod[:10] if len(raw_lastmod) >= 10 else today

        url_el = _ET.SubElement(urlset, "url")
        _ET.SubElement(url_el, "loc").text = _urljoin(_SITEMAP_BASE_URL, f"/creator/{creator_id}")
        _ET.SubElement(url_el, "lastmod").text = lastmod
        _ET.SubElement(url_el, "changefreq").text = "weekly"
        _ET.SubElement(url_el, "priority").text = "0.7"

    rough = _ET.tostring(urlset, "utf-8")
    return _minidom.parseString(rough).toprettyxml(indent="  ")


@rt("/sitemap.xml")
def sitemap_xml(req):
    """GET /sitemap.xml — dynamic sitemap, cached in-process for 24 h."""
    now = _time_module.monotonic()
    generated_at = _SITEMAP_CACHE.get("generated_at")
    if (
        _SITEMAP_CACHE["xml"] is None
        or generated_at is None
        or (now - generated_at) > _SITEMAP_CACHE_TTL
    ):
        _SITEMAP_CACHE["xml"] = _build_sitemap_xml()
        _SITEMAP_CACHE["generated_at"] = now

    return Response(
        content=_SITEMAP_CACHE["xml"],
        media_type="application/xml",
        headers={"Cache-Control": "public, max-age=3600"},
    )


# ============================================================================
# Run the app
# ============================================================================

serve()
