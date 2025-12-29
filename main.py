"""
Main entry point for the ViralVibes web app.
Modernized with Tailwind-inspired design and MonsterUI components.
"""

import asyncio
import io
import logging
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote_plus

import polars as pl
from dotenv import load_dotenv
from fasthtml.common import *
from fasthtml.common import RedirectResponse
from fasthtml.core import HtmxHeaders
from monsterui.all import *
from starlette.responses import StreamingResponse

from components import (
    AnalysisFormCard,
    AnalyticsDashboardSection,
    BenefitsCard,
    ExploreGridSection,
    FeaturesCard,
    HeaderCard,
    HomepageAccordion,
    NewsletterCard,
    SectionDivider,
    faq_section,
    features_section,
    footer,
    hero_section,
    how_it_works_section,
)
from components.processing_tips import get_tip_for_progress
from constants import (
    FLEX_COL,
    PLAYLIST_STATS_TABLE,
    PLAYLIST_STEPS_CONFIG,
    SECTION_BASE,
    SIGNUPS_TABLE,
)
from db import (
    get_cached_playlist_stats,
    get_estimated_stats,
    get_job_progress,
    get_playlist_job_status,
    get_playlist_preview_info,
    init_supabase,
    resolve_playlist_url_from_dashboard_id,
    setup_logging,
    submit_playlist_job,
    supabase_client,
    upsert_playlist_stats,
)
from services.playlist_loader import load_cached_or_stub  # get_playlist_preview
from step_components import StepProgress
from utils import compute_dashboard_id, format_number
from validators import YoutubePlaylist, YoutubePlaylistValidator
from views.dashboard import render_full_dashboard
from views.table import DISPLAY_HEADERS, get_sort_col, render_playlist_table

# Get logger instance
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

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
            "content": "Analyze YouTube playlists instantly — discover engagement, reach, and controversy.",
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

# Navigation links
scrollspy_links = (
    A("Why ViralVibes", href="#home-section"),
    A("Product", href="#analyze-section"),
    A("About", href="#explore-section"),
    Button(
        "Try ViralVibes",
        cls=ButtonT.primary,
        onclick="document.querySelector('#analysis-form').scrollIntoView({behavior:'smooth'})",
    ),
)


# Initialize application components
def init_app():
    """Initialize application components.

    This function should be called at application startup.
    It sets up logging and initializes the Supabase client.
    """
    # Configure logging
    setup_logging()

    # Initialize Supabase client ONCE
    try:
        client = init_supabase()
        if client is not None:
            # Update the global supabase_client variable
            global supabase_client
            supabase_client = client
            logger.info("Supabase integration enabled successfully")
        else:
            logger.warning("Running without Supabase integration")
    except Exception as e:
        logger.error(f"Unexpected error during Supabase initialization: {str(e)}")
        # Continue running without Supabase


# Initialize the application at the very top of main.py (before any DB usage)
init_app()


@rt("/debug/supabase")
def debug_supabase():
    """Debug endpoint to test Supabase connection and caching."""
    if supabase_client:
        try:
            # Test basic connection
            response = (
                supabase_client.table(PLAYLIST_STATS_TABLE)
                .select("count", count="exact")
                .execute()
            )
            return Div(
                H3("✅ Supabase Connection Test"),
                P(f"Status: Connected"),
                P(f"Client: {type(supabase_client).__name__}"),
                P(f"Playlist stats table accessible: Yes"),
                P(f"Count query result: {response}"),
                cls="p-6 bg-green-50 border border-green-300 rounded-lg",
            )
        except Exception as e:
            return Div(
                H3("❌ Supabase Connection Test"),
                P(f"Status: Error"),
                P(f"Error: {str(e)}"),
                cls="p-6 bg-red-50 border border-red-300 rounded-lg",
            )
    else:
        return Div(
            H3("❌ Supabase Connection Test"),
            P(f"Status: Not Available"),
            P(f"Client: None"),
            cls="p-6 bg-yellow-50 border border-yellow-300 rounded-lg",
        )


@rt
def index():
    def _Section(*c, **kwargs):
        return Section(*c, cls=f"{SECTION_BASE} space-y-3 my-48", **kwargs)

    return Titled(
        "ViralVibes",
        Container(
            # MonsterUI NavBar with sticky behavior and primary CTA
            NavBar(
                *scrollspy_links,
                brand=DivLAligned(
                    H3("ViralVibes"), UkIcon("chart-line", height=30, width=30)
                ),
                sticky=True,
                uk_scrollspy_nav=True,
                scrollspy_cls=ScrollspyT.bold,
                cls="backdrop-blur bg-white/60 shadow-sm px-4 py-3",
            ),
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


@rt("/validate/url", methods=["POST"])
def validate_url(playlist: YoutubePlaylist):
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
def preview_playlist(playlist_url: str):
    """
    Enhanced playlist preview with live YouTube data + cached data fallback.

    Priority:
    1. Try cache first (fast, may be stale)
    2. Try live YouTube API preview (fast, always current)
    3. Show loading screen with estimates
    """
    logger.info(f"Received request to preview playlist: {playlist_url}")

    # ===== PRIORITY 1: Check for CACHED analysis (complete data) =====
    cached_stats = get_cached_playlist_stats(playlist_url, check_date=True)
    if cached_stats:
        logger.info(
            f"Cache hit for playlist: {playlist_url}. Redirecting to full analysis."
        )
        return Script(
            f"htmx.ajax('POST', '/validate/full', {{target: '#preview-box', values: {{playlist_url: '{playlist_url}'}}}});"
        )

    # ===== PRIORITY 2: Check job status =====
    job_status = get_playlist_job_status(playlist_url)
    logger.info(f"Job status for {playlist_url}: {job_status}")

    # If job is complete, redirect to full analysis
    if job_status == "done":
        logger.info(f"Job complete for {playlist_url}, loading full analysis.")
        return Script(
            f"htmx.ajax('POST', '/validate/full', {{target: '#preview-box', values: {{playlist_url: '{playlist_url}'}}}});"
        )

    # Handle blocked status
    if job_status == "blocked":
        logger.warning(f"Job for {playlist_url} is blocked by YouTube.")
        return Div(
            Div(
                UkIcon("ban", width=48, height=48, cls="text-red-500 mb-4"),
                H2(
                    "YouTube Bot Challenge Detected",
                    cls="text-xl font-bold text-red-700 mb-2",
                ),
                P(
                    "Our system encountered YouTube's bot protection while analyzing this playlist.",
                    cls="text-gray-600 mb-2",
                ),
                P(
                    "This typically happens during high traffic periods. Please try again in 5-10 minutes.",
                    cls="text-gray-500 text-sm",
                ),
                cls="flex flex-col items-center text-center",
            ),
            cls="p-8 bg-red-50 border-2 border-red-200 rounded-xl shadow-sm max-w-md mx-auto",
        )

    # ===== PRIORITY 3: Get live YouTube API preview =====
    try:
        preview_data = None  # asyncio.run(get_playlist_preview(playlist_url))
        if preview_data:
            logger.info(f"Using live YouTube preview for {playlist_url}")
            title = preview_data["title"]
            channel = preview_data["channel_name"]
            thumbnail = preview_data["thumbnail"]
            video_count = preview_data["video_count"]
            description = preview_data.get("description", "")
        else:
            # YouTube API unavailable, try database stub
            logger.info(f"YouTube preview unavailable, checking database stub")
            preview_info = get_playlist_preview_info(playlist_url)
            title = preview_info.get("title", "YouTube Playlist")
            channel = preview_info.get("channel_name", "Unknown Channel")
            thumbnail = preview_info.get("thumbnail", "/static/favicon.jpeg")
            video_count = preview_info.get("video_count", 0)
            description = ""
    except Exception as e:
        logger.warning(f"Failed to get preview: {e}, using database stub")
        preview_info = get_playlist_preview_info(playlist_url)
        title = preview_info.get("title", "YouTube Playlist")
        channel = preview_info.get("channel_name", "Unknown Channel")
        thumbnail = preview_info.get("thumbnail", "/static/favicon.jpeg")
        video_count = preview_info.get("video_count", 0)
        description = ""

    # ===== Render preview screen =====
    is_submitted = job_status in ["pending", "processing"]
    button_text = "Analysis in Progress..." if is_submitted else "Start Deep Analysis"
    button_disabled = is_submitted

    # Estimate processing time
    estimated_seconds = (video_count * 2.5) if video_count else 60
    estimated_minutes = int(estimated_seconds / 60)
    estimated_display = f"~{estimated_minutes}m" if estimated_minutes > 0 else "~1m"

    # ===== Compute has_previous_analysis WITHOUT extra query =====
    if preview_info is None:
        # We used YouTube API, so check DB for previous analysis
        preview_info = get_playlist_preview_info(
            playlist_url
        )  # ← Only 1 DB call needed
    # Check if previously analyzed
    has_previous_analysis = bool(preview_info)

    return Div(
        # Header with thumbnail and title
        Div(
            Img(
                src=thumbnail,
                alt="Playlist thumbnail",
                cls="w-24 h-24 rounded-xl shadow-lg object-cover border-2 border-gray-200",
                onerror="this.src='/static/favicon.jpeg'",  # Fallback for broken images
            ),
            Div(
                H2(title, cls="text-2xl font-bold text-gray-900 mb-1"),
                Div(
                    UkIcon("link", width=16, height=16, cls="text-gray-500"),
                    Span(
                        (
                            playlist_url[:50] + "..."
                            if len(playlist_url) > 50
                            else playlist_url
                        ),
                        cls="text-gray-600 text-sm ml-1 font-mono",
                    ),
                    href=playlist_url,
                    target="_blank",
                    rel="noopener noreferrer",
                    cls="flex items-center gap-1 mb-2",
                ),
                # New vs Previously Analyzed badge
                (
                    Span(
                        "Previously Analyzed",
                        cls="px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800",
                    )
                    if has_previous_analysis
                    else Span(
                        "New Analysis",
                        cls="px-3 py-1 rounded-full text-xs font-semibold bg-green-100 text-green-800",
                    )
                ),
                cls="flex-1",
            ),
            cls="flex items-start gap-6 mb-6",
        ),
        # Stats grid
        Div(
            # Video count card
            Div(
                Div(
                    UkIcon("play-circle", width=24, height=24, cls="text-red-600"),
                    cls="mb-2",
                ),
                Div(
                    Div(
                        f"{video_count:,}" if video_count else "Unknown",
                        cls="text-3xl font-bold text-gray-900",
                    ),
                    Div("Videos", cls="text-sm text-gray-500"),
                ),
                cls="bg-gradient-to-br from-red-50 to-red-100 p-4 rounded-lg border border-red-200",
            ),
            # Processing time estimate card
            Div(
                Div(
                    UkIcon("clock", width=24, height=24, cls="text-blue-600"),
                    cls="mb-2",
                ),
                Div(
                    Div(
                        estimated_display,
                        cls="text-lg font-bold text-gray-900",
                    ),
                    Div("Est. Time", cls="text-sm text-gray-500"),
                ),
                cls="bg-gradient-to-br from-blue-50 to-blue-100 p-4 rounded-lg border border-blue-200",
            ),
            # Stats card (views or engagement)
            Div(
                Div(
                    UkIcon("users", width=24, height=24, cls="text-purple-600"),
                    cls="mb-2",
                ),
                Div(
                    Div(
                        channel[:20] + "..." if len(channel) > 20 else channel,
                        cls="text-sm font-bold text-gray-900",
                    ),
                    Div("Channel", cls="text-sm text-gray-500"),
                ),
                cls="bg-gradient-to-br from-purple-50 to-purple-100 p-4 rounded-lg border border-purple-200",
            ),
            cls="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-6",
        ),
        # Description (if available)
        (
            Div(
                P(
                    (
                        description[:200] + "..."
                        if len(description) > 200
                        else description
                    ),
                    cls="text-sm text-gray-700 italic",
                ),
                cls="bg-gray-50 p-4 rounded-lg border border-gray-200 mb-6",
            )
            if description
            else None
        ),
        # What we'll analyze
        Div(
            H3("What We'll Analyze", cls="text-sm font-semibold text-gray-700 mb-3"),
            Div(
                Div(
                    UkIcon(
                        "check-circle",
                        width=18,
                        height=18,
                        cls="text-green-600 flex-shrink-0",
                    ),
                    Span(
                        (
                            f"Process {video_count:,} videos in batches"
                            if video_count
                            else "Process all videos"
                        ),
                        cls="text-sm text-gray-700 ml-2",
                    ),
                    cls="flex items-center",
                ),
                Div(
                    UkIcon(
                        "check-circle",
                        width=18,
                        height=18,
                        cls="text-green-600 flex-shrink-0",
                    ),
                    Span(
                        "Analyze views, likes, and engagement metrics",
                        cls="text-sm text-gray-700 ml-2",
                    ),
                    cls="flex items-center mt-2",
                ),
                Div(
                    UkIcon(
                        "check-circle",
                        width=18,
                        height=18,
                        cls="text-green-600 flex-shrink-0",
                    ),
                    Span(
                        "Calculate engagement rates and category insights",
                        cls="text-sm text-gray-700 ml-2",
                    ),
                    cls="flex items-center mt-2",
                ),
                Div(
                    UkIcon(
                        "check-circle",
                        width=18,
                        height=18,
                        cls="text-green-600 flex-shrink-0",
                    ),
                    Span(
                        "Generate interactive charts and trends",
                        cls="text-sm text-gray-700 ml-2",
                    ),
                    cls="flex items-center mt-2",
                ),
                cls="space-y-0",
            ),
            cls="bg-gradient-to-br from-purple-50 to-blue-50 p-4 rounded-lg border border-purple-200 mb-6",
        ),
        # Status (if processing)
        (
            Div(
                Div(
                    Loading(cls=(LoadingT.ring, LoadingT.md, "text-blue-600")),
                    Span(
                        f"Status: {job_status.title()}",
                        cls="text-sm font-medium text-gray-700 ml-3",
                    ),
                    cls="flex items-center",
                ),
                cls="bg-blue-50 p-3 rounded-lg border border-blue-200 mb-4",
            )
            if is_submitted
            else None
        ),
        # Action button
        Button(
            (None if is_submitted else UkIcon("zap", width=24, height=24, cls="mr-2")),
            button_text,
            hx_post="/submit-job",
            hx_vals={"playlist_url": playlist_url},
            hx_target="#preview-box",
            hx_indicator="#loading-bar",
            cls=(
                "mt-10 block mx-auto w-fit px-6 py-3 rounded-xl shadow-md transition duration-300 "
                + (
                    "bg-gray-400 cursor-not-allowed"
                    if button_disabled
                    else "bg-blue-600 hover:bg-blue-700"
                )
            ),
            type="button",
            disabled=button_disabled,
        ),
        Div(
            Loading(
                id="loading-bar",
                cls=(LoadingT.bars, LoadingT.lg),
                style="margin-top:1rem; color:#393e6e;",
            ),
            id="results-box",
        ),
        cls="p-6 bg-white rounded-xl shadow-lg border border-gray-200 max-w-3xl mx-auto",
    )


def update_meter(meter_id: str, value: int = None, max_value: int = None):
    """
    Emit a <script> tag to update the progress meter.
    """
    if max_value is not None:
        yield f"<script>var el=document.getElementById('{meter_id}'); if(el){{ el.max={max_value}; }}</script>"
    if value is not None:
        yield f"<script>var el=document.getElementById('{meter_id}'); if(el) el.value={value};</script>"


@rt("/d/{dashboard_id}", methods=["GET"])
def dashboard_page(
    dashboard_id: str,
    sort_by: str = "Views",
    order: str = "desc",
):
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
    return render_full_dashboard(
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
    )


@rt("/validate/full", methods=["POST", "GET"])
def validate_full(
    htmx: HtmxHeaders,
    playlist_url: str,
    meter_id: str = "fetch-progress-meter",
    meter_max: Optional[int] = None,
    sort_by: str = "Views",
    order: str = "desc",
):
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
def newsletter(email: str):
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
def submit_job(playlist_url: str):
    """
    Submit a playlist for analysis and show the engaging processing screen.
    """
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
def check_job_status(playlist_url: str):
    """
    Checks the status of a playlist analysis job and updates the UI accordingly.
    This endpoint is designed to be polled by HTMX.
    """
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
def get_job_progress_data(playlist_url: str):
    """
    HTMX polling endpoint that returns real-time job progress.
    Called every 2-3 seconds during processing.

    Returns HTML fragment with updated progress, time, batch info, tips, etc.
    """

    # Get job status
    job = get_job_progress(playlist_url)
    if not job:
        return Div(P("Job not found", cls="text-red-600"))

    status = job.get("status")
    progress = job.get("progress") or 0.0
    started_at_str = job.get("started_at")
    error = job.get("error")

    # Calculate time metrics
    try:
        started_at = (
            datetime.fromisoformat(started_at_str.replace("Z", "+00:00"))
            if started_at_str
            else None
        )
        now = datetime.now(timezone.utc)
        elapsed_seconds = (now - started_at).total_seconds() if started_at else 0
    except Exception as e:
        logger.warning(f"Failed to parse timestamp: {e}")
        elapsed_seconds = 0

    # Estimate remaining time (based on progress rate)
    if progress > 0 and progress < 1.0:
        rate = elapsed_seconds / progress  # seconds per 1% progress
        remaining_seconds = rate * (1.0 - progress)
    else:
        remaining_seconds = 0

    # Format time displays
    def format_seconds(seconds):
        if seconds < 0:
            return "0s"
        minutes = int(seconds) // 60
        secs = int(seconds) % 60
        if minutes > 0:
            return f"{minutes}m {secs}s"
        return f"{secs}s"

    elapsed_display = format_seconds(elapsed_seconds)
    remaining_display = format_seconds(remaining_seconds)

    # Batch calculation (assume ~5 batches for smooth progress)
    batch_count = 5
    current_batch = max(1, int(progress * batch_count))

    # Get tip for current progress
    tip = get_tip_for_progress(progress)

    # Get estimated stats for preview
    preview_info = get_playlist_preview_info(playlist_url)
    video_count = preview_info.get("video_count", 0) if preview_info else 0
    estimated_stats = get_estimated_stats(video_count)

    # Determine if we should show completion
    is_complete = status == "done"

    # Build inner content as a list
    inner_content = [
        # Header
        Div(
            H2("Processing Your Playlist", cls="text-2xl font-bold text-gray-900 mb-2"),
            P(
                f"Analyzing {format_number(video_count) if video_count else '?'} videos",
                cls="text-gray-600",
            ),
            cls="mb-8",
        ),
        # Main progress section
        Div(
            # Progress percentage and time
            Div(
                Div(
                    Span(
                        f"{int(progress * 100)}%",
                        cls="text-5xl font-bold text-blue-600",
                    ),
                    Span(
                        "Complete",
                        cls="text-lg text-gray-600 ml-3",
                    ),
                    cls="flex items-baseline",
                ),
                Div(
                    Div(
                        Span("⏱️ ", cls="mr-1"),
                        Span(
                            f"Elapsed: {elapsed_display}",
                            cls="text-gray-700",
                        ),
                        cls="",
                    ),
                    (
                        Div(
                            Span("⏳ ", cls="mr-1"),
                            Span(
                                f"Remaining: {remaining_display}",
                                cls="text-gray-700",
                            ),
                            cls="mt-1",
                        )
                        if progress > 0 and progress < 1.0
                        else None
                    ),
                    cls="space-y-1 text-sm",
                ),
                cls="flex justify-between items-start mb-4",
            ),
            # Progress bar
            Div(
                cls="w-full bg-gray-200 rounded-full h-3 overflow-hidden",
                style=f"background: linear-gradient(to right, #3b82f6 0%, #3b82f6 {progress * 100}%, #e5e7eb {progress * 100}%, #e5e7eb 100%)",
            ),
            # Batch indicators
            Div(
                *[
                    Div(
                        cls=(
                            "h-2 flex-1 rounded-sm transition-colors duration-300 "
                            + ("bg-blue-600" if i < current_batch else "bg-gray-300")
                        ),
                    )
                    for i in range(batch_count)
                ],
                cls="flex gap-2 mt-4",
            ),
            Div(
                P(
                    f"Processing batch {current_batch} of {batch_count}",
                    cls="text-sm text-gray-600 mt-2",
                ),
                cls="text-center",
            ),
            cls="bg-white p-6 rounded-lg border border-gray-200 mb-6",
        ),
        # Stats preview section
        Div(
            H3("What's Being Analyzed", cls="text-lg font-semibold text-gray-900 mb-4"),
            Div(
                Div(
                    Div(
                        UkIcon(
                            "play-circle", width=24, height=24, cls="text-red-600 mb-2"
                        ),
                        Div(
                            Div(
                                format_number(video_count),
                                cls="text-2xl font-bold text-gray-900",
                            ),
                            Div("Videos", cls="text-xs text-gray-500"),
                        ),
                    ),
                    cls="bg-red-50 p-4 rounded-lg border border-red-200",
                ),
                Div(
                    Div(
                        UkIcon("eye", width=24, height=24, cls="text-blue-600 mb-2"),
                        Div(
                            Div(
                                format_number(estimated_stats["estimated_total_views"]),
                                cls="text-lg font-bold text-gray-900",
                            ),
                            Div("Est. Views", cls="text-xs text-gray-500"),
                        ),
                    ),
                    cls="bg-blue-50 p-4 rounded-lg border border-blue-200",
                ),
                Div(
                    Div(
                        UkIcon("heart", width=24, height=24, cls="text-pink-600 mb-2"),
                        Div(
                            Div(
                                format_number(estimated_stats["estimated_total_likes"]),
                                cls="text-lg font-bold text-gray-900",
                            ),
                            Div("Est. Likes", cls="text-xs text-gray-500"),
                        ),
                    ),
                    cls="bg-pink-50 p-4 rounded-lg border border-pink-200",
                ),
                Div(
                    Div(
                        UkIcon(
                            "message-circle",
                            width=24,
                            height=24,
                            cls="text-green-600 mb-2",
                        ),
                        Div(
                            Div(
                                format_number(
                                    estimated_stats["estimated_total_comments"]
                                ),
                                cls="text-lg font-bold text-gray-900",
                            ),
                            Div("Est. Comments", cls="text-xs text-gray-500"),
                        ),
                    ),
                    cls="bg-green-50 p-4 rounded-lg border border-green-200",
                ),
                cls="grid grid-cols-2 sm:grid-cols-4 gap-3",
            ),
            Div(
                P(
                    "📌 These are estimates based on typical playlist patterns. Actual metrics will be calculated once processing completes.",
                    cls="text-xs text-gray-500 mt-3",
                ),
                cls="text-center",
            ),
            cls="bg-white p-6 rounded-lg border border-gray-200 mb-6",
        ),
        # Tips section
        Div(
            Div(
                UkIcon(tip["icon"], width=24, height=24, cls="text-blue-600 mb-2"),
                H4(tip["title"], cls="font-semibold text-gray-900 mb-1"),
                P(tip["content"], cls="text-sm text-gray-600"),
                cls="",
            ),
            cls="bg-gradient-to-r from-blue-50 to-indigo-50 p-6 rounded-lg border border-blue-200 mb-6",
        ),
        # Status messages for edge cases
        (
            Div(
                P(
                    f"⚠️ Error: {error}",
                    cls="text-red-600 text-sm",
                ),
                cls="bg-red-50 p-4 rounded-lg border border-red-200",
            )
            if status == "failed" and error
            else None
        ),
        # Completion message (shown when done)
        (
            Div(
                P(
                    "✅ Processing complete! Loading results...",
                    cls="text-green-600 font-semibold",
                ),
                Script(
                    f"setTimeout(() => {{ htmx.ajax('POST', '/validate/full', {{target: '#preview-box', values: {{playlist_url: '{playlist_url}'}}}}); }}, 1000);"
                ),
            )
            if is_complete
            else None
        ),
    ]

    # Return outer container with HTMX attributes
    # HTMX will replace the entire outer div on each poll
    return Div(
        *inner_content,
        id="progress-container",
        hx_get=(
            f"/job-progress?playlist_url={quote_plus(playlist_url)}"
            if not is_complete
            else None
        ),
        hx_trigger="every 2s" if not is_complete else None,
        hx_swap="outerHTML" if not is_complete else None,
        cls="max-w-2xl mx-auto",
    )


serve()
