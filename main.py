"""
Main entry point for the ViralVibes web app.
Modernized with Tailwind-inspired design and MonsterUI components.
"""

import io
import logging
import re
from datetime import date, datetime
from typing import Optional
from urllib.parse import quote_plus

import polars as pl
from dotenv import load_dotenv
from fasthtml.common import *
from monsterui.all import *
from starlette.responses import StreamingResponse

from components import (
    AnalysisFormCard,
    AnalyticsDashboardSection,
    AnalyticsHeader,
    BenefitsCard,
    FeaturesCard,
    HeaderCard,
    HomepageAccordion,
    NewsletterCard,
    ExploreGridSection,
    hero_section,
    SectionDivider,
    benefit,
    faq_section,
    footer,
    section_header,
    section_wrapper,
)
from constants import (
    CARD_BASE,
    FLEX_BETWEEN,
    FLEX_CENTER,
    FLEX_COL,
    FORM_CARD,
    HEADER_CARD,
    NEWSLETTER_CARD,
    PLAYLIST_JOBS_TABLE,
    PLAYLIST_STATS_TABLE,
    PLAYLIST_STEPS_CONFIG,
    SECTION_BASE,
    SIGNUPS_TABLE,
)
from db import (
    get_cached_playlist_stats,
    get_playlist_job_status,
    get_playlist_preview_info,
    init_supabase,
    setup_logging,
    submit_playlist_job,
    supabase_client,
    upsert_playlist_stats,
)
from step_components import StepProgress
from utils import format_number, parse_number
from validators import YoutubePlaylist, YoutubePlaylistValidator

# Get logger instance
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# CSS Classes
CARD_BASE_CLS = CARD_BASE
HEADER_CARD_CLS = HEADER_CARD
CARD_INLINE_STYLE = FORM_CARD
FORM_CARD_CLS = FORM_CARD
NEWSLETTER_CARD_CLS = NEWSLETTER_CARD
FLEX_COL_CENTER_CLS = FLEX_COL + " " + FLEX_CENTER

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
    Button("Try ViralVibes", cls=ButtonT.primary, onclick="document.querySelector('#analysis-form').scrollIntoView({behavior:'smooth'})"),
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


# Manually define the headers here instead of importing them

DISPLAY_HEADERS = [
    "Rank",
    "Title",
    "Views",
    "Likes",
    "Dislikes",
    "Comments",
    "Duration",
    "Engagement Rate",
    "Controversy",
    "Rating",
]


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


def how_it_works_section():
    steps_msg = [
        (
            "1️⃣ Submit Playlist URL",
            "Paste your YouTube playlist link into the analysis form.",
        ),
        (
            "2️⃣ Preview",
            "See the playlist title, channel name, and thumbnail instantly.",
        ),
        (
            "3️⃣ Deep Analysis",
            "We crunch video stats—views, likes, dislikes, comments, engagement, and controversy.",
        ),
        (
            "4️⃣  Results Dashboard",
            "Get a detailed table and dashboard with trends and viral signals.",
        ),
    ]
    return section_wrapper(
        (
            Div(
                section_header(
                    "HOW IT WORKS",
                    "Analyze any YouTube playlist in seconds.",
                    "ViralVibes guides you through a simple, step-by-step workflow to decode YouTube trends and performance.",
                ),
                cls="max-w-3xl w-full mx-auto flex-col items-center text-center gap-6 mb-8 lg:mb-8",
            ),
            Div(
                *[benefit(title, content) for title, content in steps_msg],
                cls=f"{FLEX_COL} w-full lg:flex-row gap-4 items-center lg:gap-8 max-w-7xl mx-auto justify-center",
            ),
        ),
        bg_color="red-700",
        flex=False,
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
                _Section(how_it_works_section(), id="how-it-works-section"),
                SectionDivider(),
                _Section(AnalysisFormCard(), id="analyze-section"),
                # grid-first Explore, then accordion for details
                #SectionDivider(),
                #_Section(ExploreGridSection(), id="explore-grid"),
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
    logger.info(f"Received request to preview playlist: {playlist_url}")

    # Case 1: Try to get cached result. Check for full, up-to-date cache
    cached_stats = get_cached_playlist_stats(playlist_url, check_date=True)
    if cached_stats:
        logger.info(f"Cache hit for playlist: {playlist_url}. Serving cached data.")
        # If cached, forward to /validate/full
        return Script(
            f"htmx.ajax('POST', '/validate/full', {{target: '#preview-box', values: {{playlist_url: '{playlist_url}'}}}});"
        )
    logger.info(f"Cache miss for playlist: {playlist_url}. Checking job status.")

    # Case 2 & 3: Get minimal info and job status from DB (e.g., submission status)
    job_status = get_playlist_job_status(playlist_url)
    logger.info(f"Found job status for {playlist_url}: {job_status}")

    # If the job is 'done', immediately serve the full analysis
    if job_status == "done":
        logger.info(f"Job is 'done', redirecting to full analysis.")
        return Script(
            f"htmx.ajax('POST', '/validate/full', {{target: '#preview-box', values: {{playlist_url: '{playlist_url}'}}}});"
        )

    # Handle blocked status
    if job_status == "blocked":
        logger.warning(f"Job for {playlist_url} is blocked by YouTube.")
        return Div(
            H2(
                "YouTube Bot Challenge Detected",
                cls="text-lg font-semibold text-red-700",
            ),
            P(
                "Our system was blocked by YouTube while trying to analyze this playlist. This can happen with high traffic."
            ),
            P("Please try again in a few minutes.", cls="mt-2"),
            cls="p-6 bg-red-50 border border-red-300 rounded-lg text-center",
        )

    # Get minimal playlist info for preview
    preview_info = get_playlist_preview_info(playlist_url)
    if preview_info:
        logger.info(
            f"Found preview info for {playlist_url}: {preview_info.get('title')}"
        )
    else:
        logger.info(f"No preview info found for {playlist_url}. Using placeholders.")
        preview_info = {}

    # Determine button state
    is_submitted = job_status in ["pending", "processing"]
    button_text = "Analysis in Progress..." if is_submitted else "Submit for Analysis"

    # Build HTML response
    return Div(
        H2(
            preview_info.get("title", "Playlist Analysis Not Available Yet"),
            cls="text-lg font-semibold",
        ),
        Img(
            src=preview_info.get("thumbnail", "/static/placeholder.png"),
            alt="Playlist thumbnail",
            cls="mx-auto w-16 h-16 rounded-full",
        ),
        P(
            f"Videos: {preview_info.get('video_count', 'N/A')}",
            cls="text-gray-500 mt-2",
        ),
        P(f"Status: {job_status or 'Not submitted'}", cls="text-gray-400 mb-2"),
        P(f"URL: {playlist_url}", cls="text-gray-500"),
        Button(
            button_text,
            hx_post="/submit-job",
            hx_vals={"playlist_url": playlist_url},
            hx_target="#preview-box",
            hx_indicator="#loading-bar",
            cls=(
                "mt-8 block mx-auto w-fit px-6 py-3 rounded-lg shadow-md transition duration-300",
                (
                    "bg-gray-400 cursor-not-allowed"
                    if is_submitted
                    else "bg-blue-600 hover:bg-blue-700"
                ),
            ),
            type="button",
            disabled=is_submitted,
        ),
        Div(
            Loading(
                id="loading-bar",
                cls=(LoadingT.bars, LoadingT.lg),
                style="margin-top:1rem; color:#393e6e;",
            ),
            id="results-box",
        ),
    )


@rt("/validate/full", methods=["POST", "GET"])
def validate_full(
    playlist_url: str,
    meter_id: str = "fetch-progress-meter",
    meter_max: Optional[int] = None,
    sort_by: str = "Views",
    order: str = "desc",
):
    def stream():
        try:
            # --- 1) INIT: set the meter max (from preview) and reset value ---
            initial_max = meter_max or 1
            yield f"<script>var el=document.getElementById('{meter_id}'); if(el){{ el.max={initial_max}; el.value=0; }}</script>"

            # --- 2) Try cache first ---
            cached_stats = get_cached_playlist_stats(playlist_url)
            if cached_stats:
                logger.info(f"Using cached stats for playlist {playlist_url}")

                # reconstruct df other fields from cache row
                df = pl.read_json(io.BytesIO(cached_stats["df_json"].encode("utf-8")))
                playlist_name = cached_stats["title"]
                channel_name = cached_stats.get("channel_name", "")
                channel_thumbnail = cached_stats.get("channel_thumbnail", "")
                summary_stats = cached_stats["summary_stats"]

                # use cached video_count if present; fall back to df.height; then preview meter_max
                total = cached_stats.get("video_count") or df.height or initial_max
                yield f"<script>var el=document.getElementById('{meter_id}'); if(el){{ el.max={max(total, 1)}; }}</script>"

                # Tick to completion as a proxy (cache is instant)
                for i in range(1, (total or 1) + 1):
                    yield f"<script>var el=document.getElementById('{meter_id}'); if(el) el.value={i};</script>"
                    # No await needed, just simulate yield

                # Detect skeleton-only stats ---
                skeleton_mode = summary_stats.get(
                    "expanded_count"
                ) is not None and summary_stats.get(
                    "expanded_count", 0
                ) < summary_stats.get(
                    "processed_video_count", 0
                )

                if skeleton_mode:
                    yield str(
                        Div(
                            H3(
                                "⚠️ Skeleton-only Analysis",
                                cls="font-semibold text-yellow-700",
                            ),
                            P(
                                "We could not fully expand video stats due to YouTube bot protection. "
                                "Showing basic playlist info only."
                            ),
                            cls="p-4 bg-yellow-50 border border-yellow-300 rounded mb-4",
                        )
                    )

            else:
                # --- 3) Fresh fetch ---

                # (
                #     df,
                #     playlist_name,
                #     channel_name,
                #     channel_thumbnail,
                #     summary_stats,
                # ) = await yt_service.get_playlist_data(playlist_url)

                # if df.height == 0:
                #     yield str(Alert(P("No videos found."), cls=AlertT.warning))
                #     return

                # # Ensure max matches actual number of videos
                # total = df.height
                # yield f"<script>var el=document.getElementById('{meter_id}'); if(el){{ el.max={max(total, 1)}; }}</script>"

                # # Proxy tick per video (post-fetch, still gives user feedback)
                # for i in range(1, total + 1):
                #     yield f"<script>var el=document.getElementById('{meter_id}'); if(el) el.value={i};</script>"
                #     await asyncio.sleep(0)

                # --- 3) Fresh fetch (stub for now until yt_service is re-enabled) ---
                logger.warning(
                    "No cached stats found. Using stub values until worker is enabled."
                )

                df = pl.DataFrame([])  # empty dataframe
                playlist_name = "Unknown Playlist"
                channel_name = "Unknown Channel"
                channel_thumbnail = ""
                summary_stats = {
                    "total_views": 0,
                    "total_likes": 0,
                    "total_dislikes": 0,
                    "total_comments": 0,
                    "actual_playlist_count": 0,
                    "avg_duration": None,
                    "avg_engagement": 0.0,
                    "avg_controversy": 0.0,
                }

                # Cache snapshot
                stats_to_cache = {
                    "playlist_url": playlist_url,
                    "title": playlist_name,
                    "channel_name": channel_name,
                    "channel_thumbnail": channel_thumbnail,
                    "view_count": summary_stats.get("total_views"),
                    "like_count": summary_stats.get("total_likes"),
                    "dislike_count": summary_stats.get("total_dislikes"),
                    "comment_count": summary_stats.get("total_comments"),
                    "video_count": summary_stats.get(
                        "actual_playlist_count", df.height
                    ),
                    "processed_video_count": df.height,
                    "avg_duration": (
                        int(summary_stats.get("avg_duration"))
                        if summary_stats.get("avg_duration") is not None
                        else None
                    ),
                    "engagement_rate": summary_stats.get("avg_engagement"),
                    "controversy_score": summary_stats.get("avg_controversy", 0),
                    "summary_stats": summary_stats,
                    "df_json": df.write_json(),
                }
                upsert_playlist_stats(stats_to_cache)

            # --- 4) Sorting ---
            # ---    Ensure raw numeric columns exist for reliable sorting ---
            # create raw columns for common formatted counts if missing
            count_columns = {
                "View Count": "View Count Raw",
                "Like Count": "Like Count Raw",
                "Dislike Count": "Dislike Count Raw",
                "Comment Count": "Comment Count Raw",
            }
            for disp_col, raw_col in count_columns.items():
                if disp_col in df.columns and raw_col not in df.columns:
                    df = df.with_columns(
                        pl.col(disp_col)
                        .map_elements(parse_number, return_dtype=pl.Int64)
                        .alias(raw_col)
                    )

            # Engagement Rate: create numeric "Engagement Rate Raw" if needed
            if (
                "Engagement Rate (%)" in df.columns
                and "Engagement Rate Raw" not in df.columns
            ):
                df = df.with_columns(
                    pl.col("Engagement Rate (%)")
                    .str.replace("%", "")
                    .str.replace(",", "")
                    .cast(pl.Float64)
                    .alias("Engagement Rate Raw")
                )

            # Controversy: prefer Controversy Raw if present, otherwise cast Controversy
            if "Controversy Raw" not in df.columns and "Controversy" in df.columns:
                # controversy may already be 0..1 float; ensure numeric
                df = df.with_columns(
                    pl.col("Controversy").cast(pl.Float64).alias("Controversy Raw")
                )

            # --- 5) Build header-to-df column mapping (robust to differences like 'Views' vs 'View Count') ---
            # Use yt_service display headers, but map them to the actual df columns used in table
            svc_headers = (
                # yt_service.get_display_headers()
                DISPLAY_HEADERS  # e.g., ["Rank","Title","Views","Likes",...]
            )
            display_to_df = {
                "Rank": "Rank",
                "Title": "Title",
                "Views": "View Count",
                "Likes": "Like Count",
                "Dislikes": "Dislike Count",
                "Comments": "Comment Count",
                "Duration": "Duration",
                "Engagement Rate": "Engagement Rate (%)",
                "Controversy": "Controversy Raw",
                # fallback aliases (in case you used "View Count" directly earlier)
                "View Count": "View Count",
                "Like Count": "Like Count",
                "Dislike Count": "Dislike Count",
                "Comment Count": "Comment Count",
                "Engagement Rate (%)": "Engagement Rate (%)",
            }

            # Build sortable_map: display_header -> actual raw column to sort on (if available)
            sortable_map = {}
            for h in svc_headers:
                df_col = display_to_df.get(h, h)
                # prefer explicit raw column names if present
                candidates = [
                    f"{df_col} Raw",
                    "Engagement Rate Raw",
                    "Controversy Raw",
                    df_col,
                ]
                chosen = None
                for cand in candidates:
                    if cand in df.columns:
                        # only allow numeric types to be sortable
                        dtype = df[cand].dtype
                        if dtype in (pl.Int64, pl.Int32, pl.Float64, pl.Float32):
                            chosen = cand
                            break
                if chosen:
                    sortable_map[h] = chosen

            # --- Normalize incoming sort_by into a local variable ---
            sort_label = sort_by  # use the function arg as starting point

            # Normalize incoming sort_by (robust matching)
            if sort_label not in sortable_map:
                lower = (sort_label or "").lower()
                for key in sortable_map.keys():
                    if lower and lower in key.lower():
                        sort_label = key
                        break
                # if still not matched, fallback to first sortable column
                if sort_label not in sortable_map and len(sortable_map) > 0:
                    # try to pick "Views"/"View Count" if present, else first key
                    prefer = None
                    for candidate in ("Views", "View Count", "Like Count", "Likes"):
                        if candidate in sortable_map:
                            prefer = candidate
                            break
                    sort_label = prefer or next(iter(sortable_map))

            # Perform sort if we have a matching raw column
            if sort_label in sortable_map:
                sort_col = sortable_map[sort_label]
                # ensure we cast to numeric for sorting if needed
                if df[sort_col].dtype not in (
                    pl.Int64,
                    pl.Int32,
                    pl.Float64,
                    pl.Float32,
                ):
                    df = df.with_columns(pl.col(sort_col).cast(pl.Float64))
                df = df.sort(sort_col, descending=(order == "desc"))

            # --- 6) Build THEAD with HTMX sort links + arrows (toggle logic) ---
            def next_order_for(col_label):
                if col_label == sort_by:
                    return "asc" if order == "desc" else "desc"
                return "desc"  # default: new column -> descending

            thead = Thead(
                Tr(
                    *[
                        (
                            Th(
                                A(
                                    h
                                    + (
                                        " ▲"
                                        if (h == sort_by and order == "asc")
                                        else (
                                            " ▼"
                                            if (h == sort_by and order == "desc")
                                            else ""
                                        )
                                    ),
                                    href="#",
                                    hx_get=f"/validate/full?playlist_url={quote_plus(playlist_url)}&sort_by={quote_plus(h)}&order={next_order_for(h)}",
                                    hx_target="#playlist-table",
                                    hx_swap="outerHTML",
                                    cls="text-white font-semibold hover:underline",
                                ),
                                cls="px-4 py-2 text-sm",
                            )
                            if h in sortable_map
                            else Th(h, cls="px-4 py-2 text-sm text-white font-semibold")
                        )
                        for h in svc_headers
                    ],
                    cls="bg-blue-600 text-white",
                )
            )

            # --- 7) Build tbody (display values) ---
            tbody = Tbody(
                *[
                    Tr(
                        Td(row.get("Rank")),
                        Td(
                            A(
                                row.get("Title"),
                                href=f"https://youtube.com/watch?v={row.get('id')}",
                                target="_blank",
                                cls="text-blue-600 hover:underline",
                            )
                        ),
                        Td(row.get("View Count")),
                        Td(row.get("Like Count")),
                        Td(row.get("Dislike Count")),
                        Td(row.get("Comment Count")),
                        Td(row.get("Duration")),
                        Td(row.get("Engagement Rate (%)")),
                        # show controversy nicely (expect a float 0..1)
                        Td(
                            (
                                f"{row.get('Controversy Raw'):.2%}"
                                if row.get("Controversy Raw") is not None
                                else ""
                            )
                        ),
                    )
                    for row in df.iter_rows(named=True)
                ]
            )

            # --- 8) Footer (summary) ---
            tfoot = Tfoot(
                Tr(
                    Td("Total/Average"),
                    Td(""),
                    Td(format_number(summary_stats.get("total_views", 0))),
                    Td(format_number(summary_stats.get("total_likes", 0))),
                    Td(format_number(summary_stats.get("total_dislikes", 0))),
                    Td(format_number(summary_stats.get("total_comments", 0))),
                    Td(""),
                    Td(f"{summary_stats.get('avg_engagement', 0):.2f}%"),
                    Td(""),
                )
            )

            # --- 9) Final render: steps + header side-by-side, then table, then plots ---
            # --- inside a target container for HTMX swaps ---
            final_html = str(
                Div(
                    # Row 1: Steps + Header side by side
                    Div(
                        Div(
                            StepProgress(len(PLAYLIST_STEPS_CONFIG)),
                            cls="flex-1 p-4 bg-white rounded-xl shadow-sm border border-gray-100",
                        ),
                        Div(
                            AnalyticsHeader(
                                playlist_title=playlist_name,
                                channel_name=channel_name,
                                total_videos=summary_stats.get(
                                    "actual_playlist_count", 0
                                ),
                                processed_videos=df.height,
                                playlist_thumbnail=(
                                    cached_stats.get("playlist_thumbnail")
                                    if cached_stats
                                    else None
                                ),
                                channel_url=None,  # optional
                                channel_thumbnail=channel_thumbnail,
                                processed_date=date.today().strftime("%b %d, %Y"),
                                engagement_rate=summary_stats.get("avg_engagement"),
                                total_views=summary_stats.get("total_views"),
                            ),
                            cls="flex-1",
                        ),
                        cls="grid grid-cols-1 md:grid-cols-2 gap-6 items-start mb-8",
                    ),
                    # Row 2: Table
                    Div(
                        Table(
                            thead,
                            tbody,
                            tfoot,
                            id="playlist-table",
                            cls="w-full text-sm text-gray-700 border border-gray-200 rounded-lg shadow-sm overflow-hidden",
                        ),
                        cls="overflow-x-auto mb-8",
                    ),
                    # Row 3: Analytics dashboard / plots
                    Div(
                        AnalyticsDashboardSection(
                            df,
                            summary_stats,
                            playlist_name,
                            A(
                                channel_name,
                                href=playlist_url,
                                target="_blank",
                                cls="text-blue-600 hover:underline",
                            ),
                        ),
                        cls="mt-6",
                    ),
                    cls="space-y-8",
                )
            )

            yield final_html

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
        return Div("Please enter a valid email address.", style="color: red")

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


@rt("/dashboard", methods=["GET"])
def dashboard(playlist_url: str):
    cached_stats = get_cached_playlist_stats(playlist_url)
    if not cached_stats:
        return Div("No analysis found for this playlist.", cls="text-red-600")
    df = pl.read_json(io.BytesIO(cached_stats["df_json"].encode("utf-8")))
    summary_stats = cached_stats["summary_stats"]
    playlist_name = cached_stats["title"]
    channel_name = cached_stats.get("channel_name", "")
    channel_thumbnail = cached_stats.get("channel_thumbnail", "")
    return AnalyticsDashboardSection(
        df, summary_stats, playlist_name, channel_name, channel_thumbnail
    )


@rt("/submit-job", methods=["POST"])
def submit_job(playlist_url: str):
    submit_playlist_job(playlist_url)
    # Return HTMX polling instruction
    return Div(
        P("Analyzing playlist... This might take a moment."),
        Div(
            Loading(
                id="loading-bar",
                cls=(LoadingT.bars, LoadingT.lg),
                style="margin-top:1rem; color:#393e6e;",
            ),
        ),
        hx_get=f"/check-job-status?playlist_url={quote_plus(playlist_url)}",
        hx_trigger="every 3s",
        hx_swap="outerHTML",
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


serve()
