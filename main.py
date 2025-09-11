import asyncio
import io
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple, Union
from urllib.parse import parse_qs, urlparse

import polars as pl
from dotenv import load_dotenv
from fasthtml.common import *
from monsterui.all import *
from starlette.responses import StreamingResponse

from components import (
    AnalysisFormCard,
    AnalyticsDashboardSection,
    BenefitsCard,
    FeaturesCard,
    HeaderCard,
    HomepageAccordion,
    NewsletterCard,
    benefit,
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
    PLAYLIST_STEPS_CONFIG,
    SECTION_BASE,
    benefits_lst,
)
from db import (
    get_cached_playlist_stats,
    init_supabase,
    setup_logging,
    supabase_client,
    upsert_playlist_stats,
)
from step_components import StepProgress
from utils import format_number
from validators import YoutubePlaylist, YoutubePlaylistValidator
from youtube_service import YoutubePlaylistService

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
)

# Set the favicon
app.favicon = "/static/favicon.ico"

# Navigation links
scrollspy_links = (
    A("Home", href="#home-section"),
    A("Analyze", href="#analyze-section"),
    A("Explore", href="#explore-section"),
)

# Most Viewed Youtube Videos of all time
# https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC


# Initialize application components
def init_app():
    """Initialize application components.

    This function should be called at application startup.
    It sets up logging and initializes the Supabase client.
    """
    # Configure logging
    setup_logging()

    # Initialize Supabase client
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


# Initialize the application
init_app()

# Initialize YouTube service
yt_service = YoutubePlaylistService()


@rt("/debug/supabase")
def debug_supabase():
    """Debug endpoint to test Supabase connection and caching."""
    if supabase_client:
        try:
            # Test basic connection
            response = (
                supabase_client.table("playlist_stats")
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
            NavBar(
                *scrollspy_links,
                brand=DivLAligned(
                    H3("ViralVibes"), UkIcon("chart-line", height=30, width=30)
                ),
                sticky=True,
                uk_scrollspy_nav=True,
                scrollspy_cls=ScrollspyT.bold,
            ),
            NavContainer(
                *map(Li, scrollspy_links),
                uk_scrollspy_nav=True,
                sticky=True,
                cls=(NavT.primary, "pt-20 px-5 pr-10"),
            ),
            Container(
                _Section(HeaderCard(), id="home-section"),
                _Section(how_it_works_section(), id="how-it-works-section"),
                _Section(AnalysisFormCard(), id="analyze-section"),
                _Section(HomepageAccordion(), id="explore-section"),
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
async def preview_playlist(playlist_url: str):
    try:
        (
            playlist_name,
            channel_name,
            channel_thumbnail,
            playlist_length,
        ) = await yt_service.get_playlist_preview(playlist_url)

    except Exception as e:
        logger.warning("Preview fetch failed: %s", e)
        return Div("Preview unavailable.", cls="text-gray-400")

    return Div(
        H2(f"Analyzing Playlist: {playlist_name}", cls="text-lg font-semibold"),
        Img(
            src=channel_thumbnail,
            alt="Channel thumbnail",
            style="width:64px;height:64px;border-radius:50%;margin:auto;",
        ),
        P(
            Data(
                str(playlist_length),
                value=str(playlist_length),
                cls="text-white/90 font-medium",
            ),
            " videos in playlist: ",
            Meter(
                value=0,
                min=0,
                max=playlist_length or 1,
                low=10,
                high=50,
                optimum=100,
                id="fetch-progress-meter",
                cls="w-full h-2 mt-2",
            ),
        )
        if playlist_length
        else None,
        Button(
            "Start Full Analysis",
            hx_post="/validate/full",
            hx_vals={"playlist_url": playlist_url},
            hx_target="#results-box",
            hx_indicator="#loading-bar",
            cls="uk-button uk-button-primary mt-8 block mx-auto w-fit px-6 py-3 rounded-lg shadow-md hover:bg-blue-700 transition duration-300",
            type="button",
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


@rt("/validate/full", methods=["POST"])
async def validate_full(
    playlist_url: str,
    meter_id: str = "fetch-progress-meter",
    meter_max: Optional[int] = None,
):
    async def stream():
        try:
            # --- 1) INIT: set the meter max (from preview) and reset value ---
            initial_max = meter_max or 1
            yield f"<script>var el=document.getElementById('{meter_id}'); if(el){{ el.max={initial_max}; el.value=0; }}</script>"

            # --- 2) Try cache first ---
            cached_stats = await get_cached_playlist_stats(playlist_url)
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
                    await asyncio.sleep(0)  # yield control so HTMX can paint

            else:
                # --- 3) Fresh fetch ---
                (
                    df,
                    playlist_name,
                    channel_name,
                    channel_thumbnail,
                    summary_stats,
                ) = await yt_service.get_playlist_data(playlist_url)

                if df.height == 0:
                    yield str(Alert(P("No videos found."), cls=AlertT.warning))
                    return

                # Ensure max matches actual number of videos
                total = df.height
                yield f"<script>var el=document.getElementById('{meter_id}'); if(el){{ el.max={max(total, 1)}; }}</script>"

                # Proxy tick per video (post-fetch, still gives user feedback)
                for i in range(1, total + 1):
                    yield f"<script>var el=document.getElementById('{meter_id}'); if(el) el.value={i};</script>"
                    await asyncio.sleep(0)

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
                    "video_count": df.height,
                    "avg_duration": int(summary_stats.get("avg_duration"))
                    if summary_stats.get("avg_duration") is not None
                    else None,
                    "engagement_rate": summary_stats.get("avg_engagement"),
                    "controversy_score": summary_stats.get("avg_controversy", 0),
                    "summary_stats": summary_stats,
                    "df_json": df.write_json(),
                }
                await upsert_playlist_stats(stats_to_cache)

            # --- 4) Render final table + charts and append them ---
            headers = yt_service.get_display_headers()
            thead = Thead(Tr(*[Th(h) for h in headers]))
            tbody = Tbody(
                *[
                    Tr(
                        Td(row["Rank"]),
                        Td(
                            A(
                                row["Title"],
                                href=f"https://youtube.com/watch?v={row['id']}",
                                target="_blank",
                                cls="text-blue-600 hover:underline",
                            )
                        ),
                        Td(row["View Count"]),
                        Td(row["Like Count"]),
                        Td(row["Dislike Count"]),
                        Td(row["Comment Count"]),
                        Td(row["Duration"]),
                        Td(row["Engagement Rate (%)"]),
                        Td(f"{row['Controversy Raw']:.2%}"),
                    )
                    for row in df.iter_rows(named=True)
                ]
            )
            tfoot = Tfoot(
                Tr(
                    Td("Total/Average"),
                    Td(""),
                    Td(format_number(summary_stats["total_views"])),
                    Td(format_number(summary_stats["total_likes"])),
                    Td(format_number(summary_stats["total_dislikes"])),
                    Td(format_number(summary_stats["total_comments"])),
                    Td(""),
                    Td(f"{summary_stats['avg_engagement']:.2f}%"),
                    Td(""),
                )
            )

            final_html = str(
                Div(
                    StepProgress(len(PLAYLIST_STEPS_CONFIG)),
                    Table(thead, tbody, tfoot, cls="uk-table uk-table-divider"),
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
                    cls="space-y-4",
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
            Script(f"""
                setTimeout(() => {{
                    htmx.ajax('GET', '/update-steps/{step + 1}', {{target: '#playlist-steps'}});
                }}, 800);
            """),
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
        data = supabase_client.table("signups").insert(payload).execute()

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


serve()
