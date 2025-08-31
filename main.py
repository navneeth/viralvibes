import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple, Union
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv
from fasthtml.common import *
from monsterui.all import *

from components import (
    AnalysisFormCard,
    AnalyticsDashboardSection,
    BenefitsCard,
    FeaturesCard,
    HeaderCard,
    HomepageAccordion,
    NewsletterCard,
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
)
from db import init_supabase, setup_logging, supabase_client
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

app, rt = fast_app(hdrs=hdrs,
                   title="ViralVibes - YouTube Trends, Decoded",
                   static_dir="static",
                   favicon="/static/favicon.ico",
                   apple_touch_icon="/static/favicon.jpeg")

# Set the favicon
app.favicon = "/static/favicon.ico"

# Navigation links
scrollspy_links = (A("Home", href="#home-section"),
                   A("Analyze", href="#analyze-section"),
                   A("Explore", href="#explore-section"))

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
        init_supabase()
        if supabase_client is None:
            logger.warning("Running without Supabase integration")
    except Exception as e:
        logger.error(
            f"Unexpected error during Supabase initialization: {str(e)}")
        # Continue running without Supabase


# Initialize the application
init_app()

# Initialize YouTube service
yt_service = YoutubePlaylistService()


@rt
def index():

    def _Section(*c, **kwargs):
        return Section(*c, cls=f"{SECTION_BASE} space-y-3 my-48", **kwargs)

    return Titled(
        "ViralVibes",
        Container(
            NavBar(*scrollspy_links,
                   brand=DivLAligned(H3("ViralVibes"),
                                     UkIcon('chart-line', height=30,
                                            width=30)),
                   sticky=True,
                   uk_scrollspy_nav=True,
                   scrollspy_cls=ScrollspyT.bold),
            NavContainer(*map(Li, scrollspy_links),
                         uk_scrollspy_nav=True,
                         sticky=True,
                         cls=(NavT.primary, 'pt-20 px-5 pr-10')),
            Container(_Section(HeaderCard(), id="home-section"),
                      _Section(AnalysisFormCard(), id="analyze-section"),
                      _Section(HomepageAccordion(), id="explore-section"),
                      Footer("© 2025 ViralVibes. Built for creators.",
                             className="text-center text-gray-500 py-6"),
                      cls=(ContainerT.xl, 'uk-container-expand'))))


'''
@rt("/validate")
async def validate(playlist: YoutubePlaylist):
    """
    This route now renders a full HTML page for playlist analysis results,
    including success, validation errors, data fetch errors, or no videos found.
    It uses a minimal header and footer.
    """
    # Common header for the analysis result page
    analysis_result_page_header = NavBar(
        A("Home", href="/"),  # Link back to the main home page
        brand=DivLAligned(H3("ViralVibes"),
                          UkIcon('chart-line', height=30, width=30)),
        sticky=True)

    # Common footer for the analysis result page
    analysis_result_page_footer = Footer(
        "© 2025 ViralVibes. Built for creators.",
        className="text-center text-gray-500 py-6")

    # --- Step 1: Validate YouTube Playlist URL ---
    errors = YoutubePlaylistValidator.validate(playlist)
    if errors:
        return Titled(
            "Validation Error - ViralVibes",
            Container(
                analysis_result_page_header,
                Section(
                    H1("Input Error",
                       cls="text-3xl font-bold text-red-700 mb-4 text-center"),
                    Div(
                        StepProgress(0),  # Reset to initial state on error
                        Ul(*[
                            Li(error, cls="text-red-600 list-disc ml-5")
                            for error in errors
                        ],
                           cls="space-y-2"),
                        cls=
                        f"{CARD_BASE_CLS} p-6 shadow-lg rounded-lg max-w-lg mx-auto bg-red-50 border border-red-300"
                    ),
                    A("Try Again",
                      href="/analyze",
                      cls=
                      "uk-button uk-button-primary mt-8 block mx-auto w-fit px-6 py-3 rounded-lg shadow-md hover:bg-blue-700 transition duration-300"
                      ),
                    cls=f"{SECTION_BASE} space-y-3 my-20 {FLEX_COL_CENTER_CLS}"
                ),
                analysis_result_page_footer,
                cls=(ContainerT.xl, 'uk-container-expand')))

    # --- Step 2: URL validated. Show fast preview (title + thumbnail) ---
    steps_after_validation = StepProgress(2)
    
    try:
        playlist_name, channel_name, channel_thumbnail = await asyncio.to_thread(
            yt_service.get_playlist_preview, playlist.playlist_url)

        preview_ui = Div(
            P(f"Analyzing Playlist: {playlist_name}",
              cls="text-2xl font-semibold text-center"),
            Div(Img(
                src=channel_thumbnail,
                alt="Channel Thumbnail",
                style=
                "width: 64px; height: 64px; border-radius: 50%; margin: 1rem auto;"
            ),
                cls="flex justify-center"),
            cls="text-center space-y-4 my-10")

    except Exception as e:
        logger.warning("Preview fetch failed: %s", e)
        preview_ui = Div("Fetching playlist info...", cls="text-gray-500")
    
    # --- Step 3: Deep analysis (yt-dlp + dislikes) ---
    try:
        df, playlist_name, channel_name, channel_thumbnail, summary_stats = await yt_service.get_playlist_data(
            playlist.playlist_url)

        # Debug logging
        logger.info("Channel Name: %s", channel_name)
        logger.info("Channel Thumbnail URL: %s", channel_thumbnail)

    except Exception as e:
        logger.exception("Error fetching playlist data")
        return Div(
            steps_after_validation,
            Alert(DivLAligned(
                UkIcon('triangle-alert'),
                P("Valid YouTube Playlist URL, but failed to fetch videos: " +
                  str(e))),
                  cls=AlertT.error),
            style="margin-top: 1rem;")

    if df.height > 0:
        # Step 3: Data fetched successfully
        steps_after_fetch = StepProgress(
            len(PLAYLIST_STEPS_CONFIG))  # Complete all steps

        # Create table with enhanced headers from the service
        headers = yt_service.get_display_headers()
        thead = Thead(Tr(*[Th(h) for h in headers]))

        tbody_rows = []
        for row in df.iter_rows(named=True):
            # Use the 'id' field for the YouTube video ID (yt-dlp flat extraction)
            video_id = row.get("id")
            yt_link = f"https://www.youtube.com/watch?v={video_id}" if video_id else None
            title_cell = A(row["Title"],
                           href=yt_link,
                           target="_blank",
                           style="color:#2563eb;text-decoration:underline;"
                           ) if yt_link else row["Title"]
            tbody_rows.append(
                Tr(Td(row["Rank"]), Td(title_cell), Td(row["View Count"]),
                   Td(row["Like Count"]), Td(row["Dislike Count"]), Td(row["Comment Count"]),
                   Td(row["Duration"]), Td(row["Engagement Rate (%)"]), Td(f"{row['Controversy Raw']:.2%}")))
        tbody = Tbody(*tbody_rows)

        # Create table footer with comprehensive summary stats
        tfoot = Tfoot(
            Tr(Td("Total/Average"), Td(""),
               Td(format_number(summary_stats["total_views"])),
               Td(format_number(summary_stats["total_likes"])), 
               Td(format_number(summary_stats["total_dislikes"])), 
               Td(format_number(summary_stats["total_comments"])),
               Td(""), Td(f"{summary_stats['avg_engagement']:.2f}%"), Td("")))

        # Create channel info section with thumbnail
        if channel_thumbnail:
            channel_info = Div(Div(
                Img(src=channel_thumbnail,
                    alt=f"{channel_name} channel thumbnail",
                    style=
                    "width: 48px; height: 48px; border-radius: 50%; margin-right: 1rem;"
                    ),
                Div(A(
                    channel_name,
                    href=
                    f"https://www.youtube.com/channel/{df['Channel ID'].item(0)}",
                    target="_blank",
                    style="color:#2563eb;text-decoration:underline;",
                    cls="text-sm text-gray-600"),
                    cls="flex flex-col justify-center"),
                cls="flex items-center mb-2"),
                               cls="mb-2")
        else:
            channel_info = None

        # Debug logging for channel info
        logger.info("Channel Info Component: %s", channel_info)

        return Div(
            steps_after_fetch,
            Div(Alert(DivLAligned(UkIcon('check-circle'),
                                  P("Analysis Complete! ✅")),
                      cls=AlertT.success),
                P(A(f"Playlist: {playlist_name}",
                    href=playlist.playlist_url,
                    target="_blank",
                    style="color:#2563eb;text-decoration:underline;"),
                  cls="text-lg text-gray-700 mb-2"),
                channel_info,
                Table(thead, tbody, tfoot, cls="w-full mt-2"),
                AnalyticsDashboardSection(df, summary_stats),
                style="margin-top: 1rem;"))

    return Div(
        steps_after_validation,
        P("Valid YouTube Playlist URL, but no videos were found or could not be retrieved."
          ),
        style="color: orange;")
'''


@rt("/validate/url", methods=["POST"])
def validate_url(playlist: YoutubePlaylist):
    errors = YoutubePlaylistValidator.validate(playlist)
    if errors:
        return Div(
            Ul(*[Li(e, cls="text-red-600 list-disc") for e in errors]),
            cls="text-red-100 bg-red-50 p-4 border border-red-300 rounded")
    return Script(
        "htmx.ajax('POST', '/validate/preview', {target: '#preview-box', values: {playlist_url: '%s'}});"
        % playlist.playlist_url)


@rt("/validate/preview", methods=["POST"])
async def preview_playlist(playlist_url: str):
    try:
        playlist_name, channel_name, channel_thumbnail, playlist_length = await yt_service.get_playlist_preview(
            playlist_url)

    except Exception as e:
        logger.warning("Preview fetch failed: %s", e)
        return Div("Preview unavailable.", cls="text-gray-400")

    return Div(
        H2(f"Analyzing Playlist: {playlist_name}",
           cls="text-lg font-semibold"),
        Img(src=channel_thumbnail,
            alt="Channel thumbnail",
            style="width:64px;height:64px;border-radius:50%;margin:auto;"),
        P(
            Data(str(playlist_length), value=str(playlist_length)),
            " videos in playlist: ",
            Meter(value=0,
                  min=0,
                  max=playlist_length or 1,
                  low=10,
                  high=50,
                  optimum=100,
                  id="fetch-progress-meter",
                  cls="w-full h-2 mt-2")) if playlist_length else None,
        Button(
            "Start Full Analysis",
            hx_post="/validate/full",
            hx_vals={"playlist_url": playlist_url},
            hx_target="#results-box",
            hx_indicator="#loading-bar",
            cls=
            "uk-button uk-button-primary mt-8 block mx-auto w-fit px-6 py-3 rounded-lg shadow-md hover:bg-blue-700 transition duration-300",
            type="button"),
        Div(Loading(id="loading-bar",
                    cls=(LoadingT.bars, LoadingT.lg),
                    style="margin-top:1rem; color:#393e6e;"),
            id="results-box"),
    )


@rt("/validate/full", methods=["POST"])
async def validate_full(playlist_url: str):
    try:
        df, playlist_name, channel_name, channel_thumbnail, summary_stats = await yt_service.get_playlist_data(
            playlist_url)
    except Exception as e:
        logger.error("Deep analysis failed: %s", e)
        return Alert(P("Failed to fetch playlist data."), cls=AlertT.error)

    if df.height == 0:
        return Alert(P("No videos found."), cls=AlertT.warning)

    # Use the enhanced headers from the service
    headers = yt_service.get_display_headers()
    thead = Thead(Tr(*[Th(h) for h in headers]))

    tbody = Tbody(*[
        Tr(
            Td(row["Rank"]),
            Td(
                A(row["Title"],
                  href=f"https://youtube.com/watch?v={row['id']}",
                  target="_blank")), Td(row["View Count"]),
            Td(row["Like Count"]), Td(row["Dislike Count"]),
            Td(row["Comment Count"]), Td(row["Duration"]),
            Td(row["Engagement Rate (%)"]), Td(
                f"{row['Controversy Raw']:.2%}"))
        for row in df.iter_rows(named=True)
    ])

    # Use all the summary stats from youtube_service.py
    tfoot = Tfoot(
        Tr(Td("Total/Average"), Td(""),
           Td(format_number(summary_stats["total_views"])),
           Td(format_number(summary_stats["total_likes"])),
           Td(format_number(summary_stats["total_dislikes"])),
           Td(format_number(summary_stats["total_comments"])), Td(""),
           Td(f"{summary_stats['avg_engagement']:.2f}%"), Td("")))

    return Div(StepProgress(len(PLAYLIST_STEPS_CONFIG)),
               Table(thead, tbody, tfoot, cls="uk-table uk-table-divider"),
               AnalyticsDashboardSection(df, summary_stats),
               cls="space-y-4")


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
            """))

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
            style="color: orange")

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
                style="color: orange")

    except Exception as e:
        logger.exception(f"Newsletter signup failed for {email}")
        return Div(
            "We're having trouble processing your signup. Please try again later.",
            style="color: orange")


serve()
