from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse
from typing import List, Optional, Tuple, Union

from dotenv import load_dotenv
from supabase import create_client, Client
import logging
import os
import polars as pl
import re
from datetime import datetime
import yt_dlp
from fasthtml.common import *
from monsterui.all import *
from tenacity import retry, stop_after_attempt, wait_exponential

from utils import (calculate_engagement_rate, format_duration, format_number,
                   process_numeric_column)
from components import (HeaderCard, AnalysisFormCard, FeaturesCard,
                        BenefitsCard, NewsletterCard, PlaylistSteps)
from constants import (PLAYLIST_STEPS_CONFIG, FLEX_COL, FLEX_CENTER,
                       FLEX_BETWEEN, GAP_2, GAP_4, SECTION_BASE, CARD_BASE,
                       HEADER_CARD, FORM_CARD, NEWSLETTER_CARD)
from validators import YoutubePlaylist, YoutubePlaylistValidator

# Get logger instance
logger = logging.getLogger(__name__)


def setup_logging():
    """Configure logging for the application.
    
    This function should be called at application startup.
    It configures the logging format and level.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')


# Load environment variables
load_dotenv()

# Global Supabase client
supabase_client: Optional[Client] = None


@retry(stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1, min=4, max=10),
       reraise=True)
def init_supabase() -> Optional[Client]:
    """Initialize Supabase client with retry logic and proper error handling.
    
    Returns:
        Optional[Client]: Supabase client if initialization succeeds, None otherwise
        
    Note:
        - Retries up to 3 times with exponential backoff
        - Returns None instead of raising exceptions
        - Logs errors for debugging
    """
    global supabase_client

    # Return existing client if already initialized
    if supabase_client is not None:
        return supabase_client

    try:
        url: str = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")
        key: str = os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY", "")

        if not url or not key:
            logger.warning(
                "Missing Supabase environment variables - running without Supabase"
            )
            return None

        client = create_client(url, key)

        # Test the connection
        client.auth.get_session()

        supabase_client = client
        logger.info("Supabase client initialized successfully")
        return client

    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {str(e)}")
        return None


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
hdrs = Theme.red.headers()

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
                   A("Features", href="#features-section"),
                   A("Benefits", href="#benefits-section"),
                   A("Newsletter", href="#newsletter-section"))

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
        global supabase_client
        supabase_client = init_supabase()
        if supabase_client is None:
            logger.warning("Running without Supabase integration")
    except Exception as e:
        logger.error(
            f"Unexpected error during Supabase initialization: {str(e)}")
        # Continue running without Supabase
        supabase_client = None


# Initialize the application
init_app()


# --- Data Models ---
@dataclass
class YoutubePlaylist:
    playlist_url: str


# --- Utility Functions ---
def get_playlist_videos(
        playlist_url: str) -> Tuple[pl.DataFrame, str, str, str]:
    """Fetches video information from a YouTube playlist URL.
    
    Args:
        playlist_url (str): The URL of the YouTube playlist to analyze.
        
    Returns:
        Tuple[pl.DataFrame, str, str, str]: A tuple containing:
            - A Polars DataFrame with video information
            - The playlist name
            - The channel name
            - The channel thumbnail URL
    """
    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "force_generic_extractor": True
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        playlist_info = ydl.extract_info(playlist_url, download=False)

        # Debug logging
        logger.info("Playlist Info Keys: %s", playlist_info.keys())
        logger.info("Uploader Info: %s", playlist_info.get("uploader"))
        logger.info("Channel Info: %s", playlist_info.get("channel"))
        logger.info("Channel URL: %s", playlist_info.get("channel_url"))

        playlist_name = playlist_info.get("title", "Untitled Playlist")
        channel_name = playlist_info.get("uploader", "Unknown Channel")

        # Extract channel thumbnail from thumbnails
        channel_thumbnail = ""
        if "thumbnails" in playlist_info:
            thumbnails = playlist_info["thumbnails"]
            # Try to get the highest quality thumbnail
            for thumb in thumbnails:
                if thumb.get(
                        "width", 0
                ) >= 48:  # Look for thumbnail that's at least 48px wide
                    channel_thumbnail = thumb.get("url", "")
                    break
            # If no suitable thumbnail found, use the first one
            if not channel_thumbnail and thumbnails:
                channel_thumbnail = thumbnails[0].get("url", "")

        logger.info("Selected Channel Thumbnail: %s", channel_thumbnail)

        if "entries" in playlist_info:
            videos = playlist_info["entries"]
            data = [{
                "Rank": rank,
                "id": video.get("id", ""),
                "Title": video.get("title", "N/A"),
                "Views (Billions)":
                (video.get("view_count") or 0) / 1_000_000_000,
                "View Count": video.get("view_count", 0),
                "Like Count": video.get("like_count", 0),
                "Dislike Count": video.get("dislike_count", 0),
                "Uploader": video.get("uploader", "N/A"),
                "Creator": video.get("creator", "N/A"),
                "Channel ID": video.get("channel_id", "N/A"),
                "Duration": video.get("duration", 0),
                "Thumbnail": video.get("thumbnail", ""),
            } for rank, video in enumerate(videos, start=1)]

            return pl.DataFrame(
                data), playlist_name, channel_name, channel_thumbnail
    return pl.DataFrame(), "Untitled Playlist", "Unknown Channel", ""


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
                      _Section(FeaturesCard(), id="features-section"),
                      _Section(BenefitsCard(), id="benefits-section"),
                      _Section(NewsletterCard(), id="newsletter-section"),
                      Footer("© 2025 ViralVibes. Built for creators.",
                             className="text-center text-gray-500 py-6"),
                      cls=(ContainerT.xl, 'uk-container-expand'))))


@rt("/validate")
def validate(playlist: YoutubePlaylist):
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
                        PlaylistSteps(0),  # Reset to initial state on error
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

    # Step 2: URL validated
    steps_after_validation = PlaylistSteps(2)

    try:
        df, playlist_name, channel_name, channel_thumbnail = get_playlist_videos(
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
        steps_after_fetch = PlaylistSteps(
            len(PLAYLIST_STEPS_CONFIG))  # Complete all steps

        # Apply formatting functions to the DataFrame
        df = df.with_columns([
            pl.col("View Count").map_elements(format_number,
                                              return_dtype=pl.String),
            pl.col("Like Count").map_elements(format_number,
                                              return_dtype=pl.String),
            pl.col("Dislike Count").map_elements(format_number,
                                                 return_dtype=pl.String),
            pl.col("Duration").map_elements(format_duration,
                                            return_dtype=pl.String)
        ])

        # Calculate engagement rate
        view_counts_numeric = process_numeric_column(df["View Count"])
        like_counts_numeric = process_numeric_column(df["Like Count"])
        dislike_counts_numeric = process_numeric_column(df["Dislike Count"])

        df = df.with_columns([
            pl.Series(name="Engagement Rate (%)",
                      values=[
                          f"{calculate_engagement_rate(vc, lc, dc):.2f}"
                          for vc, lc, dc in
                          zip(view_counts_numeric, like_counts_numeric,
                              dislike_counts_numeric)
                      ])
        ])

        # Create table
        headers = [
            "Rank", "Title", "Views", "Likes", "Dislikes", "Duration",
            "Engagement Rate"
        ]
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
                   Td(row["Like Count"]), Td(row["Dislike Count"]),
                   Td(row["Duration"]), Td(row["Engagement Rate (%)"])))
        tbody = Tbody(*tbody_rows)

        # Create table footer with summary
        total_views = view_counts_numeric.sum()
        total_likes = like_counts_numeric.sum()
        avg_engagement = df["Engagement Rate (%)"].cast(pl.Float64).mean()

        tfoot = Tfoot(
            Tr(Td("Total/Average"), Td(""), Td(format_number(total_views)),
               Td(format_number(total_likes)), Td(""), Td(""),
               Td(f"{avg_engagement:.2f}%")))

        # Create channel info section with thumbnail
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
            cls="flex items-center mb-2") if channel_thumbnail else "",
                           cls="mb-2")

        # Debug logging for channel info
        logger.info("Channel Info Component: %s", channel_info)

        return Div(
            steps_after_fetch,
            Div(Alert("Analysis Complete! ✅", cls=AlertT.success),
                P(A(f"Playlist: {playlist_name}",
                    href=playlist.playlist_url,
                    target="_blank",
                    style="color:#2563eb;text-decoration:underline;"),
                  cls="text-lg text-gray-700 mb-2"),
                channel_info,
                Table(thead, tbody, tfoot, cls="w-full mt-2"),
                style="margin-top: 1rem;"))

    return Div(
        steps_after_validation,
        P("Valid YouTube Playlist URL, but no videos were found or could not be retrieved."
          ),
        style="color: orange;")


# Alternative approach: Progressive step updates
@rt("/update-steps/<int:step>")
def update_steps_progressive(step: int):
    """Progressively update steps to show completion"""
    steps = []
    for i, (title, icon, desc) in enumerate(PLAYLIST_STEPS_CONFIG):
        if i <= step:
            step_cls = StepT.success
        elif i == step + 1:
            step_cls = StepT.primary  # Next step is active
        else:
            step_cls = StepT.neutral

        steps.append(
            LiStep(title, cls=step_cls, data_content=icon, description=desc))

    response = Steps(*steps, cls=STEPS_CLS)

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
