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
CARD_BASE_CLS = "max-w-2xl mx-auto my-12 p-8 shadow-lg rounded-xl bg-white text-gray-900 hover:shadow-xl transition-shadow duration-300"
HEADER_CARD_CLS = "bg-gradient-to-r from-rose-500 via-red-600 to-red-700 text-white py-8 px-6 text-center rounded-xl"
CARD_INLINE_STYLE = "max-w-420px; margin: 3rem auto; padding: 2rem; box-shadow: 0 4px 24px #0001; border-radius: 1.2rem; background: #fff; color: #333; transition: all 0.3s ease;"
FORM_CARD_CLS = CARD_INLINE_STYLE + " hover:shadow-xl"
NEWSLETTER_CARD_CLS = CARD_INLINE_STYLE + " hover:shadow-xl"
FLEX_COL_CENTER_CLS = "flex flex-col items-center px-4 space-y-4"

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
def validate_youtube_playlist(playlist: YoutubePlaylist):
    errors = []
    try:
        parsed_url = urlparse(playlist.playlist_url)
        # Accept www.youtube.com, youtube.com, m.youtube.com, and music.youtube.com
        allowed_domains = {
            "www.youtube.com",
            "youtube.com",
            "m.youtube.com",
            "music.youtube.com",
        }
        if parsed_url.netloc not in allowed_domains:
            errors.append(
                "Invalid YouTube URL: Domain is not a recognized youtube.com domain"
            )
            return errors

        if parsed_url.path != "/playlist":
            errors.append("Invalid YouTube URL: Not a playlist URL")
            return errors

        query_params = parse_qs(parsed_url.query)
        if "list" not in query_params:
            errors.append("Invalid YouTube URL: Missing playlist ID")
            return errors

        playlist_id = query_params["list"][0]

        if not playlist_id:
            errors.append("Invalid YouTube URL: Empty playlist ID")
            return errors
    except Exception:
        errors.append("Invalid URL format")
        return errors
    return errors


def get_playlist_videos(playlist_url: str) -> pl.DataFrame:
    """Fetches video information from a YouTube playlist URL.
    
    Args:
        playlist_url (str): The URL of the YouTube playlist to analyze.
        
    Returns:
        pl.DataFrame: A Polars DataFrame containing video information with columns:
            - Rank (int): Position in playlist
            - Title (str): Video title
            - Views (Billions) (float): View count in billions
            - View Count (int): Raw view count
            - Like Count (int): Number of likes
            - Dislike Count (int): Number of dislikes
            - Uploader (str): Channel name
            - Creator (str): Video creator
            - Channel ID (str): YouTube channel ID
            - Duration (int): Video length in seconds
            - Thumbnail (str): URL to video thumbnail
            
    Note:
        Returns an empty Polars DataFrame if no videos are found or if the playlist is invalid.
    """
    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "force_generic_extractor": True
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        playlist_info = ydl.extract_info(playlist_url, download=False)

        if "entries" in playlist_info:
            videos = playlist_info["entries"]
            data = [{
                "Rank": rank,
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

            return pl.DataFrame(data)
    return pl.DataFrame()


def HeaderCard() -> Card:
    return Card(P("Decode YouTube virality. Instantly.",
                  cls="text-lg mt-2 text-white"),
                header=CardTitle("ViralVibes",
                                 cls="text-4xl font-bold text-white"),
                cls=HEADER_CARD_CLS)


def PlaylistSteps(completed_steps: int = 0) -> Steps:
    """Create a Steps component explaining the playlist submission process.
    
    Args:
        completed_steps (int): Number of completed steps (0-5)
    
    Returns:
        Steps: A MonsterUI Steps component showing the playlist analysis workflow
    """
    # Define step configurations
    step_configs = [
        ("Paste Playlist URL", "📋", "Copy and paste any YouTube playlist URL"),
        ("Validate URL", "✓", "We verify it's a valid YouTube playlist"),
        ("Fetch Video Data", "📊", "Retrieve video statistics and metadata"),
        ("Calculate Metrics", "🔢",
         "Process views, likes, and engagement rates"),
        ("Display Results", "📈", "View comprehensive analysis in a table"),
    ]

    steps = []
    for i, (title, icon, description) in enumerate(step_configs):
        if i < completed_steps:
            # Completed steps
            step_cls = StepT.success
        elif i == completed_steps:
            # Current active step
            step_cls = StepT.primary
        else:
            # Future steps
            step_cls = StepT.neutral

        steps.append(
            LiStep(title,
                   cls=step_cls,
                   data_content=icon,
                   description=description))

    return Steps(
        *steps,
        cls=
        "uk-steps uk-steps-horizontal min-h-[400px] my-8 mx-auto max-w-4xl text-center flex justify-center items-center"
    )


def AnalysisFormCard() -> Card:
    """Create the analysis form card component.
    
    Returns:
        Card: A MonsterUI Card component containing the analysis form
    """
    prefill_url = "https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC"
    return Card(
        Img(src="/static/celebration.webp",
            style=
            "width: 100%; max-width: 320px; margin: 0 auto 1.5rem auto; display: block;",
            alt="Celebration"),
        P("Follow these steps to analyze any YouTube playlist:",
          cls="text-lg font-semibold text-center mb-4"),
        # Center the steps container
        Div(PlaylistSteps(),
            id="playlist-steps",
            cls="flex justify-center w-full"),
        Form(LabelInput(
            "Playlist URL",
            type="text",
            name="playlist_url",
            placeholder="Paste YouTube Playlist URL",
            value=prefill_url,
            className=
            "px-4 py-2 w-full border rounded mb-4 focus:ring-2 focus:ring-red-500 focus:border-red-500 transition-all"
        ),
             Button(
                 "Analyze Now",
                 type="submit",
                 className=
                 f"{ButtonT.destructive} hover:scale-105 transition-transform"
             ),
             Loading(id="loading",
                     cls=(LoadingT.bars, LoadingT.lg),
                     style="margin-top:1rem; display:none; color:#393e6e;",
                     htmx_indicator=True),
             hx_post="/validate",
             hx_target="#playlist-steps",
             hx_indicator="#loading"),
        Div(id="result", style="margin-top:2rem;"),
        cls=FORM_CARD_CLS,
        body_cls="space-y-6")


def create_info_card(title: str,
                     items: List[Tuple[str, str, str]],
                     img_src: Optional[str] = None,
                     img_alt: Optional[str] = None) -> Card:
    """Helper function to create Feature and Benefit cards.
    
    Args:
        title (str): The title of the card
        items (List[Tuple[str, str, str]]): List of (title, description, icon) tuples
        img_src (Optional[str]): Optional path to card image
        img_alt (Optional[str]): Optional alt text for card image
        
    Returns:
        Card: A MonsterUI Card component with the specified content
    """
    cards = [
        Div(icon,
            H4(item_title, cls="mb-2 mt-2"),
            P(desc, cls="text-gray-600 text-sm text-center"),
            cls=FLEX_COL_CENTER_CLS) for item_title, desc, icon in items
    ]
    img_component = Img(
        src=img_src,
        style="width:120px; margin: 0 auto 2rem auto; display:block;",
        alt=img_alt) if img_src else ""
    return Card(img_component,
                Grid(*cards),
                header=CardTitle(
                    title, cls="text-2xl font-semibold mb-4 text-center"),
                cls=CARD_BASE_CLS,
                body_cls="space-y-6")


def FeaturesCard() -> Card:
    features = [
        ("Uncover Viral Secrets",
         "Paste a playlist and uncover the secrets behind viral videos.",
         UkIcon("search", cls="text-red-500 text-3xl mb-2")),
        ("Instant Playlist Insights", "Get instant info on trending videos.",
         UkIcon("zap", cls="text-red-500 text-3xl mb-2")),
        ("No Login Required", "Just paste a link and go. No signup needed!",
         UkIcon("unlock", cls="text-red-500 text-3xl mb-2")),
    ]
    return create_info_card("What is ViralVibes?", features,
                            "/static/virality.webp",
                            "Illustration of video viral insights")


def BenefitsCard() -> Card:
    benefits = [
        ("Real-time Analysis", "Track trends as they emerge.",
         UkIcon("activity", cls="text-red-500 text-3xl mb-2")),
        ("Engagement Metrics",
         "Understand what drives likes, shares, and comments.",
         UkIcon("heart", cls="text-red-500 text-3xl mb-2")),
        ("Top Creator Insights", "Identify breakout content and rising stars.",
         UkIcon("star", cls="text-red-500 text-3xl mb-2")),
    ]
    return create_info_card("Why You'll Love It", benefits)


def NewsletterCard() -> Card:
    return Card(
        P("Enter your email to get early access and updates. No spam ever.",
          cls="mb-4"),
        Form(LabelInput(
            "Email",
            type="email",
            name="email",
            required=True,
            pattern="[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$",
            title="Please enter a valid email address",
            placeholder="you@example.com",
            className=
            "px-4 py-2 w-full max-w-sm border rounded focus:ring-2 focus:ring-red-500 focus:border-red-500 transition-all invalid:border-red-500 invalid:focus:ring-red-500"
        ),
             Button("Notify Me",
                    type="submit",
                    className=
                    f"{ButtonT.primary} hover:scale-105 transition-transform"),
             Loading(id="loading",
                     cls=(LoadingT.bars, LoadingT.lg),
                     style="margin-top:1rem; display:none; color:#393e6e;",
                     htmx_indicator=True),
             className="flex flex-col items-center space-y-4",
             hx_post="/newsletter",
             hx_target="#newsletter-result",
             hx_indicator="#loading"),
        Div(id="newsletter-result", style="margin-top:1rem;"),
        header=CardTitle("Be the first to try it",
                         cls="text-xl font-bold mb-4"),
        cls=NEWSLETTER_CARD_CLS,
        body_cls="space-y-6")


@rt
def index():

    def _Section(*c, **kwargs):
        return Section(*c, cls='space-y-3 my-48', **kwargs)

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
    errors = validate_youtube_playlist(playlist)
    if errors:
        return Div(
            PlaylistSteps(0),  # Reset to initial state on error
            Ul(*[Li(error) for error in errors]),
            style="color: red;")

    # Step 2: URL validated
    steps_after_validation = PlaylistSteps(2)

    try:
        df = get_playlist_videos(playlist.playlist_url)
    except Exception as e:
        return Div(
            steps_after_validation,
            P("Valid YouTube Playlist URL, but failed to fetch videos: " +
              str(e)),
            style="color: orange;")

    if df.height > 0:
        # Step 3: Data fetched successfully
        steps_after_fetch = PlaylistSteps(5)  # Complete all steps

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
            tbody_rows.append(
                Tr(Td(row["Rank"]), Td(row["Title"]), Td(row["View Count"]),
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

        return Div(
            steps_after_fetch,
            Div("Analysis Complete! ✅",
                Br(),
                Table(thead, tbody, tfoot, cls="w-full mt-4"),
                style="color: green; margin-top: 2rem;"))

    return Div(
        steps_after_validation,
        P("Valid YouTube Playlist URL, but no videos were found or could not be retrieved."
          ),
        style="color: orange;")


# Alternative approach: Progressive step updates
@rt("/update-steps/<int:step>")
def update_steps_progressive(step: int):
    """Progressively update steps to show completion"""
    steps_config = [
        ("Paste Playlist URL", "📋", "Copy and paste any YouTube playlist URL"),
        ("Validate URL", "✓", "We verify it's a valid YouTube playlist"),
        ("Fetch Video Data", "📊", "Retrieve video statistics and metadata"),
        ("Calculate Metrics", "🔢",
         "Process views, likes, and engagement rates"),
        ("Display Results", "📈", "View comprehensive analysis in a table"),
    ]

    steps = []
    for i, (title, icon, desc) in enumerate(steps_config):
        if i <= step:
            step_cls = StepT.success
        elif i == step + 1:
            step_cls = StepT.primary  # Next step is active
        else:
            step_cls = StepT.neutral

        steps.append(
            LiStep(title, cls=step_cls, data_content=icon, description=desc))

    response = Steps(
        *steps,
        cls=
        ("uk-steps uk-steps-horizontal min-h-[400px] my-8 mx-auto max-w-2xl text-center"
         ))

    # If not the last step, trigger the next update
    if step < 4:
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
