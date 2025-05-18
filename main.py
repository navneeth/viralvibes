from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

import pandas as pd
import yt_dlp
from fasthtml.common import *

# MonsterUI shadows fasthtml components with the same name
from monsterui.all import *

from utils import calculate_engagement_rate, format_duration, format_number

# Get frankenui and tailwind headers via CDN using Theme.blue.headers()
# Choose a theme color (blue, green, red, etc)
hdrs = Theme.blue.headers()

app, rt = fast_app(
    hdrs=Theme.blue.headers(),
    title="ViralVibes - YouTube Trends, Decoded",
    static_dir="static",
)
# Set the favicon
app.favicon = "/static/favicon.ico"

# Most Viewed Youtube Videos of all time
# https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC


@dataclass
class YoutubePlaylist:
    playlist_url: str


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

    except ValueError:
        errors.append("Invalid URL format")
        return errors

    return errors


def get_playlist_videos(playlist_url):
    ydl_opts = {"quiet": True, "extract_flat": True, "force_generic_extractor": True}

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        playlist_info = ydl.extract_info(playlist_url, download=False)

        if "entries" in playlist_info:
            videos = playlist_info["entries"]
            data = [
                {
                    "Rank": rank,
                    "Title": video.get("title", "N/A"),
                    "Views (Billions)": (video.get("view_count") or 0) / 1_000_000_000,
                    "View Count": video.get("view_count", 0),
                    "Like Count": video.get("like_count", 0),
                    "Dislike Count": video.get("dislike_count", 0),
                    "Uploader": video.get("uploader", "N/A"),
                    "Creator": video.get("creator", "N/A"),
                    "Channel ID": video.get("channel_id", "N/A"),
                    "Duration": video.get("duration", 0),
                    "Thumbnail": video.get("thumbnail", ""),
                }
                for rank, video in enumerate(videos, start=1)
            ]

            return pd.DataFrame(data)


@rt
def index():
    prefill_url = (
        "https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC"
    )
    return Titled(
        "ViralVibes",
        Container(
            # Header
            Div(
                H1("ViralVibes", className="text-4xl font-bold text-white"),
                P(
                    "Decode YouTube virality. Instantly.",
                    className="text-lg mt-2 text-white",
                ),
                className="bg-blue-600 text-white py-6 px-4 text-center",
            ),
            # Main Form Card
            Card(
                Img(
                    src="/static/celebration.webp",
                    style="width: 100%; max-width: 320px; margin: 0 auto 1.5rem auto; display: block;",
                    alt="Celebration",
                ),
                Form(
                    Input(
                        type="text",
                        name="playlist_url",
                        placeholder="Paste YouTube Playlist URL",
                        value=prefill_url,
                        className="px-4 py-2 w-full border rounded mb-4",
                    ),
                    Button(
                        "Analyze Now",
                        type="submit",
                        className="bg-blue-600 text-white px-6 py-2 rounded hover:bg-blue-700 w-full",
                    ),
                    Loading(
                        id="loading",
                        cls=(LoadingT.bars, LoadingT.lg),
                        style="margin-top:1rem; display:none; color:#393e6e;",
                        htmx_indicator=True,
                    ),
                    hx_post="/validate",
                    hx_target="#result",
                    hx_indicator="#loading",
                ),
                Div(id="result", style="margin-top:2rem;"),
                style="max-width: 420px; margin: 3rem auto; padding: 2rem; box-shadow: 0 4px 24px #0001; border-radius: 1.2rem; background: #fff;",
            ),
            # What is ViralVibes?
            Section(
                H2("What is ViralVibes?", className="text-2xl font-semibold mb-4"),
                P(
                    "ViralVibes helps creators and marketers analyze why YouTube videos go viral — instantly. Just paste a playlist, and get actionable metrics like views, engagement rate, duration, and creator stats.",
                    className="text-lg text-gray-700",
                ),
                className="mb-10 max-w-4xl mx-auto px-4 py-10",
            ),
            # Why You'll Love It
            Section(
                H2("Why You'll Love It", className="text-2xl font-semibold mb-4"),
                Ul(
                    Li("Instantly analyze trending videos by playlist or keyword"),
                    Li("View engagement metrics and viral triggers"),
                    Li("Spot top-performing creators and channels"),
                    Li("Export tables, thumbnails, and insights"),
                    Li("No login required — just paste a link"),
                    className="list-disc list-inside text-lg text-gray-700 space-y-2",
                ),
                className="mb-10 max-w-4xl mx-auto px-4",
            ),
            # Email Signup
            Section(
                H3("Be the first to try it", className="text-xl font-bold mb-4"),
                P(
                    "Enter your email to get early access and updates. No spam ever.",
                    className="mb-4",
                ),
                Form(
                    Input(
                        type="email",
                        name="email",
                        required=True,
                        placeholder="you@example.com",
                        className="px-4 py-2 w-full max-w-sm border rounded",
                    ),
                    Button(
                        "Notify Me",
                        type="submit",
                        className="bg-blue-600 text-white px-6 py-2 rounded hover:bg-blue-700",
                    ),
                    className="flex flex-col items-center space-y-4",
                ),
                className="bg-gray-100 p-6 rounded shadow-md text-center max-w-4xl mx-auto mb-10",
            ),
            # Footer
            Footer(
                "© 2025 ViralVibes. Built for creators.",
                className="text-center text-gray-500 py-6",
            ),
        ),
    )


@rt("/validate")
def validate(playlist: YoutubePlaylist):
    errors = validate_youtube_playlist(playlist)
    if errors:
        return Div(
            Ul(*[Li(error) for error in errors]), id="result", style="color: red;"
        )

    try:
        df = get_playlist_videos(playlist.playlist_url)
    except Exception as e:
        return Div(
            "Valid YouTube Playlist URL, but failed to fetch videos: " + str(e),
            id="result",
            style="color: orange;",
        )

    if df is not None and not df.empty:
        # Apply formatting functions to the DataFrame
        df["View Count"] = df["View Count"].apply(format_number)
        df["Like Count"] = df["Like Count"].apply(format_number)
        df["Dislike Count"] = df["Dislike Count"].apply(format_number)
        df["Duration"] = df["Duration"].apply(format_duration)
        # Calculate engagement rate and add as a new column
        df["Engagement Rate (%)"] = [
            f"{calculate_engagement_rate(vc, lc, dc):.2f}"
            for vc, lc, dc in zip(
                df["View Count"]
                .replace({",": "", "N/A": 0, "": 0}, regex=True)
                .astype(str)
                .str.replace(r"[^\d.]", "", regex=True)
                .replace("", "0")
                .astype(float),
                df["Like Count"]
                .replace({",": "", "N/A": 0, "": 0}, regex=True)
                .astype(str)
                .str.replace(r"[^\d.]", "", regex=True)
                .replace("", "0")
                .astype(float),
                df["Dislike Count"]
                .replace({",": "", "N/A": 0, "": 0}, regex=True)
                .astype(str)
                .str.replace(r"[^\d.]", "", regex=True)
                .replace("", "0")
                .astype(float),
            )
        ]

        table_html = df.to_html(index=False, classes="table table-striped")
        return Div(
            "Valid YouTube Playlist URL",
            Br(),
            NotStr(table_html),  # Use NotStr to render raw HTML
            id="result",
            style="color: green;",
        )

    return Div(
        "Valid YouTube Playlist URL, but no videos were found or could not be retrieved.",
        id="result",
        style="color: orange;",
    )


serve()
