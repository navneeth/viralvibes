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
    hdrs=hdrs, title="ViralVibes - YouTube Playlist Analyzer", static_dir="static"
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


# @rt("/")
@rt
def index():
    prefill_url = (
        "https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC"
    )
    return Titled(
        "ViralVibes",
        Container(
            DivCentered(
                H1("Welcome to ViralVibes !"),
                Subtitle("Discover what makes YouTube videos go viral!"),
                id="welcome-section",
            ),
            Card(
                Img(
                    src="/static/celebration.webp",
                    style="width: 100%; max-width: 320px; margin: 0 auto 1.5rem auto; display: block;",
                    alt="Celebration",
                ),
                H1("ViralVibes", style="text-align:center; margin-bottom:0.5rem;"),
                H4(
                    "Discover what makes YouTube videos go viral! Paste a playlist and get instant stats.",
                    style="text-align:center; color:#555; margin-bottom:1.5rem;",
                ),
                Form(
                    Input(
                        type="text",
                        name="playlist_url",
                        placeholder="Paste YouTube Playlist URL",
                        value=prefill_url,
                        style="width:100%; margin-bottom:1rem;",
                    ),
                    Button("Analyze Now", type="submit", style="width:100%;"),
                    Div(
                        id="loading",
                        style="display:none; color: #393e6e; font-weight: bold; margin-top:1rem;",
                        children=["Loading..."],
                    ),
                    hx_post="/validate",
                    hx_target="#result",
                    hx_indicator="#loading",
                ),
                Div(id="result", style="margin-top:2rem;"),
                style="max-width: 420px; margin: 3rem auto; padding: 2rem 2rem 1.5rem 2rem; box-shadow: 0 4px 24px #0001; border-radius: 1.2rem; background: #fff;",
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
