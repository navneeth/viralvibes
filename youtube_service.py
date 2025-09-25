# youtube_service.py

"""
YouTube Service for fetching and processing YouTube playlist data.
This module provides functionality to fetch and process YouTube playlist data using yt-dlp.
"""

import asyncio
import logging
import os
import re
from typing import Any, Dict, List, Tuple

import httpx
import polars as pl

# Optional imports
try:
    import yt_dlp
except ImportError:
    yt_dlp = None

try:
    from googleapiclient.discovery import build
except ImportError:
    build = None

import isodate

from utils import calculate_engagement_rate, format_duration, format_number

# Get logger instance
logger = logging.getLogger(__name__)

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")  # for YouTube Data API
DISLIKE_API_URL = "https://returnyoutubedislikeapi.com/votes?videoId={}"


class YoutubePlaylistService:
    """Service for fetching and processing YouTube playlist data."""

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
    YOUTUBE_API_MAX_RESULTS = 50

    def __init__(self, backend: str = "yt-dlp", ydl_opts: dict = None):
        """
        Initialize the service with optional yt-dlp options.
        Args:
            ydl_opts (dict): Custom yt-dlp options. If None, uses default options.
            backend: "yt-dlp" or "youtubeapi"
        """
        self.backend = backend

        if backend == "yt-dlp":
            if yt_dlp is None:
                raise ImportError("yt-dlp is not installed.")
            default_opts = {
                "quiet": True,
                "nocheckcertificate": True,
                # allows lightweight fetch with URLs
                "extract_flat": "in_playlist",
                # 🚀 prevent yt-dlp from writing to ~/.cache
                "cachedir": False,
                "skip_download": True,
            }
            self.ydl_opts = ydl_opts or default_opts
            self.ydl = yt_dlp.YoutubeDL(self.ydl_opts)

        elif backend == "youtubeapi":
            if build is None:
                raise ImportError("google-api-python-client is not installed.")
            if not YOUTUBE_API_KEY:
                raise ValueError("YOUTUBE_API_KEY environment variable not set.")
            self.youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

        else:
            raise ValueError("backend must be 'yt-dlp' or 'youtubeapi'")

    # --------------------------
    # Public entrypoints
    # --------------------------

    async def get_playlist_data(
        self, playlist_url: str, max_expanded: int = 20
    ) -> Tuple[pl.DataFrame, str, str, str, Dict[str, float]]:
        if self.backend == "yt-dlp":
            return await self._get_playlist_data_ytdlp(playlist_url, max_expanded)
        else:
            return await self._get_playlist_data_youtubeapi(playlist_url, max_expanded)

    async def get_playlist_preview(
        self, playlist_url: str
    ) -> Tuple[str, str, str, int]:
        """
        "Extract lightweight playlist name, uploader, and thumbnail."
        Lightweight preview: (playlist_title, channel_name, channel_thumbnail, playlist_length)
        """
        if self.backend == "yt-dlp":
            try:
                info = await asyncio.to_thread(
                    self.ydl.extract_info, playlist_url, download=False
                )
                # Fallbacks in case metadata is sparse
                title = info.get("title", "Untitled Playlist")
                channel = info.get("uploader", "Unknown Channel")
                thumb = self._extract_channel_thumbnail(info)
                length = info.get("playlist_count", len(info.get("entries", [])))
                return title, channel, thumb, length
            except Exception as e:
                logger.warning(f"Failed to fetch yt-dlp preview: {e}")
                return "Preview unavailable", "", "", 0

        elif self.backend == "youtubeapi":
            try:
                m = re.search(r"list=([a-zA-Z0-9_-]+)", playlist_url)
                if not m:
                    raise ValueError("Invalid playlist URL")
                playlist_id = m.group(1)
                resp = (
                    self.youtube.playlists()
                    .list(part="snippet,contentDetails", id=playlist_id, maxResults=1)
                    .execute()
                )
                if not resp["items"]:
                    return "Preview unavailable", "", "", 0
                sn = resp["items"][0]["snippet"]
                cd = resp["items"][0].get("contentDetails", {})
                title = sn.get("title", "Untitled Playlist")
                channel = sn.get("channelTitle", "Unknown Channel")
                thumb = sn.get("thumbnails", {}).get("high", {}).get("url", "")
                length = cd.get("itemCount", 0)
                return title, channel, thumb, length
            except Exception as e:
                logger.warning(f"Failed to fetch API preview: {e}")
                return "Preview unavailable", "", "", 0

    # --------------------------
    # yt-dlp implementation
    # --------------------------

    async def _fetch_dislike_data_async(
        self, client: httpx.AsyncClient, video_id: str
    ) -> Tuple[str, Dict[str, Any]]:
        """Fetch dislike data for a video asynchronously.

        Args:
            client (httpx.AsyncClient): HTTP client for making requests.
            video_id (str): YouTube video ID.

        Returns:
            Tuple[str, Dict[str, Any]]: Tuple of (video_id, dislike_data).
        """
        try:
            response = await client.get(DISLIKE_API_URL.format(video_id))
            if response.status_code == 200:
                data = response.json()
                return video_id, {
                    "dislikes": data.get("dislikes", 0),
                    "likes": data.get("likes", 0),
                    "rating": data.get("rating"),
                    "viewCount_api": data.get("viewCount"),
                    "deleted": data.get("deleted", False),
                }
            logger.warning(
                f"Failed to fetch dislike data for {video_id}: HTTP {response.status_code}"
            )
        except Exception as e:
            logger.warning(f"Dislike fetch failed for video {video_id}: {e}")
        return video_id, {
            "dislikes": 0,
            "likes": 0,
            "rating": None,
            "viewCount_api": None,
            "deleted": False,
        }

    async def _fetch_video_info_async(self, video_url: str) -> Dict[str, Any]:
        """Fetch full metadata for a single video asynchronously using asyncio.to_thread."""
        try:
            return await asyncio.to_thread(
                self.ydl.extract_info, video_url, download=False
            )
        except Exception as e:
            logger.warning(f"Failed to expand video {video_url}: {e}")
            return {}

    async def _fetch_all_video_data(
        self, videos: List[Dict[str, Any]], max_expanded: int
    ) -> List[Dict[str, Any]]:
        """Fetch full metadata and dislike data for a list of videos concurrently."""
        video_urls = [v.get("url") for v in videos[:max_expanded]]
        # Concurrently fetch full video info and dislike data
        video_info_tasks = [self._fetch_video_info_async(url) for url in video_urls]
        async with httpx.AsyncClient(timeout=5.0) as client:
            dislike_tasks = [
                self._fetch_dislike_data_async(client, v.get("id", ""))
                for v in videos[:max_expanded]
                if v.get("id")
            ]
        video_infos, dislike_results = await asyncio.gather(
            asyncio.gather(*video_info_tasks), asyncio.gather(*dislike_tasks)
        )
        dislike_map = {vid: data for vid, data in dislike_results}

        combined = []
        for rank, vi in enumerate(video_infos, start=1):
            if not vi:
                continue
            vid = vi.get("id", "")
            dd = dislike_map.get(vid, {})
            combined.append(
                {
                    "Rank": rank,
                    "id": vid,
                    "Title": vi.get("title", "N/A"),
                    "Views": vi.get("view_count", 0),
                    "Likes": dd.get("likes", vi.get("like_count", 0)),
                    "Dislikes": dd.get("dislikes", 0),
                    "Comments": vi.get("comment_count", 0),
                    "Duration": vi.get("duration", 0),
                    "Uploader": vi.get("uploader", "N/A"),
                    "Thumbnail": vi.get("thumbnail", ""),
                    "Rating": dd.get("rating"),
                }
            )
        return combined

    async def _get_playlist_data_ytdlp(
        self, playlist_url: str, max_expanded: int
    ) -> Tuple[pl.DataFrame, str, str, str, Dict[str, float]]:
        """Fetch and process video information from a YouTube playlist URL.

        Args:
            playlist_url (str): The URL of the YouTube playlist to analyze.
            max_expanded (int): Maximum number of videos to process.

        Returns:
            Tuple[pl.DataFrame, str, str, str, Dict[str, float]]: A tuple containing:
                - A Polars DataFrame with video information
                - The playlist name
                - The channel name
                - The channel thumbnail URL
                - A dictionary of summary statistics

        Raises:
            Exception: If there's an error fetching or processing the playlist data
        """
        logger.info(f"Starting analysis for playlist: {playlist_url}")

        playlist_info = await asyncio.to_thread(
            self.ydl.extract_info, playlist_url, download=False
        )
        playlist_name = playlist_info.get("title", "Untitled Playlist")
        channel_name = playlist_info.get("uploader", "Unknown Channel")
        channel_thumb = self._extract_channel_thumbnail(playlist_info)
        playlist_count = playlist_info.get(
            "playlist_count", len(playlist_info.get("entries", []))
        )
        if "entries" not in playlist_info or not playlist_info["entries"]:
            return pl.DataFrame(), playlist_name, channel_name, channel_thumb, {}
        video_data = await self._fetch_all_video_data(
            playlist_info["entries"], max_expanded
        )
        if not video_data:
            logger.warning(
                "No valid videos found in playlist, returning empty DataFrame."
            )
            return pl.DataFrame(), playlist_name, channel_name, channel_thumb, {}
        # Create initial DataFrame from fetched data
        df = pl.DataFrame(video_data)
        df, stats = self._enrich_dataframe(df, playlist_count)
        return df, playlist_name, channel_name, channel_thumb, stats

    # --------------------------
    # YouTube Data API implementation
    # --------------------------

    async def _get_playlist_data_youtubeapi(
        self, playlist_url: str, max_expanded: int
    ) -> Tuple[pl.DataFrame, str, str, str, Dict[str, float]]:
        m = re.search(r"list=([a-zA-Z0-9_-]+)", playlist_url)
        if not m:
            raise ValueError("Invalid playlist URL")
        playlist_id = m.group(1)
        resp = (
            self.youtube.playlists()
            .list(part="snippet", id=playlist_id, maxResults=1)
            .execute()
        )
        if not resp["items"]:
            raise ValueError("Playlist not found")
        sn = resp["items"][0]["snippet"]
        playlist_name = sn.get("title", "Untitled Playlist")
        channel_name = sn.get("channelTitle", "Unknown Channel")
        channel_thumb = sn.get("thumbnails", {}).get("high", {}).get("url", "")

        video_ids, nextPageToken = [], None
        while len(video_ids) < max_expanded:
            items_resp = (
                self.youtube.playlistItems()
                .list(
                    part="contentDetails",
                    playlistId=playlist_id,
                    maxResults=min(
                        self.YOUTUBE_API_MAX_RESULTS, max_expanded - len(video_ids)
                    ),
                    pageToken=nextPageToken,
                )
                .execute()
            )
            for it in items_resp["items"]:
                video_ids.append(it["contentDetails"]["videoId"])
            nextPageToken = items_resp.get("nextPageToken")
            if not nextPageToken:
                break

        videos = []
        for i in range(0, len(video_ids), self.YOUTUBE_API_MAX_RESULTS):
            batch = video_ids[i : i + self.YOUTUBE_API_MAX_RESULTS]
            resp = (
                self.youtube.videos()
                .list(part="snippet,statistics,contentDetails", id=",".join(batch))
                .execute()
            )
            for idx, it in enumerate(resp["items"], start=i + 1):
                stats = it.get("statistics", {})
                sn = it.get("snippet", {})
                cd = it.get("contentDetails", {})
                videos.append(
                    {
                        "Rank": idx,
                        "id": it["id"],
                        "Title": sn.get("title", "N/A"),
                        "Views": int(stats.get("viewCount", 0)),
                        "Likes": int(stats.get("likeCount", 0)),
                        "Dislikes": 0,  # API does not expose
                        "Comments": int(stats.get("commentCount", 0)),
                        "Duration": self._parse_iso8601_duration(
                            cd.get("duration", "PT0S")
                        ),
                        "Uploader": sn.get("channelTitle", "N/A"),
                        "Thumbnail": sn.get("thumbnails", {})
                        .get("high", {})
                        .get("url", ""),
                        "Rating": None,
                    }
                )

        df = pl.DataFrame(videos)
        df, stats = self._enrich_dataframe(df, len(video_ids))
        return df, playlist_name, channel_name, channel_thumb, stats

    # --------------------------
    # Common helpers
    # --------------------------

    def _extract_channel_thumbnail(self, playlist_info: dict) -> str:
        """Extract the channel thumbnail URL from playlist info.

        Args:
            playlist_info (dict): The playlist information dictionary.

        Returns:
            str: The channel thumbnail URL.
        """
        if "thumbnails" in playlist_info:
            thumbs = sorted(
                playlist_info["thumbnails"],
                key=lambda x: x.get("width", 0),
                reverse=True,
            )
            return thumbs[0].get("url", "") if thumbs else ""
        return ""

    def _parse_iso8601_duration(self, duration: str) -> int:
        try:
            return int(isodate.parse_duration(duration).total_seconds())
        except (isodate.ISO8601Error, TypeError, ValueError) as e:
            logger.error(f"Failed to parse ISO8601 duration '{duration}': {e}")
            return 0

    def _enrich_dataframe(
        self, df: pl.DataFrame, actual_playlist_count: int = None
    ) -> Tuple[pl.DataFrame, Dict[str, Any]]:
        """Calculate summary statistics for the playlist."""
        if df.is_empty():
            return df, {
                "total_views": 0,
                "total_likes": 0,
                "total_dislikes": 0,
                "total_comments": 0,
                "avg_engagement": 0.0,
                "actual_playlist_count": actual_playlist_count or 0,
                "processed_video_count": 0,
            }
        df = df.with_columns(
            [
                (
                    1
                    - (pl.col("Likes") - pl.col("Dislikes")).abs()
                    / (pl.col("Likes") + pl.col("Dislikes"))
                )
                .fill_nan(0.0)
                .alias("Controversy"),
                (
                    (pl.col("Likes") + pl.col("Dislikes") + pl.col("Comments"))
                    / pl.col("Views")
                )
                .fill_nan(0.0)
                .alias("Engagement Rate Raw"),
            ]
        )
        stats = {
            "total_views": df["Views"].sum(),
            "total_likes": df["Likes"].sum(),
            "total_dislikes": df["Dislikes"].sum(),
            "total_comments": df["Comments"].sum(),
            "avg_engagement": df["Engagement Rate Raw"].mean(),
            "actual_playlist_count": actual_playlist_count or df.height,
            "processed_video_count": df.height,
        }
        df = df.with_columns(
            [
                pl.col("Views")
                .map_elements(format_number, return_dtype=pl.String)
                .alias("Views Formatted"),
                pl.col("Likes")
                .map_elements(format_number, return_dtype=pl.String)
                .alias("Likes Formatted"),
                pl.col("Dislikes")
                .map_elements(format_number, return_dtype=pl.String)
                .alias("Dislikes Formatted"),
                pl.col("Comments")
                .map_elements(format_number, return_dtype=pl.String)
                .alias("Comments Formatted"),
                pl.col("Duration")
                .map_elements(format_duration, return_dtype=pl.String)
                .alias("Duration Formatted"),
                pl.col("Controversy")
                .map_elements(lambda x: f"{x:.2%}", return_dtype=pl.String)
                .alias("Controversy Formatted"),
                pl.col("Engagement Rate Raw")
                .map_elements(lambda x: f"{x:.2%}", return_dtype=pl.String)
                .alias("Engagement Rate Formatted"),
            ]
        )
        return df, stats

    @classmethod
    def get_display_headers(cls) -> List[str]:
        return cls.DISPLAY_HEADERS
