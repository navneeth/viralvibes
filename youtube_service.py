"""
YouTube Service for fetching and processing YouTube playlist data.
This module provides functionality to fetch and process YouTube playlist data using yt-dlp.
"""

import asyncio
import logging
from typing import Dict, List, Tuple

import httpx
import polars as pl
import yt_dlp

from utils import (
    calculate_engagement_rate,
    format_duration,
    format_number,
)

# Get logger instance
logger = logging.getLogger(__name__)
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

    def __init__(self, ydl_opts: dict = None):
        """Initialize the service with optional yt-dlp options.

        Args:
            ydl_opts (dict): Custom yt-dlp options. If None, uses default options.
        """
        default_opts = {
            "quiet": True,
            "nocheckcertificate": True,
            # "extract_flat": True,
            # allows lightweight fetch with URLs
            "extract_flat": "in_playlist",
            # 🚀 prevent yt-dlp from writing to ~/.cache
            "cachedir": False,
            # "force_generic_extractor": True
            "skip_download": True,
            # "ignoreerrors": True,
            # "dump_single_json": True,
            # "no_warnings": True,
        }
        self.ydl_opts = ydl_opts or default_opts
        self.ydl = yt_dlp.YoutubeDL(self.ydl_opts)

    async def get_playlist_preview(
        self, playlist_url: str
    ) -> Tuple[str, str, str, int]:
        """Extract lightweight playlist name, uploader, and thumbnail."""
        """Use yt-dlp with preview-safe settings to get basic playlist info."""
        try:
            playlist_info = await asyncio.to_thread(
                self.ydl.extract_info, playlist_url, download=False
            )

            # Fallbacks in case metadata is sparse
            playlist_title = playlist_info.get("title", "Untitled Playlist")
            channel_name = playlist_info.get("channel", "Unknown Channel")
            channel_thumbnail = self._extract_channel_thumbnail(playlist_info)
            playlist_length = playlist_info.get(
                "playlist_count", len(playlist_info.get("entries", []))
            )

            return playlist_title, channel_name, channel_thumbnail, playlist_length
        except Exception as e:
            logger.warning(f"Failed to fetch playlist preview: {e}")
            return "Preview unavailable", "", "", 0

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
            full_info = await asyncio.to_thread(
                self.ydl.extract_info, video_url, download=False
            )
            return full_info
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

        video_infos = await asyncio.gather(*video_info_tasks)
        dislike_data_results = await asyncio.gather(*dislike_tasks)

        dislike_data_map = {vid: data for vid, data in dislike_data_results}

        combined_data = []
        for rank, video_info in enumerate(video_infos, start=1):
            if not video_info:
                continue

            video_id = video_info.get("id", "")
            dislike_data = dislike_data_map.get(video_id, {})

            # Use API likes/dislikes if available, otherwise fallback to yt-dlp
            like_count = dislike_data.get("likes", video_info.get("like_count", 0))
            dislike_count = dislike_data.get("dislikes", 0)

            combined_data.append(
                {
                    "Rank": rank,
                    "id": video_id,
                    "Title": video_info.get("title", "N/A"),
                    "Views": video_info.get("view_count", 0),
                    "Likes": like_count,
                    "Dislikes": dislike_count,
                    "Comments": video_info.get("comment_count", 0),
                    "Duration": video_info.get("duration", 0),
                    "Uploader": video_info.get("uploader", "N/A"),
                    "Thumbnail": video_info.get("thumbnail", ""),
                    "Rating": dislike_data.get("rating"),
                }
            )
        return combined_data

    async def get_playlist_data(
        self, playlist_url: str, max_expanded: int = 20
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
        try:
            playlist_info = await asyncio.to_thread(
                self.ydl.extract_info, playlist_url, download=False
            )

            playlist_name = playlist_info.get("title", "Untitled Playlist")
            channel_name = playlist_info.get("uploader", "Unknown Channel")
            channel_thumbnail = self._extract_channel_thumbnail(playlist_info)

            if "entries" not in playlist_info or not playlist_info["entries"]:
                return (
                    pl.DataFrame(),
                    playlist_name,
                    channel_name,
                    channel_thumbnail,
                    {},
                )

            # Fetch all video data concurrently
            video_data = await self._fetch_all_video_data(
                playlist_info["entries"], max_expanded
            )

            # Create initial DataFrame from fetched data
            df = pl.DataFrame(video_data)

            # Calculate derived columns using Polars expressions
            df = df.with_columns(
                [
                    # Calculate controversy score
                    (
                        1
                        - (pl.col("Likes") - pl.col("Dislikes")).abs()
                        / (pl.col("Likes") + pl.col("Dislikes"))
                    )
                    .fill_nan(0.0)
                    .alias("Controversy"),
                    # Calculate engagement rate
                    (
                        (pl.col("Likes") + pl.col("Dislikes") + pl.col("Comments"))
                        / pl.col("Views")
                    )
                    .fill_nan(0.0)
                    .alias("Engagement Rate Raw"),
                ]
            )

            # Calculate summary stats using the raw DataFrame
            summary_stats = self._calculate_summary_stats(df)

            # Format columns for display just before returning
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

            return df, playlist_name, channel_name, channel_thumbnail, summary_stats

        except Exception as e:
            logger.error(f"Error fetching playlist data: {e}")
            raise

    def _extract_channel_thumbnail(self, playlist_info: dict) -> str:
        """Extract the channel thumbnail URL from playlist info.

        Args:
            playlist_info (dict): The playlist information dictionary.

        Returns:
            str: The channel thumbnail URL.
        """
        if "thumbnails" in playlist_info:
            thumbnails = playlist_info["thumbnails"]
            # Sort thumbnails by width (descending) to get the highest quality
            sorted_thumbs = sorted(
                thumbnails, key=lambda x: x.get("width", 0), reverse=True
            )
            return sorted_thumbs[0].get("url", "") if sorted_thumbs else ""
        return ""

    def _calculate_summary_stats(self, df: pl.DataFrame) -> Dict[str, float]:
        """Calculate summary statistics for the playlist."""
        if df.is_empty():
            return {
                "total_views": 0,
                "total_likes": 0,
                "avg_engagement": 0.0,
            }

        total_views = df["Views"].sum()
        total_likes = df["Likes"].sum()
        total_dislikes = df["Dislikes"].sum()
        total_comments = df["Comments"].sum()
        avg_engagement = df["Engagement Rate Raw"].mean()

        return {
            "total_views": total_views,
            "total_likes": total_likes,
            "total_dislikes": total_dislikes,
            "total_comments": total_comments,
            "avg_engagement": avg_engagement,
        }

    @classmethod
    def get_display_headers(cls) -> List[str]:
        """Get the display headers for the data table.

        Returns:
            List[str]: List of column headers for display
        """
        return cls.DISPLAY_HEADERS
