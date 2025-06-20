"""
YouTube Service for fetching and processing YouTube playlist data.
This module provides functionality to fetch and process YouTube playlist data using yt-dlp.
"""
import asyncio
import logging
from typing import Dict, List, Tuple

import httpx
import polars as pl
import requests
import yt_dlp

from utils import (
    calculate_engagement_rate,
    format_duration,
    format_number,
    process_numeric_column,
)

# Get logger instance
logger = logging.getLogger(__name__)
DISLIKE_API_URL = "https://returnyoutubedislikeapi.com/votes?videoId={}"


class YoutubePlaylistService:
    """Service for fetching and processing YouTube playlist data."""
    DISPLAY_HEADERS = [
        "Rank", "Title", "Views (Billions)", "Likes", "Dislikes", "Duration",
        "Engagement Rate", "Controversy"
    ]

    def __init__(self, ydl_opts: dict = None):
        """Initialize the service with optional yt-dlp options.
        
        Args:
            ydl_opts (dict): Custom yt-dlp options. If None, uses default options.
        """
        default_opts = {
            "quiet": True,
            # "extract_flat": True,
            "extract_flat":
            "in_playlist",  # allows lightweight fetch with URLs
            #"force_generic_extractor": True
        }
        self.ydl_opts = ydl_opts or default_opts
        self.ydl = yt_dlp.YoutubeDL(self.ydl_opts)

    def get_playlist_preview(self, playlist_url: str) -> Tuple[str, str, str]:
        """Extract lightweight playlist name, uploader, and thumbnail."""
        """Use yt-dlp with preview-safe settings to get basic playlist info."""
        preview_opts = {
            "quiet": True,
            "extract_flat": True,  # lightweight mode
            "force_generic_extractor":
            True,  # <- force fallback that actually works
            "nocheckcertificate": True,
            "skip_download": True,
        }
        try:
            with yt_dlp.YoutubeDL(preview_opts) as ydl:
                info = ydl.extract_info(playlist_url, download=False)

            # Fallbacks in case metadata is sparse
            title = info.get("title") or info.get(
                "playlist_title") or "Untitled Playlist"
            uploader = info.get("uploader") or info.get(
                "channel") or "Unknown Channel"
            #thumbnail = self._extract_channel_thumbnail(info)

            return title, uploader, ""
        except Exception as e:
            logger.warning(f"Failed to fetch playlist preview: {e}")
            logger.error(
                f"[yt-dlp] _get_preview_info failed: {type(e).__name__}: {e}")

            return "Preview unavailable", "", ""

    async def get_dislike_count(self, video_id: str) -> int:
        """Fetch dislike count from Return YouTube Dislike API.
        
        Args:
            video_id (str): YouTube video ID
            
        Returns:
            int: Number of dislikes, or 0 if fetch fails
        """
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(DISLIKE_API_URL.format(video_id))
                if resp.status_code == 200:
                    return resp.json().get("dislikes", 0)
        except Exception as e:
            logger.warning(f"Dislike fetch failed for video {video_id}: {e}")
        return 0

    @classmethod
    def get_display_headers(cls):
        return cls.DISPLAY_HEADERS

    async def get_playlist_data(
        self,
        playlist_url: str,
        max_expanded: int = 20
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
            #playlist_info = self.ydl.extract_info(playlist_url, download=False)
            playlist_info = await asyncio.to_thread(self.ydl.extract_info,
                                                    playlist_url,
                                                    download=False)

            # Debug logging
            logger.info("Playlist Info Keys: %s", playlist_info.keys())
            logger.info("Uploader Info: %s", playlist_info.get("uploader"))
            logger.info("Channel Info: %s", playlist_info.get("channel"))
            logger.info("Channel URL: %s", playlist_info.get("channel_url"))

            playlist_name = playlist_info.get("title", "Untitled Playlist")
            channel_name = playlist_info.get("uploader", "Unknown Channel")

            # Extract channel thumbnail
            channel_thumbnail = self._extract_channel_thumbnail(playlist_info)

            # Process video data
            if "entries" in playlist_info:
                df = await self._process_video_data(playlist_info["entries"],
                                                    max_expanded)
                summary_stats = self._calculate_summary_stats(df)
                return df, playlist_name, channel_name, channel_thumbnail, summary_stats

            return pl.DataFrame(
            ), playlist_name, channel_name, channel_thumbnail, {}

        except Exception as e:
            logger.error(f"Error fetching playlist data: {str(e)}")
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
            sorted_thumbs = sorted(thumbnails,
                                   key=lambda x: x.get("width", 0),
                                   reverse=True)
            return sorted_thumbs[0].get("url", "") if sorted_thumbs else ""
        return ""

    def _get_preview_info(self, playlist_url: str) -> dict:
        """Use yt-dlp with preview-safe settings to get basic playlist info."""
        preview_opts = {
            "quiet": True,
            "extract_flat": True,  # lightweight mode
            #"force_generic_extractor": False,
            #"flat_playlist": True,  # get full playlist metadata
            "dump_single_json": True,
            #"skip_playlist_after_errors": -1,  # avoid failures
        }
        with yt_dlp.YoutubeDL(preview_opts) as ydl:
            return ydl.extract_info(playlist_url, download=False)

    def _expand_video_info(self, video_url: str) -> dict:
        """Fetch full metadata for a single video."""
        try:
            return self.ydl.extract_info(video_url, download=False)
        except Exception as e:
            logger.warning(f"Failed to expand video {video_url}: {e}")
            return {}

    async def _fetch_dislike_data_async(self, client: httpx.AsyncClient,
                                        video_id: str) -> tuple[str, dict]:
        """Fetch dislike data for a video asynchronously.
        
        Args:
            client (httpx.AsyncClient): HTTP client for making requests
            video_id (str): YouTube video ID
            
        Returns:
            tuple[str, dict]: Tuple of (video_id, dislike_data)
                If fetch fails, returns {"dislikes": 0}
        """
        try:
            response = await client.get(DISLIKE_API_URL.format(video_id))
            if response.status_code == 200:
                return video_id, response.json()
            logger.warning(
                f"Failed to fetch dislike data for {video_id}: HTTP {response.status_code}"
            )
        except Exception as e:
            logger.warning(f"Failed to fetch dislike data for {video_id}: {e}")
        return video_id, {"dislikes": 0}

    async def _gather_dislike_data(self, video_ids: list[str]) -> dict:
        async with httpx.AsyncClient(timeout=5.0) as client:
            tasks = [
                self._fetch_dislike_data_async(client, vid)
                for vid in video_ids
            ]
            results = await asyncio.gather(*tasks)
        return {vid: data for vid, data in results if data}

    def _calculate_controversy_score(self, likes: int | None,
                                     dislikes: int | None) -> float:
        """Calculate controversiality score for a video.
        
        Args:
            likes (int | None): Number of likes
            dislikes (int | None): Number of dislikes
            
        Returns:
            float: Controversy score between 0 and 1, where:
                  - 0 means no controversy (all likes or all dislikes)
                  - 1 means maximum controversy (equal likes and dislikes)
        """
        # Handle None values
        likes = likes or 0
        dislikes = dislikes or 0

        total = likes + dislikes
        if total == 0:
            return 0.0
        return abs(likes - dislikes) / total

    async def _process_video_data(self, videos: List[dict],
                                  max_expanded: int) -> pl.DataFrame:
        """Process and enrich video metadata into a DataFrame.
        
        Args:
            videos (list): List of video information dictionaries.
            max_expanded (int): Maximum number of videos to process.
            
        Returns:
            pl.DataFrame: Processed video data.
        """
        # Create initial DataFrame
        data = []

        for rank, video in enumerate(videos[:max_expanded], start=1):
            # Get full video info using yt-dlp
            full_info = await asyncio.to_thread(self._expand_video_info,
                                                video.get("url"))
            if not full_info:
                continue

            # Schema validation: check for required fields
            required_keys = [
                "id", "title", "view_count", "like_count", "duration"
            ]
            if not all(k in full_info for k in required_keys):
                logger.warning(
                    f"Missing fields in video: {video.get('url')}, got keys: {list(full_info.keys())}"
                )
                continue

            video_id = full_info.get("id", "")
            dislike_count = await self.get_dislike_count(video_id)
            like_count = full_info.get("like_count", 0)

            # Calculate controversy score
            controversy_score = self._calculate_controversy_score(
                like_count, dislike_count)

            data.append({
                "Rank":
                rank,
                "id":
                video_id,
                "Title":
                full_info.get("title", "N/A"),
                "Views (Billions)":
                (full_info.get("view_count") or 0) / 1_000_000_000,
                "View Count":
                full_info.get("view_count", 0),
                "Like Count":
                like_count,
                "Dislike Count":
                dislike_count,
                "Controversy":
                controversy_score,
                "Uploader":
                full_info.get("uploader", "N/A"),
                "Creator":
                full_info.get("creator", "N/A"),
                "Channel ID":
                full_info.get("channel_id", "N/A"),
                "Duration Raw":
                full_info.get("duration", 0),
                "Thumbnail":
                full_info.get("thumbnail", ""),
            })

        df = pl.DataFrame(data)
        if df.is_empty():
            # Ensure all expected columns exist, even if empty
            expected_cols = [
                ("Rank", pl.Int64),
                ("id", pl.Utf8),
                ("Title", pl.Utf8),
                ("Views (Billions)", pl.Float64),
                ("View Count", pl.Int64),
                ("Like Count", pl.Int64),
                ("Dislike Count", pl.Int64),
                ("Controversy", pl.Float64),
                ("Uploader", pl.Utf8),
                ("Creator", pl.Utf8),
                ("Channel ID", pl.Utf8),
                ("Duration Raw", pl.Int64),
                ("Thumbnail", pl.Utf8),
                ("View Count Raw", pl.Int64),
                ("Like Count Raw", pl.Int64),
                ("Dislike Count Raw", pl.Int64),
                ("Controversy Raw", pl.Float64),
                ("Duration", pl.Utf8),
                ("Engagement Rate (%)", pl.Utf8),
            ]
            df = pl.DataFrame({
                col: pl.Series([], dtype=dt)
                for col, dt in expected_cols
            })
            return df

        # Keep original numeric columns for charts and calculations
        # Create formatted display columns for the table
        df = df.with_columns([
            pl.col("View Count").alias(
                "View Count Raw"),  # Keep original for charts
            pl.col("Like Count").alias(
                "Like Count Raw"),  # Keep original for charts
            pl.col("Dislike Count").alias(
                "Dislike Count Raw"),  # Keep original for charts
            pl.col("Controversy").alias(
                "Controversy Raw"),  # Keep original for charts
            pl.col("Duration Raw").map_elements(
                format_duration, return_dtype=pl.String).alias("Duration"),
            pl.col("View Count").map_elements(
                format_number, return_dtype=pl.String).alias("View Count"),
            pl.col("Like Count").map_elements(
                format_number, return_dtype=pl.String).alias("Like Count"),
            pl.col("Dislike Count").map_elements(
                format_number, return_dtype=pl.String).alias("Dislike Count"),
            pl.col("Controversy").map_elements(lambda x: f"{x:.2%}",
                                               return_dtype=pl.String)
        ])

        # Calculate engagement rate using raw numeric values
        df = df.with_columns([
            pl.Series(name="Engagement Rate (%)",
                      values=[
                          f"{calculate_engagement_rate(vc, lc, dc):.2f}"
                          for vc, lc, dc in
                          zip(df["View Count Raw"], df["Like Count Raw"],
                              df["Dislike Count Raw"])
                      ])
        ])

        return df

    def _calculate_summary_stats(self, df: pl.DataFrame) -> Dict:
        """Calculate summary statistics for the playlist.
        
        Args:
            df (pl.DataFrame): The processed video data DataFrame.
            
        Returns:
            Dict: Dictionary containing summary statistics.
        """
        # Use raw numeric columns for summary calculations
        total_views = df["View Count Raw"].sum()
        total_likes = df["Like Count Raw"].sum()
        avg_engagement = df["Engagement Rate (%)"].cast(pl.Float64).mean()

        return {
            "total_views": total_views,
            "total_likes": total_likes,
            "avg_engagement": avg_engagement
        }
