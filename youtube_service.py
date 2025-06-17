"""
YouTube Service for fetching and processing YouTube playlist data.
This module provides functionality to fetch and process YouTube playlist data using yt-dlp.
"""

import logging
from typing import Dict, List, Tuple

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


class YoutubePlaylistService:
    """Service for fetching and processing YouTube playlist data."""
    DISPLAY_HEADERS = [
        "Rank", "Title", "Views (Billions)", "Likes", "Dislikes", "Duration",
        "Engagement Rate"
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
            "force_generic_extractor": True
        }
        self.ydl_opts = ydl_opts or default_opts
        self.ydl = yt_dlp.YoutubeDL(self.ydl_opts)

    def get_dislike_count(self, video_id: str) -> int:
        """Fetch dislike count from Return YouTube Dislike API.
        
        Args:
            video_id (str): YouTube video ID
            
        Returns:
            int: Number of dislikes, or 0 if fetch fails
        """
        try:
            resp = requests.get(
                f"https://returnyoutubedislikeapi.com/votes?videoId={video_id}",
                timeout=5)
            if resp.status_code == 200:
                return resp.json().get("dislikes", 0)
        except requests.RequestException as e:
            logger.warning(f"Dislike fetch failed for video {video_id}: {e}")
        return 0

    @classmethod
    def get_display_headers(cls):
        return cls.DISPLAY_HEADERS

    def get_playlist_data(
            self,
            playlist_url: str,
            max_expanded: int = 20
    ) -> Tuple[pl.DataFrame, str, str, str, Dict]:
        """Fetch and process video information from a YouTube playlist URL.
        
        Args:
            playlist_url (str): The URL of the YouTube playlist to analyze.
            max_expanded (int): Maximum number of videos to process.
            
        Returns:
            Tuple[pl.DataFrame, str, str, str, Dict]: A tuple containing:
                - A Polars DataFrame with video information
                - The playlist name
                - The channel name
                - The channel thumbnail URL
                - A dictionary of summary statistics
                
        Raises:
            Exception: If there's an error fetching or processing the playlist data
        """
        try:
            playlist_info = self.ydl.extract_info(playlist_url, download=False)

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
                df = self._process_video_data(playlist_info["entries"],
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
            # Try to get the highest quality thumbnail
            for thumb in thumbnails:
                if thumb.get(
                        "width", 0
                ) >= 48:  # Look for thumbnail that's at least 48px wide
                    return thumb.get("url", "")
            # If no suitable thumbnail found, use the first one
            if thumbnails:
                return thumbnails[0].get("url", "")
        return ""

    def _expand_video_info(self, video_url: str) -> dict:
        """Fetch full metadata for a single video."""
        try:
            return self.ydl.extract_info(video_url, download=False)
        except Exception as e:
            logger.warning(f"Failed to expand video {video_url}: {e}")
            return {}

    def _process_video_data(self, videos: List[dict],
                            max_expanded: int) -> pl.DataFrame:
        """Process  and enrich video metadata into a DataFrame.
        
        Args:
            videos (list): List of video information dictionaries.
            max_expanded (int): Maximum number of videos to process.
            
        Returns:
            pl.DataFrame: Processed video data.
        """
        # Create initial DataFrame
        data = []

        for rank, video in enumerate(videos[:max_expanded], start=1):
            full_info = self._expand_video_info(video.get("url"))
            if not full_info:
                continue

            video_id = full_info.get("id", "")
            dislike_count = self.get_dislike_count(video_id)

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
                full_info.get("like_count", 0),
                "Dislike Count":
                dislike_count,
                "Uploader":
                full_info.get("uploader", "N/A"),
                "Creator":
                full_info.get("creator", "N/A"),
                "Channel ID":
                full_info.get("channel_id", "N/A"),
                "Duration":
                full_info.get("duration", 0),
                "Thumbnail":
                full_info.get("thumbnail", ""),
            })

        df = pl.DataFrame(data)

        # Apply formatting
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

        return df

    def _calculate_summary_stats(self, df: pl.DataFrame) -> Dict:
        """Calculate summary statistics for the playlist.
        
        Args:
            df (pl.DataFrame): The processed video data DataFrame.
            
        Returns:
            Dict: Dictionary containing summary statistics.
        """
        # Process numeric columns for summary calculations
        view_counts_numeric = process_numeric_column(df["View Count"])
        like_counts_numeric = process_numeric_column(df["Like Count"])
        dislike_counts_numeric = process_numeric_column(df["Dislike Count"])

        # Calculate summary statistics
        total_views = view_counts_numeric.sum()
        total_likes = like_counts_numeric.sum()
        avg_engagement = df["Engagement Rate (%)"].cast(pl.Float64).mean()

        return {
            "total_views": total_views,
            "total_likes": total_likes,
            "avg_engagement": avg_engagement
        }
