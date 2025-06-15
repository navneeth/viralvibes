"""
YouTube Service for fetching and processing YouTube playlist data.
This module provides functionality to fetch and process YouTube playlist data using yt-dlp.
"""

import logging
import polars as pl
import yt_dlp
from typing import Tuple, Optional
from utils import (calculate_engagement_rate, format_duration, format_number,
                   process_numeric_column)

# Get logger instance
logger = logging.getLogger(__name__)


class YoutubePlaylistService:
    """Service for fetching and processing YouTube playlist data."""

    def __init__(self, ydl_opts: Optional[dict] = None):
        """Initialize the service with optional yt-dlp options.
        
        Args:
            ydl_opts (Optional[dict]): Custom yt-dlp options. If None, uses default options.
        """
        default_opts = {
            "quiet": True,
            "extract_flat": True,
            "force_generic_extractor": True
        }
        self.ydl_opts = ydl_opts or default_opts

    def get_playlist_data(
            self, playlist_url: str) -> Tuple[pl.DataFrame, str, str, str]:
        """Fetch and process video information from a YouTube playlist URL.
        
        Args:
            playlist_url (str): The URL of the YouTube playlist to analyze.
            
        Returns:
            Tuple[pl.DataFrame, str, str, str]: A tuple containing:
                - A Polars DataFrame with video information
                - The playlist name
                - The channel name
                - The channel thumbnail URL
                
        Raises:
            Exception: If there's an error fetching or processing the playlist data
        """
        try:
            with yt_dlp.YoutubeDL(self.ydl_opts) as ydl:
                playlist_info = ydl.extract_info(playlist_url, download=False)

                # Debug logging
                logger.info("Playlist Info Keys: %s", playlist_info.keys())
                logger.info("Uploader Info: %s", playlist_info.get("uploader"))
                logger.info("Channel Info: %s", playlist_info.get("channel"))
                logger.info("Channel URL: %s",
                            playlist_info.get("channel_url"))

                playlist_name = playlist_info.get("title", "Untitled Playlist")
                channel_name = playlist_info.get("uploader", "Unknown Channel")

                # Extract channel thumbnail
                channel_thumbnail = self._extract_channel_thumbnail(
                    playlist_info)

                # Process video data
                if "entries" in playlist_info:
                    df = self._process_video_data(playlist_info["entries"])
                    return df, playlist_name, channel_name, channel_thumbnail

                return pl.DataFrame(
                ), playlist_name, channel_name, channel_thumbnail

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

    def _process_video_data(self, videos: list) -> pl.DataFrame:
        """Process video data into a DataFrame.
        
        Args:
            videos (list): List of video information dictionaries.
            
        Returns:
            pl.DataFrame: Processed video data.
        """
        # Create initial DataFrame
        data = [{
            "Rank": rank,
            "id": video.get("id", ""),
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
        } for rank, video in enumerate(videos, start=1)]

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
