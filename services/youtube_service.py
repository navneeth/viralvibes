# youtube_service.py
"""
YouTube Service for fetching and processing YouTube playlist data.
This module provides functionality to fetch and process YouTube playlist data using yt-dlp.
Enhanced with full playlist processing, resilience features, and time estimation.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import polars as pl

try:
    from googleapiclient.discovery import build
except ImportError:
    build = None

from services.config import YouTubeConfig
from services.youtube_backend_api import YouTubeBackendAPI
from services.youtube_backend_ytdlp import YouTubeBackendYTDLP
from services.youtube_errors import (
    YouTubeBotChallengeError,
)
from services.youtube_transforms import _enrich_dataframe, normalize_columns
from services.youtube_utils import (
    extract_all_tags,
    extract_categories,
    get_category_name,
)

# Get logger instance
logger = logging.getLogger(__name__)


def transform_api_df(api_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform YouTube API list of videos to chart-friendly column names
    compatible with the existing enrichment and chart functions.
    """
    transformed = []
    for idx, v in enumerate(api_list, start=1):
        transformed.append(
            {
                "Rank": v.get("Rank", idx),
                "id": v.get("id"),
                "Title": v.get("Title", "N/A"),
                "Views": v.get("Views", 0),
                "Likes": v.get("Likes", 0),
                "Dislikes": v.get("Dislikes", 0),
                "Comments": v.get("Comments", 0),
                "Duration": v.get("Duration", 0),
                "Uploader": v.get("Uploader", "N/A"),
                "Thumbnail": v.get("Thumbnail", ""),
                "Rating": v.get("Rating", 0.0),
                "Controversy": v.get("Controversy", 0.0),
                "Engagement Rate Raw": v.get("Engagement Rate Raw", 0.0),
            }
        )
    return transformed


# ============================================================================
# Main Service Class (Facade)
# ============================================================================


class YoutubePlaylistService:
    """
    Service facade for fetching and processing YouTube playlist data.
    Delegates to backend implementations.
    Backends are lazy-loaded on first use to avoid unnecessary dependency imports.
    """

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

    def __init__(self, backend: str = "youtubeapi", ydl_opts: dict = None, config=None):
        """
        Initialize the service with optional yt-dlp options.
        Args:
            backend: "yt-dlp" or "youtubeapi"
            ydl_opts: Custom yt-dlp options (only used for yt-dlp backend)
            config: YouTubeConfig instance
        """
        self.backend = backend
        self.cfg = config or YouTubeConfig()

        self.ydl_opts = ydl_opts
        self.handler = None  # Lazy-loaded on first use
        self.ydl = None  # Backward compat: set when yt-dlp backend is loaded
        self.youtube = None  # Backward compat: set when API backend is loaded

        # Lazy-load the backend handler on first use.
        if self.handler is not None:
            return  # Already initialized

        if self.backend == "yt-dlp":
            try:
                import yt_dlp
            except ImportError:
                raise ImportError(
                    "yt-dlp is required for the yt-dlp backend. "
                    "Install with: pip install yt-dlp"
                )

            self.handler = YouTubeBackendYTDLP(self.cfg, ydl_opts)
            self.ydl = self.handler.ydl  # Backward compatible access

        elif backend == "youtubeapi":
            try:
                from googleapiclient.discovery import build
            except ImportError:
                raise ImportError(
                    "google-api-python-client is required for the youtubeapi backend. "
                    "Install with: pip install google-api-python-client"
                )

            self.handler = YouTubeBackendAPI(self.cfg)
            self.youtube = self.handler.youtube  # Backward compatible access

        else:
            raise ValueError(
                f"backend must be 'yt-dlp' or 'youtubeapi', got '{self.backend}'"
            )

        # Persistent HTTP client for dislike API
        self._dislike_client = None
        self._failed_videos = []  # Track failed videos for retry

    # --------------------------
    # Public entrypoints
    # --------------------------

    async def get_playlist_data(
        self,
        playlist_url: str,
        max_expanded: Optional[int] = None,
        progress_callback: Optional[callable] = None,
    ) -> Tuple[pl.DataFrame, str, str, str, Dict[str, float]]:
        """
        Fetch playlist data with optional limit on expanded videos.

        Args:
            playlist_url: YouTube playlist URL
            max_expanded: Maximum videos to fully process. None = process all videos
            progress_callback: Optional callback for progress updates
        Returns:
            Tuple of (DataFrame, playlist_name, channel_name, channel_thumb, stats)
        """
        return await self.handler.get_playlist_data(
            playlist_url, max_expanded, progress_callback
        )

    async def get_playlist_preview(
        self, playlist_url: str
    ) -> Tuple[str, str, str, int, str, str, str]:
        """
        Extract lightweight playlist preview.

        Args:
            playlist_url: YouTube playlist URL

        Returns:
            Tuple of (title, channel, thumbnail, length, description, privacy_status, published_date)
        """
        return await self.handler.get_playlist_preview(playlist_url)

    async def close(self):
        """Close any persistent connections."""
        await self.handler.close()

    @classmethod
    def get_display_headers(cls) -> List[str]:
        """Get the standard display headers for playlist data."""
        return cls.DISPLAY_HEADERS
