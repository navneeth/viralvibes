# services/youtube_service.py
"""
Refactored YouTube Service with backend abstraction and automatic fallback.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import polars as pl

from utils import format_duration, format_number

from .backends.base import (
    BackendError,
    BotChallengeError,
    PlaylistMetadata,
    ProcessingEstimate,
    ProcessingStats,
    QuotaExceededError,
    VideoData,
    YouTubeBackend,
)
from .backends.youtube_api_backend import YouTubeApiBackend
from .backends.yt_dlp_backend import YtDlpBackend
from .config import YouTubeConfig
from .data_utils import normalize_columns, transform_api_df

logger = logging.getLogger(__name__)


class YouTubeService:
    """
    High-level YouTube service with automatic backend fallback for fetching and processing YouTube playlist data

    Features:
    - Automatic fallback from YouTube API to yt-dlp on quota exhaustion
    - Standardized data format using Polars DataFrames
    - Progress tracking and error handling
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

    def __init__(
        self,
        primary_backend: str = "youtubeapi",
        enable_fallback: bool = True,
        config: Optional[YouTubeConfig] = None,
    ):
        """
        Initialize YouTube service with backend selection.

        Args:
            primary_backend: "youtubeapi" or "yt-dlp"
            enable_fallback: Enable automatic fallback to yt-dlp on quota errors
            config: Optional configuration object
        """
        self.config = config or YouTubeConfig()
        self.primary_backend_name = primary_backend
        self.enable_fallback = enable_fallback

        # Initialize primary backend
        self.primary = self._create_backend(primary_backend)

        # Initialize fallback if enabled
        self.fallback: Optional[YouTubeBackend] = None
        if enable_fallback and primary_backend == "youtubeapi":
            try:
                self.fallback = YtDlpBackend(
                    batch_size=self.config.batch_size,
                    max_retries=self.config.max_retries,
                    retry_delay=self.config.retry_delay,
                    min_video_delay=self.config.min_video_delay,
                    max_video_delay=self.config.max_video_delay,
                    min_batch_delay=self.config.min_batch_delay,
                    max_batch_delay=self.config.max_batch_delay,
                )
                logger.info("Fallback backend (yt-dlp) initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize fallback backend: {e}")

    def _create_backend(self, backend_name: str) -> YouTubeBackend:
        """Create and initialize a backend."""
        if backend_name == "youtubeapi":
            api_key = os.getenv("YOUTUBE_API_KEY")
            if not api_key:
                raise ValueError("YOUTUBE_API_KEY environment variable not set")
            return YouTubeApiBackend(api_key=api_key)
        elif backend_name == "yt-dlp":
            return YtDlpBackend(
                batch_size=self.config.batch_size,
                max_retries=self.config.max_retries,
                retry_delay=self.config.retry_delay,
                min_video_delay=self.config.min_video_delay,
                max_video_delay=self.config.max_video_delay,
                min_batch_delay=self.config.min_batch_delay,
                max_batch_delay=self.config.max_batch_delay,
            )
        else:
            raise ValueError(f"Unknown backend: {backend_name}")

    async def close(self):
        """Close all backend connections."""
        await self.primary.close()
        if self.fallback:
            await self.fallback.close()

    async def get_playlist_preview(
        self, playlist_url: str
    ) -> Tuple[str, str, str, int]:
        """
        Get lightweight playlist preview.

        Returns:
            Tuple of (title, channel_name, thumbnail, video_count)
        """
        try:
            metadata = await self.primary.fetch_playlist_preview(playlist_url)
            return (
                metadata.title,
                metadata.channel_name,
                metadata.channel_thumbnail,
                metadata.video_count,
            )
        except QuotaExceededError:
            if self.fallback:
                logger.warning("Quota exceeded, using fallback for preview")
                metadata = await self.fallback.fetch_playlist_preview(playlist_url)
                return (
                    metadata.title,
                    metadata.channel_name,
                    metadata.channel_thumbnail,
                    metadata.video_count,
                )
            raise
        except Exception as e:
            logger.error(f"Failed to fetch preview: {e}")
            return "Preview unavailable", "", "", 0

    async def get_playlist_data(
        self,
        playlist_url: str,
        max_expanded: Optional[int] = None,
        progress_callback: Optional[callable] = None,
    ) -> Tuple[pl.DataFrame, str, str, str, Dict[str, Any]]:
        """
        Fetch complete playlist data with automatic fallback.

        Args:
            playlist_url: YouTube playlist URL
            max_expanded: Max videos to process (None = all)
            progress_callback: Optional callback(current, total, metadata)

        Returns:
            Tuple of (DataFrame, title, channel, thumbnail, stats)
        """
        backend_used = self.primary_backend_name

        try:
            videos, metadata = await self.primary.fetch_playlist_videos(
                playlist_url, max_expanded, progress_callback
            )

        except QuotaExceededError:
            if not self.fallback:
                raise

            logger.warning("Quota exceeded, falling back to yt-dlp")
            backend_used = "yt-dlp (fallback)"
            videos, metadata = await self.fallback.fetch_playlist_videos(
                playlist_url, max_expanded, progress_callback
            )

        except BotChallengeError as e:
            logger.error(f"Bot challenge encountered: {e}")
            raise BackendError(
                "YouTube bot detection triggered. Try again later or use API."
            ) from e

        # Convert to DataFrame
        df = self._videos_to_dataframe(videos)

        # Enrich with calculated metrics
        df, stats = self._enrich_dataframe(df, metadata.video_count)

        # Add backend info to stats
        stats["backend_used"] = backend_used
        stats["total_videos"] = metadata.video_count

        return (
            df,
            metadata.title,
            metadata.channel_name,
            metadata.channel_thumbnail,
            stats,
        )

    def _videos_to_dataframe(self, videos: List[VideoData]) -> pl.DataFrame:
        """Convert list of VideoData to Polars DataFrame."""
        if not videos:
            return pl.DataFrame()

        data = {
            "Rank": [v.rank for v in videos],
            "id": [v.id for v in videos],
            "Title": [v.title for v in videos],
            "Views": [v.views for v in videos],
            "Likes": [v.likes for v in videos],
            "Dislikes": [v.dislikes for v in videos],
            "Comments": [v.comments for v in videos],
            "Duration": [v.duration for v in videos],
            "Uploader": [v.uploader for v in videos],
            "Thumbnail": [v.thumbnail for v in videos],
            "Rating": [v.rating for v in videos],
        }

        return pl.DataFrame(data)

    def _enrich_dataframe(
        self, df: pl.DataFrame, total_playlist_count: int
    ) -> Tuple[pl.DataFrame, Dict[str, Any]]:
        """Add calculated metrics and formatted columns."""
        if df.is_empty():
            return df, {
                "total_views": 0,
                "total_likes": 0,
                "total_dislikes": 0,
                "total_comments": 0,
                "avg_engagement": 0.0,
                "processed_video_count": 0,
            }

        # Calculate controversy and engagement
        df = df.with_columns(
            [
                # Controversy: how polarized the likes/dislikes are (0 = one-sided, 1 = 50/50)
                (
                    1
                    - (pl.col("Likes") - pl.col("Dislikes")).abs()
                    / (pl.col("Likes") + pl.col("Dislikes")).clip_min(1)
                )
                .fill_nan(0.0)
                .alias("Controversy"),
                # Engagement rate: (likes + dislikes + comments) / views
                (
                    (pl.col("Likes") + pl.col("Dislikes") + pl.col("Comments"))
                    / pl.col("Views").clip_min(1)
                )
                .fill_nan(0.0)
                .alias("Engagement Rate Raw"),
            ]
        )

        # Calculate summary statistics
        stats = {
            "total_views": int(df["Views"].sum()),
            "total_likes": int(df["Likes"].sum()),
            "total_dislikes": int(df["Dislikes"].sum()),
            "total_comments": int(df["Comments"].sum()),
            "avg_engagement": float(df["Engagement Rate Raw"].mean()),
            "processed_video_count": df.height,
        }

        # Add formatted display columns
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
        """Get the standard display headers."""
        return cls.DISPLAY_HEADERS


# Backwards compatibility: keep the old class name
class YoutubePlaylistService(YouTubeService):
    """Deprecated: Use YouTubeService instead."""

    def __init__(self, backend: str = "youtubeapi", ydl_opts: dict = None, config=None):
        logger.warning(
            "YoutubePlaylistService is deprecated. Use YouTubeService instead."
        )
        super().__init__(
            primary_backend=backend,
            enable_fallback=(backend == "youtubeapi"),
            config=config,
        )
