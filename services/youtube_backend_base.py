from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional, Tuple

from services.youtube_utils import ProcessingEstimate

from .config import YouTubeConfig

DISLIKE_API_URL = "https://returnyoutubedislikeapi.com/votes?videoId={}"

# ============================================================================
# Abstract Base Backend
# ============================================================================


class YouTubeBackendBase(ABC):
    """Abstract base class for YouTube data backends."""

    def __init__(self, cfg=None):
        self.cfg = cfg or YouTubeConfig()

    @abstractmethod
    async def get_playlist_data(
        self,
        playlist_url: str,
        max_expanded: Optional[int],
        progress_callback: callable,
    ):
        """Fetch playlist data. Returns (df, name, channel, thumb, stats)."""
        pass

    @abstractmethod
    async def get_playlist_preview(self, playlist_url: str):
        """Get preview. Returns (title, channel, thumb, length, desc, privacy, published)."""
        pass

    async def close(self):
        """Optional cleanup. Override if needed."""
        pass

    def estimate_processing_time(
        self, playlist_count: int, expand_all: bool = True
    ) -> ProcessingEstimate:
        """
        Estimate the time required to process a playlist.

        Args:
            playlist_count: Total number of videos in playlist
            expand_all: Whether to expand all videos (True) or use default limit

        Returns:
            ProcessingEstimate object with timing information
        """
        videos_to_expand = playlist_count if expand_all else min(20, playlist_count)

        # Time estimates (in seconds)
        flat_fetch_time = 2.0  # Initial playlist fetch
        avg_video_fetch_time = (self.cfg.min_video_delay + self.cfg.max_video_delay) / 2
        avg_dislike_fetch_time = 0.2
        avg_batch_delay = (self.cfg.min_batch_delay + self.cfg.max_batch_delay) / 2

        # Calculate batch processing time
        batch_count = (
            videos_to_expand + self.cfg.batch_size - 1
        ) // self.cfg.batch_size

        # Time per batch: max(video_fetch, dislike_fetch) since they're concurrent
        time_per_batch = (
            max(
                avg_video_fetch_time * self.cfg.batch_size,
                avg_dislike_fetch_time * self.cfg.batch_size,
            )
            + avg_batch_delay
        )

        total_seconds = flat_fetch_time + (time_per_batch * batch_count)

        # Add buffer for retries and overhead (20%)
        total_seconds *= 1.2

        return ProcessingEstimate(
            total_videos=playlist_count,
            videos_to_expand=videos_to_expand,
            estimated_seconds=total_seconds,
            estimated_minutes=total_seconds / 60,
            batch_count=batch_count,
        )
