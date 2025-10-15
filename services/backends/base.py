# services/backends/base.py
"""Abstract base class for YouTube data backends."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class PlaylistMetadata:
    """Standardized playlist metadata across backends."""

    title: str
    channel_name: str
    channel_thumbnail: str
    video_count: int


@dataclass
class VideoData:
    """Standardized video data structure."""

    rank: int
    id: str
    title: str
    views: int
    likes: int
    dislikes: int
    comments: int
    duration: int  # in seconds
    uploader: str
    thumbnail: str
    rating: Optional[float] = None


@dataclass
class ProcessingEstimate:
    """Estimate for processing time and resources."""

    total_videos: int
    videos_to_expand: int
    estimated_seconds: float
    estimated_minutes: float
    batch_count: int

    def __str__(self):
        if self.estimated_minutes < 1:
            return f"~{int(self.estimated_seconds)} seconds"
        elif self.estimated_minutes < 60:
            return f"~{int(self.estimated_minutes)} minutes"
        else:
            hours = self.estimated_minutes / 60
            return f"~{hours:.1f} hours"


@dataclass
class ProcessingStats:
    """Statistics from processing videos."""

    total_retries: int = 0
    failed_videos: int = 0
    bot_challenges: int = 0
    rate_limits: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "total_retries": self.total_retries,
            "failed_videos": self.failed_videos,
            "bot_challenges": self.bot_challenges,
            "rate_limits": self.rate_limits,
        }


class YouTubeBackend(ABC):
    """Abstract base class for YouTube data fetching backends."""

    @abstractmethod
    async def fetch_playlist_preview(self, url: str) -> PlaylistMetadata:
        """
        Fetch lightweight playlist metadata without video details.

        Args:
            url: YouTube playlist URL

        Returns:
            PlaylistMetadata object
        """
        pass

    @abstractmethod
    async def fetch_playlist_videos(
        self,
        url: str,
        max_videos: Optional[int] = None,
        progress_callback: Optional[callable] = None,
    ) -> Tuple[List[VideoData], PlaylistMetadata]:
        """
        Fetch complete video data from playlist.

        Args:
            url: YouTube playlist URL
            max_videos: Maximum number of videos to fetch (None = all)
            progress_callback: Optional callback(current, total, metadata)

        Returns:
            Tuple of (list of VideoData, PlaylistMetadata)
        """
        pass

    @abstractmethod
    async def close(self):
        """Clean up resources (connections, clients, etc.)."""
        pass


class BackendError(Exception):
    """Base exception for backend errors."""

    pass


class QuotaExceededError(BackendError):
    """Raised when API quota is exceeded."""

    pass


class RateLimitError(BackendError):
    """Raised when rate limits are hit."""

    pass


class BotChallengeError(BackendError):
    """Raised when bot detection is triggered."""

    pass


class VideoFetchError(BackendError):
    """Raised when video fetching fails."""

    pass
