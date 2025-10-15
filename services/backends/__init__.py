# services/backends/__init__.py
"""YouTube data fetching backends."""

from .base import (
    BackendError,
    BotChallengeError,
    PlaylistMetadata,
    ProcessingEstimate,
    ProcessingStats,
    QuotaExceededError,
    RateLimitError,
    VideoData,
    VideoFetchError,
    YouTubeBackend,
)
from .youtube_api_backend import YouTubeApiBackend
from .yt_dlp_backend import YtDlpBackend

__all__ = [
    # Base classes
    "YouTubeBackend",
    "PlaylistMetadata",
    "VideoData",
    "ProcessingEstimate",
    "ProcessingStats",
    # Exceptions
    "BackendError",
    "QuotaExceededError",
    "RateLimitError",
    "BotChallengeError",
    "VideoFetchError",
    # Backend implementations
    "YtDlpBackend",
    "YouTubeApiBackend",
]
