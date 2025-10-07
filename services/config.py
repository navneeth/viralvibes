# youtube/config.py
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class YouTubeConfig:
    """Configuration for YouTube service."""

    # Configuration for rate limiting
    min_video_delay: float = float(os.getenv("MIN_VIDEO_DELAY", "0.5"))
    max_video_delay: float = float(os.getenv("MAX_VIDEO_DELAY", "2.0"))
    min_batch_delay: float = float(os.getenv("MIN_BATCH_DELAY", "1.0"))
    max_batch_delay: float = float(os.getenv("MAX_BATCH_DELAY", "3.0"))
    # Resilience configuration
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    retry_delay: float = float(os.getenv("RETRY_DELAY", "5.0"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "5"))
