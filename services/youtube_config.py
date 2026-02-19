"""
YouTube API Configuration - Stage 1: Key Separation

Simple separation of API keys by use case with legacy fallback.
No token rotation yet - just clear naming.

Environment Variables:
    YOUTUBE_API_KEY_PLAYLIST: Key for playlist/video analysis
    YOUTUBE_API_KEY_CREATORS: Key for creator worker
    YOUTUBE_API_KEY: Legacy fallback for both
"""

import logging
import os

logger = logging.getLogger(__name__)


def get_playlist_api_key() -> str:
    """
    Get API key for playlist analysis.

    Environment:
        YOUTUBE_API_KEY_PLAYLIST: Dedicated key for playlists
        YOUTUBE_API_KEY: Legacy fallback

    Returns:
        API key string

    Raises:
        ValueError: If no API key configured
    """
    key = os.getenv("YOUTUBE_API_KEY_PLAYLIST") or os.getenv("YOUTUBE_API_KEY")

    if not key:
        raise ValueError(
            "No YouTube API key for playlist analysis. "
            "Set YOUTUBE_API_KEY_PLAYLIST or YOUTUBE_API_KEY environment variable."
        )

    if os.getenv("YOUTUBE_API_KEY_PLAYLIST"):
        logger.debug("Using YOUTUBE_API_KEY_PLAYLIST for playlist analysis")
    else:
        logger.debug("Using legacy YOUTUBE_API_KEY for playlist analysis")

    return key


def get_creator_worker_api_key() -> str:
    """
    Get API key for creator worker.

    Environment:
        YOUTUBE_API_KEY_CREATORS: Dedicated key for creator worker
        YOUTUBE_API_KEY: Legacy fallback

    Returns:
        API key string

    Raises:
        ValueError: If no API key configured
    """
    key = os.getenv("YOUTUBE_API_KEY_CREATORS") or os.getenv("YOUTUBE_API_KEY")

    if not key:
        raise ValueError(
            "No YouTube API key for creator worker. "
            "Set YOUTUBE_API_KEY_CREATORS or YOUTUBE_API_KEY environment variable."
        )

    if os.getenv("YOUTUBE_API_KEY_CREATORS"):
        logger.info("✅ Using YOUTUBE_API_KEY_CREATORS for creator worker")
    else:
        logger.info("✅ Using legacy YOUTUBE_API_KEY for creator worker")

    return key
