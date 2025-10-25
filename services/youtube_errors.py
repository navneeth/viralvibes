"""
youtube_errors.py
-----------------
Custom exception hierarchy for the YouTube service.
"""


class YouTubeServiceError(Exception):
    """Base exception for YouTube service errors."""


class YouTubeBotChallengeError(YouTubeServiceError):
    """Raised when YouTube serves a CAPTCHA or bot verification challenge."""


class RateLimitError(YouTubeServiceError):
    """Raised when API or network rate limits are exceeded."""
