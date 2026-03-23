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


class QuotaExceededException(YouTubeServiceError):
    """
    Raised when the YouTube API returns a quotaExceeded 403.

    This is a TRANSIENT failure — the quota resets at midnight Pacific.
    Jobs that raise this must NOT be purged; they should be retried with
    backoff. The worker loop handles this explicitly.
    """

    def __init__(self, channel_id: str):
        self.channel_id = channel_id
        super().__init__(f"YouTube quota exceeded for channel: {channel_id}")


def is_quota_exhausted_error(exc: Exception) -> bool:
    """
    Return True when exc is a YouTube API 403 quotaExceeded error.

    Checks both error_details (structured) and str(exc) (fallback) so the
    detection still works if the HttpError payload format changes.
    """
    try:
        from googleapiclient.errors import HttpError
    except ImportError:
        return False
    if not isinstance(exc, HttpError):
        return False
    if getattr(exc, "resp", None) is None:
        return False
    if getattr(exc.resp, "status", None) != 403:
        return False
    return "quotaExceeded" in str(getattr(exc, "error_details", "")) or "quotaExceeded" in str(exc)
