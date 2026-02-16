"""
Date and time formatting utilities.
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def format_duration(seconds: int) -> str:
    """
    Convert seconds into a human-readable duration string (e.g., 1:30, 2:15:45).

    Args:
        seconds (int): Duration in seconds.

    Returns:
        str: Formatted duration string in MM:SS or HH:MM:SS format.
    """
    # Convert to integer if float
    if isinstance(seconds, float):
        seconds = int(seconds)

    # Validate input type
    if not isinstance(seconds, int):
        logger.warning(f"Invalid duration type: {type(seconds)}")
        return "00:00"

    # Validate input value
    if seconds < 0:
        logger.warning(f"Negative duration: {seconds}")
        return "00:00"

    # Handle zero or None case
    if not seconds:
        return "00:00"

    # Calculate time components
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    remaining_seconds = seconds % 60

    # Format based on duration
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{remaining_seconds:02d}"
    return f"{minutes:02d}:{remaining_seconds:02d}"


def estimate_remaining_time(video_count: int, progress: float) -> tuple[int, str]:
    """
    Estimate remaining time for playlist analysis.

    Args:
        video_count: Total number of videos in playlist
        progress: Current progress (0-100)

    Returns:
        Tuple of (seconds_remaining, formatted_label)

    Example:
        >>> estimate_remaining_time(100, 50.0)
        (125, "~2 min remaining")
    """
    # Lazy import to avoid circular dependency with constants
    from constants import TimeEstimates

    if video_count <= 0 or progress <= 0:
        return (0, "Calculating...")

    # Clamp progress to valid range
    progress_decimal = max(0.0, min(1.0, progress / 100.0))

    # Calculate total estimated time
    estimated_total_seconds = video_count * TimeEstimates.SECONDS_PER_VIDEO

    # Calculate elapsed and remaining
    elapsed = estimated_total_seconds * progress_decimal
    remaining = estimated_total_seconds - elapsed

    # Format output
    if remaining <= 0:
        return (0, "Almost done...")

    remaining_minutes = max(TimeEstimates.MIN_ESTIMATE_MINUTES, int(remaining / 60))

    return (int(remaining), f"~{remaining_minutes} min remaining")


def format_seconds(seconds: int) -> str:
    """
    Format time displays like "5m 30s".

    Args:
        seconds: Time in seconds

    Returns:
        Formatted string like "5m 30s" or "30s"
    """
    if seconds < 0:
        return "0s"
    m, s = divmod(seconds, 60)
    return f"{m}m {s}s" if m else f"{s}s"


def parse_iso_duration(duration: str) -> str:
    """
    Convert ISO8601 duration to "HH:MM:SS" format.

    Args:
        duration: ISO8601 duration string (e.g., "PT1H30M")

    Returns:
        Formatted duration string (e.g., "01:30:00")
    """
    import isodate

    try:
        td = isodate.parse_duration(duration)
        total_seconds = int(td.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return (
            f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            if hours
            else f"{minutes:02d}:{seconds:02d}"
        )
    except Exception:
        return duration


def format_date_simple(date_str: str | None) -> str:
    """
    Format ISO datetime to simple date string.

    Args:
        date_str: ISO datetime string (e.g., "2026-01-29T10:30:00Z")

    Returns:
        Formatted date (e.g., "Jan 29, 2026") or "Recently" if None

    Examples:
        >>> format_date_simple("2026-01-29T10:30:00Z")
        "Jan 29, 2026"
        >>> format_date_simple("2026-01-29")
        "Jan 29, 2026"
        >>> format_date_simple(None)
        "Recently"
    """
    if not date_str:
        return "Recently"

    try:
        # Handle both datetime and date strings
        if isinstance(date_str, str):
            # Remove timezone info for parsing
            clean_date = date_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean_date)
        else:
            dt = date_str

        return dt.strftime("%b %d, %Y")
    except Exception:
        return "Recently"


def format_date_relative(date_str: str | None) -> str:
    """
    Format ISO datetime to relative time or simple date.

    Returns relative time for recent dates (e.g., "2h ago", "Yesterday"),
    otherwise returns simple date format.

    Args:
        date_str: ISO datetime string

    Returns:
        Relative time string or simple date

    Examples:
        >>> format_date_relative("2026-02-04T12:00:00Z")  # 2 hours ago
        "2h ago"
        >>> format_date_relative("2026-02-03T12:00:00Z")  # Yesterday
        "Yesterday"
        >>> format_date_relative("2026-01-20T12:00:00Z")  # 2 weeks ago
        "Jan 20, 2026"
        >>> format_date_relative(None)
        "Never updated"
    """
    if not date_str:
        return "Never updated"

    try:
        # Parse datetime and ensure timezone-aware
        clean_date = date_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean_date)

        # Ensure dt is timezone-aware
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)

        # Calculate difference between two timezone-aware datetimes
        diff = now - dt

        # Format based on age
        if diff.days == 0:
            hours = diff.seconds // 3600
            if hours == 0:
                minutes = diff.seconds // 60
                return f"{minutes}m ago" if minutes > 0 else "Just now"
            return f"{hours}h ago"
        elif diff.days == 1:
            return "Yesterday"
        elif diff.days < 7:
            return f"{diff.days}d ago"
        else:
            # Fall back to simple date for older dates
            return dt.strftime("%b %d, %Y")

    except Exception:
        return "Unknown"
