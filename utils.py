import asyncio
import functools
import hashlib
import html
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

from fasthtml.common import *

from constants import TimeEstimates

logger = logging.getLogger(__name__)


def calculate_engagement_rate(
    view_count: float, like_count: float, dislike_count: float
) -> float:
    """Calculate engagement rate as a percentage."""
    if not view_count or view_count == 0:
        return 0.0
    return ((like_count or 0) + (dislike_count or 0)) / view_count * 100


def format_number(num: float) -> str:
    """
    Convert a large number into a human-readable string (e.g., 1.2M, 3.4K).
    Args:
        num (float): The input number.
    Returns:
        str: Human-readable formatted string.
    """
    if not num:
        return "0"
    if num >= 1_000_000_000:
        return f"{num / 1_000_000_000:.1f}B"
    elif num >= 1_000_000:
        return f"{num / 1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num / 1_000:.1f}K"
    return f"{num:,.0f}"


def format_percentage(x):
    try:
        return f"{float(x):.2%}"
    except Exception:
        return ""


def format_float(value: float, decimals: int = 2) -> float:
    """
    Clean floating-point precision errors.
    Convert 0.699999999999996 → 0.70
    """
    if value is None:
        return 0.0
    return round(float(value), decimals)


def format_duration(seconds: int) -> str:
    """
    Convert seconds into a human-readable duration string (e.g., 1:30, 2:15:45).
    Args:
        seconds (int): Duration in seconds.
    Returns:
        str: Formatted duration string in MM:SS or HH:MM:SS format.
    """
    try:
        # Convert to integer if float
        if isinstance(seconds, float):
            seconds = int(seconds)

        # Validate input type
        if not isinstance(seconds, int):
            raise TypeError("Duration must be a number")

        # Validate input value
        if seconds < 0:
            raise ValueError("Duration cannot be negative")

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

    except Exception as e:
        # Log the error and return a safe default
        print(f"Error formatting duration: {str(e)}")
        return "00:00"


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
    '''# Format time displays like "5m 30s"'''
    if seconds < 0:
        return "0s"
    m, s = divmod(seconds, 60)
    return f"{m}m {s}s" if m else f"{s}s"


# --- Define helper for parsing formatted numbers into raw ints ---
# parser for formatted numbers (e.g., 12.3M, 540K, 1,234)
def parse_number(val: str) -> int:
    try:
        if val is None:
            return 0
        if isinstance(val, (int, float)):
            return int(val)
        s = str(val).strip()
        if s == "" or s in {"—", "-", "N/A"}:
            return 0
        s = s.replace(",", "").upper()
        multiplier = 1.0
        if s.endswith("B"):
            multiplier = 1e9
            s = s[:-1]
        elif s.endswith("M"):
            multiplier = 1e6
            s = s[:-1]
        elif s.endswith("K"):
            multiplier = 1e3
            s = s[:-1]
        return int(float(s) * multiplier)
    except Exception:
        return 0


# Helper: convert ISO8601 to "HH:MM:SS"
def parse_iso_duration(duration: str) -> str:
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


def safe_cell(value):
    return value if value is not None else "N/A"


def safe_get(d, k, default=""):
    return d.get(k, default) if isinstance(d, dict) else default


def safe_channel_name(channel_name: str | None, channel_url: str | None = None):
    """
    Return a sanitized channel name.
    If channel_url is provided, return it as a clickable link.
    Handles None values safely.
    """
    # Coerce None to empty string
    channel_name = (channel_name or "").strip()
    channel_url = channel_url or ""

    if not channel_name:
        channel_name = "Unknown Channel"

    if channel_url:
        return A(channel_name, href=channel_url, cls="text-blue-600 hover:underline")
    return Span(channel_name, cls="font-medium text-gray-700")


def with_retries(
    max_retries=3,
    base_delay=5.0,
    backoff=2.0,
    retry_exceptions=(Exception,),
    jitter=0.0,
):
    """
    Decorator to retry an async function on specified exceptions.

    Args:
        max_retries (int): Maximum number of retry attempts.
        base_delay (float): Initial delay between retries (seconds).
        backoff (float): Multiplicative backoff factor.
        retry_exceptions (tuple): Exception types that trigger a retry.
        jitter (float): Random jitter added/subtracted to delay, in seconds.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except retry_exceptions as e:
                    last_exc = e
                    if attempt == max_retries - 1:
                        logger.error(
                            f"{func.__name__} failed after {max_retries} attempts: {e}"
                        )
                        raise
                    delay = base_delay * (backoff**attempt)
                    if jitter:
                        import random

                        delay += random.uniform(-jitter, jitter)
                    logger.warning(
                        f"{func.__name__} attempt {attempt + 1}/{max_retries} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    await asyncio.sleep(delay)
            # Should not reach here, but fallback in case loop exits unexpectedly
            raise last_exc or RuntimeError(f"{func.__name__} failed after retries")

        return wrapper

    return decorator


# =============================================================================
# Dashboard identity helpers
# =============================================================================


def normalize_playlist_url(url: str) -> str:
    url = url.strip().lower()
    url = re.sub(r"&index=\d+", "", url)
    url = re.sub(r"&t=\d+", "", url)
    return url


def compute_dashboard_id(playlist_url: str) -> str:
    normalized = normalize_playlist_url(playlist_url)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


# =============================================================================
# Job progress helpers
# =============================================================================


def compute_time_metrics(started_at_str: str | None, progress: float):
    """Compute elapsed and remaining time in seconds"""
    try:
        started_at = (
            datetime.fromisoformat(started_at_str.replace("Z", "+00:00"))
            if started_at_str
            else None
        )
        now = datetime.now(timezone.utc)
        elapsed = (now - started_at).total_seconds() if started_at else 0
    except Exception:
        elapsed = 0

    if 0 < progress < 1.0:
        rate = elapsed / progress
        remaining = rate * (1.0 - progress)
    else:
        remaining = 0

    return int(elapsed), int(remaining)


def compute_batches(progress: float, batch_count: int = 5):
    current = max(1, int(progress * batch_count))
    return current, batch_count


def create_redirect_script(
    url: str, delay_ms: int = 500, message: str = "Redirecting..."
) -> str:
    """
    Create a safe redirect script with proper escaping.

    Args:
        url: URL to redirect to (will be escaped)
        delay_ms: Delay in milliseconds before redirect
        message: Console message to log

    Returns:
        Safe JavaScript string

    Example:
        >>> create_redirect_script("/dashboard/abc123")
        "console.log('Redirecting...');\\nsetTimeout(() => {\\n..."
    """
    # Escape for safe HTML/JS interpolation
    safe_url = html.escape(url, quote=True)
    safe_message = html.escape(message, quote=True)

    return f"""
        console.log('{safe_message}');
        setTimeout(() => {{
            window.location.href = '{safe_url}';
        }}, {delay_ms});
    """


def clamp(value: float, min_val: float, max_val: float) -> float:
    """
    Constrain a value within a min and max range.
    Works with floats, ints, and comparable types.

    Examples:
        clamp(50, 0, 100)      # → 50
        clamp(-10, 0, 100)     # → 0
        clamp(150, 0, 100)     # → 100
        clamp(4.2, 0, 3.5)     # → 3.5
    """
    return max(min_val, min(value, max_val))


# =============================================================================
# Date formatting utilities
# =============================================================================


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
        # Parse datetime
        clean_date = date_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean_date)
        now = datetime.now(timezone.utc)

        # Calculate difference
        diff = now - dt.replace(tzinfo=None)

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


# =============================================================================
# DataFrame Abstraction Layer (Native Python - No Polars)
# =============================================================================


def deserialize_dataframe(df_json: str) -> list[dict[str, Any]]:
    """Deserialize DataFrame from JSON string to list of dicts.

    Args:
        df_json: JSON string representation of DataFrame

    Returns:
        List of dictionaries, each representing a row
    """
    if not df_json:
        return []
    return json.loads(df_json)


def get_row_count(data: list[dict]) -> int:
    """Get number of rows in dataset."""
    return len(data)


def get_columns(data: list[dict]) -> list[str]:
    """Get column names from dataset."""
    return list(data[0].keys()) if data else []


def has_column(data: list[dict], column: str) -> bool:
    """Check if column exists in dataset."""
    return column in data[0] if data else False


def get_unique_count(data: list[dict], column: str) -> int:
    """Count unique values in a column."""
    if not data or column not in data[0]:
        return 0
    return len(set(row.get(column) for row in data if column in row))


def find_extreme_indices(
    data: list[dict], column: str
) -> tuple[int | None, int | None]:
    """Find indices of max and min values in a numeric column.

    Args:
        data: List of dictionaries
        column: Column name to find extremes for

    Returns:
        Tuple of (max_index, min_index). Returns (None, None) if column not found.
    """
    if not data or column not in data[0]:
        return (None, None)

    try:
        # Find max index
        max_idx = max(enumerate(data), key=lambda x: float(x[1].get(column, 0) or 0))[0]
        # Find min index
        min_idx = min(enumerate(data), key=lambda x: float(x[1].get(column, 0) or 0))[0]
        return (max_idx, min_idx)
    except (ValueError, TypeError):
        return (None, None)


def create_empty_dataframe() -> list[dict]:
    """Create an empty dataset."""
    return []


def sort_dataframe(
    data: list[dict], column: str, descending: bool = False
) -> list[dict]:
    """Sort dataset by a column.

    Args:
        data: List of dictionaries
        column: Column name to sort by
        descending: If True, sort in descending order

    Returns:
        Sorted list of dictionaries
    """
    if not data or column not in data[0]:
        return data

    def get_sort_value(row):
        val = row.get(column)

        # Handle None values - sort them to the end regardless of direction
        if val is None or val == "":
            return (True, False, "")

        # Try to convert to float for numeric sorting
        try:
            numeric_val = float(val)
            # Numeric values: (not_none=False, is_numeric=True, numeric_value)
            return (False, True, numeric_val)
        except (ValueError, TypeError):
            # String values: (not_none=False, is_numeric=False, string_value)
            return (False, False, str(val).lower())

    return sorted(data, key=get_sort_value, reverse=descending)
