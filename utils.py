import asyncio
import functools
import hashlib
import html
import json
import logging
import re
import isodate
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


def calculate_creator_stats(creators: list) -> dict:
    """
    Calculate aggregate statistics from creators list for hero section.

    Args:
        creators: List of creator dicts or Supabase objects

    Returns:
        Dict with aggregate stats (total_subscribers, total_views, avg_engagement, total_revenue)
    """
    if not creators:
        return {
            "total_subscribers": 0,
            "total_views": 0,
            "avg_engagement": 0.0,
            "total_revenue": 0,
        }

    try:
        total_subscribers = sum(
            safe_get_value(c, "current_subscribers", 0) for c in creators
        )
        total_views = sum(safe_get_value(c, "current_view_count", 0) for c in creators)

        # Calculate average engagement
        engagement_scores = [safe_get_value(c, "engagement_score", 0) for c in creators]
        avg_engagement = (
            sum(engagement_scores) / len(engagement_scores) if engagement_scores else 0
        )

        # Calculate total revenue (CPM: $4 per 1000 views)
        total_revenue = sum(
            (safe_get_value(c, "current_view_count", 0) * 4) / 1000 for c in creators
        )

        return {
            "total_subscribers": int(total_subscribers),
            "total_views": int(total_views),
            "avg_engagement": round(avg_engagement, 2),
            "total_revenue": int(total_revenue),
        }
    except Exception as e:
        import logging

        logging.getLogger(__name__).exception(f"Error calculating creator stats: {e}")
        return {
            "total_subscribers": 0,
            "total_views": 0,
            "avg_engagement": 0.0,
            "total_revenue": 0,
        }


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


# Helper: convert ISO8601 to "HH:MM:SS"
def parse_iso_duration(duration: str) -> str:

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
