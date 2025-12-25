import asyncio
import functools
import hashlib
import logging
import re

import polars as pl
from fasthtml.common import *

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


def process_numeric_column(series: "pl.Series") -> "pl.Series":
    """Helper to convert formatted string numbers to floats.

    Args:
        series (pl.Series): A Polars Series containing numeric values or formatted strings

    Returns:
        pl.Series: A Polars Series with all values converted to float

    Note:
        Handles various number formats:
        - Plain numbers (int/float)
        - Numbers with B/M/K suffixes (e.g., 1.2B, 3.4M, 5.6K)
        - Numbers with commas (e.g., 1,234,567)
    """

    def convert_to_number(value):
        if isinstance(value, (int, float)):
            return float(value)
        value = str(value).upper()
        if "B" in value:
            return float(value.replace("B", "")) * 1_000_000_000
        if "M" in value:
            return float(value.replace("M", "")) * 1_000_000
        if "K" in value:
            return float(value.replace("K", "")) * 1_000
        return float(value.replace(",", ""))

    return series.map_elements(convert_to_number, return_dtype=pl.Float64)


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
