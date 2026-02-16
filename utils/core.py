"""
Core utility functions for safe data access and basic operations.
"""

from fasthtml.common import *


def safe_get_value(obj, key: str, default=0):
    """
    Safely get value from dict or Supabase object. Returns default if None.

    This helper works with both dict objects and Supabase response objects,
    handling None values consistently across the codebase.

    Args:
        obj: Dictionary or object with attributes
        key: Key/attribute name to retrieve
        default: Default value to return if key is missing or value is None

    Returns:
        The value associated with the key, or default if missing/None
    """
    if isinstance(obj, dict):
        value = obj.get(key, default)
    else:
        value = getattr(obj, key, default)

    # If value is None, return default instead
    return value if value is not None else default


def safe_get(d, k, default=""):
    """Get value from dict safely with default."""
    return d.get(k, default) if isinstance(d, dict) else default


def safe_cell(value):
    """Return value or 'N/A' if None."""
    return value if value is not None else "N/A"


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
