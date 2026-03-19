"""
Core utility functions for safe data access and basic operations.
"""

import re
from urllib.parse import urlparse

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


def normalize_category_name(category: str) -> str:
    """
    Normalize a category name to canonical form for storage and filtering.

    Normalization steps:
    1. Strip Wikipedia URL prefix and any query string/fragment
       (e.g. "https://en.wikipedia.org/wiki/Music?foo=bar#section" → "Music")
    2. Strip leading/trailing whitespace
    3. Replace underscores with spaces
    4. Collapse multiple spaces to single space

    The /wiki/ stripping is intentionally narrow: it only fires when the value
    looks like a Wikipedia HTTP URL (starts with http and contains wikipedia.org),
    avoiding false positives on plain category names that happen to contain "/wiki/".

    This ensures consistent category names whether they come from:
    - Full Wikipedia URLs  (e.g., "https://en.wikipedia.org/wiki/Video_game_culture")
    - Wikipedia slug fragments (e.g., "Video_game_culture")
    - User input filters (e.g., "  video game  ")
    - Database storage

    Args:
        category: Raw category name or Wikipedia URL.

    Returns:
        Normalized category name (e.g., "Video game culture")

    Examples:
        >>> normalize_category_name("https://en.wikipedia.org/wiki/Music")
        'Music'
        >>> normalize_category_name("https://en.wikipedia.org/wiki/Music?foo=bar#section")
        'Music'
        >>> normalize_category_name("https://en.wikipedia.org/wiki/Video_game_culture")
        'Video game culture'
        >>> normalize_category_name("Video_game_culture")
        'Video game culture'
        >>> normalize_category_name("  hip_hop  ")
        'Hip hop'
        >>> normalize_category_name("Music")
        'Music'
    """
    if not category:
        return ""

    # Strip Wikipedia URL prefix — only when the value is a Wikipedia HTTP URL.
    # Narrow check avoids rewriting plain strings that contain "/wiki/" literally.
    if category.startswith(("http://", "https://")):
        parsed = urlparse(category)
        host = parsed.hostname or ""
        # Only treat real Wikipedia hosts as Wikipedia URLs. Accept both the
        # bare domain and any subdomain (e.g. en.wikipedia.org).
        if (host == "wikipedia.org" or host.endswith(".wikipedia.org")) and "/wiki/" in parsed.path:
            # Extract the slug portion after "/wiki/" from the URL path.
            slug_with_rest = parsed.path.split("/wiki/", 1)[-1]
            # Strip any query string or fragment from the extracted slug so that
            # "Music?foo=bar#section" and "Music" both normalise to "Music".
            category = slug_with_rest.split("?", 1)[0].split("#", 1)[0]

    # Strip, replace underscores, collapse whitespace
    normalized = " ".join(category.strip().replace("_", " ").split())

    return normalized


def slugify(text: str) -> str:
    """
    Convert a display name into a URL-safe slug.

    Centralised here so both ``views.lists`` and ``views.creators`` produce
    identical slugs without a circular import.

    Examples::

        slugify("Video game culture") → "video-game-culture"
        slugify("Music")             → "music"
    """
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-") or "unknown"
