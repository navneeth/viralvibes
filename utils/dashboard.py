"""
Dashboard-specific utilities for URL handling, job progress, and UI helpers.
"""

import hashlib
import html
import re
from datetime import datetime, timezone


def normalize_playlist_url(url: str) -> str:
    """Normalize playlist URL by removing query parameters."""
    url = url.strip().lower()
    url = re.sub(r"&index=\d+", "", url)
    url = re.sub(r"&t=\d+", "", url)
    return url


def compute_dashboard_id(playlist_url: str) -> str:
    """
    Generate unique dashboard ID from playlist URL.

    Args:
        playlist_url: YouTube playlist URL

    Returns:
        16-character hash of normalized URL
    """
    normalized = normalize_playlist_url(playlist_url)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def compute_time_metrics(started_at_str: str | None, progress: float):
    """
    Compute elapsed and remaining time in seconds.

    Args:
        started_at_str: ISO datetime string of when job started
        progress: Progress as decimal (0.0 to 1.0)

    Returns:
        Tuple of (elapsed_seconds, remaining_seconds)
    """
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
    """
    Compute current batch number from progress.

    Args:
        progress: Progress as decimal (0.0 to 1.0)
        batch_count: Total number of batches

    Returns:
        Tuple of (current_batch, total_batches)
    """
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
