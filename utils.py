def calculate_engagement_rate(view_count: float, like_count: float,
                              dislike_count: float) -> float:
    """Calculate engagement rate as a percentage."""
    if not view_count or view_count == 0:
        return 0.0
    return ((like_count or 0) + (dislike_count or 0)) / view_count * 100


def format_number(num: float) -> str:
    """Format number to human-readable format."""
    if not num:
        return "0"
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.1f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    return f"{num:,.0f}"


def format_duration(seconds: int) -> str:
    """Format duration in seconds to HH:MM:SS format."""
    if not seconds:
        return "00:00"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"
