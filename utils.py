import polars as pl


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
    """Format duration in seconds to HH:MM:SS format.
    
    Args:
        seconds (int): Duration in seconds
        
    Returns:
        str: Formatted duration string in HH:MM:SS or MM:SS format
        
    Raises:
        ValueError: If seconds is negative or not an integer
        TypeError: If seconds is not a number
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


def process_numeric_column(series: pl.Series) -> pl.Series:
    """Helper to convert formatted string numbers to floats."""

    def convert_to_number(value):
        if isinstance(value, (int, float)):
            return float(value)
        value = str(value).upper()
        if 'B' in value:
            return float(value.replace('B', '')) * 1_000_000_000
        if 'M' in value:
            return float(value.replace('M', '')) * 1_000_000
        if 'K' in value:
            return float(value.replace('K', '')) * 1_000
        return float(value.replace(',', ''))

    return series.map_elements(convert_to_number)
