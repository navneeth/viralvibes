import polars as pl


def calculate_engagement_rate(view_count: float, like_count: float,
                              dislike_count: float) -> float:
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
        return f"{num/1_000_000_000:.1f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    return f"{num:,.0f}"


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


def process_numeric_column(series: 'pl.Series') -> 'pl.Series':
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
        if 'B' in value:
            return float(value.replace('B', '')) * 1_000_000_000
        if 'M' in value:
            return float(value.replace('M', '')) * 1_000_000
        if 'K' in value:
            return float(value.replace('K', '')) * 1_000
        return float(value.replace(',', ''))

    return series.map_elements(convert_to_number, return_dtype=pl.Float64)

def safe_cell(value):
    return value if value is not None else "N/A"
