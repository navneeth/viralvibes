"""
Number and string formatting utilities.
"""


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


def parse_number(val: str) -> int:
    """
    Parse formatted numbers (e.g., 12.3M, 540K, 1,234) into raw ints.

    Args:
        val: String representation of a number (can include M, K, B suffixes)

    Returns:
        Parsed integer value
    """
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


def format_percentage(x):
    """Format number as percentage."""
    try:
        return f"{float(x):.2%}"
    except Exception:
        return ""


def format_float(value: float, decimals: int = 2) -> float:
    """
    Clean floating-point precision errors.
    Convert 0.699999999999996 → 0.70

    Args:
        value: Float value to clean
        decimals: Number of decimal places

    Returns:
        Cleaned float value
    """
    if value is None:
        return 0.0
    return round(float(value), decimals)
