"""
Number and string formatting utilities.
"""

import logging
from typing import Union


def format_number(num: float, signed: bool = False) -> str:
    """Convert a large number into a human-readable string (e.g. 1.2M, 3.4K).

    Args:
        num:    The input number.
        signed: When True, positive values are prefixed with ``+`` (useful for
                30-day deltas where direction matters).  Negative values always
                carry ``-``.

    Returns:
        Human-readable formatted string, e.g. ``"1.2M"``, ``"+280K"``, ``"-5K"``.

    Examples::

        format_number(1_200_000)          → "1.2M"
        format_number(280_000, signed=True) → "+280K"
        format_number(-5_000, signed=True)  → "-5K"
    """
    if not num:
        return "0"
    abs_num = abs(num)
    if abs_num >= 1_000_000_000:
        result = f"{abs_num / 1_000_000_000:.1f}B"
    elif abs_num >= 1_000_000:
        result = f"{abs_num / 1_000_000:.1f}M"
    elif abs_num >= 1_000:
        result = f"{abs_num / 1_000:.1f}K"
    else:
        result = f"{abs_num:,.0f}"
    if num < 0:
        result = f"-{result}"
    elif signed and num > 0:
        result = f"+{result}"
    return result


def parse_number(val: str) -> int:
    """Parse formatted numbers (e.g. 12.3M, 540K, 1,234) into raw ints.

    Args:
        val: String representation of a number (can include M, K, B suffixes).

    Returns:
        Parsed integer value, or 0 on any parse failure.
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


def format_percentage(x: Union[float, int, str, None], decimals: int = 1) -> str:
    """Format a fraction (0–1) as a percentage string.

    Args:
        x:        Value in the range 0–1 (e.g. ``0.067`` for 6.7 %).  May also
                  be an already-formatted string ending in ``%`` (e.g. ``"0.28%"``)
                  in which case the value is re-formatted without multiplying by 100.
        decimals: Number of decimal places in the output.  Defaults to 1
                  (e.g. ``"6.7%"``).  Pass ``2`` for ``"6.70%"``.  ``0`` emits
                  an integer percentage (e.g. ``"7%"``).

    Returns:
        Percentage string without trailing zeros (e.g. ``"6.7%"`` not ``"6.70%"``
        when decimals=1), unless the extra zeros are significant.  Returns
        ``"—"`` for ``None`` or values that cannot be converted to a number.

    Examples::

        format_percentage(0.067)     → "6.7%"
        format_percentage(0.067, 2)  → "6.70%"
        format_percentage(0.5)       → "50.0%"
        format_percentage("6.7%")    → "6.7%"
    """
    # Guard: if x is already a formatted percentage string (e.g. "0.28%"),
    # strip the trailing "%" and treat the value as already in percent form
    # (i.e. do NOT multiply by 100 again).
    if isinstance(x, str) and x.strip().endswith("%"):
        try:
            pct = float(x.strip().rstrip("%"))
        except (ValueError, TypeError):
            return "—"
        if decimals <= 0:
            return f"{pct:.0f}%"
        formatted = f"{pct:.{decimals}f}"
        if "." in formatted:
            integer_part, frac_part = formatted.split(".", 1)
            trimmed_frac = frac_part.rstrip("0")
            if not trimmed_frac:
                trimmed_frac = "0"
            formatted = f"{integer_part}.{trimmed_frac}"
        return f"{formatted}%"
    try:
        pct = float(x) * 100
    except (ValueError, TypeError):
        logging.warning("format_percentage: could not convert %r to float", x)
        return "—"
    if decimals <= 0:
        formatted = f"{pct:.0f}"
    else:
        formatted = f"{pct:.{decimals}f}"
        if "." in formatted:
            integer_part, frac_part = formatted.split(".", 1)
            trimmed_frac = frac_part.rstrip("0")
            if not trimmed_frac:
                trimmed_frac = "0"
            formatted = f"{integer_part}.{trimmed_frac}"
    return f"{formatted}%"


def format_float(value: float, decimals: int = 2) -> str:
    """Round *value* to *decimals* places and return as a plain string.

    Cleans floating-point precision noise (e.g. ``0.699999999999996 → "0.7"``)
    and returns a ``str`` so the result can be composed into UI components
    without a redundant format spec.

    Args:
        value:    Float value to clean and format.
        decimals: Number of decimal places.  Trailing zeros are kept only
                  when they are significant (``round`` behaviour):
                  ``format_float(7.0, 2)`` → ``"7.0"`` not ``"7.00"``.

    Returns:
        String representation of the rounded value.

    Examples::

        format_float(6.7333)        → "6.73"
        format_float(0.699999999)   → "0.7"
        format_float(2.1, 1)        → "2.1"
        format_float(7.0, 2)        → "7.0"
    """
    if value is None:
        return "0"
    return str(round(float(value), decimals))


def format_float_exact(value: float, decimals: int = 2) -> str:
    """Like ``format_float`` but always emits exactly *decimals* decimal places.

    Use this when consistent column alignment matters (e.g. a table of scores).

    Examples::

        format_float_exact(7.0, 2)   → "7.00"
        format_float_exact(6.733, 2) → "6.73"
    """
    if value is None:
        return f"{'0':>{decimals + 2}}"
    return f"{round(float(value), decimals):.{decimals}f}"
