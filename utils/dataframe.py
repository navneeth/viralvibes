"""
DataFrame operations using native Python data structures (list of dicts).
Provides a simple abstraction layer for data manipulation without Polars/Pandas.
"""

import json
from typing import Any


def deserialize_dataframe(df_json: str) -> list[dict[str, Any]]:
    """
    Deserialize DataFrame from JSON string to list of dicts.

    Args:
        df_json: JSON string representation of DataFrame

    Returns:
        List of dictionaries, each representing a row
    """
    if not df_json:
        return []
    return json.loads(df_json)


def create_empty_dataframe() -> list[dict]:
    """Create an empty dataset."""
    return []


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
    """
    Find indices of max and min values in a numeric column.

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


def sort_dataframe(
    data: list[dict], column: str, descending: bool = False
) -> list[dict]:
    """
    Sort dataset by a column.

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
