"""
Utility functions for ViralVibes.

FastAI-style organization:
- Simple, flat imports
- Organized by functionality
- Easy to use: `from utils import format_number, safe_get_value`
"""

# Core utilities
from .core import (
    clamp,
    safe_cell,
    safe_channel_name,
    safe_get,
    safe_get_value,
)

# Formatting
from .formatting import (
    format_float,
    format_number,
    format_percentage,
    parse_number,
)

# Dates and times
from .dates import (
    estimate_remaining_time,
    format_date_relative,
    format_date_simple,
    format_duration,
    format_seconds,
    parse_iso_duration,
)

# Dashboard utilities
from .dashboard import (
    compute_batches,
    compute_dashboard_id,
    compute_time_metrics,
    create_redirect_script,
    normalize_playlist_url,
)

# DataFrame operations
from .dataframe import (
    create_empty_dataframe,
    deserialize_dataframe,
    find_extreme_indices,
    get_columns,
    get_row_count,
    get_unique_count,
    has_column,
    sort_dataframe,
)

# Analytics
from .analytics import (
    calculate_engagement_rate,
)

# Async utilities
from .async_utils import (
    with_retries,
)

# Creator-specific metrics
from .creator_metrics import (
    calculate_avg_views_per_video,
    calculate_growth_rate,
    calculate_views_per_subscriber,
    estimate_monthly_revenue,
    format_channel_age,
    get_activity_badge,
    get_grade_info,
    get_growth_signal,
    get_language_emoji,
    get_language_name,
    get_sync_status_badge,
)

# Export everything for `from utils import *`
__all__ = [
    # Core
    "clamp",
    "safe_cell",
    "safe_channel_name",
    "safe_get",
    "safe_get_value",
    # Formatting
    "format_float",
    "format_number",
    "format_percentage",
    "parse_number",
    # Dates
    "estimate_remaining_time",
    "format_date_relative",
    "format_date_simple",
    "format_duration",
    "format_seconds",
    "parse_iso_duration",
    # Dashboard
    "compute_batches",
    "compute_dashboard_id",
    "compute_time_metrics",
    "create_redirect_script",
    "normalize_playlist_url",
    # DataFrame
    "create_empty_dataframe",
    "deserialize_dataframe",
    "find_extreme_indices",
    "get_columns",
    "get_row_count",
    "get_unique_count",
    "has_column",
    "sort_dataframe",
    # Analytics
    "calculate_creator_stats",
    "calculate_engagement_rate",
    # Async
    "with_retries",
    # Creator metrics
    "calculate_avg_views_per_video",
    "calculate_growth_rate",
    "calculate_views_per_subscriber",
    "estimate_monthly_revenue",
    "format_channel_age",
    "get_activity_badge",
    "get_grade_info",
    "get_growth_signal",
    "get_language_emoji",
    "get_language_name",
    "get_sync_status_badge",
]
