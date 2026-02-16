"""
Utility functions for ViralVibes.

This package contains pure utility functions organized by domain:
- creator_metrics: Creator-specific calculations and metrics
- (future) formatting: Number, date, and text formatting
- (future) engagement: Engagement rate calculations
"""

# Re-export from utils.py for backward compatibility
import sys
from pathlib import Path

# Add parent directory to path to import utils.py
parent = Path(__file__).parent.parent
sys.path.insert(0, str(parent))

from utils import (  # noqa: E402
    calculate_engagement_rate,
    compute_dashboard_id,
    deserialize_dataframe,
    format_date_relative,
    format_number,
    safe_get_value,
    serialize_dataframe,
)

# Import from submodules
from .creator_metrics import (
    calculate_avg_views_per_video,
    calculate_growth_rate,
    calculate_views_per_subscriber,
    estimate_monthly_revenue,
    format_channel_age,
    get_grade_info,
    get_growth_signal,
    get_sync_status_badge,
)

__all__ = [
    # From utils.py
    "calculate_engagement_rate",
    "compute_dashboard_id",
    "deserialize_dataframe",
    "format_date_relative",
    "format_number",
    "safe_get_value",
    "serialize_dataframe",
    # From creator_metrics
    "calculate_avg_views_per_video",
    "calculate_growth_rate",
    "calculate_views_per_subscriber",
    "estimate_monthly_revenue",
    "format_channel_age",
    "get_grade_info",
    "get_growth_signal",
    "get_sync_status_badge",
]
