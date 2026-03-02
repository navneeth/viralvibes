"""
Analytics and engagement calculation utilities.
"""

import logging

logger = logging.getLogger(__name__)


def calculate_engagement_rate(
    view_count: float, like_count: float, dislike_count: float
) -> float:
    """
    Calculate engagement rate as a percentage.

    Args:
        view_count: Total views
        like_count: Number of likes
        dislike_count: Number of dislikes

    Returns:
        Engagement rate as percentage (0-100)
    """
    if not view_count or view_count == 0:
        return 0.0
    return ((like_count or 0) + (dislike_count or 0)) / view_count * 100
