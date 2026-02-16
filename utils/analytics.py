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


def calculate_creator_stats(creators: list) -> dict:
    """
    Calculate aggregate statistics from creators list for hero section.

    Args:
        creators: List of creator dicts or Supabase objects

    Returns:
        Dict with aggregate stats (total_subscribers, total_views, avg_engagement, total_revenue)
    """
    from .core import safe_get_value

    if not creators:
        return {
            "total_subscribers": 0,
            "total_views": 0,
            "avg_engagement": 0.0,
            "total_revenue": 0,
        }

    try:
        total_subscribers = sum(
            safe_get_value(c, "current_subscribers", 0) for c in creators
        )
        total_views = sum(safe_get_value(c, "current_view_count", 0) for c in creators)

        # Calculate average engagement
        engagement_scores = [safe_get_value(c, "engagement_score", 0) for c in creators]
        avg_engagement = (
            sum(engagement_scores) / len(engagement_scores) if engagement_scores else 0
        )

        # Calculate total revenue (CPM: $4 per 1000 views)
        total_revenue = sum(
            (safe_get_value(c, "current_view_count", 0) * 4) / 1000 for c in creators
        )

        return {
            "total_subscribers": int(total_subscribers),
            "total_views": int(total_views),
            "avg_engagement": round(avg_engagement, 2),
            "total_revenue": int(total_revenue),
        }
    except Exception as e:
        logger.exception(f"Error calculating creator stats: {e}")
        return {
            "total_subscribers": 0,
            "total_views": 0,
            "avg_engagement": 0.0,
            "total_revenue": 0,
        }
