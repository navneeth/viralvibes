"""
Creators routes - browse and filter discovered YouTube creators.
"""

import logging

from fasthtml.common import *

from constants import CREATOR_TABLE
from db import supabase_client
from views.creators import render_creators_page

logger = logging.getLogger(__name__)


def get_all_creators(
    search: str = "",
    sort: str = "subscribers",
    grade_filter: str = "all",
    limit: int = 100,
) -> list[dict]:
    """
    Fetch creators from database with filtering and sorting.

    Args:
        search: Filter by channel name (case-insensitive)
        sort: Sort criteria (subscribers, views, videos, engagement, quality, recent)
        grade_filter: Filter by quality grade (all, A+, A, B+, B, C)
        limit: Maximum number of results

    Returns:
        List of creator dicts with stats and ranking position
    """
    if not supabase_client:
        logger.warning("Supabase client not available")
        return []

    try:
        # Start query
        query = supabase_client.table(CREATOR_TABLE).select("*")

        # Apply search filter
        if search:
            query = query.ilike("channel_name", f"%{search}%")

        # Apply grade filter
        if grade_filter and grade_filter != "all":
            query = query.eq("quality_grade", grade_filter)

        # Apply sorting
        sort_map = {
            "subscribers": ("current_subscribers", True),
            "views": ("current_view_count", True),
            "videos": ("current_video_count", True),
            "engagement": ("engagement_score", True),
            "quality": ("quality_grade", True),
            "recent": ("last_updated_at", True),
        }

        sort_field, descending = sort_map.get(sort, ("current_subscribers", True))
        query = query.order(sort_field, desc=descending)

        # Limit results
        query = query.limit(limit)

        # Execute query
        response = query.execute()

        creators = response.data if response.data else []

        # Add ranking position to each creator
        for idx, creator in enumerate(creators, 1):
            if isinstance(creator, dict):
                creator["_rank"] = idx
            else:
                # For Supabase objects, we can't directly set attributes
                # Convert to dict if needed
                setattr(creator, "_rank", idx)

        logger.info(
            f"Retrieved {len(creators)} creators (search='{search}', sort={sort}, grade={grade_filter})"
        )

        return creators

    except Exception as e:
        logger.exception(f"Error fetching creators: {e}")
        return []


def calculate_creator_stats(creators: list) -> dict:
    """
    Calculate aggregate statistics from creators list for hero section.

    Args:
        creators: List of creator dicts or Supabase objects

    Returns:
        Dict with aggregate stats (total_subscribers, total_views, avg_engagement, total_revenue)
    """
    if not creators:
        return {
            "total_subscribers": 0,
            "total_views": 0,
            "avg_engagement": 0.0,
            "total_revenue": 0,
        }

    def get_value(obj, key, default=0):
        """Safely get value from dict or Supabase object. Returns default if None."""
        if isinstance(obj, dict):
            value = obj.get(key, default)
        else:
            value = getattr(obj, key, default)

        # If value is None, return default instead
        return value if value is not None else default

    try:
        total_subscribers = sum(
            get_value(c, "current_subscribers", 0) for c in creators
        )
        total_views = sum(get_value(c, "current_view_count", 0) for c in creators)

        # Calculate average engagement
        engagement_scores = [get_value(c, "engagement_score", 0) for c in creators]
        avg_engagement = (
            sum(engagement_scores) / len(engagement_scores) if engagement_scores else 0
        )

        # Calculate total revenue (CPM: $4 per 1000 views)
        total_revenue = sum(
            (get_value(c, "current_view_count", 0) * 4) / 1000 for c in creators
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


def creators_route(request):
    """GET /creators - Creators discovery page."""

    # Get query parameters
    search = request.query_params.get("search", "")
    sort = request.query_params.get("sort", "subscribers")
    grade_filter = request.query_params.get("grade", "all")

    # Validate sort parameter
    valid_sorts = ["subscribers", "views", "videos", "engagement", "quality", "recent"]
    if sort not in valid_sorts:
        sort = "subscribers"

    # Validate grade filter
    valid_grades = ["all", "A+", "A", "B+", "B", "C"]
    if grade_filter not in valid_grades:
        grade_filter = "all"

    # Fetch creators
    creators = get_all_creators(
        search=search,
        sort=sort,
        grade_filter=grade_filter,
        limit=100,
    )

    # Calculate aggregate stats for hero section
    stats = calculate_creator_stats(creators)

    # Render page
    return render_creators_page(
        creators=creators,
        sort=sort,
        search=search,
        grade_filter=grade_filter,
        stats=stats,
    )
