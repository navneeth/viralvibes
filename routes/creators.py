"""
Creators routes - browse and filter discovered YouTube creators.
"""

import logging

from fasthtml.common import *

from auth.auth_service import get_current_user
from db import supabase_client
from constants import CREATOR_TABLE
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
        List of creator dicts with stats
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
            "quality": ("quality_grade", False),  # A+ first
            "recent": ("last_updated_at", True),
        }

        sort_field, descending = sort_map.get(sort, ("current_subscribers", True))
        query = query.order(sort_field, desc=descending)

        # Limit results
        query = query.limit(limit)

        # Execute query
        response = query.execute()

        creators = response.data if response.data else []
        logger.info(
            f"Retrieved {len(creators)} creators (search='{search}', sort={sort}, grade={grade_filter})"
        )

        return creators

    except Exception as e:
        logger.exception(f"Error fetching creators: {e}")
        return []


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

    # Render page
    return render_creators_page(
        creators=creators,
        sort=sort,
        search=search,
        grade_filter=grade_filter,
    )
