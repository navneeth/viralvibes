"""
Creators routes - browse and filter discovered YouTube creators.
"""

import logging

from fasthtml.common import *

from db import calculate_creator_stats, get_creators
from views.creators import render_creators_page

logger = logging.getLogger(__name__)


def creators_route(request):
    """GET /creators - Creators discovery page."""

    # Get query parameters
    search = request.query_params.get("search", "")
    sort = request.query_params.get("sort", "subscribers")
    grade_filter = request.query_params.get("grade", "all")

    # Language filter
    language_filter = request.query_params.get("language", "all")
    # Activity filter (by upload frequency)
    activity_filter = request.query_params.get(
        "activity", "all"
    )  # all, active, dormant
    # Age filter (by channel age)
    age_filter = request.query_params.get(
        "age", "all"
    )  # all, new, established, veteran

    # Fetch creators (db.py handles all logic)
    # Limit increased to 500 to show all creators (pagination can be added later)
    creators = get_creators(
        search=search,
        sort=sort,
        grade_filter=grade_filter,
        language_filter=language_filter,
        activity_filter=activity_filter,
        age_filter=age_filter,
        limit=500,  # Show all creators - add pagination if performance becomes an issue
    )

    # Calculate aggregate stats for hero section from ALL creators in DB
    # (not just the filtered/displayed ones) for accurate totals
    stats = calculate_creator_stats(creators, include_all=True)

    # Render page
    return render_creators_page(
        creators=creators,
        sort=sort,
        search=search,
        grade_filter=grade_filter,
        language_filter=language_filter,
        activity_filter=activity_filter,
        age_filter=age_filter,
        stats=stats,
    )
