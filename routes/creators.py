"""
Creators routes - browse and filter discovered YouTube creators.
"""

import logging

from fasthtml.common import *

from db import get_creators, calculate_creator_stats
from views.creators import render_creators_page

logger = logging.getLogger(__name__)


def creators_route(request):
    """GET /creators - Creators discovery page."""

    # Get query parameters
    search = request.query_params.get("search", "")
    sort = request.query_params.get("sort", "subscribers")
    grade_filter = request.query_params.get("grade", "all")

    # Fetch creators (db.py handles all logic)
    creators = get_creators(
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
