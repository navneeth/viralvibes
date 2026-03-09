"""
Creators routes - browse and filter discovered YouTube creators.
"""

import logging
from urllib.parse import urlencode

from fasthtml.common import RedirectResponse

from db import (
    add_creator_by_handle,
    calculate_creator_stats,
    find_creator_by_handle,
    get_creators,
)

# from services.youtube_backend_api import YouTubeBackendAPI
from views.creators import render_creator_preview, render_creators_page

logger = logging.getLogger(__name__)


def creators_route(request):
    """GET /creators - Creators discovery page."""

    # Get query parameters
    search = request.query_params.get("search", "")

    # ═══════════════════════════════════════════════════════════════
    # HANDLE SEARCH MODE (@username)
    # ═══════════════════════════════════════════════════════════════
    if search.strip().startswith("@"):
        handle = search.strip()
        logger.info(f"[HandleSearch] Detected handle search: {handle}")

        # Check if creator already exists in DB
        existing_creator = find_creator_by_handle(handle)

        if existing_creator:
            # Creator exists - redirect to show their card in results
            logger.info(
                f"[HandleSearch] Found existing creator: {existing_creator.get('channel_name')}"
            )
            # Fall through to normal search (will match by custom_url)
        else:
            # Creator not in DB - fetch from YouTube and show preview
            logger.info(f"[HandleSearch] Creator not found, fetching from YouTube...")

            try:
                youtube_api = None  # YouTubeBackendAPI()
                channel_info = None  # youtube_api.get_channel_by_handle(handle)

                if channel_info:
                    # Show preview card with "Add to Database" option
                    return render_creator_preview(
                        handle=handle,
                        channel_info=channel_info,
                        search=search,
                    )
                else:
                    # Handle not found on YouTube
                    logger.warning(
                        f"[HandleSearch] Handle not found on YouTube: {handle}"
                    )
                    # Fall through to show empty results with helpful message

            except Exception as e:
                logger.exception(f"[HandleSearch] Error fetching handle {handle}: {e}")
                # Fall through to normal search

    # ═══════════════════════════════════════════════════════════════
    # NORMAL SEARCH MODE
    # ═══════════════════════════════════════════════════════════════
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
    # Country filter
    country_filter = request.query_params.get(
        "country", "all"
    )  # all, or specific country code
    # Category filter (topic categories from YouTube)
    category_filter = request.query_params.get(
        "category", "all"
    )  # all, or specific category e.g. "Music"

    # Pagination parameters
    # NOTE: max(1, ...) clamps page to >= 1, so page < 1 is impossible.
    # The only boundary case we need to handle is page > total_pages (below).
    try:
        page = max(1, int(request.query_params.get("page", 1)))
    except (TypeError, ValueError):
        page = 1

    try:
        per_page = max(1, min(100, int(request.query_params.get("per_page", 50))))
    except (TypeError, ValueError):
        per_page = 50

    # Fetch creators (db.py handles all logic)
    # Using pagination to keep page load performant as creator count grows
    result = get_creators(
        search=search,
        sort=sort,
        grade_filter=grade_filter,
        language_filter=language_filter,
        activity_filter=activity_filter,
        age_filter=age_filter,
        country_filter=country_filter,
        category_filter=category_filter,
        limit=per_page,
        offset=(page - 1) * per_page,
        return_count=True,
    )

    creators = result.creators
    total_count = result.total_count

    # Calculate total pages
    total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1

    # Handle out-of-range pages: redirect to last valid page
    # This prevents confusing "no results" UI when page > total_pages
    if total_count > 0 and page > total_pages:
        # Build redirect URL to last page with all filters preserved
        redirect_params = {
            "search": search,
            "sort": sort,
            "grade": grade_filter,
            "language": language_filter,
            "activity": activity_filter,
            "age": age_filter,
            "country": country_filter,
            "category": category_filter,
            "page": str(total_pages),
            "per_page": str(per_page),
        }
        redirect_url = f"/creators?{urlencode(redirect_params)}"
        return RedirectResponse(redirect_url, status_code=303)

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
        country_filter=country_filter,
        category_filter=category_filter,
        stats=stats,
        page=page,
        per_page=per_page,
        total_count=total_count,
        total_pages=total_pages,
    )
