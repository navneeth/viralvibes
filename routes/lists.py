"""
Lists route — curated creator list pages.
"""

import logging

from fasthtml.common import Div

from db import get_creators
from db_lists import (
    get_category_groups,
    get_country_groups,
    get_lists_meta,
    get_most_active_creators,
    get_rising_creators,
    get_top_rated_creators,
    get_veteran_creators,
)
from views.lists import (
    render_lists_page,
    render_more_categories,
    render_more_countries,
    render_country_detail_page,
    render_country_creators_rows,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Tuning constants — how many groups/creators to show initially vs on load-more
# ─────────────────────────────────────────────────────────────────────────────
INITIAL_GROUPS = 8  # country / category cards visible on first load
LOAD_MORE_STEP = 8  # how many more to load per "Show more" click
CREATORS_PER_GROUP = 5  # top creators shown inside each group card


def lists_route(request):
    """
    GET /lists — Curated creator lists page.

    Loads all tab data upfront to support UIkit's client-side tab switcher.
    Also fetches live meta (country + category counts) to drive dynamic
    tab badges and load-more controls.
    """
    active_tab = request.query_params.get("tab", "top-rated")

    # ── Fetch live DB meta (one combined aggregation scan) ──────────────────
    meta = get_lists_meta()

    # ── Load data for ALL tabs upfront ──────────────────────────────────────
    tab_data = {}

    # Top Rated: quality-sorted creators
    tab_data["top_rated"] = get_top_rated_creators(limit=20)

    # Most Active: upload-frequency leaders
    tab_data["most_active"] = get_most_active_creators(limit=20)

    # By Country: first page of groups (offset=0)
    tab_data["country_rankings"] = get_country_groups(
        offset=0,
        limit=INITIAL_GROUPS,
        creators_per_group=CREATORS_PER_GROUP,
    )
    tab_data["total_countries"] = meta["total_countries"]

    # By Category: first page of groups (offset=0)
    tab_data["category_rankings"] = get_category_groups(
        offset=0,
        limit=INITIAL_GROUPS,
        creators_per_group=CREATORS_PER_GROUP,
    )
    tab_data["total_categories"] = meta["total_categories"]

    # Rising Stars: fastest growth rate
    tab_data["rising"] = get_rising_creators(limit=20)

    # Veterans: 10+ year channels
    tab_data["veterans"] = get_veteran_creators(limit=20)

    return render_lists_page(active_tab=active_tab, tab_data=tab_data)


def lists_more_countries_route(request):
    """
    GET /lists/more-countries?offset=N&total=N
    HTMX partial — returns the next batch of country group cards.

    ``total`` is forwarded from the initial page load so that we avoid
    re-running the 50k-row get_lists_meta() scan on every click.
    """
    try:
        offset = int(request.query_params.get("offset", INITIAL_GROUPS))
    except (TypeError, ValueError):
        offset = INITIAL_GROUPS

    # Prefer the precomputed total forwarded from the client; only fall back
    # to a fresh DB scan when it is missing or invalid.
    try:
        total = int(request.query_params["total"])
    except (KeyError, TypeError, ValueError):
        total = get_lists_meta()["total_countries"]

    groups = get_country_groups(
        offset=offset,
        limit=LOAD_MORE_STEP,
        creators_per_group=CREATORS_PER_GROUP,
    )

    next_offset = offset + len(groups)
    has_more = next_offset < total

    return render_more_countries(
        groups, next_offset=next_offset, has_more=has_more, total=total
    )


def lists_more_categories_route(request):
    """
    GET /lists/more-categories?offset=N&total=N
    HTMX partial — returns the next batch of category group cards.

    ``total`` is forwarded from the initial page load so that we avoid
    re-running the 50k-row get_lists_meta() scan on every click.
    """
    try:
        offset = int(request.query_params.get("offset", INITIAL_GROUPS))
    except (TypeError, ValueError):
        offset = INITIAL_GROUPS

    try:
        total = int(request.query_params["total"])
    except (KeyError, TypeError, ValueError):
        total = get_lists_meta()["total_categories"]

    groups = get_category_groups(
        offset=offset,
        limit=LOAD_MORE_STEP,
        creators_per_group=CREATORS_PER_GROUP,
    )

    next_offset = offset + len(groups)
    has_more = next_offset < total

    return render_more_categories(
        groups, next_offset=next_offset, has_more=has_more, total=total
    )


# ─────────────────────────────────────────────────────────────────────────────
# Country Detail Page — GET /lists/country/{country_code}
# ─────────────────────────────────────────────────────────────────────────────

DETAIL_PAGE_LIMIT = 20  # Creators per page on detail view


def country_detail_route(request, country_code: str):
    """
    GET /lists/country/{country_code} — Detailed country-wise creator rankings.

    Fetches all creators from a specific country with pagination.

    Args:
        request: Request object
        country_code: ISO 3166-1 alpha-2 country code (e.g., "US", "JP")

    Returns:
        FT component with detailed creator list
    """
    # Normalize country code to uppercase
    country_code = country_code.upper()

    # Get page number from query params (default 1)
    try:
        page = int(request.query_params.get("page", "1"))
        if page < 1:
            page = 1
    except (TypeError, ValueError):
        page = 1

    # Fetch creators for this country with pagination
    result = get_creators(
        country_filter=country_code,
        sort="subscribers",  # Sort by subscribers on detail page
        limit=DETAIL_PAGE_LIMIT,
        offset=(page - 1) * DETAIL_PAGE_LIMIT,
        return_count=True,
    )

    creators = result.creators if result else []
    total_count = result.total_count if result else 0

    # Calculate total pages
    total_pages = (
        (total_count + DETAIL_PAGE_LIMIT - 1) // DETAIL_PAGE_LIMIT
        if total_count > 0
        else 1
    )

    return render_country_detail_page(
        country_code=country_code,
        creators=creators,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
    )


def country_detail_more_route(request):
    """
    GET /lists/country/{country_code}/more?page=N
    HTMX partial — returns the next batch of creator rows.

    Args:
        request: Request object with country_code as optional attribute or query param

    Returns:
        FT component with creator rows
    """
    # Extract country_code from path parameter (set by main.py) or query param
    country_code = getattr(request, "country_code", None) or request.query_params.get(
        "country", ""
    )
    country_code = country_code.upper() if country_code else ""

    if not country_code:
        logger.warning("No country_code provided to country_detail_more_route")
        return Div("Error: Invalid country", cls="text-red-500")

    # Get page number from query params
    try:
        page = int(request.query_params.get("page", "1"))
        if page < 1:
            page = 1
    except (TypeError, ValueError):
        page = 1

    # Fetch creators for this country with pagination
    result = get_creators(
        country_filter=country_code,
        sort="subscribers",
        limit=DETAIL_PAGE_LIMIT,
        offset=(page - 1) * DETAIL_PAGE_LIMIT,
        return_count=True,
    )

    creators = result.creators if result else []
    total_count = result.total_count if result else 0

    # Calculate total pages
    total_pages = (
        (total_count + DETAIL_PAGE_LIMIT - 1) // DETAIL_PAGE_LIMIT
        if total_count > 0
        else 1
    )

    # Return just the creator rows (no header/footer)
    return render_country_creators_rows(
        country_code=country_code,
        creators=creators,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
    )
