"""
Lists route — curated creator list pages.
"""

import logging

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
