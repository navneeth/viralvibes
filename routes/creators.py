"""
Creators routes - browse and filter discovered YouTube creators.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode

from fasthtml.common import *
from fasthtml.common import RedirectResponse
from monsterui.all import *

from db import (
    add_creator_by_handle,
    add_favourite_creator,
    calculate_creator_stats,
    find_creator_by_handle,
    get_cached_category_box_stats,
    get_creator_add_request_status,
    get_creator_hero_stats,
    get_creator_rank,
    get_creator_stats,
    get_creators,
    get_user_favourite_creator_ids,
    is_creator_favourited,
    queue_creator_add_request,
    remove_favourite_creator,
)
from db_lists import (
    get_lists_meta,
    get_top_categories_with_counts,
    get_top_countries_with_counts,
    get_top_languages_with_counts,
)

# from services.youtube_backend_api import YouTubeBackendAPI
from controllers.auth_routes import require_auth
from views.creators import (
    render_add_creator_result,
    render_add_creator_status_result,
    render_creator_preview,
    render_creator_profile_page,
    render_creators_page,
)

logger = logging.getLogger(__name__)


def creators_route(request, is_authenticated: bool = False, user_id: str | None = None):
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
                    logger.warning(f"[HandleSearch] Handle not found on YouTube: {handle}")
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
    activity_filter = request.query_params.get("activity", "all")  # all, active, dormant
    # Age filter (by channel age)
    age_filter = request.query_params.get("age", "all")  # all, new, established, veteran
    # Country filter
    country_filter = request.query_params.get("country", "all")  # all, or specific country code
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

    # page_stats supplies grade_counts and per-page distributions (fast).
    # hero_stats overrides the scalar totals (total_creators, total_countries,
    # total_languages, growing_creators, premium_creators) with exact DB-side
    # counts from the RPC — same source of truth as the /lists page.
    # The three top_* lists are then replaced with full-DB RPC results so the
    # hero flags and filter dropdowns are consistent across both pages.
    page_stats = calculate_creator_stats(creators)
    hero_stats = get_creator_hero_stats()
    stats = {**page_stats, **hero_stats}

    # Bug fix: calculate_creator_stats() sets total_creators = len(creators) = page
    # size (e.g. 50). If get_creator_hero_stats() RPC fails and returns {}, the merge
    # keeps that page-level value, making the unfiltered hero show "50 creators".
    # total_count from get_creators(return_count=True) is always the authoritative
    # global count when no filters are active, so we use it as the fallback.
    # We must NOT override when filters are active: total_count is then a filtered
    # subset count, and writing it into stats["total_creators"] would make the hero
    # display the filtered total as if it were the global total.
    #
    # Derived from request.query_params — the single source of truth — so adding a
    # new filter param only needs wiring in one place (the extraction block above).
    _FILTER_DEFAULTS = {
        "grade": "all",
        "language": "all",
        "activity": "all",
        "age": "all",
        "country": "all",
        "category": "all",
    }
    has_active_filters = bool(search) or any(
        request.query_params.get(k, default) != default for k, default in _FILTER_DEFAULTS.items()
    )
    if not has_active_filters:
        stats["total_creators"] = total_count
    stats["top_countries"] = get_top_countries_with_counts(limit=8)
    stats["top_languages"] = get_top_languages_with_counts(limit=5)
    stats["top_categories"] = get_top_categories_with_counts(limit=4)
    stats["total_categories"] = get_lists_meta().get("total_categories", 0)

    # Fetch the authenticated user's favourites so cards can show the heart state.
    # One lightweight query (only creator_id column) per page load — acceptable.
    favourite_ids: set[str] = (
        get_user_favourite_creator_ids(user_id) if is_authenticated and user_id else set()
    )

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
        is_authenticated=is_authenticated,
        favourite_ids=favourite_ids,
    )


def _get_context_ranks(creator: dict) -> dict:
    """
    Compute this creator's subscriber rank in their country, language, and
    primary category — each is a single server-side COUNT (no row transfer).
    All three queries run in parallel via a thread pool.

    Returns:
        dict with keys country_rank, language_rank, category_rank.
        Each value is an int (1-based position) or None when unavailable.
    """
    subs = int(creator.get("current_subscribers") or 0)
    country = creator.get("country_code", "")
    language = creator.get("default_language", "")
    category = creator.get("primary_category", "")

    tasks: dict[str, tuple[str, str]] = {}
    if subs and country:
        tasks["country_rank"] = ("country_code", country)
    if subs and language:
        tasks["language_rank"] = ("default_language", language)
    if subs and category:
        tasks["category_rank"] = ("primary_category", category)

    result: dict[str, int | None] = {
        "country_rank": None,
        "language_rank": None,
        "category_rank": None,
    }
    if not tasks:
        return result

    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = {
            pool.submit(get_creator_rank, subs, key, val): name
            for name, (key, val) in tasks.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                result[name] = future.result()
            except Exception:
                logger.exception("[CreatorProfile] Rank query failed for %s", name)

    return result


def creator_profile_route(request, creator_id: str, user_id: str | None = None):
    """
    GET /creator/{creator_id} — Full profile page for a single creator.

    Args:
        request:    Starlette request (used for back_url via Referer/from param).
        creator_id: Creator UUID (primary key of the creators table).
        user_id:    Optional user UUID from session.  When provided the page
                    will show the correct initial state for the favourite button.

    Returns:
        FT component with full profile, or a 404 message Div.
    """
    creator = get_creator_stats(creator_id)

    if not creator:
        logger.warning(f"[CreatorProfile] Creator not found: {creator_id}")
        return Div(
            UkIcon("user-x", cls="w-12 h-12 text-muted-foreground mx-auto mb-4"),
            H2("Creator not found", cls="text-2xl font-bold text-foreground mb-2"),
            P(
                f"No creator with ID {creator_id!r} exists in the database.",
                cls="text-muted-foreground",
            ),
            A(
                "← Back to Creators",
                href="/creators",
                cls="mt-4 inline-flex items-center text-sm font-medium text-primary hover:underline",
            ),
            cls="max-w-2xl mx-auto px-4 py-24 text-center",
        )

    back_url = request.query_params.get("from", "/creators")
    context_ranks = _get_context_ranks(creator)
    category_stats = get_cached_category_box_stats(creator.get("primary_category", ""))
    is_fav = is_creator_favourited(user_id, creator_id) if user_id else False

    return render_creator_profile_page(
        creator,
        back_url=back_url,
        context_ranks=context_ranks,
        category_stats=category_stats,
        is_favourited=is_fav,
    )


def toggle_favourite_route(request, sess, creator_id: str):
    """
    POST /creator/{creator_id}/favourite
    HTMX endpoint — toggles the favourite state for an authenticated user.

    If the creator is not currently favourited it is added; if it is already
    favourited it is removed.  Returns the updated FavouriteButton fragment
    so HTMX can swap it in-place.

    AUTH: Requires a valid session (``sess['auth']``).  Returns 401 when the
    user is not logged in.
    """
    from views.creators import render_favourite_button

    auth = sess.get("auth") if sess else None
    user_id = sess.get("user_id") if sess else None
    auth_error = require_auth(auth)
    if auth_error:
        return Response("Authentication required", status_code=401)
    # In test mode require_auth is skipped; use a sentinel user_id so tests
    # that supply a session work, and tests that don't still get a predictable id.
    import os as _os

    if not user_id and _os.getenv("TESTING") == "1":
        user_id = "test-user-id"

    currently_favourited = is_creator_favourited(user_id, creator_id)
    if currently_favourited:
        remove_favourite_creator(user_id, creator_id)
        new_state = False
    else:
        add_favourite_creator(user_id, creator_id)
        new_state = True

    logger.info(
        "[Favourites] User %s toggled creator %s → %s",
        user_id,
        creator_id,
        "on" if new_state else "off",
    )
    return render_favourite_button(creator_id, is_favourited=new_state)


async def creator_request_route(request, sess):
    """
    POST /creators/request
    HTMX endpoint — queues a creator add request submitted by the user.
    Accepts form field ``q`` (@handle or UC channel ID).
    Returns an inline HTMX partial (no full page reload).
    """
    auth = sess.get("auth") if sess else None
    auth_error = require_auth(auth)
    if auth_error:
        return render_add_creator_result(
            success=False,
            message="You must be logged in to submit a creator.",
        )

    user_id = sess.get("user_id") if sess else None

    # Read form body
    try:
        form = await request.form()
        q = form.get("q", "").strip()
    except Exception:
        q = ""

    if not q:
        return render_add_creator_result(
            success=False,
            message="Please enter a @handle or channel ID.",
        )

    ok, message, creator_id = queue_creator_add_request(q, user_id)

    if ok:
        return render_add_creator_result(
            success=True,
            message="We'll update this notice automatically once the creator is added.",
            input_query=q,
        )

    return render_add_creator_result(success=False, message=message, creator_id=creator_id or "")


async def creator_add_status_route(request, sess):
    """
    GET /creators/add-status?q=@handle
    HTMX polling endpoint — returns an inline partial with the current job
    status for the given creator add request.

    The success card in ``render_add_creator_result`` polls this endpoint every
    3 s and replaces itself once the job reaches a terminal state.
    """
    auth = sess.get("auth") if sess else None
    auth_error = require_auth(auth)
    if auth_error:
        return render_add_creator_status_result(status="failed")

    q = request.query_params.get("q", "").strip()
    if not q:
        return render_add_creator_status_result(status="failed")

    result = get_creator_add_request_status(q)
    if result is None:
        # None means invalid input or missing Supabase client — terminal failure.
        return render_add_creator_status_result(status="failed")

    return render_add_creator_status_result(
        status=result["status"],
        creator_id=result.get("creator_id") or "",
        input_query=q,
    )
