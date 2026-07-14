"""
Lists route — curated creator list pages.
"""

import logging
from urllib.parse import unquote

from fasthtml.common import Div

from db import get_creators, get_user_favourite_list_keys
from db_lists import (
    _count_distinct_languages,
    get_category_groups,
    get_country_groups,
    get_language_groups,
    get_lists_meta,
    get_most_active_creators,
    get_new_channels,
    get_niche_heatmap_data,
    get_rising_creators,
    get_top_categories_with_counts,
    get_top_countries_with_counts,
    get_top_languages_with_counts,
    get_topic_category_country_creators,
    get_top_rated_creators,
    get_topic_category_creators,
    get_veteran_creators,
    merge_language_variants,
    resolve_category_slug,
)
from views.lists import (
    render_lists_page,
    render_more_categories,
    render_more_countries,
    render_more_languages,
    render_categories_explorer_page,
    render_countries_explorer_page,
    render_languages_explorer_page,
    render_country_detail_page,
    render_country_creators_rows,
    render_category_detail_page,
    render_category_creators_rows,
    render_language_detail_page,
    render_language_creators_rows,
    render_ranking_detail_page,
    render_ranking_creators_rows,
    _unslugify,
)
from services.rankings import resolve_country_slug, resolve_ranking_category_slug

logger = logging.getLogger(__name__)


def _auth_context(request) -> tuple[str | None, bool, frozenset]:
    """Extract (user_id, authenticated, fav_keys) from a request session."""
    user_id = request.session.get("user_id") if hasattr(request, "session") else None
    return user_id, bool(user_id), get_user_favourite_list_keys(user_id) if user_id else frozenset()


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

    # Auth context for heart buttons
    user_id, authenticated, fav_keys = _auth_context(request)

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

    # New Channels: created within the last year, sorted by engagement
    tab_data["new_channels"] = get_new_channels(limit=20)

    # Niche Heat Map: category-level aggregated momentum
    tab_data["heatmap"] = get_niche_heatmap_data()

    # By Language: first page of groups (offset=0)
    tab_data["language_rankings"] = get_language_groups(
        offset=0,
        limit=INITIAL_GROUPS,
        creators_per_group=CREATORS_PER_GROUP,
    )
    # meta["total_languages"] comes from migration 003.  On older DB schemas
    # that predate the RPC column, fall back to _count_distinct_languages() —
    # a zero-row-transfer COUNT(DISTINCT) query that has no row cap.
    tab_data["total_languages"] = meta.get("total_languages") or _count_distinct_languages()

    return render_lists_page(
        active_tab=active_tab,
        tab_data=tab_data,
        fav_keys=fav_keys,
        authenticated=authenticated,
    )


def lists_more_countries_route(request):
    """
    GET /lists/more-countries?offset=N&total=N
    HTMX partial — returns the next batch of country group cards.

    ``total`` is forwarded from the initial page load so that we avoid
    re-running the 50k-row get_lists_meta() scan on every click.
    """
    user_id, authenticated, fav_keys = _auth_context(request)

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
        groups,
        next_offset=next_offset,
        has_more=has_more,
        total=total,
        fav_keys=fav_keys,
        authenticated=authenticated,
    )


def lists_more_categories_route(request):
    """
    GET /lists/more-categories?offset=N&total=N
    HTMX partial — returns the next batch of category group cards.

    ``total`` is forwarded from the initial page load so that we avoid
    re-running the 50k-row get_lists_meta() scan on every click.
    """
    user_id, authenticated, fav_keys = _auth_context(request)

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
        groups,
        next_offset=next_offset,
        has_more=has_more,
        total=total,
        fav_keys=fav_keys,
        authenticated=authenticated,
    )


def lists_more_languages_route(request):
    """
    GET /lists/more-languages?offset=N&total=N
    HTMX partial — returns the next batch of language group cards.

    ``total`` is forwarded from the initial page load so that we avoid
    re-running the get_lists_meta() scan on every click.
    """
    user_id, authenticated, fav_keys = _auth_context(request)

    try:
        offset = int(request.query_params.get("offset", INITIAL_GROUPS))
    except (TypeError, ValueError):
        offset = INITIAL_GROUPS

    try:
        total = int(request.query_params["total"])
    except (KeyError, TypeError, ValueError):
        total = get_lists_meta().get("total_languages") or _count_distinct_languages()

    groups = get_language_groups(
        offset=offset,
        limit=LOAD_MORE_STEP,
        creators_per_group=CREATORS_PER_GROUP,
    )

    next_offset = offset + len(groups)
    has_more = next_offset < total

    return render_more_languages(
        groups,
        next_offset=next_offset,
        has_more=has_more,
        total=total,
        fav_keys=fav_keys,
        authenticated=authenticated,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Country Detail Page — GET /lists/country/{country_code}
# ─────────────────────────────────────────────────────────────────────────────

DETAIL_PAGE_LIMIT = 20  # Creators per page on detail view


def _parse_page(request) -> int:
    """Parse and clamp the ?page= query param to a safe 1-based integer."""
    try:
        return max(1, int(request.query_params.get("page", "1")))
    except (TypeError, ValueError):
        return 1


def _fetch_country_page(country_code: str, page: int) -> tuple[list, int, int]:
    """
    Fetch one page of creators for a country detail view.

    Shared by ``country_detail_route`` and ``country_detail_more_route`` so
    the query parameters, sort order, and pagination formula stay in sync.

    Args:
        country_code: Normalised (uppercase) ISO 3166-1 alpha-2 code.
        page: 1-based page number.

    Returns:
        ``(creators, total_count, total_pages)`` tuple.
    """
    result = get_creators(
        country_filter=country_code,
        sort="subscribers",
        limit=DETAIL_PAGE_LIMIT,
        offset=(page - 1) * DETAIL_PAGE_LIMIT,
        return_count=True,
    )
    creators = result.creators if result else []
    total_count = result.total_count if result else 0
    total_pages = (
        (total_count + DETAIL_PAGE_LIMIT - 1) // DETAIL_PAGE_LIMIT if total_count > 0 else 1
    )
    return creators, total_count, total_pages


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
    country_code = country_code.upper()

    page = _parse_page(request)

    creators, total_count, total_pages = _fetch_country_page(country_code, page)

    page_content = render_country_detail_page(
        country_code=country_code,
        creators=creators,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        page_size=DETAIL_PAGE_LIMIT,
    )
    return page_content, total_count


def country_detail_more_route(request):
    """
    GET /lists/country/{country_code}/more?page=N
    HTMX partial — returns the next batch of creator rows.

    ``country_code`` is always injected as ``request.country_code`` by the
    ``@rt`` handler in main.py before this function is called.

    Returns:
        FT component with creator rows
    """
    country_code = getattr(request, "country_code", "")
    country_code = country_code.upper() if country_code else ""

    if not country_code:
        logger.warning("No country_code provided to country_detail_more_route")
        return Div("Error: Invalid country", cls="text-red-500")

    page = _parse_page(request)

    creators, total_count, total_pages = _fetch_country_page(country_code, page)

    return render_country_creators_rows(
        country_code=country_code,
        creators=creators,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        page_size=DETAIL_PAGE_LIMIT,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Category Detail Page — GET /lists/category/{category_slug}
# ─────────────────────────────────────────────────────────────────────────────


def _fetch_category_page(category_slug: str, page: int) -> tuple[list, int, int, str]:
    """
    Fetch one page of creators for a category detail view.

    Shared by ``category_detail_route`` and ``category_detail_more_route`` so
    the query parameters, sort order, and pagination formula stay in sync.

    The URL slug is resolved back to the canonical topic category name via
    ``resolve_category_slug`` (counts/cards use ``topic_categories``, not
    ``primary_category``).  Creators are fetched with the same ILIKE logic as
    ``get_top_creators_by_categories``.

    Args:
        category_slug: URL slug from the path (may be hyphenated or encoded).
        page: 1-based page number.

    Returns:
        ``(creators, total_count, total_pages, category_name)`` tuple.
    """
    decoded_slug = unquote(category_slug)
    category_name = resolve_category_slug(decoded_slug) or _unslugify(decoded_slug)
    result = get_topic_category_creators(
        category_name,
        limit=DETAIL_PAGE_LIMIT,
        offset=(page - 1) * DETAIL_PAGE_LIMIT,
        return_count=True,
    )
    creators = result.creators if result else []
    total_count = result.total_count if result else 0
    total_pages = (
        (total_count + DETAIL_PAGE_LIMIT - 1) // DETAIL_PAGE_LIMIT if total_count > 0 else 1
    )
    return creators, total_count, total_pages, category_name


def _fetch_ranking_page(
    category_slug: str,
    country_slug: str,
    page: int,
) -> tuple[list, int, int, str, str | None]:
    """Fetch creators for a public category/country ranking landing page."""
    decoded_slug = unquote(category_slug)
    category_name = (
        resolve_ranking_category_slug(decoded_slug)
        or resolve_category_slug(decoded_slug)
        or _unslugify(decoded_slug)
    )
    country_code = resolve_country_slug(country_slug)
    if not country_code:
        return [], 0, 1, category_name, None

    result = get_topic_category_country_creators(
        category_name,
        country_code,
        limit=DETAIL_PAGE_LIMIT,
        offset=(page - 1) * DETAIL_PAGE_LIMIT,
        return_count=True,
    )
    creators = result.creators if result else []
    total_count = result.total_count if result else 0
    total_pages = (
        (total_count + DETAIL_PAGE_LIMIT - 1) // DETAIL_PAGE_LIMIT if total_count > 0 else 1
    )
    return creators, total_count, total_pages, category_name, country_code


def ranking_detail_route(request, category_slug: str, country_slug: str):
    """GET /rankings/{category}/{country} — SEO category/country ranking page."""
    category_slug = category_slug.lower()
    country_slug = country_slug.lower()
    page = _parse_page(request)

    creators, total_count, total_pages, category_name, country_code = _fetch_ranking_page(
        category_slug,
        country_slug,
        page,
    )

    return render_ranking_detail_page(
        category_slug=category_slug,
        country_slug=country_slug,
        category_name=category_name,
        country_code=country_code,
        creators=creators,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        page_size=DETAIL_PAGE_LIMIT,
    )


def ranking_detail_more_route(request):
    """GET /rankings/{category}/{country}/more?page=N — HTMX creator rows."""
    category_slug = getattr(request, "category_slug", "")
    country_slug = getattr(request, "country_slug", "")
    category_slug = category_slug.lower() if category_slug else ""
    country_slug = country_slug.lower() if country_slug else ""

    if not category_slug or not country_slug:
        logger.warning("Missing category_slug/country_slug for ranking_detail_more_route")
        return Div("Error: Invalid ranking", cls="text-red-500")

    page = _parse_page(request)
    creators, total_count, total_pages, category_name, country_code = _fetch_ranking_page(
        category_slug,
        country_slug,
        page,
    )

    return render_ranking_creators_rows(
        category_slug=category_slug,
        country_slug=country_slug,
        category_name=category_name,
        country_code=country_code,
        creators=creators,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        page_size=DETAIL_PAGE_LIMIT,
    )


def category_detail_route(request, category_slug: str):
    """
    GET /lists/category/{category_slug} — Detailed category-wise creator rankings.

    Fetches all creators in a specific topic category with pagination.

    Args:
        request: Request object
        category_slug: Hyphen-separated slug derived from the category display
                       name (e.g. ``"music"``, ``"video-game-culture"``)

    Returns:
        FT component with detailed creator list
    """
    category_slug = category_slug.lower()

    page = _parse_page(request)

    creators, total_count, total_pages, category_name = _fetch_category_page(category_slug, page)

    page_content = render_category_detail_page(
        category_slug=category_slug,
        category_name=category_name,
        creators=creators,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        page_size=DETAIL_PAGE_LIMIT,
    )
    return page_content, total_count


def category_detail_more_route(request):
    """
    GET /lists/category/{category_slug}/more?page=N
    HTMX partial — returns the next batch of creator rows.

    ``category_slug`` is always injected as ``request.category_slug`` by the
    ``@rt`` handler in main.py before this function is called.

    Returns:
        FT component with creator rows
    """
    category_slug = getattr(request, "category_slug", "")
    category_slug = category_slug.lower() if category_slug else ""

    if not category_slug:
        logger.warning("No category_slug provided to category_detail_more_route")
        return Div("Error: Invalid category", cls="text-red-500")

    page = _parse_page(request)

    creators, total_count, total_pages, category_name = _fetch_category_page(category_slug, page)

    return render_category_creators_rows(
        category_slug=category_slug,
        category_name=category_name,
        creators=creators,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        page_size=DETAIL_PAGE_LIMIT,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Language Detail Page — GET /lists/language/{language_code}
# ─────────────────────────────────────────────────────────────────────────────


def _fetch_language_page(language_code: str, page: int) -> tuple[list, int, int]:
    """
    Fetch one page of creators for a language detail view.

    Shared by ``language_detail_route`` and ``language_detail_more_route``.

    Args:
        language_code: Lowercase ISO 639-1 two-letter code (e.g. ``"en"``).
        page: 1-based page number.

    Returns:
        ``(creators, total_count, total_pages)`` tuple.
    """
    result = get_creators(
        language_filter=language_code,
        sort="subscribers",
        limit=DETAIL_PAGE_LIMIT,
        offset=(page - 1) * DETAIL_PAGE_LIMIT,
        return_count=True,
    )
    creators = result.creators if result else []
    total_count = result.total_count if result else 0
    total_pages = (
        (total_count + DETAIL_PAGE_LIMIT - 1) // DETAIL_PAGE_LIMIT if total_count > 0 else 1
    )
    return creators, total_count, total_pages


def language_detail_route(request, language_code: str):
    """
    GET /lists/language/{language_code} — Ranked creators for a content language.

    Args:
        request: Request object.
        language_code: ISO 639-1 two-letter language code (e.g. ``"en"``, ``"ja"``).

    Returns:
        FT component with detailed creator list.
    """
    language_code = language_code.lower()

    page = _parse_page(request)

    creators, total_count, total_pages = _fetch_language_page(language_code, page)

    return render_language_detail_page(
        language_code=language_code,
        creators=creators,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        page_size=DETAIL_PAGE_LIMIT,
    )


def language_detail_more_route(request):
    """
    GET /lists/language/{language_code}/more?page=N
    HTMX partial — returns the next batch of creator rows.

    ``language_code`` is always injected as ``request.language_code`` by the
    ``@rt`` handler in main.py before this function is called.

    Returns:
        FT component with creator rows.
    """
    language_code = getattr(request, "language_code", "")
    language_code = language_code.lower() if language_code else ""

    if not language_code:
        logger.warning("No language_code provided to language_detail_more_route")
        return Div("Error: Invalid language", cls="text-red-500")

    page = _parse_page(request)

    creators, total_count, total_pages = _fetch_language_page(language_code, page)

    return render_language_creators_rows(
        language_code=language_code,
        creators=creators,
        page=page,
        total_pages=total_pages,
        total_count=total_count,
        page_size=DETAIL_PAGE_LIMIT,
    )


def categories_explorer_route():
    """
    GET /lists/categories — Visual bar-chart explorer of all content categories.

    Fetches up to 2 000 categories with creator counts (server-side RPC,
    zero row transfer) and delegates rendering to
    render_categories_explorer_page().
    """
    categories = get_top_categories_with_counts(limit=2000)
    return render_categories_explorer_page(categories)


def countries_explorer_route():
    """
    GET /lists/countries — Visual bar-chart explorer of all countries.

    Fetches all countries with creator counts (server-side RPC,
    zero row transfer) and delegates rendering to
    render_countries_explorer_page().
    """
    countries = get_top_countries_with_counts(limit=500)  # All countries
    return render_countries_explorer_page(countries)


def languages_explorer_route():
    """
    GET /lists/languages \u2014 Visual bar-chart explorer of all content languages.

    Fetches all languages with creator counts (server-side RPC,
    zero row transfer), merges BCP-47 region variants into their base
    ISO 639-1 language code (e.g. en + en-GB + en-IN → en), then
    delegates rendering to render_languages_explorer_page().
    """
    raw = get_top_languages_with_counts(limit=500)
    languages = merge_language_variants(raw)
    return render_languages_explorer_page(languages)
