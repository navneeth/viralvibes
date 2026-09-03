"""
Creators routes - browse and filter discovered YouTube creators.
"""

import logging
import os
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode

import pycountry

from fasthtml.common import *
from fasthtml.common import RedirectResponse
from monsterui.all import *

from db import (
    add_creator_by_handle,
    add_favourite_creator,
    calculate_creator_stats,
    CreatorsResult,
    find_creator_by_handle,
    get_cached_category_box_stats,
    get_category_leaderboard,
    get_category_peer_benchmarks,
    get_creator_add_request_status,
    get_creator_hero_stats,
    get_creator_rank,
    get_creator_stats,
    get_creators,
    get_embedding_peers,
    get_user_favourite_creator_ids,
    is_creator_favourited,
    queue_creator_add_request,
    remove_favourite_creator,
)
from db_lists import (
    get_top_categories_with_counts,
    get_top_countries_with_counts,
    get_top_languages_with_counts,
    suggest_primary_categories,
    TOTAL_TOPIC_CATEGORIES,
)

# from services.youtube_backend_api import YouTubeBackendAPI
from controllers.auth_routes import require_auth, safe_local_return_url
from views.compare import render_compare_page, render_compare_pick_page
from views.creators import (
    creator_profile_url,
    render_add_creator_result,
    render_add_creator_status_result,
    render_creator_preview,
    render_creator_profile_page,
    render_creators_page,
    render_filter_suggestions,
    get_topic_category_emoji,
)
from utils.blueprint import signals_from_row, score_all_actions
from views.blueprint import render_blueprint_page
from utils.creator_metrics import get_country_flag, get_language_emoji, get_language_name

logger = logging.getLogger(__name__)


def _parse_compare_id(raw: str | None) -> str:
    """Return a normalised UUID string from the ?a= query param, or '' if invalid.

    Validates the value is a well-formed UUID so arbitrary user input is never
    propagated into rendered URLs or downstream DB calls.
    """
    if not raw:
        return ""
    try:
        return str(uuid.UUID(raw.strip()))
    except (ValueError, AttributeError):
        return ""


# Number of entries shown in the hero sidebar strips and filter dropdowns.
# Defined once so both the handle_not_found path and the parallel fan-out
# path always request the same slice.
_HERO_COUNTRIES_LIMIT = 8
_HERO_LANGUAGES_LIMIT = 7
_HERO_CATEGORIES_LIMIT = 9


@dataclass(frozen=True)
class CreatorProfileResult:
    """Bundle a rendered profile with the creator row used for SEO tags."""

    body: Any  # FT element tree from render_creator_profile_page()
    creator: dict


# ---------------------------------------------------------------------------
# Natural-language country extraction
# ---------------------------------------------------------------------------
# Matches phrases like:
#   "MKBHD from Germany"     → search="MKBHD",  country="DE"
#   "gaming creators in Japan" → search="gaming creators", country="JP"
#   "DJ based in Brazil"     → search="DJ",    country="BR"
#
# The pattern is intentionally conservative:
#   - Only triggers on explicit prepositions (from / in / based in)
#   - Requires pycountry to confirm the candidate is a real country name
#   - Does nothing when the user has already set a country_filter explicitly
# ---------------------------------------------------------------------------
_COUNTRY_PREP_RE = re.compile(
    # Capture country name at end of string, after a locating preposition.
    # Character class covers:
    #   a-z A-Z           — ASCII letters
    #   \u00C0-\u017E     — Latin Extended (accented: é, ô, ü, ñ, è, etc.)
    #   ' \u2019 . -      — apostrophes, dots, hyphens (Côte d'Ivoire, Guinea-Bissau, U.S.)
    #   space             — multi-word names (South Korea, New Zealand)
    # Trailing punctuation (!?.,) after the country name is stripped before lookup.
    "(?i)"
    r"\b(?:from|in|based\s+in)\s+"
    "([a-zA-Z\u00C0-\u017E'.\\-][a-zA-Z\u00C0-\u017E\u2019'.\\- ]{0,40}?)"
    r"\s*[!?.,]*\s*$"
)


def _extract_country(raw_search: str) -> tuple[str, str]:
    """
    Parse a country reference from the end of a free-text search string.

    Examples::

        _extract_country("mkbhd from germany")   → ("mkbhd", "DE")
        _extract_country("gaming in Japan")       → ("gaming", "JP")
        _extract_country("creators based in US") → ("creators", "US")
        _extract_country("music")                 → ("music", "all")
        _extract_country("gaming Brazil")         → ("gaming Brazil", "all")

    Returns:
        (cleaned_search, alpha_2_code_or_"all")
    """
    m = _COUNTRY_PREP_RE.search(raw_search)
    if not m:
        return raw_search, "all"

    candidate = m.group(1).strip()
    try:
        country = pycountry.countries.lookup(candidate)
    except LookupError:
        # pycountry raises LookupError when it can't match the candidate
        return raw_search, "all"

    # Strip the matched country phrase and any trailing whitespace/punctuation
    cleaned = raw_search[: m.start()].rstrip(", ").strip()
    # cleaned may be "" (e.g. bare "in Japan") — that's fine; empty search
    # means "all creators" and the country_filter does the scoping.
    return cleaned, country.alpha_2


# Whether the app is running under the test suite (set by GitHub Actions / pytest)
_IS_TESTING = os.getenv("TESTING") == "1"


def creators_suggest_route(request):
    """
    GET /creators/suggest — HTMX typeahead for country/language/category filters.

    Query params (all sent automatically via hx-include="closest form"):
        dim      — "country" | "language" | "category"
        q        — user's search text (debounced 300ms by the Input element)
        sort, search, grade, language, activity, age, country, category
                 — current filter state, preserved in suggestion links

    Returns an HTMX partial (Div with clickable links, or empty Div).
    Each link navigates to /creators with the selected filter applied while
    keeping all other active filters intact — same as clicking a pill.
    """
    q = request.query_params.get("q", "").strip().lower()
    dim = request.query_params.get("dim", "")

    if not q or dim not in ("country", "language", "category"):
        return Div()

    # Current filter state for building correct result URLs
    current = {
        "sort": request.query_params.get("sort", "subscribers"),
        "search": request.query_params.get("search", ""),
        "grade": request.query_params.get("grade", "all"),
        "language": request.query_params.get("language", "all"),
        "activity": request.query_params.get("activity", "all"),
        "age": request.query_params.get("age", "all"),
        "country": request.query_params.get("country", "all"),
        "category": request.query_params.get("category", "all"),
    }

    suggestions = []  # list of (value, display_label, count)

    if dim == "country":
        for code, count in get_top_countries_with_counts(limit=200):
            country_obj = pycountry.countries.get(alpha_2=code.upper())
            country_name = country_obj.name.lower() if country_obj else ""
            if q in code.lower() or q in country_name:
                flag = get_country_flag(code) or "🏴"
                name = country_obj.name if country_obj else code.upper()
                suggestions.append((code, f"{flag} {name}", count))
                if len(suggestions) >= 8:
                    break

    elif dim == "language":
        for code, count in get_top_languages_with_counts(limit=300):
            name = get_language_name(code)
            if q in code.lower() or q in name.lower():
                emoji = get_language_emoji(code) or "🌐"
                suggestions.append((code, f"{emoji} {name}", count))
                if len(suggestions) >= 8:
                    break

    elif dim == "category":
        for cat_name, count in suggest_primary_categories(q, limit=8):
            emoji = get_topic_category_emoji(cat_name)
            short = cat_name.split("/")[-1].strip() or cat_name
            suggestions.append((cat_name, f"{emoji} {short}", count))

    return render_filter_suggestions(dim=dim, suggestions=suggestions, current=current)


def creators_route(request, is_authenticated: bool = False, user_id: str | None = None):
    """GET /creators - Creators discovery page."""

    # Get query parameters
    search = request.query_params.get("search", "")

    # ═══════════════════════════════════════════════════════════════
    # HANDLE SEARCH MODE (@username)
    # ═══════════════════════════════════════════════════════════════
    # Set when user searched a @handle that isn't in the DB — passed to the
    # view so it can show the "add this creator" banner even when other
    # creators appear in search results.
    handle_not_found: bool = False

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
            # Creator not in DB — flag it so the view shows the add CTA
            handle_not_found = True
            logger.info(f"[HandleSearch] Creator not found in DB: {handle}")

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
                # else: fall through to normal search with banner

            except Exception as e:
                logger.exception(f"[HandleSearch] Error fetching handle {handle}: {e}")
                # Fall through to normal search

    # ═══════════════════════════════════════════════════════════════
    # NORMAL SEARCH MODE
    # ═══════════════════════════════════════════════════════════════
    search = request.query_params.get("search", "")
    # When the user typed an @handle, strip the leading @ before passing the
    # term to the SQL search.  custom_url is stored without the @, so leaving
    # it in causes ILIKE "%@mrbeast%" to match only on `keywords` (where some
    # creators SEO-stuff @mentions of larger channels) instead of the actual
    # name/URL columns. Stripping it surfaces real matches (mrbeast2,
    # mrbeastgaming) and drops the keyword-spam noise.
    # The original @handle is preserved in the banner via `handle_not_found`.
    if search.startswith("@") and not handle_not_found:
        search = search.lstrip("@").strip()
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

    # ── Natural-language country extraction ──────────────────────────────────
    # Detects patterns like "MKBHD from Germany" or "gaming creators in Japan"
    # and splits them into a name search + a country_filter, but only when the
    # user has NOT already explicitly set a country filter via the pill UI.
    #
    # We redirect rather than mutating locals so the inferred country is
    # explicit in the URL: the country pill activates, the URL is shareable,
    # and pagination page numbers stay consistent.
    if search and country_filter == "all":
        parsed_search, inferred_country = _extract_country(search)
        if inferred_country != "all":
            logger.debug(
                "[CountryExtract] inferred country=%s from query %r; redirecting",
                inferred_country,
                search,
            )
            # Build redirect params from the live request so any future filter
            # params are preserved automatically, then override the extracted
            # values and drop the page number (results will change).
            redirect_params = dict(request.query_params)
            redirect_params["country"] = inferred_country
            if parsed_search:
                redirect_params["search"] = parsed_search
            else:
                redirect_params.pop("search", None)
            redirect_params.pop("page", None)
            return RedirectResponse(
                f"{request.url.path}?{urlencode(redirect_params)}", status_code=303
            )

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

    # Single source of truth for filter field names and their default values.
    # Used both to compute _needs_exact_count (below) and has_active_filters
    # (after the if/else).  Adding a new filter only requires updating this
    # dict and the _active_filters mapping in the else branch.
    _FILTER_DEFAULTS: dict[str, str] = {
        "grade": "all",
        "language": "all",
        "activity": "all",
        "age": "all",
        "country": "all",
        "category": "all",
    }

    if handle_not_found:
        creators = []
        total_count = 0
        degraded = False
        hero_stats: dict = get_creator_hero_stats()
        top_countries = get_top_countries_with_counts(limit=_HERO_COUNTRIES_LIMIT)
        top_languages = get_top_languages_with_counts(limit=_HERO_LANGUAGES_LIMIT)
        top_categories = get_top_categories_with_counts(limit=_HERO_CATEGORIES_LIMIT)
        favourite_ids: set[str] = (
            get_user_favourite_creator_ids(user_id) if is_authenticated and user_id else set()
        )
    else:
        # ── Fan out all independent DB calls in parallel ─────────────────────
        # get_creators(), hero stats, sidebar counts, and favourites are fully
        # independent.  Running them concurrently cuts wall-clock latency from
        # Σ(individual times) to max(individual times).

        # count=exact (Prefer: count=exact via PostgREST) forces PostgreSQL to
        # COUNT(*) every matching row even when LIMIT 50 is satisfied early.
        # On the unfiltered default browse (~800K browseable creators) this
        # hits the statement timeout (57014) before returning.
        # Fix: skip count=exact for the default unfiltered+no-search case and
        # use hero_stats["total_creators"] (fetched in parallel) for pagination.
        # Filtered/searched pages still use count=exact for accurate page counts.
        _active_filters: dict[str, str] = {
            "grade": grade_filter,
            "language": language_filter,
            "activity": activity_filter,
            "age": age_filter,
            "country": country_filter,
            "category": category_filter,
        }
        _needs_exact_count = bool(search) or any(
            _active_filters[k] != default for k, default in _FILTER_DEFAULTS.items()
        )

        _futures: dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=5) as _pool:
            _futures["creators"] = _pool.submit(
                get_creators,
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
                return_count=_needs_exact_count,
            )
            _futures["hero"] = _pool.submit(get_creator_hero_stats)
            _futures["countries"] = _pool.submit(
                get_top_countries_with_counts, _HERO_COUNTRIES_LIMIT
            )
            _futures["languages"] = _pool.submit(
                get_top_languages_with_counts, _HERO_LANGUAGES_LIMIT
            )
            _futures["categories"] = _pool.submit(
                get_top_categories_with_counts, _HERO_CATEGORIES_LIMIT
            )
            if is_authenticated and user_id:
                _futures["favs"] = _pool.submit(get_user_favourite_creator_ids, user_id)

        hero_stats = _futures["hero"].result()
        creators_result = _futures["creators"].result()
        degraded = False
        if _needs_exact_count:
            # Filtered/searched: result is CreatorsResult(creators, total_count)
            creators = creators_result.creators
            total_count = creators_result.total_count
            degraded = getattr(creators_result, "degraded", False)
        else:
            # Unfiltered default browse: normally list[dict], but CreatorsResult([], 0)
            # when get_creators degraded on a timeout — don't propagate the hero
            # total as pagination count or users see 0 cards across thousands of pages.
            if isinstance(creators_result, CreatorsResult):
                creators = creators_result.creators  # []
                total_count = creators_result.total_count  # 0
                degraded = creators_result.degraded
            else:
                creators = creators_result
                _hero_total = hero_stats.get("total_creators")
                total_count = (
                    _hero_total
                    if _hero_total is not None
                    else (page - 1) * per_page + len(creators)
                )
        top_countries = _futures["countries"].result()
        top_languages = _futures["languages"].result()
        top_categories = _futures["categories"].result()
        favourite_ids = _futures["favs"].result() if "favs" in _futures else set()

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
    has_active_filters = bool(search) or any(
        request.query_params.get(k, default) != default for k, default in _FILTER_DEFAULTS.items()
    )
    if not has_active_filters:
        stats["total_creators"] = total_count
    stats["top_countries"] = top_countries
    stats["top_languages"] = top_languages
    stats["top_categories"] = top_categories
    # total_categories is already fetched by get_creator_hero_stats() (via the
    # internal get_lists_meta() call it makes) and merged into stats above.
    # Fall back to TOTAL_TOPIC_CATEGORIES constant if the RPC didn't return it.
    if "total_categories" not in stats:
        stats["total_categories"] = TOTAL_TOPIC_CATEGORIES

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
        handle_not_found=handle_not_found,
        compare_a_id=_parse_compare_id(request.query_params.get("a")),
        degraded=degraded,
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


_SIMILAR_MIN = 3  # minimum tiles before we consider the rail worth showing
_SIMILAR_MAX = 8  # tiles to display


def _get_similar_creators(creator: dict) -> list[dict]:
    """
    Fetch up to _SIMILAR_MAX creators that share the same niche as *creator*,
    excluding the creator itself.

    Strategy:
      1. Try same primary_category (any country) — best for topic discovery.
      2. If fewer than _SIMILAR_MIN results remain after exclusion, fall back
         to same country_code — still relevant, less specific.
      3. Return [] on any error so the section simply doesn't render.
    """
    creator_id = creator.get("id", "")
    category = creator.get("primary_category", "")
    country = (creator.get("country_code") or "").lower()

    def _fetch_and_exclude(category_filter="all", country_filter="all") -> list[dict]:
        try:
            results = get_creators(
                category_filter=category_filter,
                country_filter=country_filter,
                sort="subscribers",
                limit=_SIMILAR_MAX + 1,  # +1 so we can exclude self and still have _SIMILAR_MAX
            )
            return [c for c in results if c.get("id") != creator_id][:_SIMILAR_MAX]
        except Exception:
            logger.exception("[CreatorProfile] _get_similar_creators failed")
            return []

    if category:
        candidates = _fetch_and_exclude(category_filter=category)
        if len(candidates) >= _SIMILAR_MIN:
            return candidates

    if country:
        return _fetch_and_exclude(country_filter=country)

    return []


def creator_profile_route(request, creator_id: str, user_id: str | None = None):
    """
    GET /creator/{creator_id} — Full profile page for a single creator.

    Args:
        request:    Starlette request (used for back_url via Referer/from param).
        creator_id: Creator UUID (primary key of the creators table).
        user_id:    Optional user UUID from session.  When provided the page
                    will show the correct initial state for the favourite button.

    Returns:
        CreatorProfileResult with the rendered profile and creator row, or a
        friendly 404 message Div for unknown creators.
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
    # When the user navigated here from the compare step-2 state (e.g. via
    # /creator/{id}?a={compare_a_id}), thread the first creator's ID through
    # to the view so the Compare button can complete the pair directly.
    compare_a_id = request.query_params.get("a", "")
    context_ranks = _get_context_ranks(creator)
    category_stats = get_cached_category_box_stats(creator.get("primary_category", ""))
    peer_benchmarks = get_category_peer_benchmarks(creator.get("primary_category", ""))
    niche_leaderboard = get_category_leaderboard(creator.get("primary_category", ""), limit=5)
    is_fav = is_creator_favourited(user_id, creator_id) if user_id else False
    similar_creators = _get_similar_creators(creator)
    # Fetch peer list once (cheap: one JSONB read + batched IN-list hydration).
    # hydrate_limit caps step-2 to the rail size so each IN() URL stays well
    # under Cloudflare's WAF length limit; total comes from step-1's raw count.
    peers_result = get_embedding_peers(
        creator_id, limit=LOOKALIKE_LIMIT, hydrate_limit=_PROFILE_PEER_RAIL_LIMIT
    )
    embedding_peers = peers_result[0] if peers_result else None
    embedding_peer_total = peers_result[1] if peers_result else 0

    is_authenticated = user_id is not None
    body = render_creator_profile_page(
        creator,
        back_url=back_url,
        context_ranks=context_ranks,
        category_stats=category_stats,
        peer_engagement_p75=peer_benchmarks.get("peer_engagement_p75", 0.0),
        niche_leaderboard=niche_leaderboard,
        is_favourited=is_fav,
        similar_creators=similar_creators,
        embedding_peers=embedding_peers,
        embedding_peer_total=embedding_peer_total,
        is_authenticated=is_authenticated,
        compare_a_id=compare_a_id,
    )
    return CreatorProfileResult(body=body, creator=creator)


def toggle_favourite_route(request, sess, creator_id: str):
    """
    POST /creator/{creator_id}/favourite
    HTMX endpoint — toggles the favourite state for an authenticated user.

    If the creator is not currently favourited it is added; if it is already
    favourited it is removed.  Returns the updated FavouriteButton fragment
    so HTMX can swap it in-place.

    AUTH: When the session carries no ``auth`` token the endpoint returns an
    HTMX OOB tuple: the unchanged heart button (primary swap) and an
    ``AuthModal`` injected into ``#auth-modal-mount`` so no hard redirect
    is needed.  Authenticated users with a missing ``user_id`` still receive
    a 401 to guard the DB helpers.
    """
    from views.creators import render_favourite_button

    auth = sess.get("auth") if sess else None
    user_id = sess.get("user_id") if sess else None
    auth_error = require_auth(auth)
    if auth_error:
        from components.modals import AuthModal

        # Return the unchanged button as the primary HTMX swap target, and
        # inject the auth modal out-of-band so no hard redirect is needed.
        # safe_local_return_url strips scheme/host and rejects external URLs,
        # preventing an open-redirect via a spoofed Referer header.
        return_url = safe_local_return_url(request.headers.get("referer"), default="/creators")
        return (
            render_favourite_button(creator_id, is_favourited=False),
            Div(
                AuthModal(
                    return_url=return_url,
                    context_label="Sign in to save this creator",
                ),
                id="auth-modal-mount",
                hx_swap_oob="innerHTML",
            ),
        )
    # In test mode require_auth is skipped; use a sentinel user_id so tests
    # that supply a session work, and tests that don't still get a predictable id.
    if not user_id and _IS_TESTING:
        user_id = "test-user-id"
    # require_auth only validates `auth`; `user_id` can independently be absent
    # on partial/legacy sessions. Guard explicitly so we never pass None to the
    # favourite-DB helpers (which would otherwise hit the database with a NULL
    # user filter).
    if not user_id:
        return Response("Authentication required", status_code=401)

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


def compare_creators_route(request, user_id: str | None = None):
    """
    GET /compare?a=<uuid>&b=<uuid> — Side-by-side creator comparison page.

    Returns an error Div when either creator ID is missing or not found.
    """
    id_a = request.query_params.get("a", "")
    id_b = request.query_params.get("b", "")

    def _not_found(cid):
        return Div(
            H2("Creator not found", cls="text-xl font-bold text-foreground mb-2"),
            P(f"No creator with ID {cid!r} exists.", cls="text-muted-foreground"),
            A(
                "← Browse creators",
                href="/creators",
                cls="mt-4 inline-flex text-sm font-medium text-primary hover:underline",
            ),
            cls="max-w-2xl mx-auto px-4 py-24 text-center",
        )

    if not id_a:
        return Div(
            H2("Two creators required", cls="text-xl font-bold text-foreground mb-2"),
            P(
                "Visit a creator profile and click Compare to start.",
                cls="text-muted-foreground text-sm mb-4",
            ),
            A(
                "Search creators →",
                href="/creators",
                cls="inline-flex items-center px-4 py-2 rounded-lg bg-primary text-primary-foreground text-sm font-semibold no-underline hover:opacity-90 transition-opacity",
            ),
            cls="max-w-2xl mx-auto px-4 py-24 text-center",
        )

    if not id_b:
        # User clicked Compare on a profile but hasn't chosen the second creator yet.
        # Fetch both suggestion sources in parallel so the page renders without
        # the user having to navigate away to find creator B.
        creator_a = get_creator_stats(id_a)
        if not creator_a:
            return _not_found(id_a)

        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_similar = pool.submit(_get_similar_creators, creator_a)
            fut_peers = pool.submit(
                get_embedding_peers,
                id_a,
                # peer_type defaults to "embedding_v1"; pass limit + hydrate_limit
                # as kwargs because hydrate_limit is keyword-only (after *).
                limit=_PROFILE_PEER_RAIL_LIMIT,
                hydrate_limit=_PROFILE_PEER_RAIL_LIMIT,
            )
            try:
                similar_creators_a = fut_similar.result()
            except Exception:
                logger.exception("[Compare] _get_similar_creators failed in step-2")
                similar_creators_a = []
            try:
                peers_result_a = fut_peers.result()
            except Exception:
                logger.exception("[Compare] get_embedding_peers failed in step-2")
                peers_result_a = None

        embedding_peers_a = peers_result_a[0] if peers_result_a else []

        return render_compare_pick_page(
            creator_a,
            similar_creators=similar_creators_a,
            embedding_peers=embedding_peers_a or [],
        )

    creator_a = get_creator_stats(id_a)
    creator_b = get_creator_stats(id_b)

    if not creator_a:
        return _not_found(id_a)
    if not creator_b:
        return _not_found(id_b)

    # Fetch ranks in parallel — same pattern as creator profile
    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_a = pool.submit(_get_context_ranks, creator_a)
        fut_b = pool.submit(_get_context_ranks, creator_b)
        ranks_a = fut_a.result()
        ranks_b = fut_b.result()

    is_fav_a = is_creator_favourited(user_id, id_a) if user_id else False
    is_fav_b = is_creator_favourited(user_id, id_b) if user_id else False

    return render_compare_page(
        creator_a,
        creator_b,
        ranks_a=ranks_a,
        ranks_b=ranks_b,
        is_fav_a=is_fav_a,
        is_fav_b=is_fav_b,
    )


def blueprint_route(request, creator_id: str, auth=None):
    """
    GET /creator/{creator_id}/blueprint — Growth Blueprint page.

    Loads the creator row, fetches category peer benchmarks, builds the
    signal vector, runs the scorer, and delegates rendering to the view.

    Returns a Div (page fragment) — wrapped in a full Titled page by main.py.
    """
    creator = get_creator_stats(creator_id)
    if not creator:
        logger.warning("[Blueprint] Creator not found: %s", creator_id)
        return Div(
            H2("Creator not found", cls="text-2xl font-bold text-foreground mb-2"),
            A("← Back to Creators", href="/creators", cls="text-sm text-primary hover:underline"),
            cls="max-w-2xl mx-auto px-4 py-24 text-center",
        )

    category = creator.get("primary_category", "")
    back_url = request.query_params.get("from", creator_profile_url(creator))

    benchmarks = get_category_peer_benchmarks(category)
    signals = signals_from_row(
        creator,
        peer_vpv_p75=benchmarks["peer_vpv_p75"],
        peer_vc_p75=benchmarks["peer_vc_p75"],
    )
    actions = score_all_actions(signals)

    bp_path = f"/creator/{creator_id}/blueprint"
    bp_qs = request.url.query
    return_url = f"{bp_path}?{bp_qs}" if bp_qs else bp_path

    return render_blueprint_page(
        creator,
        signals=signals,
        actions=actions,
        back_url=back_url,
        auth=bool(auth),
        return_url=return_url,
    )


async def creator_request_route(request, sess):
    """
    POST /creators/request
    HTMX endpoint — queues a creator add request submitted by the user.
    Accepts form field ``q`` (@handle or UC channel ID).
    Returns an inline HTMX partial (no full page reload).
    """
    auth = sess.get("auth") if sess else None
    auth_error = require_auth(auth, "Sign in to suggest creators", return_url="/creators")
    if auth_error:
        return auth_error

    user_id = sess.get("user_id") if sess else None
    if not user_id and _IS_TESTING:
        user_id = "test-user-id"
    # require_auth only validates `auth`; `user_id` can independently be absent
    # on partial/legacy sessions — guard so we never enqueue with a NULL user.
    if not user_id:
        return require_auth(None, "Sign in to suggest creators", return_url="/creators")

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


# ---------------------------------------------------------------------------
# /creators/top — high-engagement SEO landing pages
# ---------------------------------------------------------------------------
# Hand-picked slug → primary_category mapping. The slug allowlist also acts as
# the SEO surface — anything off-list returns 404 so we never index empty or
# duplicate URLs.
TOP_CATEGORY_SLUGS: dict[str, str] = {
    "gaming": "Gaming",
    "entertainment": "Entertainment",
    "music": "Music",
    "education": "Education",
    "howto-style": "Howto & Style",
}

TOP_PAGE_SIZE = 50  # creators per page — one screen on a laptop

# ----- A+ category counts cache (powers the Editors' Shortlist rail) -------
# These counts change slowly (only after backfill runs) so a coarse in-process
# cache is plenty. We deliberately keep the TTL short enough that a fresh
# deploy reflects new data within an hour, but long enough that the rail
# never bottlenecks the discovery pages.
_APLUS_COUNTS_TTL_S = 60 * 60  # 1 hour
_APLUS_COUNTS_RETRY_TTL_S = 60 * 5  # short retry window after a failed refresh


@dataclass
class _CountsCacheEntry:
    """Thread-safe TTL cache for A+ rail counts.

    Wraps the cached payload, expiry, and a ``Lock`` so concurrent requests
    can't both miss-and-refill (which would fan 6 DB probes out into 12+).
    The lock is only held around the cache read/write; the actual probe
    fan-out happens outside it so a slow refresh never blocks readers.
    """

    data: dict[str, int] | None = None
    expires_at: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)


_aplus_counts_cache = _CountsCacheEntry()


def get_aplus_category_counts() -> dict[str, int]:
    """Return ``{slug: count}`` for each rail slug plus a synthetic ``"all"`` total.

    Issues six parallel count-only queries against ``get_creators`` (which is
    the same path the landing pages use, so any indexes already cover it).
    Cached in-process for ``_APLUS_COUNTS_TTL_S`` seconds. Returns the last
    successful payload on transient failure, or zeros on cold-start failure
    so the rail still renders gracefully.
    """
    import time

    now = time.monotonic()
    with _aplus_counts_cache.lock:
        if _aplus_counts_cache.data is not None and now < _aplus_counts_cache.expires_at:
            return _aplus_counts_cache.data
        prev_payload = _aplus_counts_cache.data  # snapshot for failure fallback

    def _probe(slug: str | None, label: str | None) -> tuple[str, int, bool]:
        """Return (key, count, success). success=False only on exception."""
        try:
            res = get_creators(
                sort="subscribers",
                grade_filter="A+",
                category_filter=label or "all",
                limit=1,
                offset=0,
                return_count=True,
            )
            return (slug or "all"), int(res.total_count or 0), True
        except Exception:
            logger.exception("get_aplus_category_counts: probe failed for %s", slug)
            return (slug or "all"), 0, False

    probes: list[tuple[str | None, str | None]] = [(None, None)]
    probes.extend((slug, label) for slug, label in TOP_CATEGORY_SLUGS.items())

    counts: dict[str, int] = {}
    failed_probes: int = 0
    try:
        with ThreadPoolExecutor(max_workers=len(probes)) as pool:
            futures = [pool.submit(_probe, slug, label) for slug, label in probes]
            for fut in as_completed(futures):
                key, n, ok = fut.result()
                counts[key] = n
                if not ok:
                    failed_probes += 1
    except Exception:
        logger.exception("get_aplus_category_counts: pool failed")
        if prev_payload is not None:
            return prev_payload  # serve stale rather than break the rail
        return {key: 0 for key in ("all", *TOP_CATEGORY_SLUGS.keys())}

    # Serve stale only when probes actually raised — not merely returned zero.
    # A genuine all-zero result (valid DB state) must be cached and served as-is.
    if failed_probes and prev_payload is not None:
        logger.warning(
            "get_aplus_category_counts: %d probe(s) failed; serving stale cache",
            failed_probes,
        )
        # Short TTL so the next refresh retries soon rather than on every request.
        with _aplus_counts_cache.lock:
            _aplus_counts_cache.expires_at = now + _APLUS_COUNTS_RETRY_TTL_S
        return prev_payload

    with _aplus_counts_cache.lock:
        _aplus_counts_cache.data = counts
        _aplus_counts_cache.expires_at = now + _APLUS_COUNTS_TTL_S
    return counts


@dataclass(frozen=True)
class CreatorsTopResult:
    """Bundles everything main.py needs to wrap the page in Titled() + <head>.

    Returning structured metadata lets the SEO ``<head>`` tags know the real
    ``total_count`` without main.py having to re-query the DB, and ``creators``
    lets ``creators_top_head`` build the JSON-LD ItemList structured data.
    """

    body: Any  # FT element tree from render_creators_top_page()
    category_slug: str | None
    category_label: str | None
    total_count: int
    page: int
    creators: list  # current-page creator rows, used for JSON-LD


def creators_top_route(request, *, category_slug: str | None = None):
    """GET /creators/top  and  GET /creators/top/{slug}

    Editorial landing pages listing high-engagement creators. Reuses
    ``get_creators(grade_filter="A+", category_filter=...)`` so all heavy
    lifting stays in the DB layer; this route adds SEO surface (canonical
    URL, OG tags, JSON-LD ItemList) and an editorial intro.

    Returns either a ``CreatorsTopResult``, a ``RedirectResponse`` (for
    out-of-range ``page``), or a Starlette ``Response`` (404 for unknown slug).
    """
    from starlette.responses import RedirectResponse, Response

    from views.creators import render_creators_top_page

    category_label: str | None = None
    if category_slug is not None:
        category_label = TOP_CATEGORY_SLUGS.get(category_slug.lower())
        if category_label is None:
            return Response("Not found", status_code=404)

    base_path = "/creators/top" if category_slug is None else f"/creators/top/{category_slug}"

    try:
        page = max(1, int(request.query_params.get("page", "1")))
    except (TypeError, ValueError):
        page = 1

    offset = (page - 1) * TOP_PAGE_SIZE
    result = get_creators(
        sort="subscribers",
        grade_filter="A+",
        category_filter=category_label or "all",
        limit=TOP_PAGE_SIZE,
        offset=offset,
        return_count=True,
    )
    creators = list(result.creators)
    total_count = int(result.total_count or 0)
    total_pages = max(1, (total_count + TOP_PAGE_SIZE - 1) // TOP_PAGE_SIZE)

    # Clamp out-of-range pages by redirecting to canonical page 1. Prevents
    # 200 responses on empty offsets (e.g. ?page=9999) from becoming
    # indexable. 302 so a transiently-thin category can't permanently cache
    # a redirect once it grows back. Page 1 is also safe when total_count=0.
    if page > total_pages:
        return RedirectResponse(base_path, status_code=302)

    body = render_creators_top_page(
        creators=creators,
        category_slug=category_slug,
        category_label=category_label,
        total_count=total_count,
        page=page,
        page_size=TOP_PAGE_SIZE,
    )
    return CreatorsTopResult(
        body=body,
        category_slug=category_slug,
        category_label=category_label,
        total_count=total_count,
        page=page,
        creators=creators,
    )


# ---------------------------------------------------------------------------
# /creators/like/{handle} — Lookalike (embedding-peer) landing pages
# ---------------------------------------------------------------------------
# Programmatic SEO + bulk-contact wedge:
#   * 344k potential URLs (one per creator with peers in `creator_peers`)
#   * Anonymous CSV download — no auth required, no plan-gate
#   * Reuses ContactExtractorService for the export so the CSV shape stays
#     identical to /me/outreach/export and the admin bulk export.

LOOKALIKE_LIMIT = 50  # peers shown on /creators/like/{handle} (SEO destination)
# Profile-page rail is intentionally shorter than the landing page so the CTA
# ("See N more lookalikes for X") has real information scent. Matches the
# fold-friendly 8-tile shape used by the category-leaderboard rail above.
_PROFILE_PEER_RAIL_LIMIT = 8


@dataclass(frozen=True)
class CreatorsLikeResult:
    """Bundle returned to main.py so the route can wrap with <head> + Titled.

    Same pattern as ``CreatorsTopResult`` — keeps the route the single owner
    of the data fetch so main.py never has to re-query.
    """

    body: object  # FT tree from render_creators_like_page()
    seed: dict  # full creator row for the seed handle
    peer_count: int  # how many peers we actually rendered
    contact_count: int  # subset of peers with at least one contact
    headers: dict = field(default_factory=dict)  # HTTP response headers (Cache-Control, ETag, etc)


def _resolve_seed_creator(handle: str) -> dict | None:
    """Lookalike-side handle resolver. Thin wrapper kept for symmetry with
    the route below so tests/refactors don't have to monkey-patch the DB
    function directly."""
    if not handle:
        return None
    return find_creator_by_handle(handle)


def creators_like_route(request, *, handle: str):
    """GET /creators/like/{handle}

    Resolves ``handle`` to a creator id, fetches up to ``LOOKALIKE_LIMIT``
    embedding peers (with full contact fields for accurate email export count),
    and hands the result to the view. Anonymous-friendly — no auth gate, no plan gate.

    Returns:
        * ``CreatorsLikeResult`` on success (with HTTP cache headers for 1 hour).
        * ``Response`` 404 when the handle is unknown or has no peers
          (the latter prevents indexable empty pages).
    """
    import hashlib
    from starlette.responses import Response

    from views.creators import render_creators_like_page

    seed = _resolve_seed_creator(handle)
    if not seed:
        return Response("Creator not found", status_code=404)

    seed_id = seed.get("id")
    if not seed_id:
        return Response("Creator not found", status_code=404)

    # Use full contact field set to accurately count exportable emails.
    # While this increases query size vs. lightweight display fields, it ensures
    # contact_count matches what ContactExtractorService.filter_email_ready_rows()
    # would export, preventing mismatched CTAs (e.g., "Export 5 contacts" → empty CSV).
    peers_result = get_embedding_peers(
        seed_id,
        limit=LOOKALIKE_LIMIT,
        include_contacts=True,  # Fetch extracted_email for accurate export count
    )
    if not peers_result or not peers_result[0]:
        # No peer row or all peer IDs were deleted → don't serve an empty SEO page.
        return Response("No lookalikes available for this creator", status_code=404)

    peers, _total = peers_result
    # Count only peers with exportable email addresses (matches export filter logic).
    # Build contact rows and filter to emails only, same as CSV export.
    from services.contact_extractor import ContactExtractorService

    contact_rows = [ContactExtractorService.build_creator_contact_row(p) for p in peers]
    email_ready_rows = ContactExtractorService.filter_email_ready_rows(contact_rows)
    contact_count = len(email_ready_rows)  # Accurate count of exportable emails

    body = render_creators_like_page(
        seed=seed,
        peers=peers,
        contact_count=contact_count,
    )

    # Compute ETag from deterministic peer IDs only (not randomized hash()).
    # Stable across process restarts so cache validation works correctly.
    peer_ids_sorted = sorted(p["id"] for p in peers if p)
    etag_content = f"{seed_id}:{len(peers)}:{'|'.join(peer_ids_sorted)}"
    etag = f'"{hashlib.md5(etag_content.encode()).hexdigest()}"'

    return CreatorsLikeResult(
        body=body,
        seed=seed,
        peer_count=len(peers),
        contact_count=contact_count,
        headers={
            "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400",
            "ETag": etag,
            "Vary": "Accept-Encoding",
        },
    )
    # Note: headers dict is populated here for reference, but actual header
    # propagation must happen in main.py's creators_like() route handler.


def creators_like_export_route(request, *, handle: str):
    """GET /creators/like/{handle}/export  — CSV download (no extension).

    Returns the same CSV shape as ``/me/outreach/export`` so downstream
    email tools (Lemlist, Apollo, Instantly, etc.) accept it directly.
    Extensionless path + ``Content-Disposition`` preserves the .csv
    filename without tripping FastHTML's static-route precedence
    (the bug we fixed for ``/admin/outreach/export``).

    Note: Exports always fetch full contact data (include_contacts=True),
    separate from the page display route which uses a lighter field set.
    This prevents the page render from fetching unnecessary contact columns.
    """
    import csv
    import io

    from starlette.responses import Response

    from services.contact_extractor import ContactExtractorService

    seed = _resolve_seed_creator(handle)
    if not seed or not seed.get("id"):
        return Response("Creator not found", status_code=404)

    # Fetch full contact data for CSV export
    peers_result = get_embedding_peers(
        seed["id"],
        limit=LOOKALIKE_LIMIT,
        include_contacts=True,
    )
    if not peers_result or not peers_result[0]:
        return Response("No lookalikes available for this creator", status_code=404)

    peers, _total = peers_result

    # Build email-tool-friendly rows and filter to those with an email.
    # Pulling base_url from the live request keeps profile URLs portable
    # across staging / production without hard-coding the domain.
    base_url = str(request.base_url).rstrip("/") if hasattr(request, "base_url") else ""
    rows = [ContactExtractorService.build_creator_contact_row(p, base_url=base_url) for p in peers]
    rows = ContactExtractorService.filter_email_ready_rows(rows)

    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=ContactExtractorService.EMAIL_EXPORT_HEADERS,
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(rows)

    # Filename is sanitised lower-case handle so downloads don't collide
    # when a user exports several lookalike lists in one session.
    safe_handle = (seed.get("custom_url") or handle or "creator").lstrip("@").lower()
    safe_handle = re.sub(r"[^a-z0-9_-]", "", safe_handle) or "creator"
    filename = f"lookalikes-{safe_handle}.csv"

    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
