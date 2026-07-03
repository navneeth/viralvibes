"""
Creators routes - browse and filter discovered YouTube creators.
"""

import logging
import os
import re
import threading
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
    get_lists_meta,
    get_top_categories_with_counts,
    get_top_countries_with_counts,
    get_top_languages_with_counts,
    suggest_primary_categories,
)

# from services.youtube_backend_api import YouTubeBackendAPI
from controllers.auth_routes import require_auth
from views.compare import render_compare_page
from views.creators import (
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
    if search and country_filter == "all":
        parsed_search, inferred_country = _extract_country(search)
        if inferred_country != "all":
            search = parsed_search
            country_filter = inferred_country
            logger.debug(
                "[CountryExtract] inferred country=%s from query",
                country_filter,
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

    if handle_not_found:
        # Exact @handle intent already missed the normalized-handle lookup.
        # Do not fall through to the broad OR-ILIKE discovery query: it is more
        # expensive, lower precision, and can time out for long-tail handles.
        # The view has a dedicated @handle empty state with an add-creator CTA.
        creators = []
        total_count = 0
    else:
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
    stats["top_languages"] = get_top_languages_with_counts(limit=7)
    stats["top_categories"] = get_top_categories_with_counts(limit=9)
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
        handle_not_found=handle_not_found,
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
    context_ranks = _get_context_ranks(creator)
    category_stats = get_cached_category_box_stats(creator.get("primary_category", ""))
    peer_benchmarks = get_category_peer_benchmarks(creator.get("primary_category", ""))
    niche_leaderboard = get_category_leaderboard(creator.get("primary_category", ""), limit=5)
    is_fav = is_creator_favourited(user_id, creator_id) if user_id else False
    similar_creators = _get_similar_creators(creator)
    # Fetch the full peer list once (cheap: one JSONB read + one IN-list hydrate)
    # then slice for the profile rail. The total length drives the CTA copy on
    # the rail ("See N more lookalikes for X") so users know there's more on
    # the dedicated landing page.
    embedding_peers_full = get_embedding_peers(creator_id, limit=LOOKALIKE_LIMIT) or []
    embedding_peers = embedding_peers_full[:_PROFILE_PEER_RAIL_LIMIT] or None
    embedding_peer_total = len(embedding_peers_full)

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
    )
    return CreatorProfileResult(body=body, creator=creator)


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

    if not id_a or not id_b:
        return Div(
            H2("Two creators required", cls="text-xl font-bold text-foreground mb-2"),
            P("Add ?a=<id>&b=<id> to compare two creators.", cls="text-muted-foreground"),
            A(
                "← Browse creators",
                href="/creators",
                cls="mt-4 inline-flex text-sm font-medium text-primary hover:underline",
            ),
            cls="max-w-2xl mx-auto px-4 py-24 text-center",
        )

    creator_a = get_creator_stats(id_a)
    creator_b = get_creator_stats(id_b)

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


def blueprint_route(request, creator_id: str):
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
    back_url = request.query_params.get("from", f"/creator/{creator_id}")

    benchmarks = get_category_peer_benchmarks(category)
    signals = signals_from_row(
        creator,
        peer_vpv_p75=benchmarks["peer_vpv_p75"],
        peer_vc_p75=benchmarks["peer_vc_p75"],
    )
    actions = score_all_actions(signals)

    return render_blueprint_page(
        creator,
        signals=signals,
        actions=actions,
        back_url=back_url,
    )


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


# ---------------------------------------------------------------------------
# /creators/top — A+ tier SEO landing pages
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

    def _probe(slug: str | None, label: str | None) -> tuple[str, int]:
        try:
            res = get_creators(
                sort="subscribers",
                grade_filter="A+",
                category_filter=label or "all",
                limit=1,
                offset=0,
                return_count=True,
            )
            return (slug or "all"), int(res.total_count or 0)
        except Exception:
            logger.exception("get_aplus_category_counts: probe failed for %s", slug)
            return (slug or "all"), 0

    probes: list[tuple[str | None, str | None]] = [(None, None)]
    probes.extend((slug, label) for slug, label in TOP_CATEGORY_SLUGS.items())

    counts: dict[str, int] = {}
    try:
        with ThreadPoolExecutor(max_workers=len(probes)) as pool:
            futures = [pool.submit(_probe, slug, label) for slug, label in probes]
            for fut in as_completed(futures):
                key, n = fut.result()
                counts[key] = n
    except Exception:
        logger.exception("get_aplus_category_counts: pool failed")
        if prev_payload is not None:
            return prev_payload  # serve stale rather than break the rail
        return {key: 0 for key in ("all", *TOP_CATEGORY_SLUGS.keys())}

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

    Editorial landing pages listing only A+ grade creators. Reuses
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
    embedding peers (with the contact-bearing field set), and hands the
    result to the view. Anonymous-friendly — no auth gate, no plan gate.

    Returns:
        * ``CreatorsLikeResult`` on success.
        * ``Response`` 404 when the handle is unknown or has no peers
          (the latter prevents indexable empty pages).
    """
    from starlette.responses import Response

    from views.creators import render_creators_like_page

    seed = _resolve_seed_creator(handle)
    if not seed:
        return Response("Creator not found", status_code=404)

    seed_id = seed.get("id")
    if not seed_id:
        return Response("Creator not found", status_code=404)

    peers = get_embedding_peers(
        seed_id,
        limit=LOOKALIKE_LIMIT,
        include_contacts=True,
    )
    if not peers:
        # No peer row → don't render an empty SEO page Google can crawl.
        return Response("No lookalikes available for this creator", status_code=404)

    contact_count = sum(1 for p in peers if p.get("has_contact_info") or p.get("extracted_email"))

    body = render_creators_like_page(
        seed=seed,
        peers=peers,
        contact_count=contact_count,
    )
    return CreatorsLikeResult(
        body=body,
        seed=seed,
        peer_count=len(peers),
        contact_count=contact_count,
    )


def creators_like_export_route(request, *, handle: str):
    """GET /creators/like/{handle}/export  — CSV download (no extension).

    Returns the same CSV shape as ``/me/outreach/export`` so downstream
    email tools (Lemlist, Apollo, Instantly, etc.) accept it directly.
    Extensionless path + ``Content-Disposition`` preserves the .csv
    filename without tripping FastHTML's static-route precedence
    (the bug we fixed for ``/admin/outreach/export``).
    """
    import csv
    import io

    from starlette.responses import Response

    from services.contact_extractor import ContactExtractorService

    seed = _resolve_seed_creator(handle)
    if not seed or not seed.get("id"):
        return Response("Creator not found", status_code=404)

    peers = get_embedding_peers(
        seed["id"],
        limit=LOOKALIKE_LIMIT,
        include_contacts=True,
    )
    if not peers:
        return Response("No lookalikes available for this creator", status_code=404)

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
