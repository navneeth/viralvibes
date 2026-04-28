"""
Database queries for creator lists/rankings.
Specialized functions for the /lists page tab content.
"""

import json
import logging
from typing import Callable

from utils import normalize_category_name, safe_get_value

logger = logging.getLogger(__name__)

# Maximum rows fetched in client-side fallback scans (when RPC is unavailable).
_MAX_FALLBACK_FETCH = 50_000

# Channels created within this many days are considered "new".
NEW_CHANNEL_MAX_AGE_DAYS = 365


def _get_supabase_client():
    """Access the Supabase client (initialized at app startup via db.init_supabase())."""
    from db import supabase_client

    return supabase_client


# Single source of truth for the zero-value meta dict.
# Add new dimensions here; all return sites pick it up automatically.
_EMPTY_META: dict[str, int] = {
    "total_creators": 0,
    "total_countries": 0,
    "total_categories": 0,
    "total_languages": 0,
}


# ─── BCP-47 language-tag normalisation ──────────────────────────────────────
# YouTube's Data API returns ``defaultLanguage`` as BCP-47 tags, which mix:
#   • ISO 639-1 base codes         : "en", "es", "zh"
#   • Region-qualified variants    : "en-GB", "en-IN", "es-419", "zh-CN"
#   • Deprecated ISO 639-1 codes   : "iw" (Hebrew), "in" (Indonesian), "ji" (Yiddish)
#
# We normalise to the base language code for display purposes so that the
# Language Explorer page shows one "English" bar instead of separate
# "en", "en-GB", "en-IN" bars.
#
# Sources:
#   BCP-47 RFC 5646 §2.1 — subtag structure
#   ISO 639-1:2002 — alpha-2 language codes
#   YouTube I18nLanguage resource (languages.list API)

# YouTube (and some pycountry fallbacks) still emit these deprecated codes.
_DEPRECATED_LANGUAGE_CODES: dict[str, str] = {
    "iw": "he",  # Hebrew  — superseded in ISO 639-1:2002
    "in": "id",  # Indonesian — superseded in ISO 639-1:2002
    "ji": "yi",  # Yiddish — superseded in ISO 639-1:2002
    "mo": "ro",  # Moldavian — merged into Romanian (ISO 639-3)
}


def _normalize_language_tag(code: str) -> str:
    """
    Reduce a BCP-47 language tag to its base ISO 639-1 language code.

    Algorithm (RFC 5646 §2.1):
      1. Lowercase and strip whitespace.
      2. Split on the first ``-`` separator; take the primary subtag.
      3. Remap any deprecated primary subtag to its modern successor.

    Examples::

        _normalize_language_tag("en-GB")  → "en"
        _normalize_language_tag("es-419") → "es"
        _normalize_language_tag("zh-CN")  → "zh"
        _normalize_language_tag("iw")     → "he"   # deprecated → Hebrew
        _normalize_language_tag("fil")    → "fil"  # no ISO 639-1 code; kept as-is
    """
    if not code:
        return ""
    code = code.lower().strip()
    base = code.split("-")[0]
    return _DEPRECATED_LANGUAGE_CODES.get(base, base)


def merge_language_variants(
    languages: list[tuple[str, int]],
) -> list[tuple[str, int]]:
    """
    Merge BCP-47 region-specific variants into their base language code.

    All region subtags are stripped and counts are summed so the Language
    Explorer shows one bar per language family rather than one per locale.

    Example (raw DB input → merged output)::

        [("en", 2600), ("en-GB", 313), ("en-IN", 79),
         ("es", 39),   ("es-419", 40), ("es-US", 9),
         ("iw", 5)]                      # deprecated Hebrew code

        → [("en", 2992), ("es", 88), ("he", 5), …]

    Args:
        languages: Raw ``(language_code, count)`` tuples from the DB.

    Returns:
        Deduplicated list sorted by count descending.
    """
    merged: dict[str, int] = {}
    for code, count in languages:
        base = _normalize_language_tag(code)
        merged[base] = merged.get(base, 0) + count
    return sorted(merged.items(), key=lambda x: x[1], reverse=True)


def _count_distinct_languages() -> int:
    """
    Return exact COUNT(DISTINCT default_language) for synced creators.

    Uses Supabase's server-side count so zero rows are transferred.
    Falls back to 0 on any error.

    Used as a fallback for ``get_lists_meta`` on DB schemas that predate
    the ``total_languages`` column in the ``get_lists_meta`` RPC (migration 003).
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        return 0
    try:
        resp = (
            supabase_client.table("creators")
            .select("default_language", count="exact")
            .not_.is_("default_language", "null")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .limit(1)
            .execute()
        )
        # count="exact" on a filtered query returns the filtered row count,
        # not distinct count — so we fall back to the column-scan for accuracy.
        # This is intentionally a lightweight call that avoids row transfer.
        # For a true DISTINCT count without row transfer we rely on the RPC;
        # this is just a best-effort approximate fallback.
        return resp.count or 0
    except Exception as e:
        logger.exception("Error counting distinct languages: %s", e)
        return 0


def get_top_rated_creators(limit: int = 20) -> list[dict]:
    """
    Get top-rated creators sorted by quality grade and subscribers.

    Grade hierarchy: A+ > A > B+ > B > C
    Within each grade, sort by subscribers descending.

    Args:
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts with stats
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        # Define grade order for sorting
        grade_order = {"A+": 1, "A": 2, "B+": 3, "B": 4, "C": 5}

        response = (
            supabase_client.table("creators")
            .select("*")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .order("current_subscribers", desc=True)
            .limit(limit * 3)  # Fetch extra to sort by grade
            .execute()
        )

        creators = response.data if response.data else []

        # Sort by grade first, then subscribers
        def sort_key(c):
            grade = safe_get_value(c, "quality_grade", "C")
            subs = safe_get_value(c, "current_subscribers", 0)
            return (grade_order.get(grade, 99), -subs)

        creators.sort(key=sort_key)
        return creators[:limit]

    except Exception as e:
        logger.exception(f"Error fetching top rated creators: {e}")
        return []


def get_most_active_creators(limit: int = 20) -> list[dict]:
    """
    Get most active creators sorted by monthly upload frequency.

    Args:
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts sorted by monthly_uploads descending
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        response = (
            supabase_client.table("creators")
            .select("*")
            .eq("sync_status", "synced")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .not_.is_("monthly_uploads", "null")
            .order("monthly_uploads", desc=True)
            .limit(limit)
            .execute()
        )

        return response.data if response.data else []

    except Exception as e:
        logger.exception(f"Error fetching most active creators: {e}")
        return []


def get_creators_by_country(country_code: str, limit: int = 10) -> list[dict]:
    """
    Get top creators from a specific country.

    Args:
        country_code: Two-letter country code (e.g., "US", "JP")
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts sorted by subscribers
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        response = (
            supabase_client.table("creators")
            .select("*")
            .eq("country_code", country_code)
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .order("current_subscribers", desc=True)
            .limit(limit)
            .execute()
        )

        return response.data if response.data else []

    except Exception as e:
        logger.exception(f"Error fetching creators for country {country_code}: {e}")
        return []


def get_creators_by_category(category: str, limit: int = 10) -> list[dict]:
    """
    Get top creators from a specific topic category.

    Args:
        category: Topic category name (e.g., "Music", "Gaming")
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts sorted by subscribers
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        # Use ILIKE for case-insensitive partial match
        # topic_categories can contain multiple comma-separated values
        response = (
            supabase_client.table("creators")
            .select("*")
            .ilike("topic_categories", f"%{category}%")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .order("current_subscribers", desc=True)
            .limit(limit)
            .execute()
        )

        return response.data if response.data else []

    except Exception as e:
        logger.exception(f"Error fetching creators for category {category}: {e}")
        return []


def get_top_creators_by_countries(
    country_codes: list[str], limit_per_country: int = 5
) -> dict[str, list[dict]]:
    """
    Batch-fetch top creators for a list of countries in a single DB query.

    Replaces N separate get_creators_by_country() calls when many country
    groups need to be rendered (e.g. the /lists page).

    Args:
        country_codes: ISO 3166-1 alpha-2 codes, e.g. ["US", "GB", "JP"]
        limit_per_country: Max creators to return per country

    Returns:
        Dict of {country_code: [creators sorted by subscribers desc]}

    Note:
        Results are globally ordered by subscribers before the per-country
        cap is applied.  In highly skewed datasets (e.g. US has far more
        creators than every other country combined) the global fetch cap
        (``* 3``, max 2000) may exhaust before low-subscriber countries are
        reached, leaving those slots empty.  This is an accepted trade-off
        for the /lists preview cards; the full detail page queries each
        country directly.
    """
    supabase_client = _get_supabase_client()
    if not supabase_client or not country_codes:
        return {}

    try:
        fetch_limit = min(len(country_codes) * limit_per_country * 3, 2000)

        response = (
            supabase_client.table("creators")
            .select("*")
            .in_("country_code", country_codes)
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .order("current_subscribers", desc=True)
            .limit(fetch_limit)
            .execute()
        )

        creators = response.data if response.data else []

        # Group by country_code, keep top N per country
        result: dict[str, list[dict]] = {code: [] for code in country_codes}
        for creator in creators:
            code = creator.get("country_code", "")
            if code in result and len(result[code]) < limit_per_country:
                result[code].append(creator)

        return result

    except Exception as e:
        logger.exception(f"Error batch-fetching creators for countries: {e}")
        return {}


def get_top_creators_by_categories(
    categories: list[str], limit_per_category: int = 5
) -> dict[str, list[dict]]:
    """
    Fetch the top creators for each category via per-category ILIKE queries.

    The previous implementation fetched a global top-N by subscribers and
    matched client-side.  That was broken for categories whose top creators
    are not globally top-ranked (e.g. "Lifestyle (sociology)" has 4 700
    creators but none are in the global top 200, so it always returned empty).

    This version issues one ILIKE query per distinct ilike_term — efficient
    because each query is DB-filtered and returns only ``limit_per_category``
    rows.

    Why not OR-ILIKE batching?
    ───────────────────────────
    A single query with OR-ed ILIKE conditions and client-side grouping
    reintroduces the subscriber-bias bug: results are globally ordered by
    subscribers, so long-tail categories (thousands of creators but none in
    the global top-N) would still return empty unless the fetch limit is
    impractically large.  Per-category queries guarantee correctness
    regardless of category size.  Query count is bounded in practice by
    INITIAL_GROUPS / LOAD_MORE_STEP (both = 8 in routes/lists.py).

    ILIKE search term construction
    ───────────────────────────────
    The RPC normalises Wikipedia URL slugs by replacing underscores with
    spaces, so category keys look like:
        "https://en.wikipedia.org/wiki/Lifestyle (sociology)"

    The raw DB value retains underscores:
        "https://en.wikipedia.org/wiki/Lifestyle_(sociology)"

    We extract the last URL segment, strip any query-string / fragment, and
    replace spaces with ``_``.  In SQL ILIKE the ``_`` character is a
    single-character wildcard, so the pattern
        ``%Lifestyle_(sociology)%``
    matches both the underscore form stored in the DB *and* any space variant.

    For plain category names like "Gaming" no transformation is needed.

    Args:
        categories: Category keys from get_top_categories_with_counts().
        limit_per_category: Max creators to return per category.

    Returns:
        Dict of {category_key: [creators sorted by subscribers desc]}
    """
    supabase_client = _get_supabase_client()
    if not supabase_client or not categories:
        return {}

    result: dict[str, list[dict]] = {cat: [] for cat in categories}

    # Build ilike_term for each category key, then group keys that share the
    # same term so we fire one query instead of N identical ones.
    term_to_cats: dict[str, list[str]] = {}
    for category in categories:
        # Extract the meaningful segment from a (possibly URL-normalised) key.
        segment = category.split("/")[-1] if "/" in category else category
        # Strip any query-string or fragment that may appear in non-canonical URLs.
        segment = segment.split("?")[0].split("#")[0].strip()
        # Spaces → _ so the ILIKE wildcard matches both storage forms.
        ilike_term = segment.replace(" ", "_")
        if ilike_term:
            term_to_cats.setdefault(ilike_term, []).append(category)

    for ilike_term, matching_cats in term_to_cats.items():
        try:
            response = (
                supabase_client.table("creators")
                .select("*")
                .ilike("topic_categories", f"%{ilike_term}%")
                .not_.is_("channel_name", "null")
                .gt("current_subscribers", 0)
                .order("current_subscribers", desc=True)
                .limit(limit_per_category)
                .execute()
            )
            creators = response.data if response.data else []
            # All keys that share this term get the same creator list.
            for cat in matching_cats:
                result[cat] = creators
        except Exception as e:
            logger.exception("Error fetching creators for category %r: %s", ilike_term, e)

    return result


def get_top_creators_by_languages(
    language_codes: list[str], limit_per_language: int = 5
) -> dict[str, list[dict]]:
    """
    Batch-fetch top creators for a list of language codes in a single DB query.

    Mirrors ``get_top_creators_by_countries`` — replaces N separate per-language
    queries with one batch fetch that is then grouped client-side.

    Args:
        language_codes: ISO 639-1 two-letter codes, e.g. ["en", "ja", "es"]
        limit_per_language: Max creators to return per language

    Returns:
        Dict of {language_code: [creators sorted by subscribers desc]}
    """
    supabase_client = _get_supabase_client()
    if not supabase_client or not language_codes:
        return {}

    try:
        fetch_limit = min(len(language_codes) * limit_per_language * 3, 2000)

        response = (
            supabase_client.table("creators")
            .select("*")
            .in_("default_language", language_codes)
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .order("current_subscribers", desc=True)
            .limit(fetch_limit)
            .execute()
        )

        creators = response.data if response.data else []

        # Group by default_language, keep top N per language
        result: dict[str, list[dict]] = {code: [] for code in language_codes}
        for creator in creators:
            code = creator.get("default_language", "")
            if code in result and len(result[code]) < limit_per_language:
                result[code].append(creator)

        return result

    except Exception as e:
        logger.exception(f"Error batch-fetching creators for languages: {e}")
        return {}


def get_language_groups(offset: int = 0, limit: int = 8, creators_per_group: int = 5) -> list[dict]:
    """
    Return paginated language groups, each with their top creators.

    Mirrors ``get_country_groups`` — uses a single batch DB query for the
    creators rather than one per language.
    Each item: { language_code, count, creators }

    Args:
        offset: How many top languages to skip (for load-more pagination)
        limit: How many language groups to return
        creators_per_group: How many creators per language

    Returns:
        List of language group dicts
    """
    top_languages = get_top_languages_with_counts(limit=offset + limit)
    page = top_languages[offset : offset + limit]

    if not page:
        return []

    # Batch-fetch creators for all languages in this page — 1 query instead of N
    language_codes = [lc for lc, _ in page]
    creators_by_language = get_top_creators_by_languages(
        language_codes, limit_per_language=creators_per_group
    )

    return [
        {
            "language_code": language_code,
            "count": count,
            "creators": creators_by_language.get(language_code, []),
        }
        for language_code, count in page
    ]


def get_rising_creators(limit: int = 20) -> list[dict]:
    """
    Get fastest-growing creators by 30-day growth rate (percentage).

    Growth rate = (subscribers_change_30d / current_subscribers) * 100

    This favors channels with explosive percentage growth regardless of size,
    so a 10k channel doubling (+10k, 100% growth) ranks higher than a 10M
    channel gaining 50k (+50k, 0.5% growth).

    Filters for creators with:
    - Positive subscriber growth
    - At least 1000 subscribers (avoid noise from tiny channels)

    Args:
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts sorted by growth rate (%) descending
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        # Fetch extra creators to ensure we have enough after calculating rates
        response = (
            supabase_client.table("creators")
            .select("*")
            .eq("sync_status", "synced")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 1000)
            .not_.is_("subscribers_change_30d", "null")
            .gt("subscribers_change_30d", 0)
            .limit(limit * 3)  # Fetch 3x to allow for rate calculation and sorting
            .execute()
        )

        creators = response.data if response.data else []

        # Calculate growth rate for each creator and attach it
        for creator in creators:
            subs_change = creator.get("subscribers_change_30d", 0)
            current_subs = creator.get("current_subscribers", 1)  # Avoid division by zero

            if current_subs > 0:
                # Growth rate as percentage
                growth_rate = (subs_change / current_subs) * 100
                creator["_growth_rate"] = growth_rate
            else:
                creator["_growth_rate"] = 0

        # Sort by growth rate descending
        creators.sort(key=lambda c: c.get("_growth_rate", 0), reverse=True)

        return creators[:limit]

    except Exception as e:
        logger.exception(f"Error fetching rising creators: {e}")
        return []


def get_veteran_creators(limit: int = 20) -> list[dict]:
    """
    Get veteran creators with channels 10+ years old.

    Filters for:
    - channel_age_days >= 3650 (10 years)
    - Sorted by subscribers to show most successful veterans

    Args:
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts sorted by subscribers descending
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        response = (
            supabase_client.table("creators")
            .select("*")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .gte("channel_age_days", 3650)  # 10 years
            .order("current_subscribers", desc=True)
            .limit(limit)
            .execute()
        )

        return response.data if response.data else []

    except Exception as e:
        logger.exception(f"Error fetching veteran creators: {e}")
        return []


def get_new_channels(limit: int = 20) -> list[dict]:
    """
    Get recently-created YouTube channels (channel_age_days <= 365).

    Sorted by engagement_score descending so the highest-quality new
    channels surface first rather than just the newest ones.

    Filters:
    - channel_age_days <= 365 (created within the last year on YouTube)
    - sync_status = 'synced'
    - current_subscribers > 0 (channel has an audience)

    Args:
        limit: Maximum number of creators to return

    Returns:
        List of creator dicts sorted by engagement_score descending
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        response = (
            supabase_client.table("creators")
            .select("*")
            .eq("sync_status", "synced")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .not_.is_("channel_age_days", "null")
            .lte("channel_age_days", NEW_CHANNEL_MAX_AGE_DAYS)
            .order("engagement_score", desc=True)
            .limit(limit)
            .execute()
        )

        return response.data if response.data else []

    except Exception as e:
        logger.exception(f"Error fetching new channels: {e}")
        return []


# ─── Private helpers shared by all three get_top_*_with_counts functions ─────


def _fetch_top_counts(
    rpc_name: str,
    key_name: str,
    fallback: Callable[[int], list[tuple[str, int]]],
    limit: int,
) -> list[tuple[str, int]]:
    """
    Dispatch a count-aggregation RPC, falling back to *fallback* on failure.

    All ``get_top_*_with_counts`` public functions delegate here so the
    RPC-dispatch, logging, and fallback policy live in one place.

    Args:
        rpc_name:  Supabase RPC function name.
        key_name:  Key in each response row that holds the group value.
        fallback:  ``Callable(limit)`` invoked when the RPC raises.
        limit:     Maximum rows to return.
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        resp = supabase_client.rpc(rpc_name, {"p_limit": limit}).execute()
        if resp.data:
            return [(row[key_name], row["creator_count"]) for row in resp.data]
        logger.warning("[Lists] %s RPC returned no data", rpc_name)
        return []

    except Exception as e:
        logger.exception("Error fetching %s via RPC: %s", rpc_name, e)
        return fallback(limit)


def _scan_column_counts(column: str, limit: int) -> list[tuple[str, int]]:
    """
    Client-side fallback: GROUP BY a single non-null creators column.

    Scans up to ``_MAX_FALLBACK_FETCH`` rows and returns the top *limit*
    ``(value, count)`` tuples sorted by count descending.  Used by the
    country and language fallbacks when their RPC is unavailable.

    Args:
        column: Column name in the ``creators`` table (e.g. ``"country_code"``).
        limit:  Maximum tuples to return.
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        response = (
            supabase_client.table("creators")
            .select(column)
            .eq("sync_status", "synced")
            .not_.is_("channel_name", "null")
            .not_.is_(column, "null")
            .gt("current_subscribers", 0)
            .limit(_MAX_FALLBACK_FETCH)
            .execute()
        )
        counts: dict[str, int] = {}
        for row in response.data or []:
            val = row.get(column)
            if val:
                counts[val] = counts.get(val, 0) + 1
        return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]
    except Exception as e:
        logger.exception("Error in %s column scan fallback: %s", column, e)
        return []


# ─── Public RPC-backed aggregation functions ─────────────────────────────────


def get_top_countries_with_counts(limit: int = 10) -> list[tuple[str, int]]:
    """
    Get top countries by creator count via DB-side RPC aggregation.

    Returns list of (country_code, creator_count) tuples.
    Used for the "By Country" tab and the /creators hero flag strip.

    Delegates to the ``get_top_countries_with_counts`` Supabase RPC
    (see db/migrations/002_lists_page_rpc_functions.sql) with a
    client-side column-scan fallback if the RPC is unavailable.
    """
    return _fetch_top_counts(
        "get_top_countries_with_counts",
        "country_code",
        lambda lim: _scan_column_counts("country_code", lim),
        limit,
    )


def get_top_languages_with_counts(limit: int = 10) -> list[tuple[str, int]]:
    """
    Get top content languages by creator count via DB-side RPC aggregation.

    Returns list of (language_code, creator_count) tuples.
    Used for the /creators filter bar and hero language stats.

    Delegates to the ``get_top_languages_with_counts`` Supabase RPC
    (see db/migrations/003_shared_stats_rpc_update.sql) with a
    client-side column-scan fallback.  Note: the DB column is
    ``default_language`` while the RPC returns it as ``language_code``;
    the fallback scans the raw column name directly.
    """
    return _fetch_top_counts(
        "get_top_languages_with_counts",
        "language_code",
        lambda lim: _scan_column_counts("default_language", lim),
        limit,
    )


def get_lists_meta() -> dict:
    """
    Return aggregate stats used to drive dynamic tab badges and load-more limits.

    Returns a dict with:
        total_creators   – exact DB COUNT(*)
        total_countries  – exact COUNT(DISTINCT country_code)
        total_categories – exact COUNT(DISTINCT normalised category)
        total_languages  – exact COUNT(DISTINCT default_language)

    All four values come from the ``get_lists_meta`` Supabase RPC function
    (see db/migrations/002_lists_page_rpc_functions.sql), which computes
    everything in a single server-side pass with zero row transfer.  Falls
    back to a combined client-side scan (approximate) if the RPC is
    unavailable.

    Note: if the DB RPC predates the ``total_languages`` column (migration
    003), the key defaults to 0 gracefully via ``row.get(...) or 0``.
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        return _EMPTY_META.copy()

    try:
        resp = supabase_client.rpc("get_lists_meta").execute()
        if resp.data:
            row = resp.data[0]
            return {
                "total_creators": int(row.get("total_creators") or 0),
                "total_countries": int(row.get("total_countries") or 0),
                "total_categories": int(row.get("total_categories") or 0),
                # total_languages added in migration 003; defaults to 0 on older DBs
                "total_languages": int(row.get("total_languages") or 0),
            }
        logger.warning("[Lists] get_lists_meta RPC returned no data")
        return _EMPTY_META.copy()

    except Exception as e:
        logger.exception(f"Error fetching lists meta via RPC: {e}")
        return _get_lists_meta_fallback()


def _get_lists_meta_fallback() -> dict:
    """Client-side fallback for get_lists_meta (RPC unavailable)."""
    supabase_client = _get_supabase_client()
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return _EMPTY_META.copy()

    try:
        response = (
            supabase_client.table("creators")
            .select("country_code, default_language, topic_categories", count="exact")
            .eq("sync_status", "synced")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .limit(_MAX_FALLBACK_FETCH)
            .execute()
        )
        rows = response.data if response.data else []
        total_creators = response.count if response.count is not None else len(rows)
        countries: set[str] = set()
        languages: set[str] = set()
        categories: set[str] = set()
        for row in rows:
            if cc := row.get("country_code"):
                countries.add(cc)
            if lang := row.get("default_language"):
                languages.add(lang)
            cats_raw = row.get("topic_categories")
            if cats_raw:
                if isinstance(cats_raw, list):
                    cat_list = cats_raw
                elif isinstance(cats_raw, str):
                    try:
                        cat_list = json.loads(cats_raw)
                        if not isinstance(cat_list, list):
                            cat_list = []
                    except (json.JSONDecodeError, ValueError):
                        cat_list = [c.strip() for c in cats_raw.split(",")]
                else:
                    cat_list = []
                for cat in cat_list:
                    clean = normalize_category_name(str(cat))
                    if clean:
                        categories.add(clean)
        return {
            "total_creators": total_creators,
            "total_countries": len(countries),
            "total_categories": len(categories),
            "total_languages": len(languages),
        }
    except Exception as e:
        logger.exception("Error in lists meta fallback: %s", e)
        return _EMPTY_META.copy()


def get_top_categories_with_counts(limit: int = 10) -> list[tuple[str, int]]:
    """
    Get top topic categories by creator count via DB-side RPC aggregation.

    Returns list of (category_name, creator_count) tuples.
    Used for the "By Category" tab and the /creators filter dropdown.

    Delegates to the ``get_top_categories_with_counts`` Supabase RPC
    (see db/migrations/002_lists_page_rpc_functions.sql), which unnests
    and normalises ``topic_categories`` server-side with zero row transfer.
    Falls back to ``_scan_categories_fallback`` if the RPC is unavailable.
    """
    return _fetch_top_counts(
        "get_top_categories_with_counts",
        "category",
        _scan_categories_fallback,
        limit,
    )


def _scan_categories_fallback(limit: int) -> list[tuple[str, int]]:
    """
    Client-side fallback for get_top_categories_with_counts (RPC unavailable).

    Categories require JSONB unnesting and normalisation that can't be expressed
    as a simple column scan, so this has its own implementation rather than
    delegating to ``_scan_column_counts``.
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        logger.warning("[Lists] No Supabase client - returning empty list")
        return []

    try:
        response = (
            supabase_client.table("creators")
            .select("topic_categories")
            .eq("sync_status", "synced")
            .not_.is_("channel_name", "null")
            .not_.is_("topic_categories", "null")
            .gt("current_subscribers", 0)
            .limit(_MAX_FALLBACK_FETCH)
            .execute()
        )
        category_counts: dict[str, int] = {}
        for row in response.data or []:
            categories_raw = row.get("topic_categories")
            if not categories_raw:
                continue
            if isinstance(categories_raw, list):
                cat_list = categories_raw
            elif isinstance(categories_raw, str):
                try:
                    cat_list = json.loads(categories_raw)
                    if not isinstance(cat_list, list):
                        cat_list = []
                except (json.JSONDecodeError, ValueError):
                    cat_list = [c.strip() for c in categories_raw.split(",")]
            else:
                continue
            for cat in cat_list:
                if cat:
                    clean = normalize_category_name(str(cat))
                    if clean:
                        category_counts[clean] = category_counts.get(clean, 0) + 1
        return sorted(category_counts.items(), key=lambda x: x[1], reverse=True)[:limit]
    except Exception as e:
        logger.exception("Error in categories scan fallback: %s", e)
        return []


def get_country_groups(offset: int = 0, limit: int = 8, creators_per_group: int = 5) -> list[dict]:
    """
    Return paginated country groups, each with their top creators.

    Uses a single batch DB query for the creators rather than one per country.
    Each item: { country_code, count, creators }

    Args:
        offset: How many top countries to skip (for load-more pagination)
        limit: How many country groups to return
        creators_per_group: How many creators per country

    Returns:
        List of country group dicts
    """
    top_countries = get_top_countries_with_counts(limit=offset + limit)
    page = top_countries[offset : offset + limit]

    if not page:
        return []

    # Batch-fetch creators for all countries in this page — 1 query instead of N
    country_codes = [cc for cc, _ in page]
    creators_by_country = get_top_creators_by_countries(
        country_codes, limit_per_country=creators_per_group
    )

    return [
        {
            "country_code": country_code,
            "count": count,
            "creators": creators_by_country.get(country_code, []),
        }
        for country_code, count in page
    ]


def get_category_groups(offset: int = 0, limit: int = 8, creators_per_group: int = 5) -> list[dict]:
    """
    Return paginated category groups, each with their top creators.

    Uses a single batch DB query for the creators rather than one per category.
    Each item: { category, count, creators }

    Args:
        offset: How many top categories to skip (for load-more pagination)
        limit: How many category groups to return
        creators_per_group: How many creators per category

    Returns:
        List of category group dicts
    """
    top_categories = get_top_categories_with_counts(limit=offset + limit)
    page = top_categories[offset : offset + limit]

    if not page:
        return []

    # Batch-fetch creators for all categories in this page — 1 query instead of N
    category_names = [cat for cat, _ in page]
    creators_by_category = get_top_creators_by_categories(
        category_names, limit_per_category=creators_per_group
    )

    return [
        {
            "category": category,
            "count": count,
            "creators": creators_by_category.get(category, []),
        }
        for category, count in page
    ]


def get_niche_heatmap_data(min_creators: int = 3) -> list[dict]:
    """
    Aggregate per-category momentum for the Niche Heat Map.

    For each normalized category with at least *min_creators* synced creators,
    computes:
      - creator_count
      - avg_engagement  (mean engagement_score, null rows excluded)
      - avg_growth_pct  (mean subscribers_change_30d / current_subscribers * 100)
      - premium_ratio   (fraction of creators with quality_grade A+ or A)

    Returns list of dicts sorted by creator_count descending (largest tiles first).
    No new DB columns required — all derived from existing creators fields.
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        return []

    try:
        response = (
            supabase_client.table("creators")
            .select(
                "topic_categories, engagement_score, "
                "subscribers_change_30d, current_subscribers, quality_grade"
            )
            .eq("sync_status", "synced")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .not_.is_("topic_categories", "null")
            .limit(_MAX_FALLBACK_FETCH)
            .execute()
        )
        rows = response.data or []
    except Exception as e:
        logger.exception("get_niche_heatmap_data: DB error: %s", e)
        return []

    # Accumulate per-category stats
    buckets: dict[str, dict] = {}
    for row in rows:
        cats_raw = row.get("topic_categories")
        if not cats_raw:
            continue
        if isinstance(cats_raw, list):
            cat_list = cats_raw
        elif isinstance(cats_raw, str):
            try:
                cat_list = json.loads(cats_raw)
                if not isinstance(cat_list, list):
                    cat_list = []
            except (json.JSONDecodeError, ValueError):
                cat_list = [c.strip() for c in cats_raw.split(",")]
        else:
            continue

        engagement = row.get("engagement_score")
        subs_change = row.get("subscribers_change_30d")
        current_subs = row.get("current_subscribers") or 0
        grade = row.get("quality_grade") or ""
        is_premium = grade in ("A+", "A")

        # Growth pct: preserve sign so declining categories surface in the Cooling panel
        growth_pct = None
        if subs_change is not None and current_subs > 0:
            growth_pct = subs_change / current_subs * 100

        for cat in cat_list:
            clean = normalize_category_name(str(cat))
            if not clean:
                continue
            if clean not in buckets:
                buckets[clean] = {
                    "category": clean,
                    "creator_count": 0,
                    "_engagement_sum": 0.0,
                    "_engagement_n": 0,
                    "_growth_sum": 0.0,
                    "_growth_n": 0,
                    "_premium_count": 0,
                }
            b = buckets[clean]
            b["creator_count"] += 1
            if engagement is not None:
                b["_engagement_sum"] += float(engagement)
                b["_engagement_n"] += 1
            if growth_pct is not None:
                b["_growth_sum"] += growth_pct
                b["_growth_n"] += 1
            if is_premium:
                b["_premium_count"] += 1

    results = []
    for b in buckets.values():
        if b["creator_count"] < min_creators:
            continue
        avg_engagement = (
            round(b["_engagement_sum"] / b["_engagement_n"], 1) if b["_engagement_n"] else None
        )
        avg_growth_pct = round(b["_growth_sum"] / b["_growth_n"], 1) if b["_growth_n"] else None
        premium_ratio = round(b["_premium_count"] / b["creator_count"], 2)
        results.append(
            {
                "category": b["category"],
                "creator_count": b["creator_count"],
                "avg_engagement": avg_engagement,
                "avg_growth_pct": avg_growth_pct,
                "premium_ratio": premium_ratio,
            }
        )

    results.sort(key=lambda x: x["creator_count"], reverse=True)
    return results
