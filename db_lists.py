"""
Database queries for creator lists/rankings.
Specialized functions for the /lists page tab content.
"""

import json
import logging
import random
import time
from typing import Callable, NamedTuple
from urllib.parse import unquote, urlparse

from constants import BROWSEABLE_SYNC_STATUSES
from utils import normalize_category_name, safe_get_value, slugify

logger = logging.getLogger(__name__)

# Maximum rows fetched in client-side fallback scans (when an RPC is unavailable).
# Deliberately small — fallbacks are emergency degraded-mode only, not a substitute
# for working RPCs. If this fires in production, an RPC is broken and needs fixing.
# (Previously 50_000 — caused full-table seq scans and statement timeouts when RPCs 500'd.)
_MAX_FALLBACK_FETCH = 1_000


def _escape_ilike(term: str) -> str:
    """Escape PostgreSQL LIKE/ILIKE wildcards in user-supplied terms.

    `%` and `_` are LIKE wildcards; leaving them unescaped lets a user's input
    (or a stray character in a category slug) overmatch or trigger expensive
    full-table scans. Backslash must be escaped first so the subsequent
    replacements don't double-escape the wildcards.
    """
    return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# YouTube channel ``topicDetails.topicCategories`` are Wikipedia/Knowledge Graph
# topic labels. They are distinct from the video-level ``primary_category``
# values in creators. The list is intentionally finite and stable enough to use
# as the basis for URL slug resolution; live DB counts only tell us which of
# these categories currently have creators.
YOUTUBE_TOPIC_CATEGORY_LABELS: tuple[str, ...] = (
    "Music",
    "Christian music",
    "Classical music",
    "Country music",
    "Electronic music",
    "Hip hop music",
    "Independent music",
    "Jazz",
    "Music of Asia",
    "Music of Latin America",
    "Pop music",
    "Reggae",
    "Rhythm and blues",
    "Rock music",
    "Soul music",
    "Action game",
    "Action-adventure game",
    "Casual game",
    "Music video game",
    "Puzzle video game",
    "Racing video game",
    "Role-playing video game",
    "Simulation video game",
    "Sports game",
    "Strategy video game",
    "American football",
    "Baseball",
    "Basketball",
    "Boxing",
    "Cricket",
    "Association football",
    "Golf",
    "Ice hockey",
    "Mixed martial arts",
    "Motorsport",
    "Tennis",
    "Volleyball",
    "Entertainment",
    "Humour",
    "Film",
    "Performing arts",
    "Professional wrestling",
    "Television program",
    "Lifestyle (sociology)",
    "Fashion",
    "Physical fitness",
    "Food",
    "Hobby",
    "Pet",
    "Physical attractiveness",
    "Society",
    "Business",
    "Health",
    "Military",
    "Politics",
    "Religion",
    "Technology",
    "Tourism",
    "Vehicle",
    "Knowledge",
    "Video game culture",
)

_FIXED_TOPIC_CATEGORY_SLUG_MAP: dict[str, str] = {
    slugify(label): label for label in YOUTUBE_TOPIC_CATEGORY_LABELS
}
_FIXED_TOPIC_CATEGORY_LABEL_SET: set[str] = set(YOUTUBE_TOPIC_CATEGORY_LABELS)
TOTAL_TOPIC_CATEGORIES = len(YOUTUBE_TOPIC_CATEGORY_LABELS)


# The YouTube Data API returns topic categories as full Wikipedia URLs under
# ``topicDetails.topicCategories``. Build and parse URLs from one place so
# category slugs, cards, and detail pages all resolve consistently.
_TOPIC_CATEGORY_WIKIPEDIA_BASE_URL = "https://en.wikipedia.org/wiki/"


def _topic_category_wiki_slug(category: str) -> str:
    """Return a Wikipedia slug token (underscored, no URL prefix)."""
    raw = unquote(str(category or "")).strip()
    if not raw:
        return ""

    if raw.startswith(("http://", "https://")):
        parsed = urlparse(raw)
        host = (parsed.hostname or "").lower()
        if (host == "wikipedia.org" or host.endswith(".wikipedia.org")) and "/wiki/" in parsed.path:
            # urlparse already strips query/fragment into dedicated fields.
            wiki_path = parsed.path.split("/wiki/", 1)[-1].strip("/")
            if wiki_path:
                normalized_path = normalize_category_name(unquote(wiki_path))
                return normalized_path.replace(" ", "_") if normalized_path else ""

    label = normalize_category_name(raw)
    return label.replace(" ", "_") if label else ""


def _topic_category_ilike_term(category: str) -> str:
    """Return a DB text-search token for category matching.

    The token mirrors YouTube/Wikipedia slugs (underscores), allowing a single
    ILIKE pattern to match legacy URL storage and normalized-label rows.
    """
    return _topic_category_wiki_slug(category)


def _topic_category_jsonb_value(category: str) -> str:
    """Return canonical topic_categories JSON entry for exact jsonb containment."""
    slug = _topic_category_wiki_slug(category)
    if not slug:
        return ""
    if str(category or "").strip().startswith(("http://", "https://")):
        return f"{_TOPIC_CATEGORY_WIKIPEDIA_BASE_URL}{slug}"

    label = _topic_category_label(category)
    if not label:
        return ""

    # Prefer fixed YouTube topic labels for canonical jsonb containment checks.
    if label in _FIXED_TOPIC_CATEGORY_LABEL_SET:
        return f"{_TOPIC_CATEGORY_WIKIPEDIA_BASE_URL}{label.replace(' ', '_')}"
    return ""


def _iter_normalized_topic_categories(categories_raw) -> list[str]:
    """Parse raw topic_categories payload and return normalized labels."""
    if not categories_raw:
        return []

    if isinstance(categories_raw, list):
        cat_list = categories_raw
    elif isinstance(categories_raw, str):
        try:
            parsed = json.loads(categories_raw)
            cat_list = parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, ValueError):
            cat_list = [c.strip() for c in categories_raw.split(",")]
    else:
        return []

    normalized: list[str] = []
    for cat in cat_list:
        clean = normalize_category_name(str(cat))
        if clean:
            normalized.append(clean)
    return normalized


def _merge_with_fixed_topic_categories(
    category_counts: list[tuple[str, int]],
) -> list[tuple[str, int]]:
    """Return counts projected onto the fixed YouTube topic taxonomy."""
    counts_by_label: dict[str, int] = {}
    for raw_name, count in category_counts:
        label = _topic_category_label(raw_name)
        if label in _FIXED_TOPIC_CATEGORY_LABEL_SET:
            counts_by_label[label] = int(count or 0)

    merged = [(label, counts_by_label.get(label, 0)) for label in YOUTUBE_TOPIC_CATEGORY_LABELS]
    merged.sort(key=lambda x: x[1], reverse=True)
    return merged


def topic_category_slug(category: str) -> str:
    """Return the stable URL slug for a topic category label."""
    return slugify(normalize_category_name(str(category or "")))


def _topic_category_label(category: str) -> str:
    """Normalize a route value, DB value, or old Wikipedia URL into a label."""
    return normalize_category_name(unquote(str(category or "").strip()))


def _topic_category_ilike_pattern(category: str) -> str:
    """Build a separator-tolerant ILIKE pattern for ``topic_categories`` text.

    The clean storage form is a JSON array of labels, e.g.
    ``["Lifestyle (sociology)"]``. Older rows may still contain Wikipedia URLs
    or underscores, so the fallback text pattern keeps word order while allowing
    arbitrary separators between words.
    """
    label = _topic_category_label(category)
    words = [w for w in label.replace("(", " ").replace(")", " ").split() if w]
    if not words:
        return ""
    return "%" + "%".join(_escape_ilike(word) for word in words) + "%"


class TopicCategoryPageResult(NamedTuple):
    creators: list[dict]
    total_count: int


class CountryPageResult(NamedTuple):
    creators: list[dict]
    total_count: int


# ---------------------------------------------------------------------------
# Per-key TTL cache for category creator listings.
# Key: (category_label, limit, offset, return_count)
# Value: (monotonic_timestamp, TopicCategoryPageResult | list[dict])
#
# TTL governs when a *fresh* DB fetch is attempted, not when stale entries
# are discarded.  An expired entry stays in the dict and is returned as-is
# when the subsequent DB call fails (e.g. statement timeout), so pages
# never go blank due to a transient DB hiccup.  Entries are only truly
# evicted by clear_category_creators_cache() (called after a worker refresh).
# ---------------------------------------------------------------------------
_CATEGORY_CREATORS_TTL_SECONDS = 600  # 10 min
_CategoryCreatorsValue = list[dict] | TopicCategoryPageResult

_category_creators_cache: dict[tuple, tuple[float, _CategoryCreatorsValue]] = {}

# Key: (category_label, country_code, limit, offset, return_count)
_category_country_creators_cache: dict[tuple, tuple[float, _CategoryCreatorsValue]] = {}

# Key: (country_code, limit, offset, return_count)
# Values are always stored as CountryPageResult (the canonical form).
# When return_count=False, the list is extracted on return to match the function signature.
_country_creators_cache: dict[tuple, tuple[float, CountryPageResult]] = {}


def clear_category_creators_cache() -> None:
    """Invalidate both category-creators caches (called after mv_category_counts refresh)."""
    _category_creators_cache.clear()
    _category_country_creators_cache.clear()


def clear_country_creators_cache() -> None:
    """Invalidate cached country detail pages."""
    _country_creators_cache.clear()


_CATEGORY_SLUG_CACHE_TTL_S = 60 * 60
_category_slug_cache: dict[str, object] = {"expires_at": 0.0, "map": {}}


def _get_observed_topic_category_labels(limit: int = 2000) -> list[str]:
    """Return normalized topic labels currently observed in the DB.

    Unlike ``get_top_categories_with_counts()``, this helper intentionally
    returns raw observed labels so slug resolution can continue to learn
    non-fixed topics seen in legacy or long-tail data.
    """
    observed = _fetch_top_counts(
        "get_top_categories_with_counts",
        "category",
        _scan_categories_fallback,
        limit,
    )
    labels: list[str] = []
    seen: set[str] = set()
    for cat_name, _ in observed:
        label = _topic_category_label(cat_name)
        if label and label not in seen:
            seen.add(label)
            labels.append(label)
    return labels


def _get_category_slug_map() -> dict[str, str]:
    """Return ``{slug: canonical_name}`` for fixed and currently observed topics."""
    now = time.monotonic()
    expires_at = float(_category_slug_cache.get("expires_at") or 0.0)
    cached_map = _category_slug_cache.get("map")
    if isinstance(cached_map, dict) and now < expires_at:
        return cached_map

    slug_map: dict[str, str] = dict(_FIXED_TOPIC_CATEGORY_SLUG_MAP)
    for label in _get_observed_topic_category_labels(limit=2000):
        slug_map.setdefault(topic_category_slug(label), label)

    _category_slug_cache["expires_at"] = now + _CATEGORY_SLUG_CACHE_TTL_S
    _category_slug_cache["map"] = slug_map
    return slug_map


def resolve_category_slug(slug: str) -> str | None:
    """Map a URL slug, encoded label, or Wikipedia URL to a canonical label."""
    if not slug or not str(slug).strip():
        return None

    raw = unquote(str(slug)).strip()
    label = _topic_category_label(raw)
    slug_key = topic_category_slug(label or raw)
    if not slug_key:
        return None

    fixed = _FIXED_TOPIC_CATEGORY_SLUG_MAP.get(slug_key)
    if fixed:
        return fixed

    resolved = _get_category_slug_map().get(slug_key)
    if resolved:
        return resolved

    # Backward compatibility for unknown-but-valid labels: degrade to the
    # best normalized label rather than generating a mangled display title.
    fallback_label = _topic_category_label(raw.replace("-", " "))
    if not fallback_label:
        return None
    return fallback_label[0].upper() + fallback_label[1:]


def _apply_browseable_constraints(query):
    """Apply the four predicates shared by every topic-category query.

    Centralised so that a change to BROWSEABLE_SYNC_STATUSES or the
    standard row-quality filters only needs to happen in one place.
    """
    return (
        query.in_("sync_status", list(BROWSEABLE_SYNC_STATUSES))
        .not_.is_("channel_name", "null")
        .not_.is_("topic_categories", "null")
        .gt("current_subscribers", 0)
    )


def _apply_topic_category_filters(query, category: str):
    """Apply filters shared by category group cards and detail pages."""
    label = _topic_category_label(category)
    if label:
        query = query.filter("topic_categories", "cs", json.dumps([label]))
    return _apply_browseable_constraints(query)


def _apply_topic_category_text_filters(query, category: str):
    """Fallback text filter for legacy rows that do not exact-match JSON labels."""
    pattern = _topic_category_ilike_pattern(category)
    if pattern:
        query = query.ilike("topic_categories", pattern)
    return _apply_browseable_constraints(query)


def get_topic_category_creators(
    category: str,
    *,
    limit: int = 20,
    offset: int = 0,
    return_count: bool = False,
) -> list[dict] | TopicCategoryPageResult:
    """
    Paginated creator listing for a topic category detail page.

    Filters on ``topic_categories`` (same column as category counts / list
    cards), not ``primary_category`` — the latter holds YouTube video-level
    categories and does not match Wikipedia topic taxonomy.

    Results are cached in-process for ``_CATEGORY_CREATORS_TTL_SECONDS``
    seconds.  On a statement-timeout the last known value is returned so
    pages never go blank.  ``clear_category_creators_cache()`` is called by
    ``refresh_hero_stats_cache()`` after ``mv_category_counts`` is refreshed.
    """
    if not category or not str(category).strip():
        return TopicCategoryPageResult([], 0) if return_count else []

    supabase_client = _get_supabase_client()

    category_label = _topic_category_label(category)
    if not category_label:
        return TopicCategoryPageResult([], 0) if return_count else []

    cache_key = (category_label, limit, offset, return_count)
    now = time.monotonic()
    cached_entry = _category_creators_cache.get(cache_key)
    if cached_entry is not None:
        ts, cached = cached_entry
        if now - ts < _CATEGORY_CREATORS_TTL_SECONDS:
            return cached  # type: ignore[return-value]

    if not supabase_client:
        if cached_entry:
            logger.warning(
                "Supabase client unavailable in get_topic_category_creators(%r) — serving stale cache",
                category_label,
            )
            return cached_entry[1]  # type: ignore[return-value]
        return TopicCategoryPageResult([], 0) if return_count else []

    def _run(use_text_fallback: bool = False):
        query = supabase_client.table("creators").select(
            "*", count="exact" if return_count else None
        )
        query = (
            _apply_topic_category_text_filters(query, category_label)
            if use_text_fallback
            else _apply_topic_category_filters(query, category_label)
        )
        query = query.order("current_subscribers", desc=True).limit(limit)
        if offset:
            query = query.offset(offset)
        return query.execute()

    response = None
    # topic_categories is a text column (not jsonb/text[]), so the PostgREST
    # containment operator cs. always fails with 42883 ("operator does not
    # exist: text @> unknown"). Skip the exact attempt entirely and go straight
    # to the ilike fallback to avoid a guaranteed-failed round-trip.
    try:
        response = _run(use_text_fallback=True)
    except Exception:
        if cached_entry:
            logger.warning(
                "Error fetching topic category creators for %r — serving stale cache (age %.0fs)",
                category_label,
                now - cached_entry[0],
            )
            return cached_entry[1]  # type: ignore[return-value]
        logger.exception(
            "Error fetching topic category creators for %r via text fallback",
            category_label,
        )
        response = None

    creators = response.data if response and response.data else []
    total_count = (getattr(response, "count", 0) or 0) if return_count and response else 0

    for idx, creator in enumerate(creators, 1):
        creator["_rank"] = offset + idx

    result: list[dict] | TopicCategoryPageResult
    if return_count:
        result = TopicCategoryPageResult(creators, total_count)
    else:
        result = creators

    if response is not None:
        _category_creators_cache[cache_key] = (now, result)

    return result


def get_topic_category_country_creators(
    category: str,
    country_code: str,
    *,
    limit: int = 20,
    offset: int = 0,
    return_count: bool = False,
) -> list[dict] | TopicCategoryPageResult:
    """Paginated creator listing for a topic category within one country."""
    if not category or not str(category).strip() or not country_code:
        return TopicCategoryPageResult([], 0) if return_count else []

    supabase_client = _get_supabase_client()

    category_label = _topic_category_label(category)
    normalized_country = str(country_code or "").strip().upper()
    if not category_label or len(normalized_country) != 2:
        return TopicCategoryPageResult([], 0) if return_count else []

    cache_key = (category_label, normalized_country, limit, offset, return_count)
    now = time.monotonic()
    cached_entry = _category_country_creators_cache.get(cache_key)
    if cached_entry is not None:
        ts, cached = cached_entry
        if now - ts < _CATEGORY_CREATORS_TTL_SECONDS:
            return cached  # type: ignore[return-value]

    if not supabase_client:
        if cached_entry:
            logger.warning(
                "Supabase client unavailable in get_topic_category_country_creators(%r, %s) — serving stale cache",
                category_label,
                normalized_country,
            )
            return cached_entry[1]  # type: ignore[return-value]
        return TopicCategoryPageResult([], 0) if return_count else []

    try:
        query = supabase_client.table("creators").select(
            "*", count="exact" if return_count else None
        )
        query = _apply_topic_category_text_filters(query, category_label)
        query = (
            query.eq("country_code", normalized_country)
            .order("current_subscribers", desc=True)
            .limit(limit)
        )
        if offset:
            query = query.offset(offset)
        response = query.execute()
    except Exception:
        if cached_entry:
            logger.warning(
                "Error fetching topic category creators for %r in %s — serving stale cache (age %.0fs)",
                category_label,
                normalized_country,
                now - cached_entry[0],
            )
            return cached_entry[1]  # type: ignore[return-value]
        logger.exception(
            "Error fetching topic category creators for %r in %s",
            category_label,
            normalized_country,
        )
        return TopicCategoryPageResult([], 0) if return_count else []

    creators = response.data if response and response.data else []
    total_count = (getattr(response, "count", 0) or 0) if return_count and response else 0

    for idx, creator in enumerate(creators, 1):
        creator["_rank"] = offset + idx

    result: list[dict] | TopicCategoryPageResult
    if return_count:
        result = TopicCategoryPageResult(creators, total_count)
    else:
        result = creators

    _category_country_creators_cache[cache_key] = (now, result)
    return result


# ---------------------------------------------------------------------------
# Transient-transport retry for Supabase RPC calls
# ---------------------------------------------------------------------------
# HTTP/2 "Server disconnected" errors are transient — the request never
# reached PostgreSQL.  We retry a small number of times with exponential
# backoff + jitter before activating the client-side fallback.
#
# This mirrors _is_transient_disconnect / _with_disconnect_retry in db.py
# but is defined locally to avoid a circular import (db.py lazily imports
# db_lists for cache-clearing, so a top-level import in the other direction
# would create a cycle).
# ---------------------------------------------------------------------------
try:
    import httpcore as _httpcore

    _HTTPCORE_ERRORS = (_httpcore.RemoteProtocolError,)
except ImportError:
    _HTTPCORE_ERRORS = ()

try:
    import httpx as _httpx

    _HTTPX_ERRORS = (
        _httpx.RemoteProtocolError,
        _httpx.ReadTimeout,
        _httpx.ConnectTimeout,
        _httpx.ConnectError,
        _httpx.ReadError,
    )
except ImportError:
    _HTTPX_ERRORS = ()

_RETRIABLE_TRANSPORT_ERRORS: tuple = _HTTPX_ERRORS + _HTTPCORE_ERRORS

if not _RETRIABLE_TRANSPORT_ERRORS:
    logger.warning(
        "[Lists] httpx/httpcore not importable — _rpc_with_retry will not retry "
        "transient transport errors; check that these packages are installed."
    )


def _rpc_with_retry(
    supabase_client,
    rpc_name: str,
    params: dict,
    *,
    max_attempts: int = 3,
    base_delay_s: float = 0.25,
    max_delay_s: float = 3.0,
):
    """
    Call ``supabase_client.rpc(rpc_name, params).execute()`` with retry logic
    for transient HTTP/2 transport errors ("Server disconnected").

    Only ``_RETRIABLE_TRANSPORT_ERRORS`` are retried.  PostgREST errors
    (bad SQL, auth failures, etc.) are re-raised immediately so genuinely
    broken RPCs don't spin through unnecessary retries.

    Args:
        max_attempts: Total attempts (default 3 = 1 original + 2 retries).
                      Must be >= 1.
        base_delay_s: Initial backoff before retry 2 (seconds).
        max_delay_s:  Cap on any single sleep.
    """
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return supabase_client.rpc(rpc_name, params).execute()
        except _RETRIABLE_TRANSPORT_ERRORS as exc:
            last_exc = exc
            if attempt == max_attempts:
                break
            delay = min(max_delay_s, base_delay_s * (2 ** (attempt - 1)))
            delay *= 0.85 + random.random() * 0.30  # ±15 % jitter
            logger.warning(
                "[Lists] %s transient error (attempt %d/%d), retrying in %.2fs: %s",
                rpc_name,
                attempt,
                max_attempts,
                delay,
                exc,
            )
            time.sleep(delay)
        except Exception:
            raise  # non-transport errors: propagate immediately
    raise last_exc  # type: ignore[misc]


# Channels created within this many days are considered "new".
NEW_CHANNEL_MAX_AGE_DAYS = 365

_LISTS_META_TTL_SECONDS = 600  # 10 min — data changes at most a few times/day
_lists_meta_cache: tuple[float, dict[str, int]] | None = None

_TOP_CATEGORIES_TTL_SECONDS = 600  # 10 min — category counts change only on worker runs
_top_categories_cache: tuple[float, list[tuple[str, int]]] | None = None

_TOP_COUNTRIES_TTL_SECONDS = 600  # 10 min — same change cadence as categories
_top_countries_cache: tuple[float, list[tuple[str, int]]] | None = None

_TOP_LANGUAGES_TTL_SECONDS = 600  # 10 min — same change cadence as categories
_top_languages_cache: tuple[float, list[tuple[str, int]]] | None = None


def clear_lists_meta_cache() -> None:
    """Clear this process's short-lived lists metadata cache."""
    global _lists_meta_cache
    _lists_meta_cache = None


def clear_top_categories_cache() -> None:
    """Clear this process's short-lived top-categories cache."""
    global _top_categories_cache
    _top_categories_cache = None


def clear_top_countries_cache() -> None:
    """Clear this process's short-lived top-countries cache."""
    global _top_countries_cache
    _top_countries_cache = None
    clear_country_creators_cache()


def clear_top_languages_cache() -> None:
    """Clear this process's short-lived top-languages cache."""
    global _top_languages_cache
    _top_languages_cache = None


def _get_supabase_client():
    """Access the Supabase client (initialized at app startup via db.init_supabase())."""
    from db import supabase_client

    return supabase_client


# Single source of truth for the zero-value meta dict.
# Add new dimensions here; all return sites pick it up automatically.
_EMPTY_META: dict[str, int] = {
    "total_creators": 0,
    "total_countries": 0,
    "total_categories": TOTAL_TOPIC_CATEGORIES,
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


def get_country_creators(
    country_code: str,
    *,
    limit: int = 20,
    offset: int = 0,
    return_count: bool = False,
) -> list[dict] | CountryPageResult:
    """
    Paginated creator listing for a country detail page.

    This intentionally mirrors the By Country preview cards: a creator is
    listable when it has sync_status='synced', a country_code, channel_name,
    and positive subscribers. The generic get_creators() path uses an IN() sync
    filter and gracefully returns empty on filtered statement timeouts; for
    large countries that made public country pages look empty even though the
    country cards had creators.
    """
    normalized_country = str(country_code or "").strip().upper()
    if len(normalized_country) != 2:
        return CountryPageResult([], 0) if return_count else []

    cache_key = (normalized_country, limit, offset, return_count)
    now = time.monotonic()
    cached_entry = _country_creators_cache.get(cache_key)
    if cached_entry is not None:
        ts, cached_result = cached_entry
        if now - ts < _CATEGORY_CREATORS_TTL_SECONDS:
            # Cache always stores CountryPageResult; extract list if needed
            return cached_result if return_count else cached_result.creators

    supabase_client = _get_supabase_client()
    if not supabase_client:
        if cached_entry:
            logger.warning(
                "Supabase client unavailable in get_country_creators(%s) - serving stale cache",
                normalized_country,
            )
            cached_result = cached_entry[1]
            return cached_result if return_count else cached_result.creators
        return CountryPageResult([], 0) if return_count else []

    try:
        query = (
            supabase_client.table("creators")
            .select("*", count="exact" if return_count else None)
            .eq("country_code", normalized_country)
            .eq("sync_status", "synced")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .order("current_subscribers", desc=True)
            .limit(limit)
        )
        if offset:
            query = query.offset(offset)
        response = query.execute()
    except Exception:
        if cached_entry:
            logger.warning(
                "Error fetching creators for country %s - serving stale cache (age %.0fs)",
                normalized_country,
                now - cached_entry[0],
            )
            cached_result = cached_entry[1]
            return cached_result if return_count else cached_result.creators
        logger.exception("Error fetching creators for country %s", normalized_country)
        return CountryPageResult([], 0) if return_count else []

    creators = response.data if response and response.data else []
    total_count = (getattr(response, "count", 0) or 0) if return_count and response else 0

    for idx, creator in enumerate(creators, 1):
        creator["_rank"] = offset + idx

    # Always store as CountryPageResult; extract on return to match function signature
    cached_result = CountryPageResult(creators, total_count)
    _country_creators_cache[cache_key] = (now, cached_result)
    return cached_result if return_count else cached_result.creators


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
            .ilike("topic_categories", f"%{_escape_ilike(category)}%")
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
    Fetch the top creators for each category via per-category DB-filtered queries.

    The previous implementation fetched a global top-N by subscribers and
    matched client-side.  That was broken for categories whose top creators
    are not globally top-ranked (e.g. "Lifestyle (sociology)" has 4 700
    creators but none are in the global top 200, so it always returned empty).

    This version issues one filtered query per distinct category token,
    preferring exact JSONB containment and falling back to ILIKE when needed.
    Each query is DB-filtered and returns only ``limit_per_category`` rows.

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
        ilike_term = _topic_category_ilike_term(category)
        if ilike_term:
            term_to_cats.setdefault(ilike_term, []).append(category)

    for ilike_term, matching_cats in term_to_cats.items():

        def _run_query(*, jsonb_contains: str | None = None, ilike: str | None = None):
            query = supabase_client.table("creators").select("*")
            if jsonb_contains:
                query = query.filter(
                    "topic_categories",
                    "cs",
                    json.dumps([jsonb_contains]),
                )
            elif ilike:
                # Use the word-level pattern builder: it handles URLs, underscore
                # slugs, and space-separated labels without escaping the separator.
                pattern = _topic_category_ilike_pattern(ilike)
                if pattern:
                    query = query.ilike("topic_categories", pattern)
            return (
                query.not_.is_("channel_name", "null")
                .gt("current_subscribers", 0)
                .order("current_subscribers", desc=True)
                .limit(limit_per_category)
                .execute()
            )

        creators: list[dict] = []

        # Tier 1 – JSONB containment with clean display label (fastest path).
        label = _topic_category_label(ilike_term)
        if label:
            try:
                r = _run_query(jsonb_contains=label)
                creators = r.data or []
            except Exception:
                logger.debug(
                    "JSONB label query failed for %r, trying URL tier", label, exc_info=True
                )

        # Tier 2 – JSONB containment with canonical Wikipedia URL.
        if not creators:
            jsonb_value = _topic_category_jsonb_value(ilike_term)
            if jsonb_value:
                try:
                    r = _run_query(jsonb_contains=jsonb_value)
                    creators = r.data or []
                except Exception:
                    logger.debug(
                        "JSONB URL query failed for %r, falling back to ILIKE",
                        jsonb_value,
                        exc_info=True,
                    )

        # Tier 3 – ILIKE text search (handles any legacy storage format).
        if not creators:
            try:
                r = _run_query(ilike=ilike_term)
                creators = r.data or []
            except Exception:
                logger.exception("All query tiers failed for category %r", ilike_term)

        # All keys that share this ilike_term get the same creator list.
        for cat in matching_cats:
            result[cat] = creators

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
        resp = _rpc_with_retry(supabase_client, rpc_name, {"p_limit": limit})
        if resp.data:
            return [(row[key_name], row["creator_count"]) for row in resp.data]
        logger.warning("[Lists] %s RPC returned no data", rpc_name)
        return []

    except Exception as e:
        # Log at ERROR so fallback activation is visible in monitoring dashboards.
        # Fallbacks scan up to _MAX_FALLBACK_FETCH rows — they are a safety net,
        # not a primary path. If this fires repeatedly, the RPC needs fixing.
        logger.error(
            "[Lists] %s RPC failed — activating client-side fallback (capped at %d rows). "
            "Fix the RPC to restore accurate counts. Error: %s",
            rpc_name,
            _MAX_FALLBACK_FETCH,
            e,
            exc_info=True,
        )
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

    Results are cached in-process for ``_TOP_COUNTRIES_TTL_SECONDS`` seconds
    so repeated requests (e.g. filter-bar renders) don't each hit the DB.
    """
    global _top_countries_cache
    now = time.monotonic()
    if _top_countries_cache is not None:
        ts, full = _top_countries_cache
        if now - ts < _TOP_COUNTRIES_TTL_SECONDS:
            return full[:limit]

    full = _fetch_top_counts(
        "get_top_countries_with_counts",
        "country_code",
        lambda lim: _scan_column_counts("country_code", lim),
        200,  # fetch all so any limit can be served from cache
    )
    _top_countries_cache = (now, full)
    return full[:limit]


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

    Results are cached in-process for ``_TOP_LANGUAGES_TTL_SECONDS`` seconds.
    """
    global _top_languages_cache
    now = time.monotonic()
    if _top_languages_cache is not None:
        ts, full = _top_languages_cache
        if now - ts < _TOP_LANGUAGES_TTL_SECONDS:
            return full[:limit]

    full = _fetch_top_counts(
        "get_top_languages_with_counts",
        "language_code",
        lambda lim: _scan_column_counts("default_language", lim),
        300,  # fetch all so any limit can be served from cache
    )
    _top_languages_cache = (now, full)
    return full[:limit]


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

    The 60-second cache is intentionally process-local. It avoids duplicate
    requests during a single page render and is cleared by local metadata
    refresh helpers; other workers may briefly diverge until their TTL expires.

    Note: if the DB RPC predates the ``total_languages`` column (migration
    003), the key defaults to 0 gracefully via ``row.get(...) or 0``.
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        return _EMPTY_META.copy()

    global _lists_meta_cache
    if _lists_meta_cache:
        expires_at, cached_meta = _lists_meta_cache
        if expires_at > time.monotonic():
            return cached_meta.copy()
        _lists_meta_cache = None

    try:
        resp = supabase_client.rpc("get_lists_meta").execute()
        if resp.data:
            row = resp.data[0]
            meta = {
                "total_creators": int(row.get("total_creators") or 0),
                "total_countries": int(row.get("total_countries") or 0),
                "total_categories": TOTAL_TOPIC_CATEGORIES,
                # total_languages added in migration 003; defaults to 0 on older DBs
                "total_languages": int(row.get("total_languages") or 0),
            }
            _lists_meta_cache = (time.monotonic() + _LISTS_META_TTL_SECONDS, meta)
            return meta.copy()
        logger.warning("[Lists] get_lists_meta RPC returned no data")
        return _get_lists_meta_cached_tables()

    except Exception as e:
        logger.exception(f"Error fetching lists meta via RPC: {e}")
        return _get_lists_meta_cached_tables()


def _get_lists_meta_cached_tables() -> dict:
    """
    Read list metadata from the small cached tables used by the fast RPC.

    This is the first fallback when ``get_lists_meta`` times out. It avoids the
    old emergency scan over creators and keeps /creators and /lists usable even
    when the RPC definition in Supabase has regressed to live aggregation.
    """
    supabase_client = _get_supabase_client()
    if not supabase_client:
        return _EMPTY_META.copy()

    try:
        meta_resp = (
            supabase_client.table("mv_lists_meta")
            .select("total_creators,total_countries,total_languages")
            .limit(1)
            .execute()
        )
        if not meta_resp.data:
            logger.warning("[Lists] mv_lists_meta fallback returned no data")
            return _get_lists_meta_fallback()

        meta_row = meta_resp.data[0]
        meta = {
            "total_creators": int(meta_row.get("total_creators") or 0),
            "total_countries": int(meta_row.get("total_countries") or 0),
            "total_categories": TOTAL_TOPIC_CATEGORIES,
            "total_languages": int(meta_row.get("total_languages") or 0),
        }

        global _lists_meta_cache
        _lists_meta_cache = (time.monotonic() + _LISTS_META_TTL_SECONDS, meta)
        return meta.copy()

    except Exception:
        logger.exception("[Lists] cached-table metadata fallback failed")
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
        for row in rows:
            if cc := row.get("country_code"):
                countries.add(cc)
            if lang := row.get("default_language"):
                languages.add(lang)
        return {
            "total_creators": total_creators,
            "total_countries": len(countries),
            "total_categories": TOTAL_TOPIC_CATEGORIES,
            "total_languages": len(languages),
        }
    except Exception as e:
        logger.exception("Error in lists meta fallback: %s", e)
        return _EMPTY_META.copy()


def get_top_categories_with_counts(limit: int = 10) -> list[tuple[str, int]]:
    """
    Get topic categories mapped to the fixed YouTube topic taxonomy.

    The YouTube ``topicDetails.topicCategories`` taxonomy is finite. This
    function always projects DB counts onto that fixed label set so endpoint
    pagination and category counts remain stable even when DB refresh jobs lag.

    Returns list of (category_name, creator_count) tuples.
    Used for the "By Category" tab and the /creators filter dropdown.
    """
    global _top_categories_cache

    if limit <= 0:
        return []

    # Return from process-level cache when fresh (avoids repeated DB round-trips
    # on a warm Vercel function instance between worker refresh cycles).
    now = time.monotonic()
    if _top_categories_cache is not None:
        cached_at, cached_result = _top_categories_cache
        if now - cached_at < _TOP_CATEGORIES_TTL_SECONDS:
            return cached_result[: min(limit, TOTAL_TOPIC_CATEGORIES)]

    # Fetch enough rows to cover the full fixed taxonomy before projection.
    fetch_limit = max(limit, TOTAL_TOPIC_CATEGORIES)
    raw_counts = _fetch_top_counts(
        "get_top_categories_with_counts",
        "category",
        _scan_categories_fallback,
        fetch_limit,
    )
    merged = _merge_with_fixed_topic_categories(raw_counts)
    full = merged[:TOTAL_TOPIC_CATEGORIES]

    # Cache the full taxonomy-sized result; individual callers slice from it.
    _top_categories_cache = (now, full)

    return full[: min(limit, TOTAL_TOPIC_CATEGORIES)]


def suggest_primary_categories(q: str, limit: int = 8) -> list[tuple[str, int]]:
    """
    Case-insensitive ILIKE search on primary_category for the filter typeahead.

    Uses idx_creators_primary_category_trgm (migration 028) so the query is
    an index scan rather than a seq scan — typically <10ms per keystroke.
    Fetches up to 500 matching rows and counts in Python to approximate
    creator counts per category (GROUP BY is not available via PostgREST).

    Returns:
        List of (category_name, creator_count) sorted by count descending,
        capped at *limit*.  Empty list when *q* is blank or on error.
    """
    supabase_client = _get_supabase_client()
    if not supabase_client or not q.strip():
        return []
    try:
        escaped = _escape_ilike(q)
        resp = (
            supabase_client.table("creators")
            .select("primary_category")
            .eq("sync_status", "synced")
            .not_.is_("primary_category", "null")
            .gt("current_subscribers", 0)
            .ilike("primary_category", f"%{escaped}%")
            .limit(500)
            .execute()
        )
        counts: dict[str, int] = {}
        for row in resp.data or []:
            cat = row.get("primary_category")
            if cat:
                counts[cat] = counts.get(cat, 0) + 1
        return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]
    except Exception as e:
        logger.exception("suggest_primary_categories error for %r: %s", q, e)
        return []


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
            .in_("sync_status", ["synced", "synced_partial"])
            .not_.is_("channel_name", "null")
            .not_.is_("topic_categories", "null")
            .gt("current_subscribers", 0)
            .limit(_MAX_FALLBACK_FETCH)
            .execute()
        )
        category_counts: dict[str, int] = {}
        for row in response.data or []:
            for clean in _iter_normalized_topic_categories(row.get("topic_categories")):
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
        cat_list = _iter_normalized_topic_categories(row.get("topic_categories"))
        if not cat_list:
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

        for clean in cat_list:
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
