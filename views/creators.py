"""
Creator Intelligence Dashboard - Analytics-first design for YouTube creators
Focused on what matters: Growth, Revenue, Engagement, Quality

Data collected by worker:
- current_subscribers, current_view_count, current_video_count (from YouTube API)
- engagement_score (calculated from recent video comments/likes)
- quality_grade (A+/A/B+/B/C based on engagement + subscriber size)
- country_code, channel_name, channel_thumbnail_url
- last_updated_at, last_synced_at (for freshness indicator)
- 30-day deltas (subscribers_change_30d, views_change_30d)

Creator perspective (Jimmy Donaldson style):
1. Growth trajectory (trending up/down?) - MOST IMPORTANT
2. Revenue potential (monthly earnings)
3. Engagement quality (audience size vs views ratio)
4. Video consistency (how many videos, posting frequency)
5. Ranking/position (competitive benchmark)
6. Quality assessment (why are they ranked this way?)
"""

from __future__ import annotations

import json
import logging
import re as _re  # private alias — wildcard `from fasthtml.common import *` cannot shadow it
from collections.abc import Callable
from urllib.parse import urlencode, unquote, quote, quote_plus, urlparse

from fasthtml.common import *
from monsterui.all import *

from utils import format_date_relative, format_number, safe_get_value, slugify
from utils.creator_metrics import (
    calculate_avg_views_per_video,
    calculate_growth_rate,
    calculate_momentum_score,
    calculate_views_per_subscriber,
    estimate_monthly_revenue_v4,
    get_momentum_label,
    format_channel_age,
    get_activity_badge,
    get_activity_title,
    get_age_emoji,
    get_age_title,
    get_country_flag,
    get_grade_info,
    get_growth_signal,
    get_language_emoji,
    get_language_name,
    get_sync_status_badge,
)
from db import calculate_creator_stats, get_creator_hero_stats
from components.category_stats import render_category_box_plots

logger = logging.getLogger(__name__)


# ============================================================================
# SOCIAL LINK EXTRACTION HELPERS
# ============================================================================
# FrankenUI uses Lucide icons directly — UkIcon("name") maps 1-to-1 to Lucide.
# Lucide stopped accepting new brand icons in 2022, so only these social
# platforms have native icons:
#   instagram  twitter  facebook  linkedin  github  youtube  mail
# Semantic fallbacks for platforms without a Lucide brand icon:
#   TikTok → "music"   Twitch → "monitor-play"
#   Discord → "message-circle"   Patreon → "heart"   Linktree → "link"

# Each entry: (compiled_regex, lucide_icon, display_label, url_template)
_SOCIAL_PATTERNS = [
    # ── Native Lucide brand icons ─────────────────────────────────────────────
    (
        _re.compile(r"instagram\.com/(?!(?:p|reel|stories|explore)/)([\w.]+)", _re.I),
        "instagram",
        "Instagram",
        "https://instagram.com/{}",
    ),
    (
        _re.compile(r"(?:instagram|ig)[:\s]+@?([\w.]+)", _re.I),
        "instagram",
        "Instagram",
        "https://instagram.com/{}",
    ),
    (
        _re.compile(r"(?:twitter|x)\.com/([\w]+)", _re.I),
        "twitter",
        "X / Twitter",
        "https://x.com/{}",
    ),
    (
        _re.compile(r"twitter[:\s]+@?([\w]+)", _re.I),
        "twitter",
        "X / Twitter",
        "https://x.com/{}",
    ),
    (
        _re.compile(r"facebook\.com/([\w.]+)", _re.I),
        "facebook",
        "Facebook",
        "https://facebook.com/{}",
    ),
    (
        _re.compile(r"linkedin\.com/(?:in|company)/([\w-]+)", _re.I),
        "linkedin",
        "LinkedIn",
        "https://linkedin.com/in/{}",
    ),
    (
        _re.compile(r"github\.com/([\w-]+)", _re.I),
        "github",
        "GitHub",
        "https://github.com/{}",
    ),
    # ── Semantic fallbacks (no Lucide brand icon exists) ──────────────────────
    (
        _re.compile(r"tiktok\.com/@([\w.]+)", _re.I),
        "music",
        "TikTok",
        "https://tiktok.com/@{}",
    ),
    (
        _re.compile(r"(?:tiktok|tt)[:\s]+@?([\w.]+)", _re.I),
        "music",
        "TikTok",
        "https://tiktok.com/@{}",
    ),
    (
        _re.compile(r"twitch\.tv/([\w]+)", _re.I),
        "monitor-play",
        "Twitch",
        "https://twitch.tv/{}",
    ),
    (
        _re.compile(r"discord\.gg/([\w-]+)", _re.I),
        "message-circle",
        "Discord",
        "https://discord.gg/{}",
    ),
    (
        _re.compile(r"patreon\.com/([\w-]+)", _re.I),
        "heart",
        "Patreon",
        "https://patreon.com/{}",
    ),
    (
        _re.compile(r"linktr\.ee/([\w-]+)", _re.I),
        "link",
        "Linktree",
        "https://linktr.ee/{}",
    ),
    # ── Generic website catch-all (must be last) ──────────────────────────────
    # group(1) = full URL (used as href); group(2) = bare domain (used for _SKIP_DOMAINS check)
    (
        _re.compile(
            r"(https?://(?:www\.)?([\w.-]+\.[a-z]{2,})(?:/[^\s]*)?)",
            _re.I,
        ),
        "globe",
        "Website",
        "{}",
    ),
]

_EMAIL_RE = _re.compile(r"\b([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})\b")

# Domains to skip in the catch-all so social platforms don't appear twice
_SKIP_DOMAINS = frozenset(
    {
        "youtube.com",
        "youtu.be",
        "instagram.com",
        "twitter.com",
        "x.com",
        "tiktok.com",
        "facebook.com",
        "twitch.tv",
        "github.com",
        "patreon.com",
        "discord.gg",
        "linkedin.com",
        "linktr.ee",
        "t.co",
        "bit.ly",
        "google.com",
        "wikipedia.org",
        "goo.gl",
        "amzn.to",
    }
)

# Brand-accurate colours keyed by Lucide icon name
_SOCIAL_COLOURS = {
    "instagram": "text-pink-500 hover:text-pink-600",
    "twitter": "text-sky-500 hover:text-sky-600",
    "facebook": "text-blue-600 hover:text-blue-700",
    "linkedin": "text-blue-700 hover:text-blue-800",
    "github": "text-foreground hover:text-primary",
    "youtube": "text-red-600 hover:text-red-700",
    "mail": "text-violet-600 hover:text-violet-700",
    "music": "text-foreground hover:text-primary",  # TikTok
    "monitor-play": "text-purple-600 hover:text-purple-700",  # Twitch
    "message-circle": "text-indigo-500 hover:text-indigo-600",  # Discord
    "heart": "text-rose-500 hover:text-rose-600",  # Patreon
    "link": "text-emerald-600 hover:text-emerald-700",  # Linktree
    "globe": "text-emerald-600 hover:text-emerald-700",
}


def _extract_socials(bio: str, keywords: str) -> list[tuple[str, str, str]]:
    """
    Parse social links and emails from bio + keywords text.

    Returns a deduplicated list of (lucide_icon, label, href) tuples,
    capped at 8 entries.
    """
    text = f"{bio or ''} {keywords or ''}".strip()
    if not text:
        return []

    found: list[tuple[str, str, str]] = []
    seen: set[str] = set()

    # Emails first — highest-value contact signal
    for email in _EMAIL_RE.findall(text):
        href = f"mailto:{email}"
        if href not in seen:
            found.append(("mail", email, href))
            seen.add(href)

    # Social patterns in priority order (specific before generic)
    for pattern, icon, label, url_tpl in _SOCIAL_PATTERNS:
        for m in pattern.finditer(text):
            if icon == "globe":
                # group(1) = full URL, group(2) = bare domain
                full_url = m.group(1)
                domain = m.group(2).lower()
                if any(skip in domain for skip in _SKIP_DOMAINS):
                    continue
                href = full_url
            else:
                handle = m.group(1).strip("/ .")
                if not handle or len(handle) < 2:
                    continue
                href = url_tpl.format(handle)
            if href not in seen:
                found.append((icon, label, href))
                seen.add(href)

    return found[:8]


# Matches youtube.com (with or without www) and the youtu.be short-link domain.
_YT_URL_RE = _re.compile(r"https?://(?:(?:www\.)?youtube\.com|youtu\.be)/\S+", _re.I)


def _parse_featured_channels(raw: str) -> list[str]:
    """Parse featured_channels_urls (newline/comma/space separated) → YouTube URLs.

    Accepts both ``youtube.com`` and ``youtu.be`` variants.
    """
    if not raw:
        return []
    seen: set[str] = set()
    result = []
    for u in _re.split(r"[\n,\s]+", raw.strip()):
        u = u.strip()
        if u and _YT_URL_RE.match(u) and u not in seen:
            seen.add(u)
            result.append(u)
    return result[:6]


# Topic Category to Emoji Mapping (for topicCategories from YouTube API)
# These are Wikipedia-based categories distinct from video categoryId
TOPIC_CATEGORY_EMOJI_MAP = {
    "music": "🎵",
    "gaming": "🎮",
    "video game": "🎮",
    "game": "🎮",
    "sport": "⚽",
    "basketball": "🏀",
    "baseball": "⚾",
    "football": "🏈",
    "soccer": "⚽",
    "entertainment": "🎬",
    "film": "🎬",
    "movie": "🎬",
    "television": "📺",
    "education": "🎓",
    "knowledge": "📚",
    "science": "🔬",
    "technology": "💻",
    "food": "🍳",
    "lifestyle": "🏠",
    "cooking": "🍳",
    "health": "💪",
    "fitness": "💪",
    "art": "🎨",
    "performing arts": "🎭",
    "society": "🎪",
    "culture": "🎪",
    "news": "📰",
    "politics": "🏛️",
    "comedy": "😂",
    "humor": "😂",
    "travel": "✈️",
    "nature": "🌿",
    "animals": "🐾",
    "pets": "🐾",
    "fashion": "👗",
    "beauty": "💄",
    "diy": "🔨",
    "craft": "✂️",
    "business": "💼",
    "finance": "💰",
    "automotive": "🚗",
    "vehicle": "🚗",
}


def get_topic_category_emoji(category_name: str) -> str:
    """
    Get emoji for a topic category name.

    Args:
        category_name: Topic category name (e.g., "Music", "Video game culture")

    Returns:
        Emoji string (e.g., "🎵", "🎮")

    Examples:
        get_topic_category_emoji("Music") → "🎵"
        get_topic_category_emoji("Video game culture") → "🎮"
        get_topic_category_emoji("Entertainment") → "🎬"
    """
    if not category_name:
        return "🏷️"

    name_lower = category_name.lower()

    # Try to find a matching emoji
    for key, emoji in TOPIC_CATEGORY_EMOJI_MAP.items():
        if key in name_lower:
            return emoji

    # Default fallback
    return "🏷️"


def _filter_valid_creators(creators: list[dict]) -> list[dict]:
    """
    Filter out creators with incomplete data.

    Only shows creators that have:
    - A channel_name (successfully resolved)
    - At least 1 subscriber (data has been synced)

    This prevents showing empty "Sync Pending" cards.
    """

    def get_val(obj, key, default=None):
        if isinstance(obj, dict):
            v = obj.get(key, default)
        else:
            v = getattr(obj, key, default)
        return v if v is not None else default

    valid = []
    for creator in creators:
        channel_name = get_val(creator, "channel_name")
        subs = get_val(creator, "current_subscribers", 0)

        if channel_name and subs > 0:
            valid.append(creator)

    return valid


# ============================================================================
# URL CONSTRUCTION HELPERS
# ============================================================================


def _build_filter_url(
    *,
    sort: str,
    search: str,
    grade: str = "all",
    language: str = "all",
    activity: str = "all",
    age: str = "all",
    country: str = "all",
    category: str = "all",
    page: int = None,
    per_page: int = None,
) -> str:
    """
    Central helper for building /creators filter URLs.

    Ensures all filter links stay in sync when query parameters change.
    By consolidating URL construction here, we prevent parameter divergence
    and reduce maintenance overhead.

    Args:
        sort: Sort criteria (subscribers, views, engagement, etc.)
        search: Search query string
        grade: Quality grade filter (all, A+, A, B+, B, C)
        language: Language filter (all, en, ja, es, etc.)
        activity: Activity level filter (all, active, dormant)
        age: Channel age filter (all, new, established, veteran)
        country: Country filter (all, or country code)
        page: Optional page number for pagination
        per_page: Optional items per page for pagination

    Returns:
        URL string for /creators with all parameters encoded

    Examples:
        _build_filter_url(sort='subscribers', search='', grade='A+')
        → '/creators?sort=subscribers&search=&grade=A%2B&language=all&...'

        _build_filter_url(sort='subscribers', search='', page=2, per_page=20)
        → '/creators?sort=subscribers&search=&page=2&per_page=20&...'
    """
    params = {
        "sort": sort,
        "search": search,
        "grade": grade,
        "language": language,
        "activity": activity,
        "age": age,
        "country": country,
        "category": category,
    }

    if page is not None:
        params["page"] = str(page)
    if per_page is not None:
        params["per_page"] = str(per_page)

    return f"/creators?{urlencode(params)}"


# ============================================================================
# ADD CREATOR SECTION
# ============================================================================


def render_add_creator_section() -> Div:
    """
    HTMX form that lets authenticated users submit a creator by @handle or
    channel ID.  No YouTube API is called on the Vercel frontend — the input
    is passed directly to the backend worker queue via ``POST /creators/request``.
    """
    return Div(
        Div(
            Div(
                UkIcon("plus-circle", cls="size-5 text-primary shrink-0 mt-0.5"),
                Div(
                    H3("Submit a Creator", cls="text-base font-semibold text-foreground"),
                    P(
                        "Know a channel that's not listed? Submit their @handle or "
                        "channel ID and our system will add them automatically.",
                        cls="text-sm text-muted-foreground mt-0.5",
                    ),
                    cls="flex-1 min-w-0",
                ),
                cls="flex items-start gap-3",
            ),
            # Input + submit (HTMX inline form)
            Form(
                Div(
                    Input(
                        type="text",
                        name="q",
                        id="creator-add-input",
                        placeholder="@MrBeast or UCX6OQ3DkcsbYNE6H8uQQuVA",
                        autocomplete="off",
                        cls="flex-1 px-3 py-2 text-sm rounded-lg border border-border "
                        "bg-background focus:outline-none focus:ring-2 focus:ring-primary/40",
                    ),
                    Button(
                        UkIcon("send", cls="size-4 mr-1.5"),
                        "Submit",
                        type="submit",
                        cls="flex items-center px-4 py-2 text-sm font-medium rounded-lg "
                        "bg-primary text-primary-foreground hover:bg-primary/90 "
                        "transition-colors shrink-0",
                    ),
                    cls="flex gap-2 mt-3",
                ),
                # Response target injected below the form
                Div(id="creator-add-result", cls="mt-3"),
                hx_post="/creators/request",
                hx_target="#creator-add-result",
                hx_swap="innerHTML",
            ),
            cls="p-4 rounded-xl border border-border bg-background",
        ),
        cls="mt-6 mb-2",
    )


def render_add_creator_result(
    success: bool,
    message: str,
    creator_id: str = "",
    input_query: str = "",
) -> Div:
    """
    HTMX partial returned by POST /creators/request.
    Renders a success or error notice inline below the submit form.

    When ``success=True`` and ``input_query`` is provided, an HTMX poll is
    attached so the card auto-updates to a profile link once the worker
    completes (without requiring a full page reload).
    """
    if success:
        poll_attrs = {}
        if input_query:
            status_url = f"/creators/add-status?{urlencode({'q': input_query})}"
            poll_attrs = dict(
                hx_get=status_url,
                hx_trigger="load, every 3s",
                hx_target="this",
                hx_swap="outerHTML",
            )
        return Div(
            UkIcon("check-circle", cls="size-4 text-green-600 shrink-0"),
            Div(
                P(
                    "Request queued!",
                    cls="text-sm font-semibold text-green-700 dark:text-green-400",
                ),
                P(message, cls="text-xs text-muted-foreground mt-0.5"),
                cls="flex-1",
            ),
            cls="flex items-start gap-2 p-3 rounded-lg bg-green-50 dark:bg-green-950/30 "
            "border border-green-200 dark:border-green-800",
            **poll_attrs,
        )

    # creator_id is non-empty when the creator already exists in the DB
    if creator_id:
        return Div(
            UkIcon("info", cls="size-4 text-blue-600 shrink-0"),
            Div(
                P(
                    "Already in the database",
                    cls="text-sm font-semibold text-blue-700 dark:text-blue-400",
                ),
                A(
                    "View their profile →",
                    href=f"/creator/{creator_id}",
                    cls="text-xs text-primary hover:underline",
                ),
                cls="flex-1",
            ),
            cls="flex items-start gap-2 p-3 rounded-lg bg-blue-50 dark:bg-blue-950/30 "
            "border border-blue-200 dark:border-blue-800",
        )

    return Div(
        UkIcon("alert-circle", cls="size-4 text-red-600 shrink-0"),
        P(message, cls="text-sm text-red-700 dark:text-red-400 flex-1"),
        cls="flex items-start gap-2 p-3 rounded-lg bg-red-50 dark:bg-red-950/30 "
        "border border-red-200 dark:border-red-800",
    )


def render_add_creator_status_result(
    status: str,
    creator_id: str = "",
    input_query: str = "",
) -> Div:
    """
    HTMX partial returned by GET /creators/add-status.

    ``status`` values:
        - ``"processing"`` — still in progress; card re-polls every 3 s.
        - ``"completed"``  — creator is ready; renders a "View profile →" link.
        - ``"failed"``     — worker could not resolve the creator.
    """
    if status == "completed" and creator_id:
        return Div(
            UkIcon("check-circle", cls="size-4 text-green-600 shrink-0"),
            Div(
                P(
                    "Creator added!",
                    cls="text-sm font-semibold text-green-700 dark:text-green-400",
                ),
                A(
                    "View their profile →",
                    href=f"/creator/{creator_id}",
                    cls="text-xs text-primary hover:underline",
                ),
                cls="flex-1",
            ),
            cls="flex items-start gap-2 p-3 rounded-lg bg-green-50 dark:bg-green-950/30 "
            "border border-green-200 dark:border-green-800",
        )

    if status == "failed":
        return Div(
            UkIcon("alert-circle", cls="size-4 text-red-600 shrink-0"),
            P(
                "We couldn't find that creator. Please check the @handle or channel ID and try again.",
                cls="text-sm text-red-700 dark:text-red-400 flex-1",
            ),
            cls="flex items-start gap-2 p-3 rounded-lg bg-red-50 dark:bg-red-950/30 "
            "border border-red-200 dark:border-red-800",
        )

    # Processing without a query to poll is unrecoverable — render as failed.
    if not input_query:
        return Div(
            UkIcon("alert-circle", cls="size-4 text-red-600 shrink-0"),
            P(
                "We couldn't find that creator. Please check the @handle or channel ID and try again.",
                cls="text-sm text-red-700 dark:text-red-400 flex-1",
            ),
            cls="flex items-start gap-2 p-3 rounded-lg bg-red-50 dark:bg-red-950/30 "
            "border border-red-200 dark:border-red-800",
        )

    # Still processing — continue polling
    status_url = f"/creators/add-status?{urlencode({'q': input_query})}"
    poll_attrs = dict(
        hx_get=status_url,
        hx_trigger="every 3s",
        hx_target="this",
        hx_swap="outerHTML",
    )
    return Div(
        Div(
            cls="size-4 rounded-full border-2 border-primary border-t-transparent animate-spin shrink-0"
        ),
        P(
            "Processing… this usually takes under a minute.",
            cls="text-sm text-muted-foreground flex-1",
        ),
        cls="flex items-center gap-2 p-3 rounded-lg bg-muted/40 border border-border",
        **poll_attrs,
    )


# ============================================================================
# FAVOURITE BUTTON COMPONENT
# ============================================================================


def render_favourite_button(creator_id: str, is_favourited: bool = False) -> Div:
    """
    Heart-shaped toggle button for marking a creator as a favourite.

    The button posts to ``POST /creator/{creator_id}/favourite`` via HTMX and
    swaps itself in-place when the server responds with the updated fragment.

    Args:
        creator_id:     UUID of the creator (used in the HTMX target URL).
        is_favourited:  Current state — True renders a filled/red heart,
                        False renders an outlined/grey heart.

    The wrapping ``div`` carries the ``id="fav-btn-{creator_id}"`` HTMX swap
    target and is ``inline-flex`` so it does not disrupt flex-row siblings.
    """
    if is_favourited:
        icon_cls = "w-4 h-4 fill-red-500 text-red-500"
        btn_cls = (
            "inline-flex items-center gap-1.5 px-3 py-1.5 sm:px-4 sm:py-2 "
            "bg-red-50 hover:bg-red-100 border border-red-200 text-red-600 "
            "text-xs sm:text-sm font-semibold rounded-lg transition-colors"
        )
        label = "Saved"
        aria_label = "Remove from favourites"
    else:
        icon_cls = "w-4 h-4 text-gray-400 hover:text-red-500"
        btn_cls = (
            "inline-flex items-center gap-1.5 px-3 py-1.5 sm:px-4 sm:py-2 "
            "bg-accent hover:bg-red-50 border border-gray-200 hover:border-red-200 "
            "text-foreground text-xs sm:text-sm font-semibold rounded-lg transition-colors"
        )
        label = "Save"
        aria_label = "Add to favourites"

    return Div(
        Button(
            UkIcon("heart", cls=icon_cls),
            Span(label),  # always visible — consistent with sibling YouTube and Back buttons
            hx_post=f"/creator/{creator_id}/favourite",
            hx_target=f"#fav-btn-{creator_id}",
            hx_swap="outerHTML",
            aria_label=aria_label,
            cls=btn_cls,
        ),
        id=f"fav-btn-{creator_id}",
        cls="inline-flex",  # size to content — behaves as an inline sibling in flex rows
    )


# ============================================================================
# MAIN PAGE FUNCTION
# ============================================================================


def render_creators_page(
    creators: list[dict],
    sort: str = "subscribers",
    search: str = "",
    grade_filter: str = "all",
    language_filter: str = "all",
    activity_filter: str = "all",
    age_filter: str = "all",
    country_filter: str = "all",
    category_filter: str = "all",
    stats: dict = None,
    page: int = 1,
    per_page: int = 50,
    total_count: int = 0,
    total_pages: int = 1,
    is_authenticated: bool = False,
    favourite_ids: set[str] | None = None,
    handle_not_found: bool = False,
) -> Div:
    """
    Analytics-first creator discovery dashboard.
    Optimized for what creators care about: growth, revenue, engagement.
    Args:
        creators: List of creator dicts with stats and ranking
        sort: Sort criteria (subscribers, views, videos, engagement, quality)
        search: Search query for filtering by name
        grade_filter: Quality grade filter (all, A+, A, B+, B, C)
        stats: Aggregate statistics dict from backend
    """
    # Grade counts for the filter modal badge ("X creators available").
    # Computed from the current page — a full-DB per-grade count would require
    # an extra query.  Acceptable approximation for the badge display.
    grade_counts = _count_by_grade(creators)

    # NOTE: _filter_valid_creators is intentionally NOT called here.
    # db.get_creators() already applies the same conditions:
    #   channel_name IS NOT NULL, current_subscribers > 0, sync_status = "synced"
    # Applying a second filter on the paginated list would silently reduce the page
    # size while total_count (used for pagination math) stays based on the DB count,
    # making some pages appear to have fewer results than expected.

    # Use provided stats or build them from the current page + RPC global counts.
    # Must use the merge pattern so distribution keys (top_countries, top_languages,
    # grade_counts, etc.) are always present — get_creator_hero_stats() alone only
    # returns the 5 numeric hero keys and would leave _render_filter_bar with nothing.
    if stats is None:
        page_stats = calculate_creator_stats(creators)
        hero_stats = get_creator_hero_stats()
        stats = {**page_stats, **hero_stats}

    # Check if any filters are active
    has_active_filters = (
        search
        or grade_filter != "all"
        or language_filter != "all"
        or activity_filter != "all"
        or age_filter != "all"
        or country_filter != "all"
        or category_filter != "all"
    )

    return Container(
        # Hero section with real stats from DB
        _render_hero(
            stats=stats,
            # total_count is the exact DB count for the current query (with or without
            # filters). Using it instead of len(creators) ensures the hero shows the
            # real filtered total ("450 of 500"), not just the current page size ("50").
            filtered_count=total_count,
            has_filters=has_active_filters,
        ),
        # Filter controls (sticky bar)
        _render_filter_bar(
            search=search,
            sort=sort,
            grade_filter=grade_filter,
            language_filter=language_filter,
            activity_filter=activity_filter,
            age_filter=age_filter,
            country_filter=country_filter,
            category_filter=category_filter,
            grade_counts=grade_counts,
            top_countries=stats.get("top_countries", []) if stats else [],
            top_categories=stats.get("top_categories", []) if stats else [],
            top_languages=stats.get("top_languages", []) if stats else [],
            total_count=total_count,
            per_page=per_page,
        ),
        # "@handle not in DB" banner — only shown alongside a real results grid.
        # When creators is empty, _render_empty_state Flow 1 handles the CTA,
        # avoiding duplicate id="creator-add-result" in the same page.
        (
            _render_handle_not_found_banner(search, is_authenticated)
            if handle_not_found and creators
            else None
        ),
        # Creators grid or empty state
        (
            Div(
                _render_creators_grid(creators, favourite_ids=favourite_ids),
                # Pagination controls
                _render_pagination(
                    page=page,
                    total_pages=total_pages,
                    search=search,
                    sort=sort,
                    grade_filter=grade_filter,
                    language_filter=language_filter,
                    activity_filter=activity_filter,
                    age_filter=age_filter,
                    country_filter=country_filter,
                    category_filter=category_filter,
                    per_page=per_page,
                    total_count=total_count,
                ),
            )
            if creators
            else _render_empty_state(search, grade_filter, has_active_filters, is_authenticated)
        ),
        cls=ContainerT.xl,
    )


def _render_hero(stats: dict, filtered_count: int = 0, has_filters: bool = False) -> Div:
    """
    Hero section with marketing-relevant statistics from database.

    Smart display:
    - No filters: Shows total creators from DB (e.g., "500 creators")
    - With filters: Shows filtered count + total (e.g., "45 of 500 creators")

    All numbers come from DB state (via stats dict), NOT from filtered results.
    Designed for agencies looking to identify collaboration opportunities.
    """
    # Extract only the metrics actually used in hero rendering
    total_creators = stats.get("total_creators", 0)
    total_countries = stats.get("total_countries", 0)
    total_languages = stats.get("total_languages", 0)
    total_categories = stats.get("total_categories", 0)
    top_countries = stats.get("top_countries") or []
    top_languages = stats.get("top_languages") or []
    top_categories = stats.get("top_categories") or []

    return Div(
        Div(
            P(
                "Creator Intelligence",
                cls="text-xs font-semibold text-muted-foreground uppercase tracking-widest",
            ),
            P(
                "Discover high-performing creators for brand collaborations.",
                cls="text-sm text-muted-foreground mt-1",
            ),
            cls="mb-5",
        ),
        # Metric Strip - marketing-focused metrics from DB
        Div(
            # Total/Filtered creators - smart display based on filter state
            Div(
                P(
                    "Filtered Results" if has_filters else "Total Creators",
                    cls="text-xs font-semibold text-muted-foreground uppercase tracking-wider",
                ),
                H2(
                    (
                        f"{format_number(filtered_count)} of {format_number(total_creators)}"
                        if has_filters
                        else format_number(total_creators)
                    ),
                    cls="text-2xl font-bold text-foreground mt-1",
                ),
                P(
                    "matching filters" if has_filters else "In database",
                    cls="text-xs text-muted-foreground mt-1",
                ),
                cls="text-center",
            ),
            # Global Reach - Countries with flag showcase
            Div(
                # Card header links to the full country rankings list
                A(
                    P(
                        "Global Reach",
                        cls="text-xs font-semibold text-blue-600 uppercase tracking-wider mb-2",
                    ),
                    H2(
                        f"{format_number(total_countries)} Nations",
                        cls="text-xl font-bold text-blue-600",
                    ),
                    href="/lists?tab=by-country",
                    cls="block no-underline hover:opacity-80 transition-opacity",
                ),
                # Top country flags — each links to that country’s ranked creator list
                Div(
                    *(
                        [
                            A(
                                get_country_flag(country_code) or "🌍",
                                href=f"/lists/country/{country_code.upper()}",
                                title=f"{country_code.upper()}: {count} creators",
                                cls="text-2xl hover:scale-110 transition-transform inline-block",
                            )
                            for country_code, count in top_countries[:4]
                        ]
                        if top_countries
                        else [Span("🌍", cls="text-2xl")]
                    ),
                    cls="flex gap-1 justify-center mt-2",
                ),
                P(
                    "worldwide creators",
                    cls="text-xs text-blue-500 mt-1",
                ),
                cls="text-center bg-blue-50 dark:bg-blue-950/30 rounded-xl p-4 border border-blue-200 dark:border-blue-900 hover:border-blue-400 hover:shadow-sm transition-all",
            ),
            # Linguistic Diversity - Languages with emoji showcase
            Div(
                # Card header links to the creators page (language filter)
                A(
                    P(
                        "Languages",
                        cls="text-xs font-semibold text-purple-600 uppercase tracking-wider mb-2",
                    ),
                    H2(
                        format_number(total_languages),
                        cls="text-xl font-bold text-purple-600",
                    ),
                    href="/lists?tab=by-language",
                    cls="block no-underline hover:opacity-80 transition-opacity",
                ),
                # Top language emojis — each links to that language’s ranked creator list
                Div(
                    *(
                        [
                            A(
                                get_language_emoji(lang_code) or "🗣️",
                                href=f"/lists/language/{lang_code}",
                                title=f"{get_language_name(lang_code)}: {count} creators",
                                cls="text-2xl hover:scale-110 transition-transform inline-block",
                            )
                            for lang_code, count in top_languages
                        ]
                        if top_languages
                        else [Span("🗣️", cls="text-2xl")]
                    ),
                    cls="flex gap-1 justify-center mt-2 flex-wrap",
                ),
                P(
                    "content languages",
                    cls="text-xs text-purple-500 mt-1",
                ),
                cls="text-center bg-purple-50 dark:bg-purple-950/30 rounded-xl p-4 border border-purple-200 dark:border-purple-900 hover:border-purple-400 hover:shadow-sm transition-all",
            ),
            # Categories — content topic diversity (same source as /lists page)
            Div(
                # Card header links to the full category rankings list
                A(
                    P(
                        "Categories",
                        cls="text-xs font-semibold text-pink-600 uppercase tracking-wider mb-2",
                    ),
                    H2(
                        format_number(total_categories),
                        cls="text-xl font-bold text-pink-600",
                    ),
                    href="/lists?tab=by-category",
                    cls="block no-underline hover:opacity-80 transition-opacity",
                ),
                # Top category emojis — each links to that category's ranked creator list
                Div(
                    *(
                        [
                            A(
                                get_topic_category_emoji(cat_name) or "🏷️",
                                href=f"/lists/category/{slugify(cat_name)}",
                                title=f"{cat_name}: {count} creators",
                                cls="text-2xl hover:scale-110 transition-transform inline-block",
                            )
                            for cat_name, count in top_categories
                        ]
                        if top_categories
                        else [Span("🏷️", cls="text-2xl")]
                    ),
                    cls="flex gap-1 justify-center mt-2 flex-wrap",
                ),
                P(
                    "content topics",
                    cls="text-xs text-pink-500 mt-1",
                ),
                cls="text-center bg-pink-50 dark:bg-pink-950/30 rounded-xl p-4 border border-pink-200 dark:border-pink-900 hover:border-pink-400 hover:shadow-sm transition-all",
            ),
            cls="grid grid-cols-2 md:grid-cols-4 gap-6 md:gap-8 py-8 border-t border-b border-border",
        ),
        cls="bg-background rounded-lg border border-border p-6 md:p-8 mb-8",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Filter pill helper — DRYs the 6 near-identical pill sections in _render_filter_bar
# ─────────────────────────────────────────────────────────────────────────────
_PILL_BASE = "px-2.5 py-1 rounded-md transition-all inline-block no-underline text-xs font-medium "
_PILL_INACTIVE = "bg-background border border-border hover:bg-accent text-foreground"


def _filter_pills(
    options: list[tuple[str, str, str]],
    current_val: str,
    active_cls: str,
    build_url: Callable[[str], str],
) -> Div:
    """Render a row of filter pills.

    Args:
        options:    [(value, label, emoji), ...]
        current_val: currently active value
        active_cls: Tailwind classes for the active state
        build_url:  ``(value: str) -> str`` — returns the href for a given filter value
    """
    return Div(
        *[
            A(
                f"{emoji} {label}",
                href=build_url(val),
                cls=_PILL_BASE + (active_cls if current_val == val else _PILL_INACTIVE),
            )
            for val, label, emoji in options
        ],
        cls="flex gap-1.5 flex-wrap",
    )


def _render_filter_bar(
    search: str,
    sort: str,
    grade_filter: str,
    grade_counts: dict,
    language_filter: str = "all",
    activity_filter: str = "all",
    age_filter: str = "all",
    country_filter: str = "all",
    category_filter: str = "all",
    top_countries: list = None,
    top_categories: list = None,
    top_languages: list = None,
    total_count: int = 0,
    per_page: int = 50,
) -> Div:
    """
    Clean horizontal card-based filter bar.

    Shows search + sort on top line, then 4 filter cards below.
    All filters visible at once, no accordion clicks needed.

    Space: ~150px, all filters visible
    Clicks: 0 (vs 1-4 with accordion)
    Design: Modern, card-based, professional
    """

    # ═══════════════════════════════════════════════════════════════
    # 1. SEARCH BAR — full-width pill, icon inside, inline clear button
    # ═══════════════════════════════════════════════════════════════
    _clear_url = _build_filter_url(
        sort=sort,
        search="",
        grade=grade_filter,
        language=language_filter,
        activity=activity_filter,
        age=age_filter,
        country=country_filter,
        category=category_filter,
    )
    search_bar = Form(
        Div(
            Div(
                UkIcon("search", cls="size-4 text-muted-foreground"),
                cls="absolute left-3.5 top-1/2 -translate-y-1/2 pointer-events-none",
            ),
            Input(
                type="search",
                name="search",
                placeholder="Search by name or @handle…",
                value=search,
                cls="w-full h-11 pl-10 pr-9 rounded-full border border-border bg-background "
                "text-foreground text-sm placeholder:text-muted-foreground/70 "
                "focus:outline-none focus:ring-2 focus:ring-primary/25 focus:border-primary "
                "transition-shadow",
                autofocus=bool(search),
            ),
            (
                A(
                    "×",
                    href=_clear_url,
                    cls="absolute right-3 top-1/2 -translate-y-1/2 size-5 flex items-center "
                    "justify-center rounded-full bg-muted-foreground/20 "
                    "hover:bg-muted-foreground/35 text-foreground text-sm font-bold "
                    "leading-none no-underline transition-colors",
                )
                if search
                else None
            ),
            cls="relative",
        ),
        Input(type="hidden", name="sort", value=sort),
        Input(type="hidden", name="grade", value=grade_filter),
        Input(type="hidden", name="language", value=language_filter),
        Input(type="hidden", name="activity", value=activity_filter),
        Input(type="hidden", name="age", value=age_filter),
        Input(type="hidden", name="country", value=country_filter),
        Input(type="hidden", name="category", value=category_filter),
        method="GET",
        action="/creators",
    )

    # ═══════════════════════════════════════════════════════════════
    # 2. SORT CHIPS — <a> links, horizontally scrollable, zero JS
    #    Replaces onchange form submit (unreliable) with plain href navigation,
    #    consistent with how all other filter pills work.
    # ═══════════════════════════════════════════════════════════════
    _sort_opts = [
        ("subscribers", "📊", "Subscribers"),
        ("views", "👁", "Views"),
        ("engagement", "🔥", "Engagement"),
        ("quality", "⭐", "Quality"),
        ("recent", "🕐", "Recent"),
        ("consistency", "📈", "Consistent"),
        ("newest_channel", "🎉", "Newest"),
        ("oldest_channel", "👑", "Oldest"),
    ]
    _SORT_CHIP_BASE = (
        "inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full "
        "no-underline transition-all shrink-0 "
    )
    _SORT_CHIP_ACTIVE = "bg-foreground text-background font-semibold border border-foreground"
    _SORT_CHIP_INACTIVE = (
        "bg-background border border-border text-muted-foreground "
        "hover:text-foreground hover:border-foreground/40"
    )
    sort_chips = Div(
        *[
            A(
                Span(emoji, cls="text-sm leading-none"),
                Span(label, cls="text-xs font-medium leading-none"),
                href=_build_filter_url(
                    sort=val,
                    search=search,
                    grade=grade_filter,
                    language=language_filter,
                    activity=activity_filter,
                    age=age_filter,
                    country=country_filter,
                    category=category_filter,
                ),
                cls=_SORT_CHIP_BASE + (_SORT_CHIP_ACTIVE if sort == val else _SORT_CHIP_INACTIVE),
            )
            for val, emoji, label in _sort_opts
        ],
        cls="flex gap-2 overflow-x-auto py-0.5",
        style="-ms-overflow-style:none;scrollbar-width:none;",
    )

    # ═══════════════════════════════════════════════════════════════
    # 3. QUALITY GRADE PILLS
    # ═══════════════════════════════════════════════════════════════
    grade_options = [
        ("all", "All", "🎯"),
        ("A+", "Elite", "👑"),
        ("A", "Star", "⭐"),
        ("B+", "Rising", "📈"),
        ("B", "Good", "💎"),
        ("C", "New", "🔍"),
    ]

    grade_pills = _filter_pills(
        grade_options,
        grade_filter,
        active_cls="bg-primary text-primary-foreground shadow-sm",
        build_url=lambda v: _build_filter_url(
            sort=sort,
            search=search,
            grade=v,
            language=language_filter,
            activity=activity_filter,
            age=age_filter,
            country=country_filter,
            category=category_filter,
        ),
    )

    # ═══════════════════════════════════════════════════════════════
    # 4. LANGUAGE FILTER PILLS
    # ═══════════════════════════════════════════════════════════════
    # Dynamic from RPC — reflects the actual language distribution in the DB.
    # Falls back to popular defaults if the RPC returns nothing.
    _fallback_languages = [
        ("en", "English", "🇺🇸"),
        ("es", "Español", "🇪🇸"),
        ("ja", "日本語", "🇯🇵"),
        ("ko", "Korean", "🇰🇷"),
        ("zh", "Chinese", "🇨🇳"),
    ]
    language_options = [("all", "All", "🌍")]
    _lang_source = top_languages or []
    if _lang_source:
        for _lang_code, _lang_count in _lang_source[:7]:
            if not _lang_code:
                continue
            language_options.append(
                (
                    _lang_code,
                    get_language_name(_lang_code),
                    get_language_emoji(_lang_code),
                )
            )
    else:
        language_options.extend(_fallback_languages)

    language_pills = _filter_pills(
        language_options,
        language_filter,
        active_cls="bg-blue-100 text-blue-700 border border-blue-300",
        build_url=lambda v: _build_filter_url(
            sort=sort,
            search=search,
            grade=grade_filter,
            language=v,
            activity=activity_filter,
            age=age_filter,
            country=country_filter,
            category=category_filter,
            per_page=per_page,
        ),
    )

    # ═══════════════════════════════════════════════════════════════
    # 5. ACTIVITY FILTER PILLS
    # ═══════════════════════════════════════════════════════════════
    activity_options = [
        ("all", "All", "📊"),
        ("active", "Active (>5/mo)", "🔥"),
        ("dormant", "Dormant (<1/mo)", "⚠️"),
    ]

    activity_pills = _filter_pills(
        activity_options,
        activity_filter,
        active_cls="bg-green-100 text-green-700 border border-green-300",
        build_url=lambda v: _build_filter_url(
            sort=sort,
            search=search,
            grade=grade_filter,
            language=language_filter,
            activity=v,
            age=age_filter,
            country=country_filter,
            category=category_filter,
        ),
    )

    # ═══════════════════════════════════════════════════════════════
    # 6. CHANNEL AGE FILTER PILLS
    # ═══════════════════════════════════════════════════════════════
    age_options = [
        ("all", "All", "📅"),
        ("new", "0–1 yr", "🆕"),
        ("established", "1–10 yrs", "🏆"),
        ("veteran", "10+ yrs", "👑"),
    ]

    age_pills = _filter_pills(
        age_options,
        age_filter,
        active_cls="bg-purple-100 text-purple-700 border border-purple-300",
        build_url=lambda v: _build_filter_url(
            sort=sort,
            search=search,
            grade=grade_filter,
            language=language_filter,
            activity=activity_filter,
            age=v,
            country=country_filter,
            category=category_filter,
        ),
    )

    # ═══════════════════════════════════════════════════════════════
    # 7. COUNTRY FILTER PILLS
    # ═══════════════════════════════════════════════════════════════
    # Use top countries from stats, default to popular ones if not provided
    if top_countries is None:
        top_countries = []

    country_options = [("all", "All", "🌍")]

    # Add top countries from database stats
    for country_code, count in top_countries[:8] if top_countries else []:
        flag = get_country_flag(country_code) or "🏴"
        country_options.append((country_code, f"{flag} {country_code.upper()}", flag))

    # Country pills use a custom label format (flag + code only), so built manually
    country_pills = Div(
        *[
            A(
                label if emoji == "🌍" else f"{emoji} {label.split()[-1]}",
                href=_build_filter_url(
                    sort=sort,
                    search=search,
                    grade=grade_filter,
                    language=language_filter,
                    activity=activity_filter,
                    age=age_filter,
                    country=val,
                    category=category_filter,
                ),
                cls=_PILL_BASE
                + (
                    "bg-orange-100 text-orange-700 border border-orange-300"
                    if country_filter == val
                    else _PILL_INACTIVE
                ),
            )
            for val, label, emoji in country_options
        ],
        cls="flex gap-1.5 flex-wrap",
    )

    # ═══════════════════════════════════════════════════════════════
    # 8. CATEGORY FILTER PILLS (dynamic from DB stats)
    # ═══════════════════════════════════════════════════════════════
    if top_categories is None:
        top_categories = []

    category_options = [("all", "All", "🏷️")]
    for cat_name, _count in top_categories[:9]:
        if not cat_name:  # Skip None/empty to avoid crashes
            continue
        emoji = get_topic_category_emoji(cat_name)
        # Shorten long Wikipedia-style names for pill display
        short_name = cat_name.split("/")[-1].strip()  # "Music" not "https://...Music"
        if not short_name:  # Skip if splitting resulted in empty string
            continue
        category_options.append((cat_name, short_name, emoji))

    category_pills = _filter_pills(
        category_options,
        category_filter,
        active_cls="bg-pink-100 text-pink-700 border border-pink-300",
        build_url=lambda v: _build_filter_url(
            sort=sort,
            search=search,
            grade=grade_filter,
            language=language_filter,
            activity=activity_filter,
            age=age_filter,
            country=country_filter,
            category=v,
            per_page=per_page,
        ),
    )

    # ═══════════════════════════════════════════════════════════════
    # 9. COUNT ACTIVE FILTERS
    # Includes search so the FAB badge correctly reflects a text-only search.
    # Strip whitespace so "   " doesn't inflate the badge when the input is blank.
    # ═══════════════════════════════════════════════════════════════
    active_filters = bool(search and search.strip()) + sum(
        f != "all"
        for f in (
            grade_filter,
            language_filter,
            activity_filter,
            age_filter,
            country_filter,
            category_filter,
        )
    )

    # ═══════════════════════════════════════════════════════════════
    # 10. BUILD FLOATING FILTER BUTTON
    # ═══════════════════════════════════════════════════════════════
    filter_button = A(
        # Icon and text
        Div(
            # Filter icon (using emoji for consistency)
            Span("🔍", cls="text-xl md:text-2xl", aria_hidden="true"),
            # Keep label available to screen readers on small screens, visible from sm and up
            Span("Filters", cls="text-xs md:text-sm font-semibold sr-only sm:not-sr-only"),
            cls="flex items-center gap-2",
        ),
        # Active count badge (only show if filters are active)
        (
            Span(
                str(active_filters),
                cls="absolute -top-1 -right-1 md:-top-1.5 md:-right-1.5 bg-red-500 text-white text-[10px] md:text-xs font-bold w-4 h-4 md:w-5 md:h-5 rounded-full flex items-center justify-center",
                aria_label=f"{active_filters} active filters",
            )
            if active_filters > 0
            else None
        ),
        href="#filter-modal",
        uk_toggle=True,
        aria_label="Open filters menu",
        cls="fixed bottom-4 right-4 md:bottom-6 md:right-6 lg:bottom-8 lg:right-8 z-[999] bg-purple-600 hover:bg-purple-700 active:bg-purple-800 text-white rounded-full px-4 py-3 md:px-5 md:py-3.5 shadow-2xl hover:shadow-purple-500/50 transition-all hover:scale-105 active:scale-95 no-underline",
    )

    # ═══════════════════════════════════════════════════════════════
    # 11. BUILD FILTER MODAL WITH ACCORDION
    # ═══════════════════════════════════════════════════════════════

    # Reset filters link
    reset_link = (
        A(
            "Reset All Filters",
            href=_build_filter_url(
                sort=sort,
                search=search,
                grade="all",
                language="all",
                activity="all",
                age="all",
                country="all",
                category="all",
            ),
            cls="text-sm font-medium text-purple-600 hover:text-purple-700 hover:underline",
        )
        if active_filters > 0
        else None
    )

    filter_modal = Div(
        Div(
            Div(
                # Header
                Div(
                    Div(
                        H3(
                            "Filter Creators",
                            cls="text-xl font-bold text-foreground mb-1",
                        ),
                        P(
                            (
                                f"{total_count:,} creators"
                                if total_count
                                else f"{grade_counts.get('all', 0)} on this page"
                            ),
                            cls="text-sm text-muted-foreground",
                        ),
                        cls="flex-1",
                    ),
                    # Close button
                    Button(
                        Span("✕", cls="text-xl"),
                        cls="uk-modal-close-default p-2 hover:bg-accent rounded-lg transition-colors",
                        type="button",
                    ),
                    cls="flex items-start justify-between mb-4 pb-4 border-b border-border",
                ),
                # Reset link
                (
                    Div(
                        reset_link,
                        cls="mb-4",
                    )
                    if reset_link
                    else None
                ),
                # Accordion with filters
                Accordion(
                    AccordionItem(
                        "Quality Grade",
                        grade_pills,
                        open=(grade_filter != "all"),
                    ),
                    AccordionItem(
                        "Category",
                        category_pills,
                        open=(category_filter != "all"),
                    ),
                    AccordionItem(
                        "Language",
                        language_pills,
                        open=(language_filter != "all"),
                    ),
                    AccordionItem(
                        "Country",
                        country_pills,
                        open=(country_filter != "all"),
                    ),
                    AccordionItem(
                        "Activity Level",
                        activity_pills,
                        open=(activity_filter != "all"),
                    ),
                    AccordionItem(
                        "Channel Age",
                        age_pills,
                        open=(age_filter != "all"),
                    ),
                    multiple=True,
                    collapsible=True,
                    cls="space-y-2",
                ),
                cls="uk-modal-body bg-background rounded-t-3xl md:rounded-2xl p-6 max-h-[85vh] overflow-y-auto",
            ),
            cls="uk-modal-dialog uk-margin-auto-vertical",
        ),
        id="filter-modal",
        uk_modal="bg-close: true; esc-close: true;",
        cls="uk-modal",
    )

    # ═══════════════════════════════════════════════════════════════
    # 12. RETURN CLEAN TOP BAR + FLOATING BUTTON + MODAL
    # ═══════════════════════════════════════════════════════════════
    return Div(
        Div(
            search_bar,
            sort_chips,
            cls="flex flex-col gap-2.5",
        ),
        # Floating filter button (fixed position)
        filter_button,
        # Filter modal (hidden until toggled)
        filter_modal,
        cls="sticky top-0 bg-background/95 backdrop-blur-sm border-b border-border px-4 py-3 shadow-sm z-30",
    )


def _render_creators_grid(creators: list[dict], favourite_ids: set[str] | None = None) -> Div:
    """Grid of creator cards with strict row-based layout."""
    _favs = favourite_ids or set()
    return Div(
        *[
            _render_creator_card(creator, is_favourited=safe_get_value(creator, "id", "") in _favs)
            for creator in creators
        ],
        cls="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5",
    )


# =============================================================================
# CREATOR CARD SECTION BUILDERS
# =============================================================================


def _build_card_header(
    thumbnail_url: str,
    channel_name: str,
    channel_url: str,
    custom_url: str,
    current_subs: int,
    current_videos: int,
    rank: str,
    grade_icon: str,
    grade_label: str,
    grade_bg: str,
    quality_grade: str,
    channel_age_days: int,
) -> Div:
    """Build card header: award-showcase rank badge, avatar, name, grade pill.

    No nested <a> tags — the whole card is already wrapped in an <a> by the
    caller, so channel_name is plain H3 text.  The YouTube link lives only in
    the footer as a <button onclick> to avoid invalid HTML nesting.
    """
    # Normalize custom URL to handle both "@" and non-"@" formats, but display with "@" for familiarity
    handle_display = f"@{custom_url.lstrip('@')}" if custom_url else None

    # Award-style rank colouring: gold top-3, silver top-10, neutral otherwise
    try:
        rank_int = int(rank)
    except (ValueError, TypeError):
        rank_int = 999
    rank_cls = (
        "bg-amber-400 text-amber-900"
        if rank_int <= 3
        else (
            "bg-slate-300 dark:bg-slate-600 text-slate-700 dark:text-slate-200"
            if rank_int <= 10
            else "bg-accent text-muted-foreground border border-border"
        )
    )

    return Div(
        # Avatar with rank badge anchored to its bottom-left corner
        Div(
            Img(
                src=thumbnail_url,
                alt=channel_name,
                cls="w-14 h-14 rounded-xl object-cover ring-2 ring-border",
            ),
            Div(
                f"#{rank}",
                cls=f"absolute -bottom-2 -left-2 {rank_cls} text-xs font-bold px-1.5 py-0.5 rounded-md shadow-sm whitespace-nowrap",
            ),
            cls="relative shrink-0",
        ),
        # Channel name + handle + quick stats — plain text, no nested <a>
        Div(
            H3(
                channel_name,
                cls="font-bold text-base text-foreground leading-tight truncate group-hover:text-primary transition-colors",
            ),
            (
                P(handle_display, cls="text-xs text-muted-foreground truncate mt-0.5")
                if handle_display
                else None
            ),
            P(
                f"{format_number(current_subs)} subs · {current_videos} videos",
                cls="text-xs text-muted-foreground truncate mt-0.5",
            ),
            cls="flex-1 min-w-0",
        ),
        # Grade pill — omitted for grade C (unscored/new channels)
        (
            Div(
                Span(grade_icon, cls="text-base leading-none"),
                Span(grade_label, cls="text-xs font-semibold leading-none"),
                cls=f"flex flex-col items-center gap-0.5 px-2 py-1.5 rounded-lg {grade_bg} shrink-0",
            )
            if quality_grade and quality_grade != "C"
            else None
        ),
        cls="flex items-start gap-3",
    )


def _build_primary_metrics(
    current_subs: int, subs_change: int, current_views: int, views_change: int
) -> Div:
    """Build primary metrics section (subscribers and views)."""
    return Div(
        # Subscribers
        Div(
            P(
                "SUBSCRIBERS",
                cls="text-xs font-semibold text-muted-foreground uppercase tracking-wide",
            ),
            H2(
                format_number(current_subs),
                cls="text-3xl font-bold text-blue-600 dark:text-blue-400 mt-1",
            ),
            P(
                (
                    f"{'+' if subs_change > 0 else ''}{format_number(subs_change)} (30d)"
                    if subs_change is not None
                    else "—"
                ),
                cls="text-xs text-muted-foreground mt-1",
            ),
            cls="bg-blue-50 dark:bg-blue-900/20 rounded-lg p-3 text-center",
        ),
        # Views
        Div(
            P(
                "VIEWS",
                cls="text-xs font-semibold text-muted-foreground uppercase tracking-wide",
            ),
            H2(
                format_number(current_views),
                cls="text-3xl font-bold text-purple-600 dark:text-purple-400 mt-1",
            ),
            P(
                (
                    f"{'+' if views_change > 0 else ''}{format_number(views_change)} (30d)"
                    if views_change is not None
                    else "—"
                ),
                cls="text-xs text-muted-foreground mt-1",
            ),
            cls="bg-purple-50 dark:bg-purple-900/20 rounded-lg p-3 text-center",
        ),
        cls="grid grid-cols-2 gap-3 mb-4",
    )


def _build_performance_metrics(
    avg_views_per_video: int,
    current_videos: int,
    estimated_revenue: int,
    views_per_sub: float,
) -> Div:
    """Build performance metrics grid."""
    return Div(
        Div(
            P(
                "AVG VIEWS",
                cls="text-xs font-semibold text-muted-foreground uppercase",
            ),
            P(
                format_number(avg_views_per_video),
                cls="text-lg font-bold text-foreground mt-1",
            ),
            P("per video", cls="text-xs text-muted-foreground"),
            cls="bg-accent rounded-lg p-3 text-center",
        ),
        Div(
            P(
                "VIDEOS",
                cls="text-xs font-semibold text-muted-foreground uppercase",
            ),
            P(
                format_number(current_videos),
                cls="text-lg font-bold text-foreground mt-1",
            ),
            P("published", cls="text-xs text-muted-foreground"),
            cls="bg-accent rounded-lg p-3 text-center",
        ),
        Div(
            P(
                "VIEWS/SUB",
                cls="text-xs font-semibold text-indigo-700 dark:text-indigo-400 uppercase",
            ),
            P(
                f"{views_per_sub:.1f}x",
                cls="text-lg font-bold text-indigo-600 dark:text-indigo-400 mt-1",
            ),
            P("audience reach", cls="text-xs text-indigo-500 dark:text-indigo-400"),
            cls="bg-indigo-50 dark:bg-indigo-900/20 rounded-lg p-3 text-center",
        ),
        Div(
            P(
                "EST. REVENUE",
                cls="text-xs font-semibold text-green-700 dark:text-green-400 uppercase",
            ),
            P(
                f"${format_number(estimated_revenue)}",
                cls="text-lg font-bold text-green-600 dark:text-green-400 mt-1",
            ),
            P("/month est.", cls="text-xs text-green-600 dark:text-green-400"),
            cls="bg-green-50 dark:bg-green-900/20 rounded-lg p-3 text-center",
        ),
        cls="grid grid-cols-2 gap-3 mb-4",
    )


def _build_growth_trend(
    growth_rate: float,
    growth_label: str,
    growth_style: str,
    subs_change: int | None = None,
) -> Div:
    """
    Build growth trend indicator section.

    Handles three states:
    1. Valid growth data: Shows percentage, label, and bar
    2. Tracking in progress (NULL/None): Shows "tracking" badge
    3. Zero growth with valid baseline: Shows 0% (legitimate)
    """
    # Check if growth data is available (not None/NULL from DB)
    has_growth_data = subs_change is not None

    if not has_growth_data:
        return Div(
            Div(
                P(
                    "GROWTH TRACKING",
                    cls="text-xs font-semibold text-muted-foreground",
                ),
                Div(
                    Span("📊", cls="text-2xl"),
                    Span(
                        "Initializing...",
                        cls="px-3 py-1.5 text-xs font-semibold rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 border border-blue-200 dark:border-blue-800",
                    ),
                    cls="flex items-center gap-3",
                ),
                P(
                    "Growth metrics available in 7+ days",
                    cls="text-xs text-muted-foreground mt-2",
                ),
                cls="flex flex-col items-center justify-center gap-2",
            ),
            cls="bg-gradient-to-br from-blue-50 dark:from-blue-900/10 to-indigo-50 dark:to-indigo-900/10 rounded-lg p-4 mb-4 border border-blue-100 dark:border-blue-900/30",
        )

    # Has valid growth data - show normal growth bar
    return Div(
        Div(
            P(
                "30-DAY TREND",
                cls="text-xs font-semibold text-muted-foreground",
            ),
            Div(
                P(
                    f"{growth_rate:+.1f}%",
                    cls="text-sm font-bold text-foreground",
                ),
                Span(
                    growth_label,
                    cls=f"px-2 py-1 text-xs font-semibold rounded-full border {growth_style}",
                ),
                cls="flex items-center gap-2",
            ),
            cls="flex justify-between items-center mb-3",
        ),
        # Growth bar
        Div(
            Div(
                cls=(
                    "h-2 bg-green-500 rounded-full"
                    if growth_rate >= 0
                    else "h-2 bg-red-500 rounded-full"
                ),
                style=f"width: {min(100, max(0, abs(growth_rate) * 5))}%",
            ),
            cls="w-full h-2 bg-border rounded-full overflow-hidden",
        ),
        cls=(
            "bg-green-50 dark:bg-green-900/20 rounded-lg p-3 mb-4"
            if growth_rate >= 0
            else "bg-red-50 dark:bg-red-900/20 rounded-lg p-3 mb-4"
        ),
    )


def _render_topic_categories(topic_categories: str | None) -> Div | None:
    """
    Render topic categories as clean emoji-only pills.

    Args:
        topic_categories: Comma-separated category names from DB
                         (e.g., "Music,Entertainment,Video game culture")

    Returns:
        Div with minimal emoji pills, or None if no categories

    Design: Ultra-clean emoji-only badges with subtle colors.
            Full category name appears on hover with Wikipedia link.
            Reduces visual overload while maintaining context.
    """
    if not topic_categories:
        return None

    # Parse categories - may be JSON array or comma-separated string
    raw_categories = []
    try:
        # Try parsing as JSON first (e.g., '["https://...", "https://..."]')
        parsed = json.loads(topic_categories)
        if isinstance(parsed, list):
            raw_categories = [str(item).strip() for item in parsed if item]
        else:
            raw_categories = [str(parsed).strip()]
    except (json.JSONDecodeError, TypeError):
        # Fallback to comma-separated string
        raw_categories = [cat.strip() for cat in str(topic_categories).split(",") if cat.strip()]

    if not raw_categories:
        return None

    # Extract category names from Wikipedia URLs
    categories = []
    for item in raw_categories:
        # If it's a Wikipedia URL, extract the category name from the slug
        if "wikipedia.org/wiki/" in item:
            try:
                # Extract everything after the last /wiki/
                slug = item.split("/wiki/")[-1].rstrip("/")
                # Decode URL encoding and convert underscores to spaces
                name = unquote(slug).replace("_", " ")
                if name:
                    categories.append(name)
            except Exception:
                continue
        else:
            # It's already a category name
            clean_name = item.strip("\"'[]").strip()
            if clean_name:
                categories.append(clean_name)

    if not categories:
        return None

    # Color palette for pills (subtle, professional)
    pill_colors = [
        "bg-blue-100 dark:bg-blue-900/40 hover:bg-blue-200 dark:hover:bg-blue-800/60",
        "bg-purple-100 dark:bg-purple-900/40 hover:bg-purple-200 dark:hover:bg-purple-800/60",
        "bg-green-100 dark:bg-green-900/40 hover:bg-green-200 dark:hover:bg-green-800/60",
        "bg-pink-100 dark:bg-pink-900/40 hover:bg-pink-200 dark:hover:bg-pink-800/60",
        "bg-indigo-100 dark:bg-indigo-900/40 hover:bg-indigo-200 dark:hover:bg-indigo-800/60",
    ]

    # Build emoji-only pills (limit to 5 for clean display)
    category_pills = []
    for idx, cat in enumerate(categories[:5]):
        emoji = get_topic_category_emoji(cat)
        # Create clean Wikipedia URL from category name (URL-encoded for special characters)
        wiki_slug = quote(cat.replace(" ", "_"))
        wiki_url = f"https://en.wikipedia.org/wiki/{wiki_slug}"
        color = pill_colors[idx % len(pill_colors)]

        category_pills.append(
            A(
                emoji,
                href=wiki_url,
                target="_blank",
                rel="noopener noreferrer",
                cls=f"inline-flex items-center justify-center w-8 h-8 rounded-full {color} text-base transition-all duration-200 no-underline hover:scale-110",
                title=f"{cat} (click to learn more)",
            )
        )

    return Div(
        *category_pills,
        cls="flex items-center justify-center gap-2 py-2 px-3 mb-3",
    )


def _render_bio(bio: str | None, max_chars: int = 130) -> P | None:
    """Render a truncated bio paragraph, or None if no bio is available."""
    if not bio:
        return None
    text = bio[:max_chars].rstrip() + "…" if len(bio) > max_chars else bio
    return P(
        text,
        cls="text-xs text-muted-foreground leading-relaxed mb-4 line-clamp-2",
    )


def _build_card_footer(
    last_updated: str, channel_url: str, creator_id: str = "", is_favourited: bool = False
) -> Div:
    """Card footer: last-updated timestamp + optional heart + YouTube link.

    The YouTube link and favourite heart are <button> elements rather than <a>
    because the whole card is already wrapped in an <a> (profile link).
    Nested <a> tags are invalid HTML and cause browsers to silently drop the
    inner link.  Both buttons call event.stopPropagation() to prevent the card
    click from also triggering the outer link.
    """
    # json.dumps produces a fully JS-safe quoted string (handles backslashes,
    # newlines, and all special chars), not just single-quote substitution.
    js_url = json.dumps(channel_url)  # e.g. '"https://youtube.com/..."'

    # Heart button — only rendered when creator_id is available
    if creator_id:
        heart_cls = "w-3.5 h-3.5" + (
            " fill-red-500 text-red-500" if is_favourited else " text-gray-400"
        )
        heart_btn = Div(
            Button(
                UkIcon("heart", cls=heart_cls),
                hx_post=f"/creator/{creator_id}/favourite",
                hx_target=f"#fav-btn-{creator_id}",
                hx_swap="outerHTML",
                type="button",
                onclick="event.stopPropagation(); event.preventDefault();",
                aria_label="Toggle favourite",
                cls="flex items-center text-xs font-semibold text-gray-400 hover:text-red-500 dark:text-gray-500 dark:hover:text-red-400 transition-colors bg-transparent border-0 p-0 cursor-pointer",
            ),
            id=f"fav-btn-{creator_id}",
        )
    else:
        heart_btn = None

    return Div(
        Div(
            UkIcon("clock", cls="w-3 h-3 mr-1 opacity-50"),
            Span(
                format_date_relative(last_updated),
                cls="text-xs text-muted-foreground",
            ),
            cls="flex items-center gap-0.5",
        ),
        Div(
            heart_btn,
            # <button> stops the card-level click; JS opens YouTube in a new tab
            Button(
                UkIcon("youtube", cls="w-3.5 h-3.5 mr-1"),
                "YouTube",
                type="button",
                onclick=f"event.stopPropagation(); event.preventDefault(); window.open({js_url}, '_blank', 'noopener,noreferrer')",
                cls="flex items-center text-xs font-semibold text-red-500 hover:text-red-600 dark:text-red-400 dark:hover:text-red-300 transition-colors bg-transparent border-0 p-0 cursor-pointer",
            ),
            cls="flex items-center gap-3",
        ),
        cls="flex justify-between items-center mt-auto pt-3 border-t border-border",
    )


def _build_info_strip(
    language: str,
    country_code: str,
    channel_age_days: int,
    monthly_uploads: float,
    custom_url: str = "",
) -> Div | None:
    """Build clean emoji/icon strip showing key channel info."""
    # Build icon list
    icons = []

    # Country flag
    country_flag = get_country_flag(country_code)
    if country_flag:
        icons.append(
            Span(
                country_flag,
                title=f"Country: {country_code.upper()}",
                cls="text-lg",
            )
        )

    # Language
    if language:
        icons.append(
            Span(
                get_language_emoji(language),
                title=f"Language: {get_language_name(language)}",
                cls="text-lg",
            )
        )

    if channel_age_days:
        icons.append(
            Span(
                f"{get_age_emoji(channel_age_days)} {format_channel_age(channel_age_days)}",
                title=f"Channel age: {get_age_title(channel_age_days)}",
                cls="text-xs font-medium text-muted-foreground",
            )
        )

    activity_badge = get_activity_badge(monthly_uploads)
    if activity_badge:
        icons.append(
            Span(
                activity_badge,
                title=f"Upload frequency: {get_activity_title(monthly_uploads)}",
                cls="text-xs font-medium text-muted-foreground",
            )
        )

    if not icons:
        return None

    return Div(
        *icons,
        cls="flex items-center justify-center gap-3 py-2 bg-accent rounded-lg",
    )


def _render_creator_card(creator: dict, is_favourited: bool = False) -> Div:
    """
    Creator card - clean, data-driven design.

    Layout:
    [Thumbnail + Rank] | [Name + Badge]
    ────────────────────────────────────
    SUBSCRIBERS | VIEWS (large 2-col metrics)
    ────────────────────────────────────
    AVG/VID | VIDEOS | ENGAGEMENT | REVENUE (small 4-col metrics)
    ────────────────────────────────────
    30-Day Trend bar
    ────────────────────────────────────
    Updated · ❤ · Analyze → (footer)
    """

    # Extract all data
    channel_id = safe_get_value(creator, "channel_id", "N/A")
    channel_name = safe_get_value(creator, "channel_name", "Unknown")
    # Preserve existing channel_url if present, otherwise construct from channel_id
    channel_url = (
        safe_get_value(creator, "channel_url") or f"https://youtube.com/channel/{channel_id}"
    )
    quality_grade = safe_get_value(creator, "quality_grade", "C")
    rank = safe_get_value(creator, "_rank", "—")
    thumbnail_url = (
        safe_get_value(creator, "channel_thumbnail_url")
        or safe_get_value(creator, "thumbnail_url")
        or "https://via.placeholder.com/64x64?text=No+Image"
    )
    channel_age_days = safe_get_value(creator, "channel_age_days", 0)

    # Numeric fields
    current_subs = int(safe_get_value(creator, "current_subscribers", 0) or 0)
    current_views = int(safe_get_value(creator, "current_view_count", 0) or 0)
    current_videos = int(safe_get_value(creator, "current_video_count", 0) or 0)
    # Preserve None for delta fields (growth tracking initializing)
    subs_change_raw = safe_get_value(creator, "subscribers_change_30d", None)
    subs_change = int(subs_change_raw) if subs_change_raw is not None else None
    views_change_raw = safe_get_value(creator, "views_change_30d", None)
    views_change = int(views_change_raw) if views_change_raw is not None else None
    engagement_score = float(safe_get_value(creator, "engagement_score", 0) or 0)
    last_updated = safe_get_value(creator, "last_updated_at", "")

    # === CALCULATIONS ===
    avg_views_per_video = calculate_avg_views_per_video(current_views, current_videos)
    growth_rate = calculate_growth_rate(subs_change, current_subs)
    views_per_sub = calculate_views_per_subscriber(current_views, current_subs)

    # === STATUS & STYLING ===
    sync_status = safe_get_value(creator, "sync_status", "pending")
    sync_badge_info = get_sync_status_badge(sync_status)
    card_border = f"border-l-4 border-amber-400" if sync_status != "synced" else ""

    grade_icon, grade_label, grade_bg = get_grade_info(quality_grade)
    growth_label, growth_style = get_growth_signal(growth_rate)

    # === INFO STRIP DATA ===
    custom_url = safe_get_value(creator, "custom_url", "")
    language = safe_get_value(creator, "default_language", "")
    country_code = safe_get_value(creator, "country_code", "")
    primary_category = safe_get_value(creator, "primary_category", "general") or "general"
    monthly_uploads = safe_get_value(creator, "monthly_uploads", 0)

    # v4 revenue model — uses country + niche for accurate CPM, Shorts split, sponsorships
    _rev = estimate_monthly_revenue_v4(
        total_subs=current_subs,
        total_views=current_views,
        video_count=current_videos,
        country_code=country_code or "US",
        niche=primary_category,
    )
    # Round to the nearest whole dollar instead of truncating to avoid systematic under-reporting
    estimated_revenue = round(_rev["est_monthly_total"])
    keywords = safe_get_value(creator, "keywords", "")

    info_strip = _build_info_strip(
        language, country_code, channel_age_days, monthly_uploads, custom_url
    )

    # === COMPOSE CARD ===
    # Sync status banner — inline at the top of the body (not a MonsterUI header=
    # slot) so it doesn't inherit uk-card-header border/padding styling.
    sync_banner = (
        Div(
            f"{sync_badge_info[0]} {sync_badge_info[1]}",
            cls=f"text-xs font-semibold text-center py-1 px-3 rounded-md {sync_badge_info[2]}",
        )
        if sync_badge_info
        else None
    )

    # All content goes into the Card body as positional args — including the
    # channel header block.  This matches the MonsterUI team-card pattern (ex_card3)
    # where DivLAligned(avatar, info) is the first body child, not a header= slot.
    # Using header= caused uk-card-header to apply its own border/padding, which
    # visually hid the body metric sections.
    card = Card(
        # ── Identity ──────────────────────────────────────────────────────────
        sync_banner,
        _build_card_header(
            thumbnail_url,
            channel_name,
            channel_url,
            custom_url,
            current_subs,
            current_videos,
            rank,
            grade_icon,
            grade_label,
            grade_bg,
            quality_grade,
            channel_age_days,
        ),
        # ── Context ───────────────────────────────────────────────────────────
        # Topic categories rendered as clean emoji pills with Wikipedia links, plus
        _render_topic_categories(safe_get_value(creator, "topic_categories")),
        _render_bio(
            safe_get_value(creator, "bio") or safe_get_value(creator, "channel_description")
        ),
        # ── Metrics ───────────────────────────────────────────────────────────
        _build_primary_metrics(current_subs, subs_change, current_views, views_change),
        # Performance metrics grid: avg views/video, total videos, views/sub, est. revenue
        _build_performance_metrics(
            avg_views_per_video, current_videos, estimated_revenue, views_per_sub
        ),
        # Growth trend (pass subs_change to determine if tracking is initializing)
        _build_growth_trend(growth_rate, growth_label, growth_style, subs_change),
        # Keywords if available, rendered as a single line of small italic text (not a full tag cloud)
        (
            P(
                keywords,
                cls="text-xs text-muted-foreground italic line-clamp-1 text-center",
            )
            if keywords
            else None
        ),
        # Info strp at bottom of the card body, showing language, country, channel age, and activity badges as emojis with tooltips
        info_strip,
        # ── Footer slot (only the action row — no body content here) ──────────
        footer=_build_card_footer(
            last_updated,
            channel_url,
            creator_id=safe_get_value(creator, "id", ""),
            is_favourited=is_favourited,
        ),
        body_cls="space-y-3",
        cls=(
            CardT.hover,
            f"min-w-0 overflow-hidden w-full cursor-pointer {card_border}",
        ),
    )

    # Single outer <a> — the only link wrapping the card.
    # No <a> tags exist inside the card body; the YouTube button uses onclick.
    creator_uuid = safe_get_value(creator, "id", "")
    if not creator_uuid:
        return card
    return A(
        card,
        href=f"/creator/{creator_uuid}?name={quote(channel_name)}",
        cls="block no-underline group min-w-0 w-full",
    )


def _render_pagination(
    page: int,
    total_pages: int,
    search: str,
    sort: str,
    grade_filter: str,
    language_filter: str,
    activity_filter: str,
    age_filter: str,
    country_filter: str,
    category_filter: str,
    per_page: int,
    total_count: int,
) -> Div:
    """
    Render pagination controls with smart page button display.

    Pattern inspired by FastHTML best practices:
    - Shows first, last, and current pages
    - Uses ellipsis (...) for skipped ranges
    - Highlights current page
    - Preserves all filter state in URLs
    """
    if total_pages <= 1:
        return Div()  # No pagination needed

    def page_link(page_num: int, label: str = None, is_current: bool = False):
        """Generate a page link button."""
        if label is None:
            label = str(page_num)

        url = _build_filter_url(
            sort=sort,
            search=search,
            grade=grade_filter,
            language=language_filter,
            activity=activity_filter,
            age=age_filter,
            country=country_filter,
            category=category_filter,
            page=page_num,
            per_page=per_page,
        )

        if is_current:
            return Span(
                label,
                cls="px-3 py-2 bg-purple-600 text-white font-semibold rounded-lg cursor-default",
            )
        else:
            return A(
                label,
                href=url,
                cls="px-3 py-2 bg-background border border-border text-foreground font-medium rounded-lg hover:bg-accent transition-colors no-underline",
            )

    def ellipsis():
        """Render ellipsis for skipped pages."""
        return Span("...", cls="px-2 text-muted-foreground")

    # Smart page button display logic (like the FastHTML example)
    buttons = []

    if total_pages <= 7:
        # Show all pages if 7 or fewer
        buttons = [page_link(p, is_current=(p == page)) for p in range(1, total_pages + 1)]
    elif page <= 3:
        # Near start: [1] [2] [3] [4] ... [last]
        buttons = [page_link(p, is_current=(p == page)) for p in range(1, 5)]
        buttons.append(ellipsis())
        buttons.append(page_link(total_pages))
    elif page >= total_pages - 2:
        # Near end: [1] ... [last-3] [last-2] [last-1] [last]
        buttons.append(page_link(1))
        buttons.append(ellipsis())
        buttons.extend(
            [page_link(p, is_current=(p == page)) for p in range(total_pages - 3, total_pages + 1)]
        )
    else:
        # Middle: [1] ... [current-1] [current] [current+1] ... [last]
        buttons.append(page_link(1))
        buttons.append(ellipsis())
        buttons.extend([page_link(p, is_current=(p == page)) for p in range(page - 1, page + 2)])
        buttons.append(ellipsis())
        buttons.append(page_link(total_pages))

    # Previous/Next buttons
    prev_button = (
        A(
            "← Previous",
            href=_build_filter_url(
                sort=sort,
                search=search,
                grade=grade_filter,
                language=language_filter,
                activity=activity_filter,
                age=age_filter,
                country=country_filter,
                category=category_filter,
                page=page - 1,
                per_page=per_page,
            ),
            cls="px-4 py-2 bg-background border border-border text-foreground font-medium rounded-lg hover:bg-accent transition-colors no-underline",
        )
        if page > 1
        else Span(
            "← Previous",
            cls="px-4 py-2 bg-accent text-muted-foreground font-medium rounded-lg cursor-not-allowed",
        )
    )

    next_button = (
        A(
            "Next →",
            href=_build_filter_url(
                sort=sort,
                search=search,
                grade=grade_filter,
                language=language_filter,
                activity=activity_filter,
                age=age_filter,
                country=country_filter,
                category=category_filter,
                page=page + 1,
                per_page=per_page,
            ),
            cls="px-4 py-2 bg-background border border-border text-foreground font-medium rounded-lg hover:bg-accent transition-colors no-underline",
        )
        if page < total_pages
        else Span(
            "Next →",
            cls="px-4 py-2 bg-accent text-muted-foreground font-medium rounded-lg cursor-not-allowed",
        )
    )

    # Results info
    start_result = (page - 1) * per_page + 1
    end_result = min(page * per_page, total_count)

    return Div(
        # Results summary
        Div(
            P(
                f"Showing {start_result:,}–{end_result:,} of {total_count:,} creators",
                cls="text-sm text-muted-foreground",
            ),
            cls="text-center mb-4",
        ),
        # Pagination controls
        Div(
            prev_button,
            Div(*buttons, cls="flex gap-1"),
            next_button,
            cls="flex items-center justify-center gap-4 flex-wrap",
        ),
        cls="py-8",
    )


def _render_handle_not_found_banner(search: str, is_authenticated: bool) -> Div:
    """
    Compact banner shown above the results grid when a @handle search
    returned no exact DB match — even if related creators are shown below.
    Reuses the same HTMX endpoint as the empty-state Flow 1.
    """
    result_slot = Div(id="creator-add-result", cls="mt-2")

    if is_authenticated:
        action = Form(
            Input(type="hidden", name="q", value=search),
            Button(
                UkIcon("plus-circle", cls="size-4 mr-1.5"),
                f"Add {search}",
                type="submit",
                cls="flex items-center px-4 py-1.5 text-sm font-semibold rounded-lg "
                "bg-primary text-primary-foreground hover:bg-primary/90 transition-colors shrink-0",
            ),
            result_slot,
            hx_post="/creators/request",
            hx_target="#creator-add-result",
            hx_swap="innerHTML",
            cls="flex flex-col items-start gap-0",
        )
    else:
        action = A(
            UkIcon("log-in", cls="size-4 mr-1.5"),
            "Log in to add",
            href="/login",
            cls="inline-flex items-center px-4 py-1.5 text-sm font-semibold rounded-lg "
            "border border-border hover:bg-accent transition-colors shrink-0",
        )

    return Div(
        Div(
            Div(
                UkIcon("info", cls="size-4 text-blue-500 shrink-0 mt-0.5"),
                P(
                    Span(search, cls="font-semibold"),
                    " isn't in our database yet. " "Related results are shown below.",
                    cls="text-sm text-foreground",
                ),
                cls="flex items-start gap-2 flex-1",
            ),
            action,
            cls="flex items-start justify-between gap-4 flex-wrap",
        ),
        cls="mb-4 px-4 py-3 rounded-xl border border-blue-200 bg-blue-50 "
        "dark:bg-blue-950/30 dark:border-blue-800",
    )


def _render_empty_state(
    search: str,
    grade_filter: str,
    has_active_filters: bool,
    is_authenticated: bool = False,
) -> Div:
    """Empty state when no creators found.

    Three progressive-disclosure flows:
    1. @handle typed → one-click "Add to ViralVibes" CTA (handle pre-filled)
    2. Name search / filters, no results → inline handle submit form
    3. Truly empty DB → encourage playlist analysis
    """

    # ── shared submit result target (HTMX drops response here) ──────────────
    result_slot = Div(id="creator-add-result", cls="mt-3")

    # ── shared "back to all" link ────────────────────────────────────────────
    back_link = A(
        "← Browse all creators",
        href="/creators",
        cls="text-sm text-muted-foreground hover:underline",
    )

    # ────────────────────────────────────────────────────────────────────────
    # FLOW 1 — @handle typed, no match in DB
    # Known handle → pre-fill the form so user needs only one click
    # ────────────────────────────────────────────────────────────────────────
    if search and search.startswith("@"):
        if is_authenticated:
            add_cta = Form(
                Input(type="hidden", name="q", value=search),
                Button(
                    UkIcon("plus-circle", cls="size-4 mr-1.5"),
                    f"Add {search} to ViralVibes",
                    type="submit",
                    cls="flex items-center px-5 py-2.5 text-sm font-semibold rounded-lg "
                    "bg-primary text-primary-foreground hover:bg-primary/90 transition-colors",
                ),
                result_slot,
                hx_post="/creators/request",
                hx_target="#creator-add-result",
                hx_swap="innerHTML",
                cls="flex flex-col items-center gap-0",
            )
        else:
            add_cta = A(
                UkIcon("log-in", cls="size-4 mr-1.5"),
                "Log in to add this creator",
                href=f"/login",
                cls="inline-flex items-center px-5 py-2.5 text-sm font-semibold rounded-lg "
                "border border-border hover:bg-accent transition-colors",
            )

        return Card(
            Div(
                Span("👀", cls="text-5xl block text-center mb-3"),
                H2(
                    f"{search} isn't in our database yet",
                    cls="text-center text-xl font-bold mb-1",
                ),
                P(
                    "We'll sync their stats automatically once added.",
                    cls="text-center text-sm text-muted-foreground mb-5",
                ),
                Div(add_cta, cls="flex justify-center"),
                Div(back_link, cls="flex justify-center mt-5"),
                cls="p-10 space-y-0",
            ),
            cls="bg-background border border-border max-w-sm mx-auto",
        )

    # ────────────────────────────────────────────────────────────────────────
    # FLOW 2 — name/filter search, no results
    # Surface the submit form inline — no need to scroll anywhere
    # ────────────────────────────────────────────────────────────────────────
    if has_active_filters:
        label = f'No creators match "{search}"' if search else "No creators match these filters"
        if is_authenticated:
            submit_area = Form(
                Div(
                    Input(
                        type="text",
                        name="q",
                        placeholder="@handle or channel ID…",
                        autocomplete="off",
                        cls="flex-1 px-3 py-2 text-sm rounded-lg border border-border "
                        "bg-background focus:outline-none focus:ring-2 focus:ring-primary/40",
                    ),
                    Button(
                        "Submit",
                        type="submit",
                        cls="px-4 py-2 text-sm font-semibold rounded-lg "
                        "bg-primary text-primary-foreground hover:bg-primary/90 "
                        "transition-colors shrink-0",
                    ),
                    cls="flex gap-2",
                ),
                result_slot,
                hx_post="/creators/request",
                hx_target="#creator-add-result",
                hx_swap="innerHTML",
            )
        else:
            submit_area = P(
                A("Log in", href="/login", cls="text-primary hover:underline font-medium"),
                " to submit a creator by @handle.",
                cls="text-sm text-muted-foreground",
            )

        return Card(
            Div(
                Span("🔍", cls="text-5xl block text-center mb-3"),
                H2(label, cls="text-center text-xl font-bold mb-1"),
                P(
                    "Know their @handle? Submit it and they'll be added automatically.",
                    cls="text-center text-sm text-muted-foreground mb-5",
                ),
                submit_area,
                Div(
                    A(
                        "Clear filters",
                        href="/creators",
                        cls="text-sm text-muted-foreground hover:underline",
                    ),
                    cls="flex justify-center mt-4",
                ),
                cls="p-10 space-y-3",
            ),
            cls="bg-accent max-w-md mx-auto",
        )

    # ────────────────────────────────────────────────────────────────────────
    # FLOW 3 — truly empty DB
    # ────────────────────────────────────────────────────────────────────────
    return Card(
        Div(
            Span("🚀", cls="text-6xl block text-center mb-4"),
            H2(
                "No creators discovered yet",
                cls="text-center text-2xl font-bold mb-2",
            ),
            P(
                "Analyze YouTube playlists to automatically discover and track creators.",
                cls="text-center text-muted-foreground mb-2",
            ),
            Div(
                A(
                    Button("📊 Analyze Your First Playlist", cls=ButtonT.primary),
                    href="/#analyze-section",
                ),
                cls="flex justify-center",
            ),
            cls="space-y-4 p-12",
        ),
        cls="bg-background max-w-md mx-auto border border-border",
    )


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def _count_by_grade(creators: list[dict]) -> dict:
    """Count creators by quality grade for filter pills."""
    counts = {"all": len(creators), "A+": 0, "A": 0, "B+": 0, "B": 0, "C": 0}
    for creator in creators:
        grade = safe_get_value(creator, "quality_grade", "C")
        if grade in counts:
            counts[grade] += 1
    return counts


# NOTE: Helper functions moved to utils/creator_metrics.py for better organization
# - get_language_emoji, get_language_name, get_activity_badge
# - estimate_monthly_revenue_v4 (revenue model)


# ============================================================================
# CREATOR PROFILE PAGE
# ============================================================================


def _render_similar_creators(
    creators: list[dict],
    category: str,
    country_code: str,
    current_creator_id: str = "",
) -> Div | None:
    """
    Horizontal Apple-Music-style scroll rail of similar creators.

    Each tile is a fixed-width card with a square avatar, channel name,
    subscriber count, and quality grade. The rail uses scroll-snap so
    swiping feels native on mobile.

    Returns None when the list is empty so callers can safely omit it.
    """
    if not creators:
        return None

    _chip_cls = {
        "A+": "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300",
        "A": "bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300",
        "B+": "bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300",
        "B": "bg-sky-100 text-sky-700 dark:bg-sky-900/40 dark:text-sky-300",
        "C": "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/40 dark:text-yellow-300",
    }

    def _tile(c: dict):
        cid = c.get("id", "")
        name = c.get("channel_name") or "Creator"
        thumb = c.get("channel_thumbnail_url") or ""
        subs = int(c.get("current_subscribers") or 0)
        grade = c.get("quality_grade") or ""
        grade_cls = _chip_cls.get(grade, "bg-accent text-muted-foreground")

        avatar = (
            Img(
                src=thumb,
                alt=name,
                cls="w-full aspect-square object-cover rounded-xl",
                loading="lazy",
            )
            if thumb
            else Div(
                Span(name[:1].upper(), cls="text-xl font-bold text-muted-foreground"),
                cls="w-full aspect-square rounded-xl bg-accent flex items-center justify-center",
            )
        )

        return Div(
            A(
                Div(
                    avatar,
                    Div(
                        P(
                            name,
                            cls="text-xs font-semibold text-foreground leading-tight line-clamp-2 mt-2",
                        ),
                        Div(
                            Span(
                                format_number(subs),
                                cls="text-xs text-muted-foreground",
                            ),
                            *(
                                [
                                    Span(
                                        grade,
                                        cls=f"text-[10px] font-bold px-1.5 py-0.5 rounded-full {grade_cls}",
                                    )
                                ]
                                if grade
                                else []
                            ),
                            cls="flex items-center gap-1.5 mt-0.5",
                        ),
                    ),
                    cls="flex flex-col",
                ),
                href=f"/creator/{cid}",
                cls="no-underline",
            ),
            # ⇄ Compare link below the tile
            *(
                [
                    A(
                        UkIcon("git-compare", cls="w-3 h-3 mr-0.5"),
                        "Compare",
                        href=f"/compare?a={current_creator_id}&b={cid}",
                        cls="text-[10px] text-muted-foreground hover:text-primary no-underline flex items-center justify-center mt-1 transition-colors",
                    )
                ]
                if current_creator_id
                else []
            ),
            cls="flex-shrink-0 w-28 sm:w-32 snap-start flex flex-col",
        )

    tiles = [_tile(c) for c in creators]

    # Determine the "See all" link — prefer category, fall back to country
    if category:
        see_all_href = f"/lists/category/{slugify(category)}"
        see_all_label = f"{get_topic_category_emoji(category)} {category}"
    elif country_code:
        see_all_href = f"/lists/country/{country_code.upper()}"
        see_all_label = f"{get_country_flag(country_code)} {country_code.upper()}"
    else:
        see_all_href = "/creators"
        see_all_label = "All creators"

    rail_id = f"similar-rail-{current_creator_id or 'x'}"
    return Card(
        Div(
            H2("You may also like", cls="text-base font-bold text-foreground"),
            A(
                see_all_label + " →",
                href=see_all_href,
                cls="text-xs font-medium text-primary hover:underline no-underline shrink-0",
            ),
            cls="flex items-center justify-between mb-3",
        ),
        Div(
            *tiles,
            id=rail_id,
            cls=(
                "flex gap-3 overflow-x-auto pb-2"
                " snap-x snap-mandatory"
                " scrollbar-thin scrollbar-thumb-border scrollbar-track-transparent"
                " -mx-1 px-1"
            ),
        ),
        body_cls="p-5",
        cls="mb-6",
    )


def render_creator_profile_page(
    creator: dict,
    back_url: str = "/creators",
    context_ranks: dict | None = None,
    category_stats: dict | None = None,
    is_favourited: bool = False,
    similar_creators: list[dict] | None = None,
    peer_engagement_p75: float = 0.0,
    niche_leaderboard: list[dict] | None = None,
    recent_upload: dict | None = None,
) -> Div:
    """
    Full-page creator profile — award-showcase design.

    Args:
        creator:         Creator dict from get_creator_stats().
        back_url:        Href for the ← Back button.
        context_ranks:   Optional {country_rank, language_rank, category_rank}
                         ints from _get_context_ranks() in routes/creators.py.
        category_stats:  Optional pre-aggregated box plot stats from
                         get_cached_category_box_stats() — passed through to
                         render_category_box_plots(). None = show placeholder.
        is_favourited:   Whether the current user has already favourited this
                         creator.  Drives the initial heart-button state.
                         Pass False (default) for unauthenticated visitors.

    Layout:
      1. Cinematic banner + overlapping avatar + identity strip
      2. 4-up stat cards with 30d deltas
      3. Two-column body:
           Left  — About + Channel Info + Social Links + Featured Channels
           Right — Performance + trend bar + Topics (with rank chips) +
                   Rankings card + Category breakdown
      4. Similar creators horizontal scroll rail
      5. Sync / freshness footer
    """
    context_ranks = context_ranks or {}
    # ── identity ──────────────────────────────────────────────────────────────
    creator_id = safe_get_value(creator, "id", "")
    channel_id = safe_get_value(creator, "channel_id", "")
    channel_name = safe_get_value(creator, "channel_name", "Unknown Creator")
    custom_url = safe_get_value(creator, "custom_url", "")
    channel_url = (
        safe_get_value(creator, "channel_url") or f"https://www.youtube.com/channel/{channel_id}"
    )
    thumbnail_url = safe_get_value(creator, "channel_thumbnail_url") or "/static/favicon.jpeg"
    banner_url = safe_get_value(creator, "banner_image_url", "")
    bio = safe_get_value(creator, "channel_description") or safe_get_value(creator, "bio", "")
    keywords = safe_get_value(creator, "keywords", "")
    country_code = safe_get_value(creator, "country_code", "")
    language = safe_get_value(creator, "default_language", "")
    quality_grade = safe_get_value(creator, "quality_grade", "C")
    official = safe_get_value(creator, "official", False)
    hidden_subs = safe_get_value(creator, "hidden_subscriber_count", False)
    primary_category = safe_get_value(creator, "primary_category", "")
    published_at = safe_get_value(creator, "published_at", "")
    channel_age_days = safe_get_value(creator, "channel_age_days", 0) or 0
    monthly_uploads = safe_get_value(creator, "monthly_uploads", 0) or 0
    topic_categories_raw = safe_get_value(creator, "topic_categories")
    category_distribution = safe_get_value(creator, "category_distribution")
    featured_channels_raw = safe_get_value(creator, "featured_channels_urls", "")
    last_updated = safe_get_value(creator, "last_updated_at", "")
    last_synced = safe_get_value(creator, "last_synced_at", "")
    sync_status = safe_get_value(creator, "sync_status", "pending")

    # ── numeric stats ─────────────────────────────────────────────────────────
    current_subs = int(safe_get_value(creator, "current_subscribers", 0) or 0)
    current_views = int(safe_get_value(creator, "current_view_count", 0) or 0)
    current_videos = int(safe_get_value(creator, "current_video_count", 0) or 0)
    subs_change_raw = safe_get_value(creator, "subscribers_change_30d", None)
    subs_change = int(subs_change_raw) if subs_change_raw is not None else None
    views_change_raw = safe_get_value(creator, "views_change_30d", None)
    views_change = int(views_change_raw) if views_change_raw is not None else None
    videos_change_raw = safe_get_value(creator, "videos_change_30d", None)
    videos_change = int(videos_change_raw) if videos_change_raw is not None else None
    engagement_score = float(safe_get_value(creator, "engagement_score", 0) or 0)

    # ── derived ───────────────────────────────────────────────────────────────
    grade_icon, grade_label, grade_bg = get_grade_info(quality_grade)
    growth_rate = calculate_growth_rate(subs_change, current_subs)
    growth_label, growth_style = get_growth_signal(growth_rate)
    avg_views = calculate_avg_views_per_video(current_views, current_videos)
    views_per_sub = calculate_views_per_subscriber(current_views, current_subs)
    momentum_score = calculate_momentum_score(views_change, subs_change, current_subs)
    if momentum_score is not None:
        momentum_label, momentum_style = get_momentum_label(momentum_score)
    else:
        momentum_label, momentum_style = None, ""
    # v4 revenue model — country + niche-aware, Shorts-split, sponsorships included
    _rev = estimate_monthly_revenue_v4(
        total_subs=current_subs,
        total_views=current_views,
        video_count=current_videos,
        country_code=country_code or "US",
        niche=primary_category or "general",
    )
    estimated_revenue = int(_rev["est_monthly_total"])
    revenue_split = _rev["revenue_split"]
    assumed_shorts_pct = _rev["assumed_shorts_pct"]
    handle_display = f"@{custom_url.lstrip('@')}" if custom_url else ""
    country_flag = get_country_flag(country_code) if country_code else ""
    lang_emoji = get_language_emoji(language) if language else ""
    lang_name = get_language_name(language) if language else ""

    # ── parsed extras ─────────────────────────────────────────────────────────
    social_links = _extract_socials(bio or "", keywords or "")
    featured_ch_urls = _parse_featured_channels(featured_channels_raw or "")

    # ── context ranks (from route layer) ──────────────────────────────────────
    country_rank = context_ranks.get("country_rank")
    language_rank = context_ranks.get("language_rank")
    category_rank = context_ranks.get("category_rank")

    # ── small helpers (private to this call) ─────────────────────────────────
    def _delta_badge(val: int | None, pct: float | None = None) -> Span | None:
        """Inline +/- badge for 30-day delta next to a stat value.

        When pct is provided (subscribers only) renders e.g. "+280K 30d  ↑6.7%".
        pct == 0 is treated as neutral (no arrow, grey) to avoid implying growth.
        """
        if val is None:
            return None
        colour = (
            "text-green-600 bg-green-50 dark:bg-green-900/30"
            if val > 0
            else (
                "text-red-600 bg-red-50 dark:bg-red-900/30"
                if val < 0
                else "text-muted-foreground bg-accent"
            )
        )
        sign = "+" if val > 0 else ""
        text = f"{sign}{format_number(val)} 30d"
        if pct is not None and pct != 0:
            arrow = "↑" if pct > 0 else "↓"
            text += f"  {arrow}{abs(pct):.1f}%"
        return Span(
            text,
            cls=f"text-xs font-semibold px-2 py-0.5 rounded-full {colour} ml-1.5",
        )

    def _info_row(icon: str, label: str, value: str):
        """One metadata row: UkIcon | label / value."""
        return Div(
            UkIcon(icon, cls="w-4 h-4 text-muted-foreground shrink-0 mt-0.5"),
            Div(
                Span(label, cls="text-xs text-muted-foreground block leading-none"),
                Span(value, cls="text-sm font-medium text-foreground leading-snug"),
            ),
            cls="flex items-start gap-2.5",
        )

    def _perf_row(label: str, value: str, value_cls: str = "text-foreground"):
        return Div(
            Span(label, cls="text-sm text-muted-foreground"),
            Span(value, cls=f"text-sm font-semibold {value_cls}"),
            cls="flex justify-between items-center py-2 border-b border-border last:border-0",
        )

    def _rank_chip(text: str, href: str, title: str):
        """Linked pill chip for rank badges in the identity strip."""
        return A(
            text,
            href=href,
            title=title,
            cls="text-xs font-semibold px-2 py-0.5 rounded-full bg-accent text-foreground hover:bg-accent/80 no-underline transition-colors",
        )

    def _stat_card(label, value, delta_val, number_cls, bg_cls, *, delta_pct=None, rank_line=None):
        return Card(
            Div(
                P(
                    label,
                    cls="text-xs font-semibold text-muted-foreground uppercase tracking-widest",
                ),
                Div(
                    Span(value, cls=f"text-3xl font-bold {number_cls}"),
                    _delta_badge(delta_val, pct=delta_pct),
                    cls="flex items-baseline flex-wrap mt-1",
                ),
                rank_line,
            ),
            cls=f"{bg_cls} border-0",
            body_cls="p-4",
        )

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 1 — Cinematic banner + overlapping avatar + identity strip
    # ═══════════════════════════════════════════════════════════════════════════
    # Banner layer
    banner_layer = (
        Div(
            # Dark gradient overlay — ensures text legibility on any banner image
            Div(
                cls="absolute inset-0 bg-gradient-to-t from-black/50 via-transparent to-transparent"
            ),
            style=(
                f"background-image:url('{banner_url}');"
                "background-size:cover;background-position:center top;"
                "height:220px;"
            ),
            cls="relative w-full rounded-t-xl",
        )
        if banner_url
        else Div(
            cls="w-full rounded-t-xl bg-gradient-to-br from-blue-700 via-violet-700 to-pink-600",
            style="height:220px;",
        )
    )

    # Identity strip — sits below the banner, avatar overlaps upward
    identity_strip = Div(
        # ── Row 1: avatar (overlapping banner) + CTA buttons flush right ─────
        # Keeping avatar and buttons in the same row ensures buttons are always
        # reachable and never clip off-screen on narrow viewports.
        Div(
            Img(
                src=thumbnail_url,
                alt=channel_name,
                # Smaller avatar on mobile so the negative-margin lift stays proportional
                cls="w-20 h-20 sm:w-28 sm:h-28 rounded-2xl object-cover ring-4 ring-background shadow-xl -mt-10 sm:-mt-14",
            ),
            Div(
                A(
                    UkIcon("youtube", cls="w-4 h-4 mr-1.5"),
                    "YouTube",
                    href=channel_url,
                    target="_blank",
                    rel="noopener noreferrer",
                    cls="inline-flex items-center px-3 py-1.5 sm:px-4 sm:py-2 bg-red-600 hover:bg-red-700 text-white text-xs sm:text-sm font-semibold rounded-lg no-underline transition-colors",
                ),
                render_favourite_button(creator_id, is_favourited=is_favourited),
                A(
                    UkIcon("git-compare", cls="w-4 h-4 mr-1"),
                    "Compare",
                    href=f"/compare?a={creator_id}&b=",
                    id="compare-btn",
                    title="Compare with another creator — paste a creator profile URL as ?b=<id>",
                    cls="inline-flex items-center px-3 py-1.5 sm:px-4 sm:py-2 bg-accent hover:bg-accent/80 text-foreground text-xs sm:text-sm font-semibold rounded-lg no-underline transition-colors",
                ),
                A(
                    UkIcon("map", cls="w-4 h-4 mr-1"),
                    "Blueprint",
                    href=f"/creator/{creator_id}/blueprint?from=/creator/{creator_id}&name={quote_plus(channel_name)}",
                    title="Growth Blueprint — Studio-grounded actions ranked by confidence",
                    cls="inline-flex items-center px-3 py-1.5 sm:px-4 sm:py-2 bg-primary/10 hover:bg-primary/20 text-primary text-xs sm:text-sm font-semibold rounded-lg no-underline transition-colors",
                ),
                A(
                    UkIcon("arrow-left", cls="w-4 h-4 mr-1"),
                    "Back",
                    href=back_url,
                    cls="inline-flex items-center px-3 py-1.5 sm:px-4 sm:py-2 bg-accent hover:bg-accent/80 text-foreground text-xs sm:text-sm font-semibold rounded-lg no-underline transition-colors",
                ),
                cls="flex gap-2 ml-auto items-center flex-wrap justify-end",
            ),
            cls="flex items-end justify-between px-4 sm:px-5 pt-3",
        ),
        # ── Row 2: name + meta — full width so tags can wrap freely ──────────
        Div(
            # Name + badges
            Div(
                H1(
                    channel_name,
                    cls="text-xl sm:text-2xl lg:text-3xl font-bold text-foreground leading-tight",
                ),
                (
                    Span(
                        UkIcon("badge-check", cls="w-4 h-4 mr-1 inline"),
                        "Official",
                        cls="inline-flex items-center text-xs font-semibold bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300 px-2 py-0.5 rounded-full shrink-0",
                    )
                    if official
                    else None
                ),
                cls="flex items-center gap-2 flex-wrap",
            ),
            # Rank chips — grade + subscriber rank badges, links to list pages
            *(
                [
                    Div(
                        (
                            Span(
                                f"{grade_icon} {grade_label}",
                                cls=f"text-xs font-semibold px-2 py-0.5 rounded-full {grade_bg}",
                            )
                            if quality_grade
                            else None
                        ),
                        (
                            _rank_chip(
                                f"#{country_rank} {country_flag} {country_code.upper()}",
                                href=f"/lists/country/{country_code.upper()}",
                                title=f"#{country_rank} by subscribers in {country_code.upper()}",
                            )
                            if country_rank is not None and country_code
                            else None
                        ),
                        (
                            _rank_chip(
                                f"#{category_rank} {get_topic_category_emoji(primary_category)} {primary_category[:20]}",
                                href=f"/lists/category/{slugify(primary_category)}",
                                title=f"#{category_rank} by subscribers in {primary_category}",
                            )
                            if category_rank is not None and primary_category
                            else None
                        ),
                        cls="flex flex-wrap items-center gap-1.5 mt-1",
                    )
                ]
                if (quality_grade or country_rank is not None or category_rank is not None)
                else []
            ),
            # Handle + country + language + age tags
            # Previously these were width-starved inside a flex child with no
            # declared width, forcing each tag onto its own line. Now they sit
            # in a full-width block and wrap naturally.
            Div(
                (
                    Span(handle_display, cls="text-sm text-muted-foreground font-mono")
                    if handle_display
                    else None
                ),
                (
                    Span(
                        f"{country_flag} {country_code.upper()}",
                        cls="text-sm text-muted-foreground",
                        title=country_code.upper(),
                    )
                    if country_code
                    else None
                ),
                (
                    Span(f"{lang_emoji} {lang_name}", cls="text-sm text-muted-foreground")
                    if language
                    else None
                ),
                (
                    Span(
                        f"📅 {format_channel_age(channel_age_days)}",
                        cls="text-xs text-muted-foreground px-2 py-0.5 bg-accent rounded-full",
                    )
                    if channel_age_days
                    else None
                ),
                (
                    Span(
                        f"📹 {monthly_uploads:.1f}/mo",
                        cls="text-xs text-muted-foreground px-2 py-0.5 bg-accent rounded-full",
                    )
                    if monthly_uploads
                    else None
                ),
                cls="flex flex-wrap items-center gap-x-3 gap-y-1.5 mt-1.5",
            ),
            cls="px-4 sm:px-5 pb-4 sm:pb-5 pt-3",
        ),
        cls="bg-background",
    )

    banner_section = Div(
        banner_layer,
        identity_strip,
        cls="bg-background rounded-xl border border-border shadow-sm mb-6 overflow-hidden",
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 2 — 4-up stat cards (dark-mode safe)
    # ═══════════════════════════════════════════════════════════════════════════
    # Rank line for Subscribers card — prefer country, fall back to category
    _subs_rank_line = None
    if not hidden_subs:
        if country_rank is not None and country_code:
            _subs_rank_line = Div(
                Span(f"#{country_rank}", cls="text-xs font-bold text-blue-500 dark:text-blue-400"),
                Span(
                    f" in {country_flag} {country_code.upper()}",
                    cls="text-xs text-muted-foreground",
                ),
                cls="mt-2",
            )
        elif category_rank is not None and primary_category:
            _subs_rank_line = Div(
                Span(f"#{category_rank}", cls="text-xs font-bold text-blue-500 dark:text-blue-400"),
                Span(f" in {primary_category}", cls="text-xs text-muted-foreground"),
                cls="mt-2",
            )

    stats_row = Grid(
        _stat_card(
            "Subscribers",
            format_number(current_subs) if not hidden_subs else "Hidden",
            subs_change,
            "text-blue-600 dark:text-blue-400",
            "bg-blue-50 dark:bg-blue-950/40",
            delta_pct=growth_rate if subs_change is not None else None,
            rank_line=_subs_rank_line,
        ),
        _stat_card(
            "Total Views",
            format_number(current_views),
            views_change,
            "text-violet-600 dark:text-violet-400",
            "bg-violet-50 dark:bg-violet-950/40",
        ),
        _stat_card(
            "Videos Published",
            format_number(current_videos),
            videos_change,
            "text-foreground",
            "bg-accent",
        ),
        _stat_card(
            "Est. Revenue",
            f"${format_number(estimated_revenue)}/mo",
            None,
            "text-emerald-600 dark:text-emerald-400",
            "bg-emerald-50 dark:bg-emerald-950/40",
        ),
        cols_sm=2,
        cols_lg=4,
        cls="mb-6",
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 3 — Two-column body
    # ═══════════════════════════════════════════════════════════════════════════

    # ── Left column: About + Channel Info ─────────────────────────────────────
    keyword_list = [k.strip() for k in keywords.split(",") if k.strip()] if keywords else []
    keyword_pills = (
        Div(
            *[
                Span(
                    kw,
                    cls="text-xs px-2.5 py-1 bg-accent text-muted-foreground rounded-full",
                )
                for kw in keyword_list[:15]
            ],
            cls="flex flex-wrap gap-1.5 mt-3",
        )
        if keyword_list
        else None
    )

    about_card = Card(
        H2("About", cls="text-base font-bold text-foreground mb-3"),
        (
            P(
                bio,
                cls="text-sm text-muted-foreground leading-relaxed whitespace-pre-line",
            )
            if bio
            else P("No description available.", cls="text-sm text-muted-foreground italic")
        ),
        keyword_pills,
        body_cls="p-5",
    )

    channel_info_items = [
        _info_row("link", "Handle", handle_display) if handle_display else None,
        _info_row("calendar", "Founded", published_at[:10] if published_at else "Unknown"),
        _info_row(
            "clock",
            "Channel Age",
            format_channel_age(channel_age_days) if channel_age_days else "Unknown",
        ),
        _info_row(
            "globe",
            "Country",
            f"{country_flag} {country_code.upper()}" if country_code else "Unknown",
        ),
        _info_row(
            "languages",
            "Language",
            f"{lang_emoji} {lang_name}" if language else "Unknown",
        ),
        _info_row("tag", "Category", primary_category) if primary_category else None,
        _info_row("video", "Total Videos", format_number(current_videos)),
        _info_row(
            "upload",
            "Upload Rate",
            f"{monthly_uploads:.1f} videos/month" if monthly_uploads else "—",
        ),
        (_info_row("eye-off", "Subscriber Count", "Hidden on YouTube") if hidden_subs else None),
    ]

    channel_info_card = Card(
        H2("Channel Info", cls="text-base font-bold text-foreground mb-4"),
        Div(*[i for i in channel_info_items if i], cls="space-y-3"),
        body_cls="p-5",
    )

    # ── Recent Upload card ─────────────────────────────────────────────────────
    # Use the parameter if explicitly provided, otherwise fall back to the
    # cached value stored in the creator row.
    _ru = recent_upload or safe_get_value(creator, "recent_upload")
    if isinstance(_ru, str):
        try:
            _ru = json.loads(_ru)
        except Exception:
            _ru = None
    recent_upload_card = None
    if isinstance(_ru, dict) and _ru.get("video_id"):
        ru_vid_id = _ru["video_id"]
        ru_title = _ru.get("title") or "Untitled"
        ru_thumb = _ru.get("thumbnail_url") or ""
        ru_views = int(_ru.get("view_count") or 0)
        ru_pub = _ru.get("published_at") or ""
        ru_dur_sec = int(_ru.get("duration_sec") or 0)
        ru_is_short = bool(_ru.get("is_short"))
        ru_url = f"https://www.youtube.com/watch?v={ru_vid_id}"
        ru_dur_str = f"{ru_dur_sec // 60}:{ru_dur_sec % 60:02d}" if ru_dur_sec > 0 else ""
        recent_upload_card = Card(
            Div(
                UkIcon("play-circle", cls="w-4 h-4 text-red-500 mr-2"),
                H2("Latest Upload", cls="text-base font-bold text-foreground"),
                *(
                    [
                        Span(
                            "#Shorts",
                            cls="text-xs font-semibold text-purple-600 dark:text-purple-400 ml-auto",
                        )
                    ]
                    if ru_is_short
                    else []
                ),
                cls="flex items-center mb-3",
            ),
            A(
                Div(
                    *(
                        [
                            Img(
                                src=ru_thumb,
                                alt=ru_title,
                                cls="w-full rounded-lg object-cover aspect-video",
                                loading="lazy",
                            )
                        ]
                        if ru_thumb
                        else []
                    ),
                    cls="relative",
                ),
                P(
                    ru_title,
                    cls="text-sm font-semibold text-foreground mt-2 line-clamp-2 hover:underline",
                ),
                href=ru_url,
                target="_blank",
                rel="noopener noreferrer",
                cls="no-underline block",
            ),
            Div(
                Span(
                    f"{format_number(ru_views)} views",
                    cls="text-xs text-muted-foreground",
                ),
                *(
                    [
                        Span("·", cls="text-border mx-1"),
                        Span(ru_dur_str, cls="text-xs text-muted-foreground"),
                    ]
                    if ru_dur_str
                    else []
                ),
                *(
                    [
                        Span("·", cls="text-border mx-1"),
                        Span(format_date_relative(ru_pub), cls="text-xs text-muted-foreground"),
                    ]
                    if ru_pub
                    else []
                ),
                cls="flex items-center flex-wrap mt-2",
            ),
            body_cls="p-5",
        )

    # Default left_col — overridden below if social/featured cards are present
    left_col = Div(
        about_card,
        channel_info_card,
        *(recent_upload_card and [recent_upload_card] or []),
        cls="flex flex-col gap-4",
    )

    # ── Social links card ──────────────────────────────────────────────────────
    if social_links:
        social_card = Card(
            Div(
                UkIcon("share-2", cls="w-4 h-4 text-muted-foreground mr-2"),
                H2("Connect", cls="text-base font-bold text-foreground"),
                cls="flex items-center mb-4",
            ),
            Div(
                *[
                    A(
                        UkIcon(icon, cls="w-4 h-4 shrink-0"),
                        # For email show the address; for social show the label
                        Span(
                            label if icon != "mail" else href.replace("mailto:", ""),
                            cls="text-sm font-medium truncate",
                        ),
                        href=href,
                        target=None if href.startswith("mailto:") else "_blank",
                        rel=(None if href.startswith("mailto:") else "noopener noreferrer"),
                        cls=f"inline-flex items-center gap-2 no-underline transition-colors {_SOCIAL_COLOURS.get(icon, 'text-foreground hover:text-primary')}",
                    )
                    for icon, label, href in social_links
                ],
                cls="flex flex-col gap-2.5",
            ),
            body_cls="p-5",
        )
        left_col = Div(
            about_card,
            channel_info_card,
            social_card,
            *(recent_upload_card and [recent_upload_card] or []),
            cls="flex flex-col gap-4",
        )

    # ── Featured channels card ─────────────────────────────────────────────────
    if featured_ch_urls:
        featured_card = Card(
            Div(
                UkIcon("users", cls="w-4 h-4 text-muted-foreground mr-2"),
                H2("Featured Channels", cls="text-base font-bold text-foreground"),
                cls="flex items-center mb-4",
            ),
            Div(
                *[
                    A(
                        UkIcon("external-link", cls="w-3.5 h-3.5 shrink-0"),
                        Span(
                            urlparse(url).path.lstrip("/@").split("?")[0] or url,
                            cls="text-sm truncate",
                        ),
                        href=url,
                        target="_blank",
                        rel="noopener noreferrer",
                        cls="inline-flex items-center gap-2 text-primary hover:underline no-underline",
                    )
                    for url in featured_ch_urls
                ],
                cls="flex flex-col gap-2",
            ),
            body_cls="p-5",
        )
        left_col = Div(
            about_card,
            channel_info_card,
            *(social_links and [social_card] or []),
            featured_card,
            *(recent_upload_card and [recent_upload_card] or []),
            cls="flex flex-col gap-4",
        )

    # ── Right column: Performance + Growth trend ───────────────────────────────
    # Hero-level secondary metrics
    # ── engagement comparison vs category peers ───────────────────────────────
    if peer_engagement_p75 > 0 and engagement_score > 0:
        eng_ratio = engagement_score / peer_engagement_p75
        if eng_ratio >= 1.1:
            eng_vs = f"{eng_ratio:.1f}× category avg"
            eng_vs_cls = "text-emerald-600 dark:text-emerald-400"
        elif eng_ratio <= 0.75:
            eng_vs = f"{eng_ratio:.1f}× category avg"
            eng_vs_cls = "text-red-500"
        else:
            eng_vs = "≈ category avg"
            eng_vs_cls = "text-muted-foreground"
    else:
        eng_vs = None
        eng_vs_cls = ""

    secondary_metrics = Div(
        _perf_row("Avg Views / Video", format_number(avg_views)),
        _perf_row("Views / Subscriber", f"{views_per_sub:.2f}x"),
        _perf_row(
            "Upload Rate",
            f"{monthly_uploads:.1f} / month" if monthly_uploads else "—",
        ),
        Div(
            Div(
                Span(
                    "Engagement Score",
                    cls="text-sm text-muted-foreground",
                ),
                Div(
                    Span(
                        f"{engagement_score:.2f} / 10",
                        cls="text-sm font-semibold "
                        + ("text-blue-600" if engagement_score >= 7 else "text-foreground"),
                    ),
                    *([Span(eng_vs, cls=f"text-xs ml-2 {eng_vs_cls}")] if eng_vs else []),
                    cls="flex items-center gap-1",
                ),
                cls="flex justify-between items-center py-1.5",
            ),
        ),
        *(
            [
                Div(
                    Span("Momentum", cls="text-sm text-muted-foreground"),
                    Div(
                        Span(f"{momentum_score:.0f}", cls="text-sm font-bold text-foreground"),
                        Span(
                            momentum_label,
                            cls=f"text-xs font-semibold px-2 py-0.5 rounded-full border ml-2 {momentum_style}",
                        ),
                        cls="flex items-center",
                    ),
                    cls="flex justify-between items-center py-1.5",
                )
            ]
            if momentum_label
            else []
        ),
    )
    has_growth_data = subs_change is not None
    if has_growth_data:
        bar_width = min(100, max(2, abs(growth_rate) * 5))
        bar_colour = "bg-emerald-500" if growth_rate >= 0 else "bg-red-500"
        trend_bg = (
            "bg-emerald-50 dark:bg-emerald-950/30"
            if growth_rate >= 0
            else "bg-red-50 dark:bg-red-950/30"
        )
        trend_section = Div(
            DivFullySpaced(
                P(
                    "30-Day Growth",
                    cls="text-xs font-semibold text-muted-foreground uppercase tracking-wide",
                ),
                Div(
                    Span(f"{growth_rate:+.1f}%", cls="text-sm font-bold text-foreground"),
                    Span(
                        growth_label,
                        cls=f"text-xs font-semibold px-2 py-0.5 rounded-full border {growth_style} ml-2",
                    ),
                    cls="flex items-center",
                ),
            ),
            Div(
                Div(
                    cls=f"h-2 {bar_colour} rounded-full transition-all",
                    style=f"width:{bar_width}%",
                ),
                cls="w-full h-2 bg-border rounded-full overflow-hidden mt-2",
            ),
            cls=f"{trend_bg} rounded-xl p-4 mt-4",
        )
    else:
        trend_section = Div(
            UkIcon("bar-chart-2", cls="w-6 h-6 text-muted-foreground mb-1"),
            P(
                "Growth tracking initializing",
                cls="text-sm font-medium text-foreground",
            ),
            P("Check back in 7+ days", cls="text-xs text-muted-foreground mt-0.5"),
            cls="flex flex-col items-center text-center bg-accent rounded-xl p-4 mt-4",
        )

    performance_card = Card(
        H2("Performance", cls="text-base font-bold text-foreground mb-1"),
        secondary_metrics,
        trend_section,
        # ── Revenue breakdown (v4 model) ─────────────────────────────────────
        Div(
            DivFullySpaced(
                Div(
                    UkIcon("circle-dollar-sign", cls="w-4 h-4 text-emerald-500 mr-1.5"),
                    Span(
                        "Est. Monthly Revenue",
                        cls="text-xs font-semibold text-muted-foreground uppercase tracking-wide",
                    ),
                    cls="flex items-center",
                ),
                Span(
                    f"${format_number(estimated_revenue)}",
                    cls="text-sm font-bold text-emerald-600 dark:text-emerald-400",
                ),
                cls="mb-2",
            ),
            Div(
                _perf_row(
                    "AdSense — long-form",
                    f"${format_number(revenue_split['adsense_long'])}",
                    "text-emerald-600 dark:text-emerald-400",
                ),
                _perf_row(
                    "AdSense — Shorts",
                    f"${format_number(revenue_split['adsense_shorts'])}",
                    "text-emerald-600 dark:text-emerald-400",
                ),
                _perf_row(
                    "Brand deals (est.)",
                    f"${format_number(revenue_split['brand_deals'])}",
                    "text-emerald-600 dark:text-emerald-400",
                ),
                _perf_row(
                    "Assumed Shorts mix",
                    assumed_shorts_pct,
                ),
            ),
            cls="bg-emerald-50 dark:bg-emerald-950/30 rounded-xl p-4 mt-4",
        ),
        body_cls="p-5",
    )

    # ── Rankings card (country / language / category) ─────────────────────────
    ranking_rows = []
    if country_rank is not None and country_code:
        ranking_rows.append(
            Div(
                Span(
                    f"{country_flag} {country_code.upper()}",
                    cls="text-sm text-muted-foreground",
                ),
                Div(
                    A(
                        f"#{country_rank}",
                        href=f"/lists/country/{country_code.upper()}",
                        cls="text-sm font-bold text-primary hover:underline no-underline",
                        title=f"Ranked #{country_rank} in {country_code.upper()} by subscribers",
                    ),
                    Span("in country", cls="text-xs text-muted-foreground ml-1"),
                    cls="flex items-center",
                ),
                cls="flex justify-between items-center py-2 border-b border-border last:border-0",
            )
        )
    if language_rank is not None and language:
        ranking_rows.append(
            Div(
                Span(f"{lang_emoji} {lang_name}", cls="text-sm text-muted-foreground"),
                Div(
                    A(
                        f"#{language_rank}",
                        href=f"/lists/language/{language.lower()}",
                        cls="text-sm font-bold text-primary hover:underline no-underline",
                        title=f"Ranked #{language_rank} in {lang_name} by subscribers",
                    ),
                    Span("in language", cls="text-xs text-muted-foreground ml-1"),
                    cls="flex items-center",
                ),
                cls="flex justify-between items-center py-2 border-b border-border last:border-0",
            )
        )
    if category_rank is not None and primary_category:
        ranking_rows.append(
            Div(
                Span(
                    f"{get_topic_category_emoji(primary_category)} {primary_category}",
                    cls="text-sm text-muted-foreground",
                ),
                Div(
                    A(
                        f"#{category_rank}",
                        href=f"/lists/category/{slugify(primary_category)}",
                        cls="text-sm font-bold text-primary hover:underline no-underline",
                        title=f"Ranked #{category_rank} in {primary_category} by subscribers",
                    ),
                    Span("in category", cls="text-xs text-muted-foreground ml-1"),
                    cls="flex items-center",
                ),
                cls="flex justify-between items-center py-2 border-b border-border last:border-0",
            )
        )

    rankings_card = (
        Card(
            Div(
                UkIcon("trophy", cls="w-4 h-4 text-amber-500 mr-2"),
                H2("Rankings", cls="text-base font-bold text-foreground"),
                cls="flex items-center mb-3",
            ),
            Div(*ranking_rows),
            body_cls="p-5",
        )
        if ranking_rows
        else None
    )

    # Topic categories card — pills link to list pages; primary cat shows rank chip
    topic_pill_section = None
    if topic_categories_raw:
        parsed_cats = []
        try:
            parsed = json.loads(topic_categories_raw)
            raw_list = parsed if isinstance(parsed, list) else [str(parsed)]
        except (json.JSONDecodeError, TypeError):
            raw_list = [c.strip() for c in str(topic_categories_raw).split(",") if c.strip()]
        for item in raw_list:
            if "wikipedia.org/wiki/" in str(item):
                try:
                    slug = str(item).split("/wiki/")[-1].rstrip("/")
                    name = unquote(slug).replace("_", " ")
                    if name:
                        parsed_cats.append(name)
                except Exception:
                    pass
            else:
                clean = str(item).strip("\"'[]").strip()
                if clean:
                    parsed_cats.append(clean)

        if parsed_cats:
            pill_palette = [
                "bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300",
                "bg-violet-100 dark:bg-violet-900/40 text-violet-700 dark:text-violet-300",
                "bg-emerald-100 dark:bg-emerald-900/40 text-emerald-700 dark:text-emerald-300",
                "bg-pink-100 dark:bg-pink-900/40 text-pink-700 dark:text-pink-300",
                "bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300",
            ]
            pills = [
                A(
                    f"{get_topic_category_emoji(cat)} {cat}",
                    href=f"/lists/category/{slugify(cat)}",
                    cls=f"text-xs font-medium no-underline hover:underline inline-flex items-center px-3 py-1 rounded-full {pill_palette[i % len(pill_palette)]}",
                )
                for i, cat in enumerate(parsed_cats)
            ]
            topic_pill_section = Card(
                H2("Topics", cls="text-base font-bold text-foreground mb-3"),
                Div(*pills, cls="flex flex-wrap gap-2"),
                body_cls="p-5",
            )

    # Category distribution bars
    cat_dist_card = None
    if category_distribution:
        try:
            dist = (
                json.loads(category_distribution)
                if isinstance(category_distribution, str)
                else category_distribution
            )
            if isinstance(dist, dict) and dist:
                total_dist = sum(dist.values()) or 1
                bar_colours = [
                    "bg-blue-400",
                    "bg-violet-400",
                    "bg-emerald-400",
                    "bg-pink-400",
                    "bg-amber-400",
                    "bg-indigo-400",
                    "bg-teal-400",
                    "bg-rose-400",
                ]
                bars = [
                    Div(
                        Span(
                            f"{get_topic_category_emoji(cat)} {cat}",
                            cls="text-sm text-foreground w-44 shrink-0 truncate",
                        ),
                        Div(
                            Div(
                                cls=f"h-4 {bar_colours[i % len(bar_colours)]} rounded-r-full transition-all",
                                style=f"width:{min(100, round(count / total_dist * 100))}%",
                            ),
                            cls="flex-1 h-4 bg-accent rounded-full overflow-hidden",
                        ),
                        Span(
                            f"{round(count / total_dist * 100)}%",
                            cls="text-xs font-semibold text-muted-foreground w-10 text-right shrink-0",
                        ),
                        cls="flex items-center gap-3",
                    )
                    for i, (cat, count) in enumerate(sorted(dist.items(), key=lambda x: -x[1])[:8])
                ]
                cat_dist_card = Card(
                    H2(
                        "Content Breakdown",
                        cls="text-base font-bold text-foreground mb-4",
                    ),
                    Div(*bars, cls="space-y-3"),
                    body_cls="p-5",
                )
        except Exception:
            pass

    # ── Niche Leaderboard card ──────────────────────────────────────────────
    leaderboard_card = None
    if niche_leaderboard and primary_category:
        lb_rows = []
        for i, peer in enumerate(niche_leaderboard):
            peer_id = peer.get("id", "")
            peer_name = peer.get("channel_name", "Unknown")
            peer_thumb = peer.get("channel_thumbnail_url") or "/static/favicon.jpeg"
            peer_eng = float(peer.get("engagement_score") or 0)
            peer_subs = int(peer.get("current_subscribers") or 0)
            is_self = peer_id == creator_id
            row_cls = (
                "flex items-center gap-3 py-2 px-2 rounded-lg bg-primary/10 border border-primary/30"
                if is_self
                else "flex items-center gap-3 py-2"
            )
            lb_rows.append(
                Div(
                    Span(f"{i + 1}", cls="text-xs font-bold text-muted-foreground w-4 shrink-0"),
                    Img(
                        src=peer_thumb,
                        alt=peer_name,
                        cls="size-7 rounded-full object-cover shrink-0",
                    ),
                    Div(
                        A(
                            peer_name,
                            href=f"/creator/{peer_id}" if peer_id else "#",
                            cls="text-xs font-semibold text-foreground hover:underline line-clamp-1"
                            + (" text-primary" if is_self else ""),
                        ),
                        Span(
                            format_number(peer_subs),
                            cls="text-xs text-muted-foreground",
                        ),
                        cls="flex-1 min-w-0",
                    ),
                    Span(
                        f"{peer_eng:.2f}",
                        cls="text-xs font-bold text-blue-600 shrink-0",
                        title="Engagement score",
                    ),
                    cls=row_cls,
                )
            )
        leaderboard_card = Card(
            Div(
                UkIcon("flame", cls="w-4 h-4 text-orange-500 mr-2"),
                H2(
                    f"Top {primary_category}",
                    cls="text-base font-bold text-foreground",
                ),
                Span(
                    "by engagement",
                    cls="text-xs text-muted-foreground ml-auto",
                ),
                cls="flex items-center mb-3",
            ),
            Div(*lb_rows, cls="space-y-0.5"),
            A(
                f"See all {primary_category} creators →",
                href=f"/lists/category/{slugify(primary_category)}",
                cls="mt-3 block text-xs text-primary hover:underline text-right",
            ),
            body_cls="p-5",
        )

    right_col = Div(
        performance_card,
        rankings_card,
        *(leaderboard_card and [leaderboard_card] or []),
        topic_pill_section,
        cat_dist_card,
        cls="flex flex-col gap-4",
    )

    body_cols = Grid(left_col, right_col, cols_sm=1, cols_lg=2, cls="mb-6")

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 4 — Similar creators rail
    # ═══════════════════════════════════════════════════════════════════════════
    similar_section = _render_similar_creators(
        similar_creators or [],
        primary_category,
        country_code,
        current_creator_id=creator_id,
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 5 — Sync / freshness footer
    # ═══════════════════════════════════════════════════════════════════════════
    sync_colour = {
        "synced": "text-emerald-600",
        "pending": "text-amber-600",
        "error": "text-red-600",
    }.get(sync_status, "text-muted-foreground")
    sync_icon_map = {"synced": "check-circle", "pending": "clock", "error": "x-circle"}
    sync_uk_icon = sync_icon_map.get(sync_status, "circle")

    footer_section = Div(
        UkIcon(sync_uk_icon, cls=f"w-3.5 h-3.5 {sync_colour}"),
        Span(sync_status.title(), cls=f"text-xs font-semibold {sync_colour}"),
        Span("·", cls="text-border mx-1"),
        Span(
            f"Updated {format_date_relative(last_updated)}",
            cls="text-xs text-muted-foreground",
        ),
        Span("·", cls="text-border mx-1"),
        Span(
            f"Synced {format_date_relative(last_synced)}",
            cls="text-xs text-muted-foreground",
        ),
        Span("·", cls="text-border mx-1"),
        Span(
            f"ID {creator_id[:8]}…",
            cls="text-xs text-muted-foreground font-mono",
            title=creator_id,
        ),
        cls="flex items-center justify-center flex-wrap gap-1 py-4 text-center",
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # SECTION 5 — Category comparison box plots
    # ═══════════════════════════════════════════════════════════════════════════
    box_plot_section = render_category_box_plots(creator, category_stats)

    return Div(
        banner_section,
        stats_row,
        body_cols,
        similar_section,
        box_plot_section,
        footer_section,
        cls="max-w-5xl mx-auto px-4 pb-16 pt-6",
    )


# ============================================================================
# HANDLE SEARCH PREVIEW CARD
# ============================================================================


def render_creator_preview(handle: str, channel_info: dict, search: str = "") -> Container:
    """Render preview card for new creator found via handle search.

    Shows basic info from YouTube API with "Add to Database" button.
    """
    channel_id = channel_info.get("channel_id")
    title = channel_info.get("title", "Unknown Creator")
    custom_url = channel_info.get("custom_url", handle.lstrip("@"))
    thumbnail = channel_info.get("thumbnail", "")
    description = channel_info.get("description", "")
    subs = channel_info.get("subscriber_count", 0)
    views = channel_info.get("view_count", 0)
    videos = channel_info.get("video_count", 0)
    country = channel_info.get("country")

    return Container(
        # Hero banner
        Div(
            H1(
                "📍 Creator Found on YouTube",
                cls="text-4xl font-bold text-foreground mb-2",
            ),
            P(
                f"Found {handle} on YouTube. Add them to your database to track their stats.",
                cls="text-lg text-muted-foreground",
            ),
            cls="mb-8",
        ),
        # Preview card
        Card(
            # Channel header with avatar
            Div(
                Div(
                    Img(
                        src=thumbnail or "/static/favicon.jpeg",
                        alt=title,
                        cls="w-24 h-24 rounded-full object-cover border-4 border-white shadow-lg",
                    ),
                    cls="flex-shrink-0",
                ),
                Div(
                    H2(title, cls="text-2xl font-bold text-foreground"),
                    P(
                        handle,
                        cls="text-lg text-muted-foreground font-mono",
                    ),
                    (
                        Div(
                            get_country_flag(country),
                            Span(
                                country.upper(),
                                cls="ml-2 text-sm text-muted-foreground",
                            ),
                            cls="flex items-center mt-2",
                        )
                        if country
                        else None
                    ),
                    cls="ml-6",
                ),
                cls="flex items-center mb-6",
            ),
            # Stats grid
            Div(
                Div(
                    P("Subscribers", cls="text-xs text-muted-foreground uppercase"),
                    P(
                        format_number(subs),
                        cls="text-2xl font-bold text-foreground mt-1",
                    ),
                    cls="text-center bg-blue-50 rounded-lg p-4",
                ),
                Div(
                    P("Total Views", cls="text-xs text-muted-foreground uppercase"),
                    P(
                        format_number(views),
                        cls="text-2xl font-bold text-foreground mt-1",
                    ),
                    cls="text-center bg-green-50 rounded-lg p-4",
                ),
                Div(
                    P("Videos", cls="text-xs text-muted-foreground uppercase"),
                    P(
                        format_number(videos),
                        cls="text-2xl font-bold text-foreground mt-1",
                    ),
                    cls="text-center bg-purple-50 rounded-lg p-4",
                ),
                cls="grid grid-cols-3 gap-4 mb-6",
            ),
            # Description
            (
                Div(
                    P("About", cls="text-sm font-semibold text-foreground mb-2"),
                    P(
                        description[:200] + ("..." if len(description) > 200 else ""),
                        cls="text-sm text-muted-foreground leading-relaxed",
                    ),
                    cls="mb-6",
                )
                if description
                else None
            ),
            # Action buttons
            Div(
                Form(
                    Input(type="hidden", name="handle", value=handle),
                    Input(type="hidden", name="channel_id", value=channel_id),
                    Input(type="hidden", name="channel_name", value=title),
                    Input(type="hidden", name="custom_url", value=custom_url),
                    Input(type="hidden", name="thumbnail", value=thumbnail),
                    Button(
                        "✅ Add to Database & Track Stats",
                        type="submit",
                        cls="w-full bg-gradient-to-r from-blue-600 to-blue-500 text-white font-semibold py-3 px-6 rounded-lg hover:from-blue-700 hover:to-blue-600 transition-all shadow-md hover:shadow-lg",
                    ),
                    method="POST",
                    action="/creators/add",
                ),
                A(
                    "← Back to Search",
                    href="/creators",
                    cls="block text-center text-muted-foreground hover:text-foreground font-medium mt-4 no-underline",
                ),
                cls="border-t pt-6 mt-6",
            ),
            cls="max-w-2xl mx-auto",
        ),
        cls=ContainerT.xl,
    )
