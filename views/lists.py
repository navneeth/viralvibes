"""
Creator Lists view — curated, pre-filtered creator rankings.
Each tab is a different lens on the creators database.
"""

import pycountry
from fasthtml.common import *
from monsterui.all import *
from urllib.parse import quote

from utils import format_number, safe_get_value, slugify
from utils.creator_metrics import (
    get_country_flag,
    get_grade_info,
    get_language_emoji,
    get_language_name,
    calculate_growth_rate,
    format_channel_age,
)
from views.creators import get_topic_category_emoji


# ─────────────────────────────────────────────────────────────────────────────
# Helper wrappers
# ─────────────────────────────────────────────────────────────────────────────


def get_country_flag_emoji(country_code: str) -> str:
    """Get flag emoji for country code, fallback to 🌐 if not found."""
    flag = get_country_flag(country_code)
    return flag if flag else "🌐"


# ─────────────────────────────────────────────────────────────────────────────
# ISO 3166-1 country code → display name (via pycountry)
# ─────────────────────────────────────────────────────────────────────────────
# Override entries where the official ISO name is too verbose or non-standard
# for a UI context.  Everything else falls through to pycountry.
_COUNTRY_NAME_OVERRIDES: dict[str, str] = {
    "AE": "UAE",
    "BO": "Bolivia",
    "CD": "DR Congo",
    "CF": "C. African Rep.",
    "CZ": "Czech Republic",
    "DO": "Dominican Rep.",
    "GB": "United Kingdom",
    "IR": "Iran",
    "KP": "North Korea",
    "KR": "South Korea",
    "LA": "Laos",
    "MD": "Moldova",
    "MK": "North Macedonia",
    "PS": "Palestine",
    "RU": "Russia",
    "SY": "Syria",
    "TW": "Taiwan",
    "TZ": "Tanzania",
    "US": "United States",
    "VA": "Vatican",
    "VE": "Venezuela",
    "VN": "Vietnam",
}


def get_country_name(country_code: str) -> str:
    """Return a UI-friendly country name for a two-letter ISO 3166-1 alpha-2 code.

    Checks a small overrides dict first (for cases where the official ISO name
    is too verbose), then falls back to pycountry's database, preferring
    ``common_name`` over the formal ``name`` when available.
    """
    code = country_code.upper()
    if code in _COUNTRY_NAME_OVERRIDES:
        return _COUNTRY_NAME_OVERRIDES[code]
    country = pycountry.countries.get(alpha_2=code)
    if not country:
        return code
    return getattr(country, "common_name", country.name)


def _unslugify(slug: str) -> str:
    """Convert a URL slug back to a space-separated search term / display name."""
    return slug.replace("-", " ")


# ─────────────────────────────────────────────────────────────────────────────
# Tab definitions
# Each entry: (id, label, icon, description, coming_soon)
# ─────────────────────────────────────────────────────────────────────────────
LISTS_TABS = [
    (
        "top-rated",
        "Top Rated",
        "star",
        "Creators with the highest quality scores — consistent upload cadence, strong engagement, and growing audiences.",
        False,
    ),
    (
        "most-active",
        "Most Active",
        "zap",
        "Channels uploading the most frequently. Great for finding creators who publish consistently.",
        False,
    ),
    (
        "by-country",
        "By Country",
        "globe",
        "Browse creators grouped by their home country. Discover regional talent around the world.",
        False,
    ),
    (
        "by-category",
        "By Category",
        "grid",
        "Explore creators grouped by topic/niche. Find channels in specific content categories.",
        False,
    ),
    (
        "by-language",
        "By Language",
        "languages",
        "Browse creators grouped by their primary content language. Discover talent across linguistic communities.",
        False,
    ),
    (
        "rising",
        "Rising Stars",
        "trending-up",
        "Channels with the fastest subscriber and view growth over the past 30 days.",
        False,
    ),
    (
        "veterans",
        "Veterans",
        "award",
        "Channels that have been publishing for 10+ years. Proven, durable creators.",
        False,
    ),
    (
        "new-channels",
        "New Channels",
        "sparkles",
        "Channels created in the last year. Early-stage creators to watch.",
        False,
    ),
]


# ─────────────────────────────────────────────────────────────────────────────
# Creator Row Component (reusable)
# ─────────────────────────────────────────────────────────────────────────────


def _creator_row(creator: dict, rank: int, show_growth: bool = False, show_activity: bool = False):
    """
    Renders a single creator row for list views.

    Args:
        creator: Creator dict with stats
        rank: Ranking position (1-based)
        show_growth: If True, shows 30-day growth instead of total views
        show_activity: If True, shows monthly uploads instead of total views

    Returns:
        Div with creator row layout
    """
    channel_name = safe_get_value(creator, "channel_name", "Unknown Creator")
    channel_url = safe_get_value(creator, "channel_url", "#")
    custom_url = safe_get_value(creator, "custom_url", "")
    thumbnail_url = safe_get_value(creator, "channel_thumbnail_url", "")
    current_subs = safe_get_value(creator, "current_subscribers", 0)
    current_views = safe_get_value(creator, "current_view_count", 0)
    current_videos = safe_get_value(creator, "current_video_count", 0)
    quality_grade = safe_get_value(creator, "quality_grade", "C")
    country_code = safe_get_value(creator, "country_code", "")
    language = safe_get_value(creator, "language", "en")
    subs_change = safe_get_value(creator, "subscribers_change_30d", 0)
    monthly_uploads = safe_get_value(creator, "monthly_uploads", 0)

    # Get grade badge info
    grade_icon, grade_label, grade_bg = get_grade_info(quality_grade)

    # Calculate growth rate if showing growth
    growth_rate = 0
    if show_growth and subs_change and current_subs:
        growth_rate = calculate_growth_rate(subs_change, current_subs)

    # Determine what metric to show in stats column
    if show_growth:
        stat_value = (
            f"+{format_number(subs_change)}" if subs_change > 0 else format_number(subs_change)
        )
        stat_label = "30d growth"
    elif show_activity:
        stat_value = f"{monthly_uploads:.1f}/mo" if monthly_uploads else "—"
        stat_label = "uploads"
    else:
        stat_value = format_number(current_views)
        stat_label = "views"

    return Div(
        # Rank number
        Div(
            Span(str(rank), cls="text-sm font-bold text-muted-foreground"),
            cls="w-8 shrink-0 text-center",
        ),
        # Avatar
        Div(
            Img(
                src=thumbnail_url or "/static/favicon.jpeg",
                alt=channel_name,
                cls="size-10 rounded-full object-cover",
            ),
            cls="shrink-0",
        ),
        # Name + handle
        Div(
            A(
                channel_name,
                href=(
                    f"/creator/{safe_get_value(creator, 'id', '')}"
                    if safe_get_value(creator, "id")
                    else channel_url
                ),
                cls="text-sm font-semibold text-foreground hover:underline line-clamp-1",
            ),
            Div(
                Span(
                    custom_url or channel_name[:20],
                    cls="text-xs text-muted-foreground",
                ),
                # Country + language badges (inline)
                (
                    Span(
                        f"{get_country_flag(country_code)} {get_language_emoji(language)}",
                        cls="text-xs ml-1",
                        title=f"{country_code} · {get_language_name(language)}",
                    )
                    if country_code or language != "en"
                    else None
                ),
                cls="flex items-center gap-1",
            ),
            cls="flex-1 min-w-0",
        ),
        # Subscribers (always show)
        Div(
            Div(
                Span(
                    format_number(current_subs),
                    cls="text-sm font-semibold text-foreground",
                ),
                Span("subs", cls="text-xs text-muted-foreground"),
                cls="flex flex-col items-end",
            ),
            cls="hidden sm:block shrink-0 w-20 text-right",
        ),
        # Metric column (views/growth/activity)
        Div(
            Div(
                Span(stat_value, cls="text-sm font-medium text-foreground"),
                Span(stat_label, cls="text-xs text-muted-foreground"),
                cls="flex flex-col items-end",
            ),
            cls="hidden md:block shrink-0 w-24 text-right",
        ),
        # Grade badge
        Div(
            Span(
                grade_icon,
                cls=f"inline-flex items-center justify-center size-7 rounded-full text-xs font-bold {grade_bg}",
                title=grade_label,
            ),
            cls="shrink-0",
        ),
        cls="flex items-center gap-3 p-3 rounded-xl border border-border bg-background hover:bg-accent transition-colors",
    )


def _creator_mini_row(creator: dict, rank: int):
    """
    Compact creator row for grouped sections (by country/category).
    Shows less info to fit 5 per group.

    Args:
        creator: Creator dict with stats
        rank: Ranking position within group (1-based)

    Returns:
        Div with compact creator row layout
    """
    channel_name = safe_get_value(creator, "channel_name", "Unknown Creator")
    channel_url = safe_get_value(creator, "channel_url", "#")
    creator_id = safe_get_value(creator, "id", "")
    thumbnail_url = safe_get_value(creator, "channel_thumbnail_url", "")
    current_subs = safe_get_value(creator, "current_subscribers", 0)
    quality_grade = safe_get_value(creator, "quality_grade", "C")

    # Get grade badge info
    grade_icon, grade_label, grade_bg = get_grade_info(quality_grade)

    profile_href = f"/creator/{creator_id}" if creator_id else channel_url

    return Div(
        # Rank
        Span(
            str(rank),
            cls="text-xs font-bold text-muted-foreground w-4 shrink-0 text-center",
        ),
        # Avatar (smaller)
        Img(
            src=thumbnail_url or "/static/favicon.jpeg",
            alt=channel_name,
            cls="size-8 rounded-full object-cover shrink-0",
        ),
        # Name
        A(
            channel_name,
            href=profile_href,
            cls="text-sm font-medium text-foreground hover:underline line-clamp-1 flex-1 min-w-0",
        ),
        # Subscribers
        Span(
            format_number(current_subs),
            cls="text-xs font-semibold text-muted-foreground shrink-0 hidden sm:inline",
        ),
        # Grade badge (smaller)
        Span(
            grade_icon,
            cls=f"inline-flex items-center justify-center size-6 rounded-full text-xs font-bold {grade_bg} shrink-0",
            title=grade_label,
        ),
        cls="flex items-center gap-2 p-2 rounded-lg border border-border bg-background hover:bg-accent transition-colors",
    )


# ─────────────────────────────────────────────────────────────────────────────
# List Heart Button — save/unsave a list to the user dashboard
# ─────────────────────────────────────────────────────────────────────────────


def _list_heart_btn(
    list_key: str,
    list_label: str,
    list_url: str,
    is_fav: bool,
    *,
    authenticated: bool = False,
):
    """
    Heart toggle button that bookmarks/unbookmarks a curated list.

    Authenticated users see a clickable heart that toggles via HTMX POST.
    Unauthenticated users see a greyed-out heart that links to login.

    The POST response is this same component with the new is_fav state, so
    hx-swap=outerHTML replaces the entire form in-place.
    """
    btn_id = f"heart-{list_key.replace(':', '-').replace(' ', '-')}"
    icon_cls = "size-4 " + (
        "fill-red-500 text-red-500" if is_fav else "text-gray-300 group-hover:text-red-400"
    )
    if not authenticated:
        return A(
            UkIcon("heart", cls="size-4 text-gray-200"),
            id=btn_id,
            href="/login",
            title="Sign in to save lists to your dashboard",
            cls="inline-flex items-center p-1",
        )
    return Form(
        Input(type="hidden", name="list_key", value=list_key),
        Input(type="hidden", name="list_label", value=list_label),
        Input(type="hidden", name="list_url", value=list_url),
        Button(
            UkIcon("heart", cls=icon_cls),
            type="submit",
            title="Remove from dashboard" if is_fav else "Save to dashboard",
            cls="inline-flex items-center p-1 rounded hover:bg-gray-100 transition-colors group",
        ),
        id=btn_id,
        hx_post="/me/favourite-list",
        hx_swap="outerHTML",
        hx_target=f"#{btn_id}",
        cls="inline m-0 p-0",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Group card builders (country / category) — used by both initial render
# and HTMX load-more partials
# ─────────────────────────────────────────────────────────────────────────────


def _country_group_card(
    country_data: dict,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> Div:
    """Render a single country group card with its top creators."""
    country_code = country_data["country_code"]
    count = country_data["count"]
    creators = country_data["creators"]

    country_name = get_country_name(country_code)
    list_key = f"country:{country_code}"
    list_label = f"{get_country_flag(country_code) or '🌍'} {country_name}"
    list_url = f"/lists/country/{country_code}"

    return Div(
        # Header: flag + name + creator count + heart
        Div(
            A(
                Span(get_country_flag(country_code), cls="text-2xl shrink-0"),
                Div(
                    H3(
                        country_name,
                        cls="text-base font-bold text-foreground leading-tight",
                    ),
                    Span(country_code, cls="text-xs text-muted-foreground font-mono"),
                    cls="flex flex-col min-w-0",
                ),
                Span(
                    f"{format_number(count)} creators →",
                    cls="ml-auto shrink-0 text-xs font-medium text-primary",
                ),
                href=list_url,
                cls="flex items-center gap-2 hover:opacity-75 transition-opacity flex-1 min-w-0",
            ),
            _list_heart_btn(
                list_key, list_label, list_url, list_key in fav_keys, authenticated=authenticated
            ),
            cls="flex items-center gap-2",
        ),
        (
            Div(
                *[_creator_mini_row(c, rank=i + 1) for i, c in enumerate(creators)],
                cls="space-y-2 mt-3",
            )
            if creators
            else P("No creators yet", cls="text-xs text-muted-foreground mt-2")
        ),
        cls="p-4 rounded-xl border border-border bg-background hover:border-primary/40 transition-colors",
    )


def _category_group_card(
    category_data: dict,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> Div:
    """Render a single category group card with its top creators."""
    category = category_data["category"]
    count = category_data["count"]
    creators = category_data["creators"]

    emoji = get_topic_category_emoji(category)
    # Strip any Wikipedia URL prefix that may slip through
    display_name = category.split("/")[-1].strip() if "/" in category else category
    # Title-case for readability
    display_name = display_name.title() if display_name == display_name.lower() else display_name

    list_key = f"category:{display_name}"
    list_label = f"{emoji} {display_name}"
    list_url = f"/lists/category/{quote(display_name, safe='')}"

    return Div(
        # Header: emoji + name + creator count + heart
        Div(
            A(
                Span(emoji, cls="text-2xl shrink-0"),
                Span(
                    display_name,
                    cls="text-base font-bold text-foreground leading-tight flex-1 min-w-0",
                ),
                Span(
                    f"{format_number(count)} creators →",
                    cls="ml-auto shrink-0 text-xs font-medium text-primary",
                ),
                href=list_url,
                cls="flex items-center gap-2 hover:opacity-75 transition-opacity flex-1 min-w-0",
            ),
            _list_heart_btn(
                list_key, list_label, list_url, list_key in fav_keys, authenticated=authenticated
            ),
            cls="flex items-center gap-2",
        ),
        (
            Div(
                *[_creator_mini_row(c, rank=i + 1) for i, c in enumerate(creators)],
                cls="space-y-2 mt-3",
            )
            if creators
            else P("No creators yet", cls="text-xs text-muted-foreground mt-2")
        ),
        cls="p-4 rounded-xl border border-border bg-background hover:border-primary/40 transition-colors",
    )


def _language_group_card(
    language_data: dict,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> Div:
    """Render a single language group card with its top creators."""
    language_code = language_data["language_code"]
    count = language_data["count"]
    creators = language_data["creators"]

    language_name = get_language_name(language_code)
    emoji = get_language_emoji(language_code)

    list_key = f"language:{language_code.lower()}"
    list_label = f"{emoji} {language_name}"
    list_url = f"/lists/language/{language_code.lower()}"

    return Div(
        # Header: emoji + name + creator count + heart
        Div(
            A(
                Span(emoji, cls="text-2xl shrink-0"),
                Div(
                    H3(
                        language_name,
                        cls="text-base font-bold text-foreground leading-tight",
                    ),
                    Span(language_code.upper(), cls="text-xs text-muted-foreground font-mono"),
                    cls="flex flex-col min-w-0",
                ),
                Span(
                    f"{format_number(count)} creators →",
                    cls="ml-auto shrink-0 text-xs font-medium text-primary",
                ),
                href=list_url,
                cls="flex items-center gap-2 hover:opacity-75 transition-opacity flex-1 min-w-0",
            ),
            _list_heart_btn(
                list_key, list_label, list_url, list_key in fav_keys, authenticated=authenticated
            ),
            cls="flex items-center gap-2",
        ),
        (
            Div(
                *[_creator_mini_row(c, rank=i + 1) for i, c in enumerate(creators)],
                cls="space-y-2 mt-3",
            )
            if creators
            else P("No creators yet", cls="text-xs text-muted-foreground mt-2")
        ),
        cls="p-4 rounded-xl border border-border bg-background hover:border-primary/40 transition-colors",
    )


def _load_more_button(
    url: str,
    target_id: str,
    next_offset: int,
    label: str,
    *,
    oob: bool = False,
    total: int = 0,
) -> Div:
    """
    HTMX "Show more" button that appends the next batch into *target_id*.
    Uses hx-get + hx-swap=beforeend so existing cards stay in place.

    Args:
        oob: When True, sets hx-swap-oob="true" so HTMX replaces this
             element out-of-band.  Pass oob=True when this button is part of
             a load-more partial response (so it swaps itself in place).
        total: When non-zero, forwarded as &total=N in the URL so the
               partial handler can skip re-running get_lists_meta().
    """
    href = f"{url}?offset={next_offset}"
    if total:
        href += f"&total={total}"

    return Div(
        Button(
            UkIcon("chevrons-down", cls="size-4"),
            label,
            hx_get=href,
            hx_target=f"#{target_id}",
            hx_swap="beforeend",
            hx_indicator=f"#{target_id}-spinner",
            hx_disabled_elt="this",
            cls="flex items-center gap-2 text-sm font-medium px-4 py-2 rounded-lg border border-border bg-background hover:bg-accent transition-colors",
        ),
        Span(id=f"{target_id}-spinner", cls="htmx-indicator"),
        id=f"{target_id}-load-more",
        hx_swap_oob="true" if oob else None,
        cls="flex justify-center mt-4",
    )


def _page_based_load_more_button(
    endpoint_url: str,
    section_type: str,
    page: int,
    total_pages: int,
) -> Div:
    """
    HTMX "Load More" button for page-based detail views (country/category/language).

    Generates a button that fetches the next page and appends rows to a list,
    or an empty out-of-band placeholder when at the last page.

    Args:
        endpoint_url: Full URL with {section_type} (e.g., "/lists/country/US/more")
        section_type: "country", "category", or "language" — used for ID names
        page: Current page number (1-based)
        total_pages: Total number of pages

    Returns:
        Div with load-more button or empty OOB placeholder
    """
    list_id = f"{section_type}-creators-list"
    btn_id = f"{section_type}-load-more-btn"

    return (
        Div(
            Button(
                "Load More",
                hx_get=f"{endpoint_url}?page={page + 1}",
                hx_target=f"#{list_id}",
                hx_swap="beforeend",
                cls="w-full px-4 py-2 rounded-lg border border-border bg-background hover:bg-accent transition-colors",
            ),
            id=btn_id,
            hx_swap_oob="true",
            cls="mt-8 text-center",
        )
        if page < total_pages
        else Div(id=btn_id, hx_swap_oob="true")
    )


# ─────────────────────────────────────────────────────────────────────────────
# Tab Content Renderers
# ─────────────────────────────────────────────────────────────────────────────


def _render_top_rated_content(
    creators: list[dict],
    description: str,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> Div:
    """Renders Top Rated tab with quality-sorted creators."""
    if not creators:
        return _placeholder_content(description, coming_soon=False)

    return Div(
        # Description + count + heart
        DivFullySpaced(
            P(description, cls="text-sm text-muted-foreground max-w-xl"),
            Div(
                Span(
                    f"{len(creators)} creators",
                    cls="text-sm font-medium text-foreground",
                ),
                _list_heart_btn(
                    "top-rated",
                    "🏆 Top Rated",
                    "/lists?tab=top-rated",
                    "top-rated" in fav_keys,
                    authenticated=authenticated,
                ),
                cls="shrink-0 hidden sm:flex items-center gap-2",
            ),
            cls="mb-6 gap-4 flex-col sm:flex-row items-start sm:items-center",
        ),
        # Creator rows
        Div(
            *[_creator_row(creator, rank=i + 1) for i, creator in enumerate(creators)],
            cls="space-y-3",
        ),
        cls="min-h-64",
    )


def _render_most_active_content(
    creators: list[dict],
    description: str,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> Div:
    """Renders Most Active tab with upload frequency."""
    if not creators:
        return _placeholder_content(description, coming_soon=False)

    return Div(
        DivFullySpaced(
            P(description, cls="text-sm text-muted-foreground max-w-xl"),
            Div(
                Span(
                    f"{len(creators)} creators",
                    cls="text-sm font-medium text-foreground",
                ),
                _list_heart_btn(
                    "most-active",
                    "⚡ Most Active",
                    "/lists?tab=most-active",
                    "most-active" in fav_keys,
                    authenticated=authenticated,
                ),
                cls="shrink-0 hidden sm:flex items-center gap-2",
            ),
            cls="mb-6 gap-4 flex-col sm:flex-row items-start sm:items-center",
        ),
        Div(
            *[
                _creator_row(creator, rank=i + 1, show_activity=True)
                for i, creator in enumerate(creators)
            ],
            cls="space-y-3",
        ),
        cls="min-h-64",
    )


def _render_by_country_content(
    country_rankings: list[dict],
    description: str,
    total_countries: int = 0,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> Div:
    """
    Renders By Country tab with grouped country sections.

    Shows the initial batch of country cards plus a dynamic load-more button
    when more countries exist in the database.

    Args:
        country_rankings: First page of {country_code, count, creators} dicts
        description: Tab description text
        total_countries: Total distinct countries in DB (for load-more badge)
    """
    if not country_rankings:
        return _placeholder_content(description, coming_soon=False)

    shown = len(country_rankings)
    has_more = total_countries > shown

    return Div(
        DivFullySpaced(
            P(description, cls="text-sm text-muted-foreground max-w-xl"),
            Div(
                A(
                    (f"{total_countries} countries" if total_countries else f"{shown} countries"),
                    href="/lists/countries",
                    cls="text-sm font-medium text-primary hover:underline no-underline",
                    title="Explore all countries",
                ),
                cls="shrink-0 hidden sm:block",
            ),
            cls="mb-6 gap-4 flex-col sm:flex-row items-start sm:items-center",
        ),
        # Country cards grid — HTMX load-more appends into this div
        Div(
            *[
                _country_group_card(c, fav_keys=fav_keys, authenticated=authenticated)
                for c in country_rankings
            ],
            id="country-groups-grid",
            cls="grid grid-cols-1 md:grid-cols-2 gap-4",
        ),
        # Load-more button (only shown when more exist)
        (
            _load_more_button(
                url="/lists/more-countries",
                target_id="country-groups-grid",
                next_offset=shown,
                label=f"Show more countries ({total_countries - shown} remaining)",
                total=total_countries,
            )
            if has_more
            else None
        ),
        cls="min-h-64",
    )


def _render_by_category_content(
    category_rankings: list[dict],
    description: str,
    total_categories: int = 0,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> Div:
    """
    Renders By Category tab with grouped category sections.

    Shows the initial batch of category cards plus a dynamic load-more button
    when more categories exist in the database.

    Args:
        category_rankings: First page of {category, count, creators} dicts
        description: Tab description text
        total_categories: Total distinct categories in DB (for load-more badge)
    """
    if not category_rankings:
        return _placeholder_content(description, coming_soon=False)

    shown = len(category_rankings)
    has_more = total_categories > shown

    return Div(
        DivFullySpaced(
            P(description, cls="text-sm text-muted-foreground max-w-xl"),
            Div(
                A(
                    (
                        f"{total_categories} categories"
                        if total_categories
                        else f"{shown} categories"
                    ),
                    href="/lists/categories",
                    cls="text-sm font-medium text-primary hover:underline no-underline",
                    title="Explore all categories",
                ),
                cls="shrink-0 hidden sm:block",
            ),
            cls="mb-6 gap-4 flex-col sm:flex-row items-start sm:items-center",
        ),
        # Category cards grid — HTMX load-more appends into this div
        Div(
            *[
                _category_group_card(c, fav_keys=fav_keys, authenticated=authenticated)
                for c in category_rankings
            ],
            id="category-groups-grid",
            cls="grid grid-cols-1 md:grid-cols-2 gap-4",
        ),
        # Load-more button (only shown when more exist)
        (
            _load_more_button(
                url="/lists/more-categories",
                target_id="category-groups-grid",
                next_offset=shown,
                label=f"Show more categories ({total_categories - shown} remaining)",
                total=total_categories,
            )
            if has_more
            else None
        ),
        cls="min-h-64",
    )


def _render_by_language_content(
    language_rankings: list[dict],
    description: str,
    total_languages: int = 0,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> Div:
    """
    Renders By Language tab with grouped language sections.

    Shows the initial batch of language cards plus a dynamic load-more button
    when more languages exist in the database.

    Args:
        language_rankings: First page of {language_code, count, creators} dicts
        description: Tab description text
        total_languages: Total distinct languages in DB (for load-more badge)
    """
    if not language_rankings:
        return _placeholder_content(description, coming_soon=False)

    shown = len(language_rankings)
    has_more = total_languages > shown

    return Div(
        DivFullySpaced(
            P(description, cls="text-sm text-muted-foreground max-w-xl"),
            Div(
                A(
                    (f"{total_languages} languages" if total_languages else f"{shown} languages"),
                    href="/lists/languages",
                    cls="text-sm font-medium text-primary hover:underline no-underline",
                    title="Explore all languages",
                ),
                cls="shrink-0 hidden sm:block",
            ),
            cls="mb-6 gap-4 flex-col sm:flex-row items-start sm:items-center",
        ),
        # Language cards grid — HTMX load-more appends into this div
        Div(
            *[
                _language_group_card(c, fav_keys=fav_keys, authenticated=authenticated)
                for c in language_rankings
            ],
            id="language-groups-grid",
            cls="grid grid-cols-1 md:grid-cols-2 gap-4",
        ),
        # Load-more button (only shown when more exist)
        (
            _load_more_button(
                url="/lists/more-languages",
                target_id="language-groups-grid",
                next_offset=shown,
                label=f"Show more languages ({total_languages - shown} remaining)",
                total=total_languages,
            )
            if has_more
            else None
        ),
        cls="min-h-64",
    )


def _render_rising_content(
    creators: list[dict],
    description: str,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> Div:
    """Renders Rising Stars tab with growth metrics."""
    if not creators:
        return _placeholder_content(description, coming_soon=False)

    return Div(
        DivFullySpaced(
            P(description, cls="text-sm text-muted-foreground max-w-xl"),
            Div(
                Span(
                    f"{len(creators)} creators",
                    cls="text-sm font-medium text-foreground",
                ),
                _list_heart_btn(
                    "rising",
                    "🚀 Rising Stars",
                    "/lists?tab=rising",
                    "rising" in fav_keys,
                    authenticated=authenticated,
                ),
                cls="shrink-0 hidden sm:flex items-center gap-2",
            ),
            cls="mb-6 gap-4 flex-col sm:flex-row items-start sm:items-center",
        ),
        Div(
            *[
                _creator_row(creator, rank=i + 1, show_growth=True)
                for i, creator in enumerate(creators)
            ],
            cls="space-y-3",
        ),
        cls="min-h-64",
    )


def _render_veterans_content(
    creators: list[dict],
    description: str,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> Div:
    """Renders Veterans tab with channel age."""
    if not creators:
        return _placeholder_content(description, coming_soon=False)

    return Div(
        DivFullySpaced(
            P(description, cls="text-sm text-muted-foreground max-w-xl"),
            Div(
                Span(
                    f"{len(creators)} creators",
                    cls="text-sm font-medium text-foreground",
                ),
                _list_heart_btn(
                    "veterans",
                    "🏅 Veterans",
                    "/lists?tab=veterans",
                    "veterans" in fav_keys,
                    authenticated=authenticated,
                ),
                cls="shrink-0 hidden sm:flex items-center gap-2",
            ),
            cls="mb-6 gap-4 flex-col sm:flex-row items-start sm:items-center",
        ),
        Div(
            *[_creator_row(creator, rank=i + 1) for i, creator in enumerate(creators)],
            cls="space-y-3",
        ),
        cls="min-h-64",
    )


def _render_new_channels_content(
    creators: list[dict],
    description: str,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> Div:
    """Renders New Channels tab — channels created within the last year."""
    if not creators:
        return _placeholder_content(description, coming_soon=False)

    return Div(
        DivFullySpaced(
            P(description, cls="text-sm text-muted-foreground max-w-xl"),
            Div(
                Span(
                    f"{len(creators)} creators",
                    cls="text-sm font-medium text-foreground",
                ),
                _list_heart_btn(
                    "new-channels",
                    "✨ New Channels",
                    "/lists?tab=new-channels",
                    "new-channels" in fav_keys,
                    authenticated=authenticated,
                ),
                cls="shrink-0 hidden sm:flex items-center gap-2",
            ),
            cls="mb-6 gap-4 flex-col sm:flex-row items-start sm:items-center",
        ),
        Div(
            *[
                _creator_row(creator, rank=i + 1, show_activity=True)
                for i, creator in enumerate(creators)
            ],
            cls="space-y-3",
        ),
        cls="min-h-64",
    )


def _placeholder_content(description: str, coming_soon: bool = False):
    """Renders the placeholder body for a single tab panel."""

    if coming_soon:
        return Div(
            Div(
                UkIcon("clock", cls="size-10 text-muted-foreground mb-3"),
                H3("Coming Soon", cls="text-lg font-semibold text-foreground mb-1"),
                P(
                    description,
                    cls="text-sm text-muted-foreground max-w-sm text-center",
                ),
                cls="flex flex-col items-center justify-center py-20 px-6",
            ),
            cls="min-h-64",
        )

    return Div(
        # Top bar — filter hint + count placeholder
        DivFullySpaced(
            P(
                description,
                cls="text-sm text-muted-foreground max-w-xl",
            ),
            Div(
                Span("— creators", cls="text-sm font-medium text-foreground"),
                cls="shrink-0 hidden sm:block",
            ),
            cls="mb-6 gap-4 flex-col sm:flex-row items-start sm:items-center",
        ),
        # Skeleton rows — 8 placeholder cards
        Div(
            *[_skeleton_row(i) for i in range(8)],
            cls="space-y-3",
        ),
        cls="min-h-64",
    )


def _skeleton_row(index: int):
    """A single animated skeleton placeholder for a creator row."""
    # Stagger opacity slightly so they don't pulse in perfect unison
    opacity = (
        "opacity-100" if index % 3 == 0 else ("opacity-80" if index % 3 == 1 else "opacity-60")
    )
    return Div(
        # Rank number
        Div(
            Span(str(index + 1), cls="text-sm font-bold text-muted-foreground"),
            cls="w-8 shrink-0 text-center",
        ),
        # Avatar skeleton
        Div(cls="size-10 rounded-full bg-muted animate-pulse shrink-0"),
        # Name + meta skeletons
        Div(
            Div(cls="h-4 bg-muted animate-pulse rounded w-36 mb-1.5"),
            Div(cls="h-3 bg-muted animate-pulse rounded w-24"),
            cls="flex-1 min-w-0",
        ),
        # Stats skeletons (hidden on small screens)
        Div(
            Div(cls="h-4 bg-muted animate-pulse rounded w-16"),
            cls="hidden sm:block shrink-0",
        ),
        Div(
            Div(cls="h-4 bg-muted animate-pulse rounded w-12"),
            cls="hidden md:block shrink-0",
        ),
        # Grade badge skeleton
        Div(cls="h-6 bg-muted animate-pulse rounded-full w-8 shrink-0"),
        cls=f"flex items-center gap-3 p-3 rounded-xl border border-border bg-background {opacity}",
    )


def _tab_li(tab_id: str, label: str, icon: str, is_active: bool, badge: str = ""):
    """Renders a single tab <li> with icon + label and optional count badge."""
    active_cls = "uk-active" if is_active else ""
    return Li(
        A(
            UkIcon(icon, cls="size-4 shrink-0"),
            Span(label, cls="hidden sm:inline"),
            Span(label[0], cls="sm:hidden font-medium"),  # Initial on mobile
            # Live count badge (e.g. "24" countries)
            (
                Span(
                    badge,
                    cls="hidden sm:inline text-xs font-semibold bg-muted text-muted-foreground px-1.5 py-0.5 rounded-full ml-0.5",
                )
                if badge
                else None
            ),
            href="#",
            cls="flex items-center gap-1.5 text-sm font-medium",
        ),
        cls=active_cls,
    )


# ─────────────────────────────────────────────────────────────────────────────
# HTMX Partial Renderers — called by routes/lists.py partial handlers
# ─────────────────────────────────────────────────────────────────────────────


def render_more_countries(
    groups: list[dict],
    next_offset: int,
    has_more: bool,
    total: int = 0,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> FT:
    """
    HTMX partial response for load-more countries.

    Returns:
        - New country group cards (appended into #country-groups-grid by hx-swap=beforeend)
        - An oob-swap to replace the old load-more button div
    """
    cards = [_country_group_card(g, fav_keys=fav_keys, authenticated=authenticated) for g in groups]

    # Build the replacement load-more element; oob=True makes the element
    # carry hx-swap-oob itself so no extra wrapper div is needed.
    new_button = (
        _load_more_button(
            url="/lists/more-countries",
            target_id="country-groups-grid",
            next_offset=next_offset,
            label="Show more countries",
            oob=True,
            total=total,
        )
        if has_more
        else Div(id="country-groups-grid-load-more", hx_swap_oob="true")  # clears the button
    )

    return (*cards, new_button)


def render_more_categories(
    groups: list[dict],
    next_offset: int,
    has_more: bool,
    total: int = 0,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> FT:
    """
    HTMX partial response for load-more categories.

    Returns:
        - New category group cards (appended into #category-groups-grid)
        - An oob-swap to replace the old load-more button div
    """
    cards = [
        _category_group_card(g, fav_keys=fav_keys, authenticated=authenticated) for g in groups
    ]

    new_button = (
        _load_more_button(
            url="/lists/more-categories",
            target_id="category-groups-grid",
            next_offset=next_offset,
            label="Show more categories",
            oob=True,
            total=total,
        )
        if has_more
        else Div(id="category-groups-grid-load-more", hx_swap_oob="true")  # clears the button
    )

    return (*cards, new_button)


def render_more_languages(
    groups: list[dict],
    next_offset: int,
    has_more: bool,
    total: int = 0,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> FT:
    """
    HTMX partial response for load-more languages.

    Returns:
        - New language group cards (appended into #language-groups-grid by hx-swap=beforeend)
        - An oob-swap to replace the old load-more button div
    """
    cards = [
        _language_group_card(g, fav_keys=fav_keys, authenticated=authenticated) for g in groups
    ]

    new_button = (
        _load_more_button(
            url="/lists/more-languages",
            target_id="language-groups-grid",
            next_offset=next_offset,
            label="Show more languages",
            oob=True,
            total=total,
        )
        if has_more
        else Div(id="language-groups-grid-load-more", hx_swap_oob="true")  # clears the button
    )

    return (*cards, new_button)


def render_lists_page(
    active_tab: str = "top-rated",
    tab_data: dict = None,
    fav_keys: frozenset = frozenset(),
    authenticated: bool = False,
) -> FT:
    """
    Renders the full /lists page with tabbed creator list navigation.
    Tabs use UIkit's switcher for zero-JS panel switching.

    Args:
        active_tab: Currently active tab ID
        tab_data: Dict with data for each tab type, including total_countries
                  and total_categories counts for dynamic badges + load-more
    """
    if tab_data is None:
        tab_data = {}

    total_countries = tab_data.get("total_countries", 0)
    total_categories = tab_data.get("total_categories", 0)
    total_languages = tab_data.get("total_languages", 0)

    # Dynamic badges driven by live DB counts
    tab_badges: dict[str, str] = {}
    if total_countries:
        tab_badges["by-country"] = str(total_countries)
    if total_categories:
        tab_badges["by-category"] = str(total_categories)
    if total_languages:
        tab_badges["by-language"] = str(total_languages)

    tab_items = []
    panel_items = []

    # Normalize active_tab to a known tab id; fall back to the first tab if invalid.
    valid_tab_ids = [tab_id for tab_id, *_ in LISTS_TABS]
    normalized_active_tab = active_tab if active_tab in valid_tab_ids else valid_tab_ids[0]

    for i, (tab_id, label, icon, description, coming_soon) in enumerate(LISTS_TABS):
        is_active = tab_id == normalized_active_tab
        badge = tab_badges.get(tab_id, "")
        tab_items.append(_tab_li(tab_id, label, icon, is_active, badge=badge))

        # Render appropriate content based on tab type
        if coming_soon:
            panel_items.append(Li(_placeholder_content(description, coming_soon=True)))
        elif tab_id == "top-rated":
            creators = tab_data.get("top_rated", [])
            panel_items.append(
                Li(_render_top_rated_content(creators, description, fav_keys, authenticated))
            )
        elif tab_id == "most-active":
            creators = tab_data.get("most_active", [])
            panel_items.append(
                Li(_render_most_active_content(creators, description, fav_keys, authenticated))
            )
        elif tab_id == "by-country":
            country_rankings = tab_data.get("country_rankings", [])
            panel_items.append(
                Li(
                    _render_by_country_content(
                        country_rankings,
                        description,
                        total_countries=total_countries,
                        fav_keys=fav_keys,
                        authenticated=authenticated,
                    )
                )
            )
        elif tab_id == "by-category":
            category_rankings = tab_data.get("category_rankings", [])
            panel_items.append(
                Li(
                    _render_by_category_content(
                        category_rankings,
                        description,
                        total_categories=total_categories,
                        fav_keys=fav_keys,
                        authenticated=authenticated,
                    )
                )
            )
        elif tab_id == "by-language":
            language_rankings = tab_data.get("language_rankings", [])
            panel_items.append(
                Li(
                    _render_by_language_content(
                        language_rankings,
                        description,
                        total_languages=total_languages,
                        fav_keys=fav_keys,
                        authenticated=authenticated,
                    )
                )
            )
        elif tab_id == "rising":
            creators = tab_data.get("rising", [])
            panel_items.append(
                Li(_render_rising_content(creators, description, fav_keys, authenticated))
            )
        elif tab_id == "veterans":
            creators = tab_data.get("veterans", [])
            panel_items.append(
                Li(_render_veterans_content(creators, description, fav_keys, authenticated))
            )
        elif tab_id == "new-channels":
            creators = tab_data.get("new_channels", [])
            panel_items.append(
                Li(_render_new_channels_content(creators, description, fav_keys, authenticated))
            )
        else:
            panel_items.append(Li(_placeholder_content(description, coming_soon=False)))

    return Div(
        # ── Page header ────────────────────────────────────────────────────────
        Div(
            H1(
                "Creator Lists",
                cls="text-2xl sm:text-3xl font-bold text-foreground",
            ),
            P(
                "Curated rankings of YouTube creators across different dimensions.",
                cls="text-muted-foreground mt-1 text-sm sm:text-base",
            ),
            cls="mb-6 pt-6",
        ),
        # ── Tab bar (horizontally scrollable on mobile) ────────────────────────
        Div(
            TabContainer(
                *tab_items,
                uk_switcher="connect: #lists-panels; animation: uk-animation-fade",
                alt=True,
                cls="flex-nowrap overflow-x-auto",
            ),
            cls="overflow-x-auto -mx-4 px-4 sm:mx-0 sm:px-0",
        ),
        # ── Tab panels ─────────────────────────────────────────────────────────
        Ul(
            *panel_items,
            id="lists-panels",
            cls="uk-switcher mt-6",
        ),
        cls="max-w-4xl mx-auto px-4 pb-16",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Detail Page Shared Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _empty_detail_state(message: str) -> Div:
    """Empty-state body for detail pages when no creators are found."""
    return Div(
        UkIcon("users", cls="size-10 text-muted-foreground mb-3"),
        P(message, cls="text-sm text-muted-foreground"),
        cls="mt-6 py-16 flex flex-col items-center justify-center text-center",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Country Detail Page View
# ─────────────────────────────────────────────────────────────────────────────


def render_country_detail_page(
    country_code: str,
    creators: list[dict],
    page: int = 1,
    total_pages: int = 1,
    total_count: int = 0,
    page_size: int = 20,
) -> Div:
    """
    Render the detailed country-wise creator rankings page.

    Shows all creators from a specific country with pagination support.

    Args:
        country_code: ISO 3166-1 alpha-2 country code
        creators: List of creator dicts for this page
        page: Current page number (1-based)
        total_pages: Total number of pages
        total_count: Total creator count for this country
        page_size: Number of creators per page (must match the route limit)

    Returns:
        Div component with full detail page
    """
    country_name = get_country_name(country_code)
    country_flag = get_country_flag(country_code) or "🌍"

    # Pagination info
    start_rank = (page - 1) * page_size + 1
    end_rank = min(page * page_size, total_count)

    return Div(
        # ── Page header ────────────────────────────────────────────────────
        Div(
            Div(
                Span(country_flag, cls="text-5xl mr-4"),
                Div(
                    H1(
                        country_name,
                        cls="text-3xl sm:text-4xl font-bold text-foreground",
                    ),
                    P(
                        f"{format_number(total_count)} creators from {country_name}",
                        cls="text-muted-foreground mt-1",
                    ),
                    cls="flex-1",
                ),
                cls="flex items-center gap-2 mb-6",
            ),
            # Back button
            A(
                "← Back to Lists",
                href="/lists?tab=by-country",
                cls="inline-block text-sm text-primary hover:underline",
            ),
            cls="pt-6 pb-4 border-b border-border",
        ),
        # ── Creator list ───────────────────────────────────────────────────
        (
            _empty_detail_state(f"No creators from {country_name} yet.")
            if not total_count
            else Div(
                P(
                    f"Showing {format_number(start_rank)}\u2013{format_number(end_rank)} of {format_number(total_count)} creators",
                    cls="text-sm text-muted-foreground mb-6",
                ),
                Div(
                    *[
                        _creator_row(creator, rank=start_rank + i)
                        for i, creator in enumerate(creators)
                    ],
                    id="country-creators-list",
                    cls="space-y-3",
                ),
                cls="mt-6",
            )
        ),
        # ── Load-more pagination ───────────────────────────────────────────
        (
            Div(
                Button(
                    "Load More Creators",
                    hx_get=f"/lists/country/{country_code}/more?page={page + 1}",
                    hx_target="#country-creators-list",
                    hx_swap="beforeend",
                    cls="w-full px-4 py-2 rounded-lg border border-border bg-background hover:bg-accent transition-colors",
                ),
                id="country-load-more-btn",
                cls="mt-8 text-center",
            )
            if page < total_pages
            else None
        ),
        cls="max-w-4xl mx-auto px-4 pb-16",
    )


def render_country_creators_rows(
    country_code: str,
    creators: list[dict],
    page: int = 1,
    total_pages: int = 1,
    total_count: int = 0,
    page_size: int = 20,
) -> Div:
    """
    Render just the creator rows for the HTMX load-more endpoint.

    This is used by the /lists/country/{country_code}/more endpoint
    to return new creator rows without page headers/footers.

    Args:
        country_code: ISO 3166-1 alpha-2 country code
        creators: List of creator dicts for this page
        page: Current page number (1-based)
        total_pages: Total number of pages
        total_count: Total creator count (unused here, for signature consistency)
        page_size: Number of creators per page (must match the route limit)

    Returns:
        Tuple of creator row components and load-more button
    """
    start_rank = (page - 1) * page_size + 1
    rows = [_creator_row(creator, rank=start_rank + i) for i, creator in enumerate(creators)]
    next_btn = _page_based_load_more_button(
        endpoint_url=f"/lists/country/{country_code}/more",
        section_type="country",
        page=page,
        total_pages=total_pages,
    )
    return (*rows, next_btn)


# ─────────────────────────────────────────────────────────────────────────────
# Category Detail Page View
# ─────────────────────────────────────────────────────────────────────────────


def render_category_detail_page(
    category_slug: str,
    creators: list[dict],
    page: int = 1,
    total_pages: int = 1,
    total_count: int = 0,
    page_size: int = 20,
) -> Div:
    """
    Render the detailed category-wise creator rankings page.

    Shows all creators in a specific topic category with pagination support.

    Args:
        category_slug: URL slug derived from the category display name
        creators: List of creator dicts for this page
        page: Current page number (1-based)
        total_pages: Total number of pages
        total_count: Total creator count for this category
        page_size: Number of creators per page (must match the route limit)

    Returns:
        Div component with full detail page
    """
    display_name = _unslugify(category_slug).title()
    emoji = get_topic_category_emoji(display_name)

    start_rank = (page - 1) * page_size + 1
    end_rank = min(page * page_size, total_count)

    return Div(
        # ── Page header ────────────────────────────────────────────────────
        Div(
            Div(
                Span(emoji, cls="text-5xl mr-4"),
                Div(
                    H1(
                        display_name,
                        cls="text-3xl sm:text-4xl font-bold text-foreground",
                    ),
                    P(
                        f"{format_number(total_count)} creators in {display_name}",
                        cls="text-muted-foreground mt-1",
                    ),
                    cls="flex-1",
                ),
                cls="flex items-center gap-2 mb-6",
            ),
            # Back button
            A(
                "← Back to Lists",
                href="/lists?tab=by-category",
                cls="inline-block text-sm text-primary hover:underline",
            ),
            cls="pt-6 pb-4 border-b border-border",
        ),
        # ── Creator list ───────────────────────────────────────────────────
        (
            _empty_detail_state(f"No creators in {display_name} yet.")
            if not total_count
            else Div(
                P(
                    f"Showing {format_number(start_rank)}\u2013{format_number(end_rank)} of {format_number(total_count)} creators",
                    cls="text-sm text-muted-foreground mb-6",
                ),
                Div(
                    *[
                        _creator_row(creator, rank=start_rank + i)
                        for i, creator in enumerate(creators)
                    ],
                    id="category-creators-list",
                    cls="space-y-3",
                ),
                cls="mt-6",
            )
        ),
        # ── Load-more pagination ───────────────────────────────────────────
        (
            Div(
                Button(
                    "Load More Creators",
                    hx_get=f"/lists/category/{category_slug}/more?page={page + 1}",
                    hx_target="#category-creators-list",
                    hx_swap="beforeend",
                    cls="w-full px-4 py-2 rounded-lg border border-border bg-background hover:bg-accent transition-colors",
                ),
                id="category-load-more-btn",
                cls="mt-8 text-center",
            )
            if page < total_pages
            else None
        ),
        cls="max-w-4xl mx-auto px-4 pb-16",
    )


def render_category_creators_rows(
    category_slug: str,
    creators: list[dict],
    page: int = 1,
    total_pages: int = 1,
    total_count: int = 0,
    page_size: int = 20,
) -> Div:
    """
    Render just the creator rows for the HTMX load-more endpoint.

    Used by /lists/category/{category_slug}/more to return creator rows
    without page headers/footers.

    Args:
        category_slug: URL slug for the category
        creators: List of creator dicts for this page
        page: Current page number (1-based)
        total_pages: Total number of pages
        total_count: Total creator count (unused here, for signature consistency)
        page_size: Number of creators per page (must match the route limit)

    Returns:
        Tuple of creator row components and load-more button
    """
    start_rank = (page - 1) * page_size + 1
    rows = [_creator_row(creator, rank=start_rank + i) for i, creator in enumerate(creators)]
    next_btn = _page_based_load_more_button(
        endpoint_url=f"/lists/category/{category_slug}/more",
        section_type="category",
        page=page,
        total_pages=total_pages,
    )
    return (*rows, next_btn)


# ─────────────────────────────────────────────────────────────────────────────
# Language Detail Page View
# ─────────────────────────────────────────────────────────────────────────────


def render_language_detail_page(
    language_code: str,
    creators: list[dict],
    page: int = 1,
    total_pages: int = 1,
    total_count: int = 0,
    page_size: int = 20,
) -> Div:
    """
    Render the detailed language-wise creator rankings page.

    Shows all creators whose primary content language matches *language_code*,
    with pagination support mirroring the country and category detail pages.

    Args:
        language_code: ISO 639-1 two-letter language code (e.g. ``"en"``, ``"ja"``).
        creators: List of creator dicts for this page.
        page: Current page number (1-based).
        total_pages: Total number of pages.
        total_count: Total creator count for this language.
        page_size: Number of creators per page (must match the route limit).

    Returns:
        Div component with full detail page.
    """
    language_name = get_language_name(language_code)
    emoji = get_language_emoji(language_code)

    start_rank = (page - 1) * page_size + 1
    end_rank = min(page * page_size, total_count)

    return Div(
        # ── Page header ──────────────────────────────────────────────────
        Div(
            Div(
                Span(emoji, cls="text-5xl mr-4"),
                Div(
                    H1(
                        language_name,
                        cls="text-3xl sm:text-4xl font-bold text-foreground",
                    ),
                    P(
                        f"{format_number(total_count)} {language_name}-language creators",
                        cls="text-muted-foreground mt-1",
                    ),
                    cls="flex-1",
                ),
                cls="flex items-center gap-2 mb-6",
            ),
            A(
                "← Back to Creators",
                href="/creators",
                cls="inline-block text-sm text-primary hover:underline",
            ),
            cls="pt-6 pb-4 border-b border-border",
        ),
        # ── Creator list ─────────────────────────────────────────────────
        (
            _empty_detail_state(f"No {language_name}-language creators yet.")
            if not total_count
            else Div(
                P(
                    f"Showing {format_number(start_rank)}\u2013{format_number(end_rank)} of {format_number(total_count)} creators",
                    cls="text-sm text-muted-foreground mb-6",
                ),
                Div(
                    *[
                        _creator_row(creator, rank=start_rank + i)
                        for i, creator in enumerate(creators)
                    ],
                    id="language-creators-list",
                    cls="space-y-3",
                ),
                cls="mt-6",
            )
        ),
        # ── Load-more pagination ─────────────────────────────────────────
        (
            Div(
                Button(
                    "Load More Creators",
                    hx_get=f"/lists/language/{language_code}/more?page={page + 1}",
                    hx_target="#language-creators-list",
                    hx_swap="beforeend",
                    cls="w-full px-4 py-2 rounded-lg border border-border bg-background hover:bg-accent transition-colors",
                ),
                id="language-load-more-btn",
                cls="mt-8 text-center",
            )
            if page < total_pages
            else None
        ),
        cls="max-w-4xl mx-auto px-4 pb-16",
    )


def render_language_creators_rows(
    language_code: str,
    creators: list[dict],
    page: int = 1,
    total_pages: int = 1,
    total_count: int = 0,
    page_size: int = 20,
) -> Div:
    """
    Render just the creator rows for the HTMX load-more endpoint.

    Used by /lists/language/{language_code}/more to return creator rows
    without page headers/footers.

    Args:
        language_code: ISO 639-1 two-letter language code.
        creators: List of creator dicts for this page.
        page: Current page number (1-based).
        total_pages: Total number of pages.
        total_count: Total creator count (unused, for signature consistency).
        page_size: Number of creators per page (must match the route limit).

    Returns:
        Tuple of creator row components and load-more button.
    """
    start_rank = (page - 1) * page_size + 1
    rows = [_creator_row(creator, rank=start_rank + i) for i, creator in enumerate(creators)]
    next_btn = _page_based_load_more_button(
        endpoint_url=f"/lists/language/{language_code}/more",
        section_type="language",
        page=page,
        total_pages=total_pages,
    )
    return (*rows, next_btn)


# ─────────────────────────────────────────────────────────────────────────────
# Category Explorer — /lists/categories
# ─────────────────────────────────────────────────────────────────────────────


def render_categories_explorer_page(
    categories: list[tuple[str, int]],
) -> Div:
    """
    Full-page bar-chart explorer for all categories in the creator database.

    Each row is a clickable horizontal bar proportional to creator count,
    linking to the existing /lists/category/{slug} detail page.
    Client-side JS handles search filtering and sort toggling with no
    extra round-trips.

    Args:
        categories: List of (category_name, creator_count) tuples,
                    sorted by count descending (from get_top_categories_with_counts).
    """
    if not categories:
        return Div(
            P("No categories found.", cls="text-muted-foreground text-center py-16"),
        )

    # Guard against zero max_count to prevent division by zero
    max_count = max((c for _, c in categories), default=1)
    total_creators = sum(c for _, c in categories)

    # ── Header ───────────────────────────────────────────────────────
    header = Div(
        A(
            UkIcon("arrow-left", cls="w-4 h-4 mr-1.5"),
            "Back to Lists",
            href="/lists?tab=by-category",
            cls="inline-flex items-center text-sm font-medium text-muted-foreground hover:text-foreground no-underline transition-colors",
        ),
        Div(
            Div(
                UkIcon("grid", cls="w-5 h-5 text-pink-500 mr-2"),
                H1("Category Explorer", cls="text-2xl font-bold text-foreground"),
                cls="flex items-center",
            ),
            P(
                f"{len(categories)} content categories · {format_number(total_creators)} creator tags across the database",
                cls="text-sm text-muted-foreground mt-1",
            ),
            cls="mt-4 mb-6",
        ),
        cls="mb-2",
    )

    # ── Search + sort controls ─────────────────────────────────────────
    controls = Div(
        Input(
            type="search",
            id="cat-search",
            placeholder="Filter categories…",
            cls="flex-1 px-4 py-2 rounded-lg border border-border bg-background text-sm focus:outline-none focus:ring-2 focus:ring-pink-300",
            oninput="filterCategories()",
            autocomplete="off",
        ),
        Div(
            Button(
                UkIcon("arrow-down-wide-narrow", cls="w-4 h-4 mr-1"),
                "By count",
                id="sort-count-btn",
                type="button",
                onclick="sortCategories('count')",
                cls="sort-btn is-active inline-flex items-center px-3 py-2 rounded-lg text-xs font-semibold border border-border bg-background text-muted-foreground hover:bg-accent transition-colors",
            ),
            Button(
                UkIcon("arrow-down-a-z", cls="w-4 h-4 mr-1"),
                "A → Z",
                id="sort-alpha-btn",
                type="button",
                onclick="sortCategories('alpha')",
                cls="sort-btn inline-flex items-center px-3 py-2 rounded-lg text-xs font-semibold border border-border bg-background text-muted-foreground hover:bg-accent transition-colors",
            ),
            cls="flex gap-2 shrink-0",
        ),
        cls="flex gap-3 items-center mb-6",
    )

    # ── Bar rows ──────────────────────────────────────────────────────────
    # Palette cycles through 5 accent colours
    bar_colours = [
        "bg-pink-400 dark:bg-pink-500",
        "bg-violet-400 dark:bg-violet-500",
        "bg-blue-400 dark:bg-blue-500",
        "bg-emerald-400 dark:bg-emerald-500",
        "bg-amber-400 dark:bg-amber-500",
    ]

    bar_rows = []
    for i, (cat_name, count) in enumerate(categories):
        pct = round(count / max_count * 100)
        emoji = get_topic_category_emoji(cat_name)
        slug = quote(cat_name, safe="")
        bar_cls = bar_colours[i % len(bar_colours)]

        bar_rows.append(
            A(
                # Label column
                Div(
                    Span(emoji, cls="text-base w-6 shrink-0 text-center"),
                    Span(
                        cat_name,
                        cls="text-sm font-medium text-foreground truncate cat-name",
                    ),
                    cls="flex items-center gap-2 w-44 sm:w-56 shrink-0",
                ),
                # Bar
                Div(
                    Div(
                        cls=f"h-full {bar_cls} rounded-r-full transition-all duration-300",
                        style=f"width:{pct}%",
                    ),
                    cls="flex-1 h-5 bg-accent rounded-full overflow-hidden",
                ),
                # Count
                Span(
                    format_number(count),
                    cls="text-xs font-semibold text-muted-foreground w-14 text-right shrink-0",
                ),
                href=f"/lists/category/{slug}",
                # data-name for JS search, data-count for JS sort
                data_name=cat_name.lower(),
                data_count=str(count),
                cls="cat-row flex items-center gap-3 px-3 py-2 rounded-lg hover:bg-accent transition-colors no-underline group",
            )
        )

    chart = Div(
        *bar_rows,
        id="cat-chart",
        cls="space-y-1",
    )

    # ── Client-side search + sort (no round-trip) ─────────────────────────
    js = Script(
        """
    function filterCategories() {
        const q = document.getElementById('cat-search').value.toLowerCase();
        document.querySelectorAll('.cat-row').forEach(row => {
            row.style.display = row.dataset.name.includes(q) ? '' : 'none';
        });
    }
    function sortCategories(mode) {
        const chart = document.getElementById('cat-chart');
        const rows  = Array.from(chart.querySelectorAll('.cat-row'));
        rows.sort((a, b) =>
            mode === 'alpha'
                ? a.dataset.name.localeCompare(b.dataset.name)
                : parseInt(b.dataset.count) - parseInt(a.dataset.count)
        );
        rows.forEach(r => chart.appendChild(r));
        document.querySelectorAll('.sort-btn').forEach(btn => btn.classList.remove('is-active'));
        document.getElementById(mode === 'alpha' ? 'sort-alpha-btn' : 'sort-count-btn')
                .classList.add('is-active');
    }
    """
    )

    style_tag = Style(
        """
.sort-btn.is-active {
    background: rgb(252 231 243);
    color: rgb(190 24 93);
    border-color: rgb(249 168 212);
}
.dark .sort-btn.is-active {
    background: rgba(131, 24, 67, 0.4);
    color: rgb(249 168 212);
    border-color: rgb(249 168 212);
}
"""
    )

    return Div(
        style_tag,
        header,
        controls,
        Card(chart, body_cls="p-4"),
        js,
        cls="max-w-3xl mx-auto px-4 py-8",
    )


def render_countries_explorer_page(
    countries: list[tuple[str, int]],
) -> Div:
    """
    Full-page bar-chart explorer for all countries in the creator database.

    Each row is a clickable horizontal bar proportional to creator count,
    with country flag emoji and 2-letter code,
    linking to the existing /lists/country/{code} detail page.
    Client-side JS handles search filtering and sort toggling with no
    extra round-trips.

    Args:
        countries: List of (country_code, creator_count) tuples,
                   sorted by count descending (from get_top_countries_with_counts).
    """
    if not countries:
        return Div(
            P("No countries found.", cls="text-muted-foreground text-center py-16"),
        )

    # Guard against zero max_count to prevent division by zero
    max_count = max((c for _, c in countries), default=1)
    total_creators = sum(c for _, c in countries)

    # ── Header ───────────────────────────────────────────────────────
    header = Div(
        A(
            UkIcon("arrow-left", cls="w-4 h-4 mr-1.5"),
            "Back to Lists",
            href="/lists?tab=by-country",
            cls="inline-flex items-center text-sm font-medium text-muted-foreground hover:text-foreground no-underline transition-colors",
        ),
        Div(
            Div(
                UkIcon("globe", cls="w-5 h-5 text-blue-500 mr-2"),
                H1("Country Explorer", cls="text-2xl font-bold text-foreground"),
                cls="flex items-center",
            ),
            P(
                f"{len(countries)} countries · {format_number(total_creators)} creators across the world",
                cls="text-sm text-muted-foreground mt-1",
            ),
            cls="mt-4 mb-6",
        ),
        cls="mb-2",
    )

    # ── Search + sort controls ─────────────────────────────────────────
    controls = Div(
        Input(
            type="search",
            id="country-search",
            placeholder="Filter countries…",
            cls="flex-1 px-4 py-2 rounded-lg border border-border bg-background text-sm focus:outline-none focus:ring-2 focus:ring-blue-300",
            oninput="filterCountries()",
            autocomplete="off",
        ),
        Div(
            Button(
                UkIcon("arrow-down-wide-narrow", cls="w-4 h-4 mr-1"),
                "By count",
                id="sort-count-btn",
                type="button",
                onclick="sortCountries('count')",
                cls="sort-btn is-active inline-flex items-center px-3 py-2 rounded-lg text-xs font-semibold border border-border bg-background text-muted-foreground hover:bg-accent transition-colors",
            ),
            Button(
                UkIcon("arrow-down-a-z", cls="w-4 h-4 mr-1"),
                "A → Z",
                id="sort-alpha-btn",
                type="button",
                onclick="sortCountries('alpha')",
                cls="sort-btn inline-flex items-center px-3 py-2 rounded-lg text-xs font-semibold border border-border bg-background text-muted-foreground hover:bg-accent transition-colors",
            ),
            cls="flex gap-2 shrink-0",
        ),
        cls="flex gap-3 items-center mb-6",
    )

    # ── Bar rows ──────────────────────────────────────────────────────────
    # Palette cycles through 5 accent colours
    bar_colours = [
        "bg-blue-400 dark:bg-blue-500",
        "bg-emerald-400 dark:bg-emerald-500",
        "bg-violet-400 dark:bg-violet-500",
        "bg-amber-400 dark:bg-amber-500",
        "bg-pink-400 dark:bg-pink-500",
    ]

    bar_rows = []
    for i, (country_code, count) in enumerate(countries):
        pct = round(count / max_count * 100)
        flag = get_country_flag_emoji(country_code)
        country_name = get_country_name(country_code)
        bar_cls = bar_colours[i % len(bar_colours)]

        bar_rows.append(
            A(
                # Label column
                Div(
                    Span(flag, cls="text-base w-6 shrink-0 text-center"),
                    Span(
                        country_code.upper(),
                        cls="text-xs font-mono font-semibold text-muted-foreground w-8 shrink-0",
                    ),
                    Span(
                        country_name,
                        cls="text-sm font-medium text-foreground truncate country-name",
                    ),
                    cls="flex items-center gap-2 w-56 sm:w-64 shrink-0",
                ),
                # Bar
                Div(
                    Div(
                        cls=f"h-full {bar_cls} rounded-r-full transition-all duration-300",
                        style=f"width:{pct}%",
                    ),
                    cls="flex-1 h-5 bg-accent rounded-full overflow-hidden",
                ),
                # Count
                Span(
                    format_number(count),
                    cls="text-xs font-semibold text-muted-foreground w-14 text-right shrink-0",
                ),
                href=f"/lists/country/{country_code.lower()}",
                # data-name for JS search, data-count for JS sort
                data_name=f"{country_code.lower()} {country_name.lower()}",
                data_count=str(count),
                cls="country-row flex items-center gap-3 px-3 py-2 rounded-lg hover:bg-accent transition-colors no-underline group",
            )
        )

    chart = Div(
        *bar_rows,
        id="country-chart",
        cls="space-y-1",
    )

    # ── Client-side search + sort (no round-trip) ─────────────────────────
    js = Script(
        """
    function filterCountries() {
        const q = document.getElementById('country-search').value.toLowerCase();
        document.querySelectorAll('.country-row').forEach(row => {
            row.style.display = row.dataset.name.includes(q) ? '' : 'none';
        });
    }
    function sortCountries(mode) {
        const chart = document.getElementById('country-chart');
        const rows  = Array.from(chart.querySelectorAll('.country-row'));
        rows.sort((a, b) =>
            mode === 'alpha'
                ? a.dataset.name.localeCompare(b.dataset.name)
                : parseInt(b.dataset.count) - parseInt(a.dataset.count)
        );
        rows.forEach(r => chart.appendChild(r));
        document.querySelectorAll('.sort-btn').forEach(btn => btn.classList.remove('is-active'));
        document.getElementById(mode === 'alpha' ? 'sort-alpha-btn' : 'sort-count-btn')
                .classList.add('is-active');
    }
    """
    )

    style_tag = Style(
        """
.sort-btn.is-active {
    background: rgb(219 234 254);
    color: rgb(29 78 216);
    border-color: rgb(147 197 253);
}
.dark .sort-btn.is-active {
    background: rgba(30, 58, 138, 0.4);
    color: rgb(147 197 253);
    border-color: rgb(147 197 253);
}
"""
    )

    return Div(
        style_tag,
        header,
        controls,
        Card(chart, body_cls="p-4"),
        js,
        cls="max-w-3xl mx-auto px-4 py-8",
    )


def render_languages_explorer_page(
    languages: list[tuple[str, int]],
) -> Div:
    """
    Full-page bar-chart explorer for all content languages in the creator database.

    Each row is a clickable horizontal bar proportional to creator count,
    with language emoji and two-letter code,
    linking to the existing /lists/language/{code} detail page.
    Client-side JS handles search filtering and sort toggling with no
    extra round-trips.

    Args:
        languages: List of (language_code, creator_count) tuples,
                   sorted by count descending (from get_top_languages_with_counts).
    """
    if not languages:
        return Div(
            P("No languages found.", cls="text-muted-foreground text-center py-16"),
        )

    max_count = max(1, max((c for _, c in languages), default=0))
    total_creators = sum(c for _, c in languages)

    # ── Header ───────────────────────────────────────────────────────
    header = Div(
        A(
            UkIcon("arrow-left", cls="w-4 h-4 mr-1.5"),
            "Back to Lists",
            href="/lists?tab=by-language",
            cls="inline-flex items-center text-sm font-medium text-muted-foreground hover:text-foreground no-underline transition-colors",
        ),
        Div(
            Div(
                UkIcon("languages", cls="w-5 h-5 text-emerald-500 mr-2"),
                H1("Language Explorer", cls="text-2xl font-bold text-foreground"),
                cls="flex items-center",
            ),
            P(
                f"{len(languages)} languages · {format_number(total_creators)} creators across the database",
                cls="text-sm text-muted-foreground mt-1",
            ),
            cls="mt-4 mb-6",
        ),
        cls="mb-2",
    )

    # ── Search + sort controls ─────────────────────────────────────────
    controls = Div(
        Input(
            type="search",
            id="lang-search",
            placeholder="Filter languages…",
            cls="flex-1 px-4 py-2 rounded-lg border border-border bg-background text-sm focus:outline-none focus:ring-2 focus:ring-emerald-300",
            oninput="filterLanguages()",
            autocomplete="off",
        ),
        Div(
            Button(
                UkIcon("arrow-down-wide-narrow", cls="w-4 h-4 mr-1"),
                "By count",
                id="sort-count-btn",
                type="button",
                onclick="sortLanguages('count')",
                cls="sort-btn is-active inline-flex items-center px-3 py-2 rounded-lg text-xs font-semibold border border-border bg-background text-muted-foreground hover:bg-accent transition-colors",
            ),
            Button(
                UkIcon("arrow-down-a-z", cls="w-4 h-4 mr-1"),
                "A → Z",
                id="sort-alpha-btn",
                type="button",
                onclick="sortLanguages('alpha')",
                cls="sort-btn inline-flex items-center px-3 py-2 rounded-lg text-xs font-semibold border border-border bg-background text-muted-foreground hover:bg-accent transition-colors",
            ),
            cls="flex gap-2 shrink-0",
        ),
        cls="flex gap-3 items-center mb-6",
    )

    bar_colours = [
        "bg-emerald-400 dark:bg-emerald-500",
        "bg-teal-400 dark:bg-teal-500",
        "bg-cyan-400 dark:bg-cyan-500",
        "bg-green-400 dark:bg-green-500",
        "bg-lime-400 dark:bg-lime-500",
    ]

    bar_rows = []
    for i, (language_code, count) in enumerate(languages):
        pct = round(count / max_count * 100)
        emoji = get_language_emoji(language_code)
        language_name = get_language_name(language_code)
        bar_cls = bar_colours[i % len(bar_colours)]

        bar_rows.append(
            A(
                # Label column
                Div(
                    Span(emoji, cls="text-base w-6 shrink-0 text-center"),
                    Span(
                        language_code.upper(),
                        cls="text-xs font-mono font-semibold text-muted-foreground w-8 shrink-0",
                    ),
                    Span(
                        language_name,
                        cls="text-sm font-medium text-foreground truncate lang-name",
                    ),
                    cls="flex items-center gap-2 w-56 sm:w-64 shrink-0",
                ),
                # Bar
                Div(
                    Div(
                        cls=f"h-full {bar_cls} rounded-r-full transition-all duration-300",
                        style=f"width:{pct}%",
                    ),
                    cls="flex-1 h-5 bg-accent rounded-full overflow-hidden",
                ),
                # Count
                Span(
                    format_number(count),
                    cls="text-xs font-semibold text-muted-foreground w-14 text-right shrink-0",
                ),
                href=f"/lists/language/{language_code.lower()}",
                data_name=f"{language_code.lower()} {(language_name or language_code).lower()}",
                data_count=str(count),
                cls="lang-row flex items-center gap-3 px-3 py-2 rounded-lg hover:bg-accent transition-colors no-underline group",
            )
        )

    chart = Div(
        *bar_rows,
        id="lang-chart",
        cls="space-y-1",
    )

    js = Script(
        """
    function filterLanguages() {
        const q = document.getElementById('lang-search').value.toLowerCase();
        document.querySelectorAll('.lang-row').forEach(row => {
            row.style.display = row.dataset.name.includes(q) ? '' : 'none';
        });
    }
    function sortLanguages(mode) {
        const chart = document.getElementById('lang-chart');
        const rows  = Array.from(chart.querySelectorAll('.lang-row'));
        rows.sort((a, b) =>
            mode === 'alpha'
                ? a.dataset.name.localeCompare(b.dataset.name)
                : parseInt(b.dataset.count) - parseInt(a.dataset.count)
        );
        rows.forEach(r => chart.appendChild(r));
        document.querySelectorAll('.sort-btn').forEach(btn => btn.classList.remove('is-active'));
        document.getElementById(mode === 'alpha' ? 'sort-alpha-btn' : 'sort-count-btn')
                .classList.add('is-active');
    }
    """
    )

    style_tag = Style(
        """
.sort-btn.is-active {
    background: rgb(209 250 229);
    color: rgb(6 95 70);
    border-color: rgb(110 231 183);
}
.dark .sort-btn.is-active {
    background: rgba(6, 78, 59, 0.4);
    color: rgb(110 231 183);
    border-color: rgb(110 231 183);
}
"""
    )

    return Div(
        style_tag,
        header,
        controls,
        Card(chart, body_cls="p-4"),
        js,
        cls="max-w-3xl mx-auto px-4 py-8",
    )
