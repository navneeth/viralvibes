"""
Creator Lists view — curated, pre-filtered creator rankings.
Each tab is a different lens on the creators database.
"""

import re

from fasthtml.common import *
from monsterui.all import *

from utils import format_number, safe_get_value
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
# ISO 3166-1 country code → display name
# ─────────────────────────────────────────────────────────────────────────────
_COUNTRY_NAMES: dict[str, str] = {
    "AD": "Andorra",
    "AE": "UAE",
    "AF": "Afghanistan",
    "AG": "Antigua & Barbuda",
    "AL": "Albania",
    "AM": "Armenia",
    "AO": "Angola",
    "AR": "Argentina",
    "AT": "Austria",
    "AU": "Australia",
    "AZ": "Azerbaijan",
    "BA": "Bosnia",
    "BB": "Barbados",
    "BD": "Bangladesh",
    "BE": "Belgium",
    "BF": "Burkina Faso",
    "BG": "Bulgaria",
    "BH": "Bahrain",
    "BI": "Burundi",
    "BJ": "Benin",
    "BN": "Brunei",
    "BO": "Bolivia",
    "BR": "Brazil",
    "BS": "Bahamas",
    "BT": "Bhutan",
    "BW": "Botswana",
    "BY": "Belarus",
    "BZ": "Belize",
    "CA": "Canada",
    "CD": "DR Congo",
    "CF": "C. African Rep.",
    "CG": "Congo",
    "CH": "Switzerland",
    "CI": "Côte d'Ivoire",
    "CL": "Chile",
    "CM": "Cameroon",
    "CN": "China",
    "CO": "Colombia",
    "CR": "Costa Rica",
    "CU": "Cuba",
    "CV": "Cape Verde",
    "CY": "Cyprus",
    "CZ": "Czech Republic",
    "DE": "Germany",
    "DJ": "Djibouti",
    "DK": "Denmark",
    "DM": "Dominica",
    "DO": "Dominican Rep.",
    "DZ": "Algeria",
    "EC": "Ecuador",
    "EE": "Estonia",
    "EG": "Egypt",
    "ER": "Eritrea",
    "ES": "Spain",
    "ET": "Ethiopia",
    "FI": "Finland",
    "FJ": "Fiji",
    "FR": "France",
    "GA": "Gabon",
    "GB": "United Kingdom",
    "GD": "Grenada",
    "GE": "Georgia",
    "GH": "Ghana",
    "GM": "Gambia",
    "GN": "Guinea",
    "GQ": "Equatorial Guinea",
    "GR": "Greece",
    "GT": "Guatemala",
    "GW": "Guinea-Bissau",
    "GY": "Guyana",
    "HN": "Honduras",
    "HR": "Croatia",
    "HT": "Haiti",
    "HU": "Hungary",
    "ID": "Indonesia",
    "IE": "Ireland",
    "IL": "Israel",
    "IN": "India",
    "IQ": "Iraq",
    "IR": "Iran",
    "IS": "Iceland",
    "IT": "Italy",
    "JM": "Jamaica",
    "JO": "Jordan",
    "JP": "Japan",
    "KE": "Kenya",
    "KG": "Kyrgyzstan",
    "KH": "Cambodia",
    "KI": "Kiribati",
    "KM": "Comoros",
    "KN": "St Kitts",
    "KP": "North Korea",
    "KR": "South Korea",
    "KW": "Kuwait",
    "KZ": "Kazakhstan",
    "LA": "Laos",
    "LB": "Lebanon",
    "LC": "St Lucia",
    "LI": "Liechtenstein",
    "LK": "Sri Lanka",
    "LR": "Liberia",
    "LS": "Lesotho",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "LV": "Latvia",
    "LY": "Libya",
    "MA": "Morocco",
    "MC": "Monaco",
    "MD": "Moldova",
    "ME": "Montenegro",
    "MG": "Madagascar",
    "MH": "Marshall Islands",
    "MK": "North Macedonia",
    "ML": "Mali",
    "MM": "Myanmar",
    "MN": "Mongolia",
    "MR": "Mauritania",
    "MT": "Malta",
    "MU": "Mauritius",
    "MV": "Maldives",
    "MW": "Malawi",
    "MX": "Mexico",
    "MY": "Malaysia",
    "MZ": "Mozambique",
    "NA": "Namibia",
    "NE": "Niger",
    "NG": "Nigeria",
    "NI": "Nicaragua",
    "NL": "Netherlands",
    "NO": "Norway",
    "NP": "Nepal",
    "NR": "Nauru",
    "NZ": "New Zealand",
    "OM": "Oman",
    "PA": "Panama",
    "PE": "Peru",
    "PG": "Papua New Guinea",
    "PH": "Philippines",
    "PK": "Pakistan",
    "PL": "Poland",
    "PT": "Portugal",
    "PW": "Palau",
    "PY": "Paraguay",
    "QA": "Qatar",
    "RO": "Romania",
    "RS": "Serbia",
    "RU": "Russia",
    "RW": "Rwanda",
    "SA": "Saudi Arabia",
    "SB": "Solomon Islands",
    "SC": "Seychelles",
    "SD": "Sudan",
    "SE": "Sweden",
    "SG": "Singapore",
    "SI": "Slovenia",
    "SK": "Slovakia",
    "SL": "Sierra Leone",
    "SM": "San Marino",
    "SN": "Senegal",
    "SO": "Somalia",
    "SR": "Suriname",
    "SS": "South Sudan",
    "ST": "São Tomé",
    "SV": "El Salvador",
    "SY": "Syria",
    "SZ": "Eswatini",
    "TD": "Chad",
    "TG": "Togo",
    "TH": "Thailand",
    "TJ": "Tajikistan",
    "TL": "Timor-Leste",
    "TM": "Turkmenistan",
    "TN": "Tunisia",
    "TO": "Tonga",
    "TR": "Turkey",
    "TT": "Trinidad & Tobago",
    "TV": "Tuvalu",
    "TZ": "Tanzania",
    "UA": "Ukraine",
    "UG": "Uganda",
    "US": "United States",
    "UY": "Uruguay",
    "UZ": "Uzbekistan",
    "VA": "Vatican",
    "VC": "St Vincent",
    "VE": "Venezuela",
    "VN": "Vietnam",
    "VU": "Vanuatu",
    "WS": "Samoa",
    "YE": "Yemen",
    "ZA": "South Africa",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
}


def get_country_name(country_code: str) -> str:
    """Return a human-readable country name for a two-letter ISO code."""
    return _COUNTRY_NAMES.get(country_code.upper(), country_code)


def _slugify(text: str) -> str:
    """Convert a display name into a URL-safe slug for detail page hrefs."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-") or "unknown"


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
        True,
    ),
]


# ─────────────────────────────────────────────────────────────────────────────
# Creator Row Component (reusable)
# ─────────────────────────────────────────────────────────────────────────────


def _creator_row(
    creator: dict, rank: int, show_growth: bool = False, show_activity: bool = False
):
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
            f"+{format_number(subs_change)}"
            if subs_change > 0
            else format_number(subs_change)
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
                href=channel_url,
                target="_blank",
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
    thumbnail_url = safe_get_value(creator, "channel_thumbnail_url", "")
    current_subs = safe_get_value(creator, "current_subscribers", 0)
    quality_grade = safe_get_value(creator, "quality_grade", "C")

    # Get grade badge info
    grade_icon, grade_label, grade_bg = get_grade_info(quality_grade)

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
            href=channel_url,
            target="_blank",
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
# Group card builders (country / category) — used by both initial render
# and HTMX load-more partials
# ─────────────────────────────────────────────────────────────────────────────


def _country_group_card(country_data: dict) -> Div:
    """Render a single country group card with its top creators."""
    country_code = country_data["country_code"]
    count = country_data["count"]
    creators = country_data["creators"]

    country_name = get_country_name(country_code)

    return Div(
        # Header links to the full ranked list (detail route added in next step)
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
            href=f"/lists/country/{country_code}",
            cls="flex items-center gap-2 hover:opacity-75 transition-opacity",
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


def _category_group_card(category_data: dict) -> Div:
    """Render a single category group card with its top creators."""
    category = category_data["category"]
    count = category_data["count"]
    creators = category_data["creators"]

    emoji = get_topic_category_emoji(category)
    # Strip any Wikipedia URL prefix that may slip through
    display_name = category.split("/")[-1].strip() if "/" in category else category
    # Title-case for readability
    display_name = (
        display_name.title() if display_name == display_name.lower() else display_name
    )

    return Div(
        # Header links to the full ranked list (detail route added in next step)
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
            href=f"/lists/category/{_slugify(display_name)}",
            cls="flex items-center gap-2 hover:opacity-75 transition-opacity",
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
            cls="flex items-center gap-2 text-sm font-medium px-4 py-2 rounded-lg border border-border bg-background hover:bg-accent transition-colors",
        ),
        Span(id=f"{target_id}-spinner", cls="htmx-indicator"),
        id=f"{target_id}-load-more",
        hx_swap_oob="true" if oob else None,
        cls="flex justify-center mt-4",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Tab Content Renderers
# ─────────────────────────────────────────────────────────────────────────────


def _render_top_rated_content(creators: list[dict], description: str) -> Div:
    """Renders Top Rated tab with quality-sorted creators."""
    if not creators:
        return _placeholder_content(description, coming_soon=False)

    return Div(
        # Description + count
        DivFullySpaced(
            P(description, cls="text-sm text-muted-foreground max-w-xl"),
            Div(
                Span(
                    f"{len(creators)} creators",
                    cls="text-sm font-medium text-foreground",
                ),
                cls="shrink-0 hidden sm:block",
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


def _render_most_active_content(creators: list[dict], description: str) -> Div:
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
                cls="shrink-0 hidden sm:block",
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
                Span(
                    (
                        f"{total_countries} countries"
                        if total_countries
                        else f"{shown} countries"
                    ),
                    cls="text-sm font-medium text-foreground",
                ),
                cls="shrink-0 hidden sm:block",
            ),
            cls="mb-6 gap-4 flex-col sm:flex-row items-start sm:items-center",
        ),
        # Country cards grid — HTMX load-more appends into this div
        Div(
            *[_country_group_card(c) for c in country_rankings],
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
                Span(
                    (
                        f"{total_categories} categories"
                        if total_categories
                        else f"{shown} categories"
                    ),
                    cls="text-sm font-medium text-foreground",
                ),
                cls="shrink-0 hidden sm:block",
            ),
            cls="mb-6 gap-4 flex-col sm:flex-row items-start sm:items-center",
        ),
        # Category cards grid — HTMX load-more appends into this div
        Div(
            *[_category_group_card(c) for c in category_rankings],
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


def _render_rising_content(creators: list[dict], description: str) -> Div:
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
                cls="shrink-0 hidden sm:block",
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


def _render_veterans_content(creators: list[dict], description: str) -> Div:
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
                cls="shrink-0 hidden sm:block",
            ),
            cls="mb-6 gap-4 flex-col sm:flex-row items-start sm:items-center",
        ),
        Div(
            *[_creator_row(creator, rank=i + 1) for i, creator in enumerate(creators)],
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
        "opacity-100"
        if index % 3 == 0
        else ("opacity-80" if index % 3 == 1 else "opacity-60")
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
) -> FT:
    """
    HTMX partial response for load-more countries.

    Returns:
        - New country group cards (appended into #country-groups-grid by hx-swap=beforeend)
        - An oob-swap to replace the old load-more button div
    """
    cards = [_country_group_card(g) for g in groups]

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

    return Div(*cards, new_button)


def render_more_categories(
    groups: list[dict],
    next_offset: int,
    has_more: bool,
    total: int = 0,
) -> FT:
    """
    HTMX partial response for load-more categories.

    Returns:
        - New category group cards (appended into #category-groups-grid)
        - An oob-swap to replace the old load-more button div
    """
    cards = [_category_group_card(g) for g in groups]

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

    return Div(*cards, new_button)


def render_lists_page(active_tab: str = "top-rated", tab_data: dict = None) -> FT:
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

    # Dynamic badges driven by live DB counts
    tab_badges: dict[str, str] = {}
    if total_countries:
        tab_badges["by-country"] = str(total_countries)
    if total_categories:
        tab_badges["by-category"] = str(total_categories)

    tab_items = []
    panel_items = []

    # Normalize active_tab to a known tab id; fall back to the first tab if invalid.
    valid_tab_ids = [tab_id for tab_id, *_ in LISTS_TABS]
    normalized_active_tab = (
        active_tab if active_tab in valid_tab_ids else valid_tab_ids[0]
    )

    for i, (tab_id, label, icon, description, coming_soon) in enumerate(LISTS_TABS):
        is_active = tab_id == normalized_active_tab
        badge = tab_badges.get(tab_id, "")
        tab_items.append(_tab_li(tab_id, label, icon, is_active, badge=badge))

        # Render appropriate content based on tab type
        if coming_soon:
            panel_items.append(Li(_placeholder_content(description, coming_soon=True)))
        elif tab_id == "top-rated":
            creators = tab_data.get("top_rated", [])
            panel_items.append(Li(_render_top_rated_content(creators, description)))
        elif tab_id == "most-active":
            creators = tab_data.get("most_active", [])
            panel_items.append(Li(_render_most_active_content(creators, description)))
        elif tab_id == "by-country":
            country_rankings = tab_data.get("country_rankings", [])
            panel_items.append(
                Li(
                    _render_by_country_content(
                        country_rankings,
                        description,
                        total_countries=total_countries,
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
                    )
                )
            )
        elif tab_id == "rising":
            creators = tab_data.get("rising", [])
            panel_items.append(Li(_render_rising_content(creators, description)))
        elif tab_id == "veterans":
            creators = tab_data.get("veterans", [])
            panel_items.append(Li(_render_veterans_content(creators, description)))
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
