"""
Creator metrics calculation helpers.

Extracts calculation logic from views to keep components clean.
"""

import pycountry
from typing import Optional, Tuple


def calculate_growth_rate(subs_change: int | None, current_subs: int) -> float:
    """Calculate 30-day growth rate percentage. Returns 0.0 when data is unavailable."""
    if subs_change is None or current_subs <= 0:
        return 0.0
    return subs_change / current_subs * 100


def calculate_avg_views_per_video(total_views: int, video_count: int) -> int:
    """Calculate average views per video."""
    return int(total_views / video_count) if video_count > 0 else 0


# Market RPM table (creator net take-home per 1 000 views, 2026)
_MARKET_DATA: dict[str, dict[str, float]] = {
    "US": {"long": 14.50, "shorts": 0.33},
    "JP": {"long": 11.20, "shorts": 0.14},
    "GB": {"long": 10.50, "shorts": 0.17},
    "IN": {"long": 1.10, "shorts": 0.01},
    "BR": {"long": 1.40, "shorts": 0.04},
    "DEFAULT": {"long": 4.50, "shorts": 0.08},
}

# Niche CPM multipliers (applied to ad revenue only)
_NICHE_MULTIPLIERS: dict[str, float] = {
    "finance": 2.2,
    "tech": 1.7,
    "science & technology": 1.7,  # YouTube category name
    "gaming": 0.7,
}


def estimate_monthly_revenue_v4(
    total_subs: int,
    total_views: int,
    video_count: int,
    country_code: str = "US",
    niche: str = "general",
) -> dict:
    """
    2026 model: tuned for the Shorts-heavy YouTube economy.

    All inputs come directly from the YouTube API; no extra worker quota needed.

    Args:
        total_subs:   statistics.subscriberCount
        total_views:  statistics.viewCount (lifetime)
        video_count:  statistics.videoCount
        country_code: snippet.country (2-letter ISO, e.g. "US")
        niche:        primary_category from DB (e.g. "Gaming", "Finance")

    Returns a dict with keys:
        est_monthly_total      float  — total estimated monthly revenue ($)
        revenue_split          dict   — breakdown: adsense_long, adsense_shorts, brand_deals
        assumed_shorts_pct     str    — e.g. "80%"
    """
    rates = _MARKET_DATA.get(country_code.upper(), _MARKET_DATA["DEFAULT"])

    # View-mix heuristic: high video_count relative to subs → Shorts factory
    shorts_ratio = 0.8 if (video_count / (total_subs + 1)) > 0.01 else 0.4

    # Estimate monthly views (≈6% of lifetime total for an active channel)
    monthly_views = total_views * 0.06
    views_long = monthly_views * (1 - shorts_ratio)
    views_shorts = monthly_views * shorts_ratio

    # Niche multiplier — case-insensitive lookup, default 1.0
    niche_mod = _NICHE_MULTIPLIERS.get(niche.lower(), 1.0)

    # Ad revenue (AdSense)
    ad_rev = (views_long / 1_000) * rates["long"] * niche_mod + (views_shorts / 1_000) * rates[
        "shorts"
    ]

    # Sponsorships: $25 CPM on long-form views only (brands ignore Shorts)
    sponsorships = (views_long / 1_000) * 25.0 * niche_mod

    # Direct monetisation (merch/memberships): scales with loyal long-form audience
    direct_mon = ad_rev * (0.25 if total_subs > 500_000 else 0.05)

    total_monthly = ad_rev + sponsorships + direct_mon

    return {
        "est_monthly_total": round(total_monthly, 2),
        "revenue_split": {
            "adsense_long": round((views_long / 1_000) * rates["long"] * niche_mod, 2),
            "adsense_shorts": round((views_shorts / 1_000) * rates["shorts"], 2),
            "brand_deals": round(sponsorships, 2),
        },
        "assumed_shorts_pct": f"{int(shorts_ratio * 100)}%",
    }


def calculate_views_per_subscriber(total_views: int, current_subs: int) -> float:
    """Calculate views per subscriber ratio."""
    return total_views / current_subs if current_subs > 0 else 0.0


def calculate_momentum_score(
    views_change_30d: int | None,
    subs_change_30d: int | None,
    current_subs: int,
) -> float | None:
    """
    Compute a 0–100 momentum score that combines subscriber growth velocity
    with view velocity.

    The score blends two signals with equal weight:
      - Sub velocity:  subs_change_30d / current_subs * 100  (30-day growth %)
      - View velocity: views_change_30d / current_subs        (viral coefficient)

    Both are normalised against representative ceilings so the result sits
    naturally in the 0–100 range for typical creators:
      - sub_pct     normalised at 5 %  (5 % 30d growth → score contribution 50)
      - viral_coeff normalised at 5.0  (viral_coeff 5 → score contribution 50)

    Returns None when neither delta is available (channel not yet tracked).
    """
    if views_change_30d is None and subs_change_30d is None:
        return None
    if current_subs <= 0:
        return None

    _views = max(views_change_30d or 0, 0)
    _subs = max(subs_change_30d or 0, 0)

    # Normalised sub growth (ceiling = 5%)
    sub_component = min((_subs / current_subs * 100) / 5.0, 1.0) * 50

    # Normalised view velocity / viral coeff (ceiling = 5.0)
    view_component = min((_views / current_subs) / 5.0, 1.0) * 50

    return round(sub_component + view_component, 1)


def get_momentum_label(score: float) -> tuple[str, str]:
    """
    Map a momentum score to a display label and badge CSS classes.

    Returns (label, tailwind_classes).
    """
    if score >= 70:
        return (
            "🚀 Surging",
            "bg-emerald-100 text-emerald-800 border-emerald-300 dark:bg-emerald-900/30 dark:text-emerald-300",
        )
    elif score >= 40:
        return (
            "📈 Building",
            "bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-900/20 dark:text-blue-300",
        )
    elif score >= 15:
        return (
            "→ Holding",
            "bg-gray-50 text-gray-700 border-gray-200 dark:bg-gray-800 dark:text-gray-300",
        )
    else:
        return (
            "📉 Cooling",
            "bg-red-50 text-red-700 border-red-200 dark:bg-red-900/20 dark:text-red-300",
        )


def format_channel_age(channel_age_days: Optional[int]) -> str:
    """
    Format channel age in human-readable format.

    Returns: "5y", "8mo", or "New"
    """
    if not channel_age_days:
        return "New"

    years = channel_age_days // 365
    if years >= 1:
        return f"{years}y"

    months = channel_age_days // 30
    return f"{months}mo" if months > 0 else "New"


def get_growth_signal(growth_rate: float) -> Tuple[str, str]:
    """
    Get growth signal interpretation.

    Returns:
        Tuple of (label, style_classes). The label includes the emoji
        (e.g. "🚀 Rapid Growth") so callers don't need to handle it separately.
    """
    if growth_rate > 5:
        return ("🚀 Rapid Growth", "bg-green-100 text-green-800 border-green-300")
    elif growth_rate > 1:
        return ("📈 Growing", "bg-green-50 text-green-700 border-green-200")
    elif growth_rate < -2:
        return ("📉 Declining", "bg-red-50 text-red-700 border-red-200")
    else:
        return ("→ Stable", "bg-gray-50 text-gray-700 border-gray-200")


def get_grade_info(quality_grade: str) -> Tuple[str, str, str]:
    """
    Get quality grade display info.

    Returns:
        Tuple of (emoji, label, style_classes)
    """
    grade_map = {
        "A+": ("👑", "Elite", "bg-purple-600 text-white"),
        "A": ("⭐", "Star", "bg-blue-600 text-white"),
        "B+": ("📈", "Rising", "bg-cyan-600 text-white"),
        "B": ("💎", "Good", "bg-gray-600 text-white"),
        "C": ("🔍", "New", "bg-gray-500 text-white"),
    }
    return grade_map.get(quality_grade, ("?", "Unrated", "bg-gray-400 text-white"))


def get_sync_status_badge(sync_status: str) -> Optional[Tuple[str, str, str]]:
    """
    Get sync status badge info (only for non-synced states).

    Returns:
        Tuple of (emoji, label, style_classes) or None if synced
    """
    badge_map = {
        "pending": ("⏳", "Syncing", "bg-amber-100 text-amber-800"),
        "invalid": ("⚠️", "Invalid", "bg-red-100 text-red-800"),
        "failed": ("❌", "Error", "bg-orange-100 text-orange-800"),
    }
    return badge_map.get(sync_status, None)


# Curated display names for the most common YouTube content languages.
# Keys are base ISO 639-1 codes (post-normalisation by merge_language_variants).
# Values show the English name so the UI is readable to all users.
# pycountry handles the long tail automatically.
_LANGUAGE_NAMES: dict[str, str] = {
    "af": "Afrikaans",
    "am": "Amharic",
    "ar": "Arabic",
    "as": "Assamese",
    "az": "Azerbaijani",
    "be": "Belarusian",
    "bg": "Bulgarian",
    "bn": "Bengali",
    "bs": "Bosnian",
    "cs": "Czech",
    "da": "Danish",
    "de": "German",
    "el": "Greek",
    "en": "English",
    "es": "Spanish",
    "et": "Estonian",
    "eu": "Basque",
    "fa": "Persian",
    "fi": "Finnish",
    "fil": "Filipino",
    "fr": "French",
    "gl": "Galician",
    "gu": "Gujarati",
    "he": "Hebrew",
    "hi": "Hindi",
    "hr": "Croatian",
    "hu": "Hungarian",
    "hy": "Armenian",
    "id": "Indonesian",
    "is": "Icelandic",
    "it": "Italian",
    "ja": "Japanese",
    "ka": "Georgian",
    "kk": "Kazakh",
    "km": "Khmer",
    "kn": "Kannada",
    "ko": "Korean",
    "ky": "Kyrgyz",
    "lo": "Lao",
    "lt": "Lithuanian",
    "lv": "Latvian",
    "mk": "Macedonian",
    "ml": "Malayalam",
    "mn": "Mongolian",
    "mr": "Marathi",
    "ms": "Malay",
    "my": "Burmese",
    "ne": "Nepali",
    "nl": "Dutch",
    "no": "Norwegian",
    "or": "Odia",
    "pa": "Punjabi",
    "pl": "Polish",
    "pt": "Portuguese",
    "ro": "Romanian",
    "ru": "Russian",
    "si": "Sinhala",
    "sk": "Slovak",
    "sl": "Slovenian",
    "sq": "Albanian",
    "sr": "Serbian",
    "sv": "Swedish",
    "sw": "Swahili",
    "ta": "Tamil",
    "te": "Telugu",
    "th": "Thai",
    "tr": "Turkish",
    "uk": "Ukrainian",
    "ur": "Urdu",
    "uz": "Uzbek",
    "vi": "Vietnamese",
    "yi": "Yiddish",
    "zh": "Chinese",
    "zu": "Zulu",
}

# Representative flag emoji per language (base code).
# Preference: most populous speaker country, or most culturally associated flag.
_LANGUAGE_EMOJIS: dict[str, str] = {
    "af": "🇿🇦",
    "am": "🇪🇹",
    "ar": "🇸🇦",
    "as": "🇮🇳",
    "az": "🇦🇿",
    "be": "🇧🇾",
    "bg": "🇧🇬",
    "bn": "🇧🇩",
    "bs": "🇧🇦",
    "cs": "🇨🇿",
    "da": "🇩🇰",
    "de": "🇩🇪",
    "el": "🇬🇷",
    "en": "🇺🇸",
    "es": "🇪🇸",
    "et": "🇪🇪",
    "eu": "🏴",
    "fa": "🇮🇷",
    "fi": "🇫🇮",
    "fil": "🇵🇭",
    "fr": "🇫🇷",
    "gl": "🇪🇸",
    "gu": "🇮🇳",
    "he": "🇮🇱",
    "hi": "🇮🇳",
    "hr": "🇭🇷",
    "hu": "🇭🇺",
    "hy": "🇦🇲",
    "id": "🇮🇩",
    "is": "🇮🇸",
    "it": "🇮🇹",
    "ja": "🇯🇵",
    "ka": "🇬🇪",
    "kk": "🇰🇿",
    "km": "🇰🇭",
    "kn": "🇮🇳",
    "ko": "🇰🇷",
    "ky": "🇰🇬",
    "lo": "🇱🇦",
    "lt": "🇱🇹",
    "lv": "🇱🇻",
    "mk": "🇲🇰",
    "ml": "🇮🇳",
    "mn": "🇲🇳",
    "mr": "🇮🇳",
    "ms": "🇲🇾",
    "my": "🇲🇲",
    "ne": "🇳🇵",
    "nl": "🇳🇱",
    "no": "🇳🇴",
    "or": "🇮🇳",
    "pa": "🇮🇳",
    "pl": "🇵🇱",
    "pt": "🇧🇷",
    "ro": "🇷🇴",
    "ru": "🇷🇺",
    "si": "🇱🇰",
    "sk": "🇸🇰",
    "sl": "🇸🇮",
    "sq": "🇦🇱",
    "sr": "🇷🇸",
    "sv": "🇸🇪",
    "sw": "🇰🇪",
    "ta": "🇱🇰",
    "te": "🇮🇳",
    "th": "🇹🇭",
    "tr": "🇹🇷",
    "uk": "🇺🇦",
    "ur": "🇵🇰",
    "uz": "🇺🇿",
    "vi": "🇻🇳",
    "yi": "🌍",
    "zh": "🇨🇳",
    "zu": "🇿🇦",
}


def get_language_emoji(language_code: str) -> str:
    """
    Return a representative flag emoji for a base ISO 639-1 language code.

    Falls back to 🌍 for codes not in the curated table.
    """
    return _LANGUAGE_EMOJIS.get(language_code.lower(), "🌍")


def get_language_name(language_code: str) -> str:
    """
    Return the English display name for a base ISO 639-1 language code.

    Lookup order:
    1. Curated ``_LANGUAGE_NAMES`` dict (fast, covers all YouTube-common codes).
    2. ``pycountry.languages`` by alpha_2 (ISO 639-1 two-letter codes).
    3. ``pycountry.languages`` by alpha_3 (ISO 639-2/3 three-letter codes,
       e.g. ``fil`` for Filipino).
    4. Uppercase code as last resort (e.g. ``"XYZ"``).

    The ISO name is cleaned by stripping any parenthetical qualifier
    (e.g. ``"Malay (individual language)"`` → ``"Malay"`` ).
    """
    code = language_code.lower()
    if code in _LANGUAGE_NAMES:
        return _LANGUAGE_NAMES[code]

    # pycountry fallback — try alpha_2 first, then alpha_3 for longer codes
    lang = pycountry.languages.get(alpha_2=code) or pycountry.languages.get(alpha_3=code)
    if lang:
        # Strip verbose ISO parentheticals like " (macrolanguage)" or " (individual language)"
        return lang.name.split(" (")[0]

    return code.upper()


def get_activity_badge(monthly_uploads: Optional[float]) -> Optional[str]:
    """Get activity badge based on monthly uploads."""
    if not monthly_uploads:
        return None

    # Use same thresholds as emoji/title for consistency
    if monthly_uploads > 5:
        return "🔥 Very Active"
    elif monthly_uploads >= 1:
        return "✅ Active"
    elif monthly_uploads > 0:
        return "📅 Occasional"
    else:
        return "⏸️ Dormant"


def get_age_emoji(channel_age_days: int) -> str:
    """Get emoji for channel age."""
    if channel_age_days > 3650:  # 10+ years
        return "👑"  # Veteran
    elif channel_age_days > 1825:  # 5+ years
        return "🏆"  # Established
    elif channel_age_days > 365:  # 1+ year
        return "📈"  # Growing
    else:
        return "🆕"  # New


def get_age_title(channel_age_days: int) -> str:
    """Get title text for channel age."""
    if channel_age_days > 3650:
        return "10+ years"
    elif channel_age_days > 1825:
        return "5+ years"
    elif channel_age_days > 365:
        return "1+ year"
    else:
        return "<1 year"


def get_activity_emoji(monthly_uploads: float) -> str:
    """Get emoji for channel activity level."""
    if monthly_uploads > 5:
        return "🔥"  # Very active
    elif monthly_uploads >= 1:
        return "✅"  # Active
    elif monthly_uploads > 0:
        return "📅"  # Occasional
    else:
        return "⏸️"  # Dormant


def get_activity_title(monthly_uploads: float) -> str:
    """Get title text for channel activity level."""
    if monthly_uploads > 5:
        return "Very active (>5/mo)"
    elif monthly_uploads >= 1:
        return "Active (1-5/mo)"
    elif monthly_uploads > 0:
        return "Occasional (<1/mo)"
    else:
        return "Dormant (0/mo)"


def get_country_flag(country_code: str) -> Optional[str]:
    """Get country flag emoji from country code using emoji-country-flag package."""
    if not country_code:
        return None

    try:
        import flag

        return flag.flag(country_code.upper())
    except (ValueError, KeyError, ImportError):
        return None
