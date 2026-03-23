"""
Creator metrics calculation helpers.

Extracts calculation logic from views to keep components clean.
"""

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


def get_language_emoji(language_code: str) -> str:
    """Get emoji flag for language code."""
    language_emojis = {
        "en": "🇺🇸",
        "ja": "🇯🇵",
        "es": "🇪🇸",
        "ko": "🇰🇷",
        "zh": "🇨🇳",
        "ru": "🇷🇺",
        "fr": "🇫🇷",
        "de": "🇩🇪",
        "pt": "🇵🇹",
        "it": "🇮🇹",
    }
    return language_emojis.get(language_code, "🌍")


def get_language_name(language_code: str) -> str:
    """Get full language name from code."""
    language_names = {
        "en": "English",
        "ja": "日本語",
        "es": "Español",
        "ko": "한국어",
        "zh": "中文",
        "ru": "Русский",
        "fr": "Français",
        "de": "Deutsch",
        "pt": "Português",
        "it": "Italiano",
    }
    return language_names.get(language_code, language_code)


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
