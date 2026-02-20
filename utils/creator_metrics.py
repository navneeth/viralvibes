"""
Creator metrics calculation helpers.

Extracts calculation logic from views to keep components clean.
"""

from typing import Optional, Tuple


def calculate_growth_rate(subs_change: int, current_subs: int) -> float:
    """Calculate 30-day growth rate percentage."""
    return (subs_change / current_subs * 100) if current_subs > 0 else 0.0


def calculate_avg_views_per_video(total_views: int, video_count: int) -> int:
    """Calculate average views per video."""
    return int(total_views / video_count) if video_count > 0 else 0


def estimate_monthly_revenue(total_views: int, cpm: float = 4.0) -> int:
    """
    Estimate monthly revenue based on total views.

    Args:
        total_views: Total channel views
        cpm: Cost per mille (default $4 per 1000 views)

    Returns:
        Estimated monthly revenue in dollars
    """
    return int((total_views * cpm) / 1000)


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


def get_growth_signal(growth_rate: float) -> Tuple[str, str, str]:
    """
    Get growth signal interpretation.

    Returns:
        Tuple of (label, emoji, style_classes)
    """
    if growth_rate > 5:
        return ("🚀 Rapid Growth", "🚀", "bg-green-100 text-green-800 border-green-300")
    elif growth_rate > 1:
        return ("📈 Growing", "📈", "bg-green-50 text-green-700 border-green-200")
    elif growth_rate < -2:
        return ("📉 Declining", "📉", "bg-red-50 text-red-700 border-red-200")
    else:
        return ("→ Stable", "→", "bg-gray-50 text-gray-700 border-gray-200")


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

    if monthly_uploads > 10:
        return "🔥 Very Active"
    elif monthly_uploads > 5:
        return "📈 Active"
    elif monthly_uploads > 2:
        return "📊 Regular"
    else:
        return "📅 Occasional"


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
    elif monthly_uploads > 1:
        return "✅"  # Regular
    elif monthly_uploads > 0:
        return "📅"  # Occasional
    else:
        return "⏸️"  # Dormant


def get_activity_title(monthly_uploads: float) -> str:
    """Get title text for channel activity level."""
    if monthly_uploads > 5:
        return "Very active (>5/mo)"
    elif monthly_uploads > 1:
        return "Active (1-5/mo)"
    else:
        return "Occasional (<1/mo)"


def get_country_flag(country_code: str) -> Optional[str]:
    """Get country flag emoji from country code using emoji-country-flag package."""
    if not country_code:
        return None

    try:
        import flag

        return flag.flag(country_code.upper())
    except (ValueError, KeyError, ImportError):
        return None
