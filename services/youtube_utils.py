"""
youtube_utils.py
----------------
Dataclasses and constants for YouTube playlist processing.
Helper utilities for tag extraction, category mapping, and formatting.
"""

from collections import Counter
from dataclasses import dataclass
from typing import Dict, List

# YouTube category ID mapping
YOUTUBE_CATEGORIES = {
    "1": "Film & Animation",
    "2": "Autos & Vehicles",
    "10": "Music",
    "15": "Pets & Animals",
    "17": "Sports",
    "19": "Travel & Events",
    "20": "Gaming",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Tech",
    "29": "Nonprofits & Activism",
}


@dataclass
class ProcessingEstimate:
    """Estimate for processing time and resources."""

    total_videos: int
    videos_to_expand: int
    estimated_seconds: float
    estimated_minutes: float
    batch_count: int

    def __str__(self):
        if self.estimated_minutes < 1:
            return f"~{int(self.estimated_seconds)} seconds"
        elif self.estimated_minutes < 60:
            return f"~{int(self.estimated_minutes)} minutes"
        else:
            hours = self.estimated_minutes / 60
            return f"~{hours:.1f} hours"


def get_category_name(category_id: str) -> str:
    """Convert YouTube category ID to human-readable name."""
    return YOUTUBE_CATEGORIES.get(category_id, f"Category {category_id}")


def extract_all_tags(videos: List[Dict]) -> List[str]:
    """Extract and aggregate all tags from videos."""
    all_tags = []
    for video in videos:
        tags = video.get("Tags", [])
        if isinstance(tags, list):
            all_tags.extend(tags)

    tag_counts = Counter(all_tags)
    return [tag for tag, count in tag_counts.most_common(50)]


def extract_categories(videos: List[Dict]) -> Dict[str, int]:
    """Extract and count video categories."""
    categories = [v.get("CategoryName", "Unknown") for v in videos]
    return dict(Counter(categories))


# ============================================================================
# YouTube Category ID to Emoji Mapping
# ============================================================================

CATEGORY_EMOJI_MAP = {
    "1": "🎬",  # Film & Animation
    "2": "🎵",  # Music
    "10": "🎵",  # Music
    "15": "🎮",  # Gaming
    "17": "⚽",  # Sports
    "18": "🎬",  # Short Movies
    "19": "🎓",  # Education
    "20": "🎓",  # Science & Technology
    "21": "🎬",  # Movies
    "22": "📺",  # Trailers
    "23": "🎞️",  # Short Films
    "24": "📺",  # Television
    "25": "🎸",  # Music
    "26": "🎙️",  # Podcasts & Music
    "27": "🎪",  # Entertainment
    "28": "🎓",  # Online Courses
    "29": "🎓",  # Education
    "30": "🎥",  # Movies
    "31": "🎥",  # Movies & Entertainment
    "32": "👨‍🎓",  # Education
    "33": "🔬",  # Science & Technology
    "34": "🎓",  # Science & Technology
    "35": "🚗",  # Autos & Vehicles
    "36": "🎥",  # Shorts
    "37": "🎬",  # Trailers
    "38": "🎬",  # Movies
    "39": "🎵",  # Music
    "40": "🎪",  # Entertainment
    "41": "🎮",  # Gaming
    "42": "⚽",  # Sports
    "43": "🎓",  # Courses
    "44": "🎨",  # Arts & Crafts
    "45": "🏠",  # Lifestyle
    "46": "🎨",  # Entertainment
}

CATEGORY_EMOJI_DEFAULTS = {
    "default": "📹",  # Generic video
    "music": "🎵",
    "gaming": "🎮",
    "sports": "⚽",
    "education": "🎓",
    "entertainment": "🎪",
    "lifestyle": "🏠",
    "news": "📰",
    "vlog": "👤",
    "tutorial": "📚",
    "trailer": "🎬",
    "short": "📱",
}


def get_category_emoji(category_id: str, category_name: str = None) -> str:
    """
    Get emoji for a YouTube category.

    Args:
        category_id: YouTube category ID (e.g., "10" for Music)
        category_name: Human-readable category name (fallback)

    Returns:
        Emoji string (e.g., "🎵")

    Examples:
        get_category_emoji("10")  # → "🎵" (Music)
        get_category_emoji("15")  # → "🎮" (Gaming)
        get_category_emoji("unknown", "Music")  # → "🎵" (from name)
    """
    # Try direct category ID lookup
    if category_id and str(category_id) in CATEGORY_EMOJI_MAP:
        return CATEGORY_EMOJI_MAP[str(category_id)]

    # Try category name lookup (fallback)
    if category_name:
        name_lower = category_name.lower()
        for key, emoji in CATEGORY_EMOJI_DEFAULTS.items():
            if key in name_lower:
                return emoji

    # Default fallback
    return CATEGORY_EMOJI_DEFAULTS["default"]
