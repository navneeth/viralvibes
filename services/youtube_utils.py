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
