"""
Processing tips that rotate to keep users engaged.
"""

from typing import Dict

PROCESSING_TIPS = [
    {
        "icon": "trending-up",
        "title": "What You'll Discover",
        "content": "The final analysis will show you which videos drove the most engagement and which generated the most discussion.",
    },
    {
        "icon": "filter",
        "title": "Powerful Filtering",
        "content": "Once complete, sort videos by views, likes, comments, engagement rate, or controversy score.",
    },
    {
        "icon": "download",
        "title": "Export Your Data",
        "content": "Download your analysis as CSV and use it in spreadsheets, dashboards, or presentations.",
    },
    {
        "icon": "bar-chart-2",
        "title": "Visual Insights",
        "content": "Interactive charts will show trends over time, content performance patterns, and audience engagement patterns.",
    },
    {
        "icon": "alert-circle",
        "title": "Identify Controversies",
        "content": "Discover videos that sparked discussion - high comment-to-like ratios reveal controversial content.",
    },
    {
        "icon": "clock",
        "title": "Duration Analysis",
        "content": "See which video lengths perform best in your playlist and optimize for your audience.",
    },
    {
        "icon": "target",
        "title": "Engagement Rates",
        "content": "Calculate which videos had the highest engagement (likes + comments vs views) to understand audience interest.",
    },
    {
        "icon": "zap",
        "title": "Real-time Processing",
        "content": "ViralVibes uses batch processing to analyze large playlists efficiently without timeouts.",
    },
]


def get_tip_for_progress(progress: float) -> Dict[str, str]:
    """
    Select a tip based on progress percentage.
    Cycles through tips as progress increases.
    """
    tip_index = int((progress or 0) * len(PROCESSING_TIPS)) % len(PROCESSING_TIPS)
    return PROCESSING_TIPS[tip_index]
