"""Table cell renderers and table-related components for the ViralVibes application."""

import logging
from typing import Any

from fasthtml.common import *
from monsterui.all import *

from constants import FLEX_COL
from utils import find_extreme_indices, get_columns, has_column
from utils import format_duration, format_number

logger = logging.getLogger(__name__)


# =============================================================================
# Metric Formatters
# =============================================================================
METRIC_FORMATTERS = {
    "Views": format_number,
    "Likes": format_number,
    "Comments": format_number,
    "Dislikes": format_number,
    "Duration": format_duration,
}


def format_metric(value: Any, metric_key: str) -> str:
    """
    Format a metric value based on its type.
    Falls back to string conversion if metric type is unknown.
    """
    formatter = METRIC_FORMATTERS.get(metric_key, str)
    try:
        return formatter(value) if value is not None else "—"
    except (ValueError, TypeError):
        return str(value)


# =============================================================================
# Helpers
# =============================================================================
def youtube_watch_url(vid: str | None) -> str:
    """Generate YouTube watch URL or fallback."""
    return f"https://youtube.com/watch?v={vid}" if vid else "#"


def Badge(label: str, cls: str = "") -> Span:
    """Reusable badge component."""
    return Span(
        label,
        cls=f"inline-block px-3 py-1 rounded-full text-xs font-bold {cls}",
    )


# =============================================================================
# Table Cell Renderers
# =============================================================================
def thumbnail_cell(url: str, vid: str, title: str = None) -> A:
    """Render a clickable thumbnail that opens YouTube in a new tab."""
    fallback_url = "/static/favicon.jpeg"  # Consider moving to constants
    if not url:
        # small placeholder
        return Div(
            A(
                Div("No thumbnail", cls="text-xs text-gray-400"),
                href=youtube_watch_url(vid),
                target="_blank",
                cls="inline-block px-2 py-1 bg-gray-50 rounded",
            ),
            cls="text-center",
        )
    return A(
        Img(
            src=url,
            alt=title or "Video thumbnail",
            cls="h-14 w-28 object-cover rounded-lg shadow-sm hover:opacity-90 transition",
            loading="lazy",
            onerror=f"this.src='{fallback_url}'",
        ),
        href=youtube_watch_url(vid),
        target="_blank",
        title=title or "",
        cls="inline-block",
    )


def title_cell(row: dict) -> Div:
    """Render a title cell with video metadata, tags, and uploader info."""
    title = row.get("Title", "Untitled")
    vid = row.get("id", "")
    uploader = row.get("Uploader", "Unknown")
    tags = row.get("Tags", []) if isinstance(row.get("Tags"), (list, tuple)) else []

    tag_nodes = [
        # First two tags only
        Span(
            tag,
            cls="inline-block px-2 py-1 mr-1 text-xs bg-blue-100 text-blue-700 rounded-full font-medium",
        )
        for tag in tags[:2]
    ]
    meta = Div(
        Div(
            A(
                title,
                href=youtube_watch_url(vid),
                target="_blank",
                cls="text-blue-700 hover:underline font-semibold",
            ),
            cls="truncate max-w-md",
        ),
        Div(*tag_nodes, cls="mt-1 flex flex-wrap gap-1"),  # Added flex-wrap
        cls="space-y-1",
    )
    # uploader avatar / name small inline
    uploader_part = Div(
        DiceBearAvatar(uploader, h=20, w=20),
        A(uploader or "Unknown", href="#", cls="text-sm text-gray-600"),
        cls="flex items-center mt-1 space-x-2",
    )
    return Div(meta, uploader_part, cls=FLEX_COL)


def category_emoji_cell(row: dict) -> Div:
    """Render category with emoji in table (reads from row dict)."""
    category_emoji = row.get("Category Emoji", "📹")
    category_name = row.get("CategoryName", "Unknown")

    return Div(
        Span(category_emoji, cls="text-xl mr-2"),
        Span(category_name, cls="text-sm text-gray-700"),
        cls="flex items-center gap-1",
        title=f"Category: {category_name}",
    )


def number_cell(val: Any) -> Div:
    """Render a number cell with proper formatting."""
    if val is None:
        return Div("—", cls="text-right font-medium text-gray-500")
    try:
        numeric_val = float(val) if isinstance(val, str) else val
        return Div(format_number(numeric_val), cls="text-right font-medium")
    except (ValueError, TypeError):
        return Div(str(val), cls="text-right font-medium")


# ---------------------------------------------------------------------
# 2. Reusable comparison card (UI-only responsibility)
# ---------------------------------------------------------------------
def VideoComparisonCard(
    row: dict,
    label: str = "",
    badge_cls: str = "",
    metric_key: str = "",
    accent_cls: str = "",
) -> Div:
    """Reusable card for video extremes with optional metric."""
    video_id = row.get("id") or row.get("Id") or ""
    video_url = youtube_watch_url(video_id)
    thumbnail = row.get("Thumbnail") or row.get("thumbnail") or "/static/favicon.jpeg"
    title = row.get("Title", "Untitled")
    uploader = row.get("Uploader", "Unknown")
    metric_val = row.get(metric_key, 0)

    # Format metric
    if metric_key == "Views":
        metric_value = format_number(metric_val)
    elif metric_key == "Duration":
        metric_value = format_duration(metric_val)
    else:
        metric_value = None

    return Div(
        Badge(label, badge_cls) if label else None,
        # Thumbnail
        A(
            Img(
                src=thumbnail,
                alt=title,
                cls="w-full object-cover rounded-lg shadow-md hover:shadow-lg transition-shadow",
                style="aspect-ratio: 16 / 9;",
                loading="lazy",
                onerror="this.src='/static/favicon.jpeg'",
            ),
            href=video_url,
            target="_blank",
            rel="noopener noreferrer",
            cls="block no-underline mb-4",
        ),
        # Title
        A(
            title,
            href=video_url,
            target="_blank",
            rel="noopener noreferrer",
            cls="block font-semibold text-sm text-blue-700 hover:underline line-clamp-2 mb-3",
        ),
        # Views (primary metric)
        (
            Div(
                Span(
                    metric_value,
                    cls=f"text-3xl font-bold {accent_cls}" if metric_value else "",
                ),
                (
                    Span(metric_key, cls="text-xs text-gray-500 ml-1")
                    if metric_key
                    else None
                ),
                cls="flex items-baseline gap-1 mb-2",
            )
            if metric_value
            else None
        ),
        # Uploader
        P(uploader, cls="text-xs text-gray-600"),
        cls="flex flex-col",
    )


# -----------------------------------------------------------------------------
# Configuration: Paired extremes (product intent lives here)
# -----------------------------------------------------------------------------
"""
Paired extreme comparisons to display.

Each pair is a dict with:
  - pair_id: unique identifier
  - title: section title with emoji
  - subtitle: description
  - metric_key: DataFrame column name to display
  - left/right: config with:
    - key: extremes dict key
    - label: badge text
    - badge_cls: Tailwind classes for badge
    - accent_cls: Tailwind classes for metric value
"""
EXTREME_PAIRS = [
    {
        "pair_id": "views",
        "title": "🎯 Viral vs Quiet",
        "subtitle": "Most and least viewed videos in your playlist",
        "metric_key": "Views",
        "left": {
            "key": "most_viewed",
            "label": "🔥 VIRAL",
            "badge_cls": "bg-red-100 text-red-700",
            "accent_cls": "text-red-600",
        },
        "right": {
            "key": "least_viewed",
            "label": "❄️ QUIET",
            "badge_cls": "bg-blue-100 text-blue-700",
            "accent_cls": "text-blue-600",
        },
    },
    {
        "pair_id": "duration",
        "title": "⏱ Long vs Short",
        "subtitle": "Video length extremes",
        "metric_key": "Duration",
        "left": {
            "key": "longest",
            "label": "⏱ LONG",
            "badge_cls": "bg-purple-100 text-purple-700",
            "accent_cls": "text-purple-600",
        },
        "right": {
            "key": "shortest",
            "label": "⚡ SHORT",
            "badge_cls": "bg-yellow-100 text-yellow-700",
            "accent_cls": "text-yellow-600",
        },
    },
    {
        "pair_id": "engagement",
        "title": "💬 Hot vs Cold",
        "subtitle": "Most and least engaged videos",
        "metric_key": "Comments",  # or "Engagement Rate (%)"
        "left": {
            "key": "most_engaged",
            "label": "🔥 HOT",
            "badge_cls": "bg-orange-100 text-orange-700",
            "accent_cls": "text-orange-600",
        },
        "right": {
            "key": "least_engaged",
            "label": "❄️ COLD",
            "badge_cls": "bg-slate-100 text-slate-700",
            "accent_cls": "text-slate-600",
        },
    },
    {
        "pair_id": "likes",
        "title": "👍 Popular vs Ignored",
        "subtitle": "Most and least liked videos",
        "metric_key": "Likes",
        "left": {
            "key": "most_liked",
            "label": "👍 LIKED",
            "badge_cls": "bg-green-100 text-green-700",
            "accent_cls": "text-green-600",
        },
        "right": {
            "key": "least_liked",
            "label": "😐 IGNORED",
            "badge_cls": "bg-gray-100 text-gray-700",
            "accent_cls": "text-gray-600",
        },
    },
]


# -----------------------------------------------------------------------------
# UI primitives
# -----------------------------------------------------------------------------
def ComparisonConnector() -> Div:
    """Visual connector emphasizing contrast."""
    return Div(
        Div(
            cls="h-1 flex-1 bg-gradient-to-r from-red-500 via-gray-300 to-blue-500 rounded-full"
        ),
        Div("↔️", cls="mx-3 text-xl"),
        Div(
            cls="h-1 flex-1 bg-gradient-to-l from-red-500 via-gray-300 to-blue-500 rounded-full"
        ),
        cls="flex items-center px-4 flex-shrink-0",
    )


def ExtremeCard(
    row: dict,
    metric_key: str,
    label: str,
    badge_cls: str,
    accent_cls: str,
    **_,
) -> Div:
    """Single extreme card. UI only; no data logic."""
    video_id = row.get("id") or row.get("Id") or ""
    video_url = youtube_watch_url(video_id)
    thumbnail = row.get("Thumbnail") or row.get("thumbnail") or "/static/favicon.jpeg"
    title = row.get("Title", "Untitled")
    uploader = row.get("Uploader", "Unknown")
    value = row.get(metric_key, 0)

    metric_value = format_metric(value, metric_key)

    return Div(
        # Badge
        Span(
            label,
            cls=f"inline-block px-3 py-1 rounded-full text-xs font-bold mb-4 {badge_cls}",
        ),
        # Thumbnail
        A(
            Img(
                src=thumbnail,
                alt=title,
                cls="w-full object-cover rounded-lg shadow-md hover:shadow-lg transition-shadow",
                style="aspect-ratio: 16 / 9;",
                loading="lazy",
                onerror="this.src='/static/favicon.jpeg'",
            ),
            href=video_url,
            target="_blank",
            rel="noopener noreferrer",
            cls="block no-underline mb-4",
        ),
        # Title
        A(
            title,
            href=video_url,
            target="_blank",
            rel="noopener noreferrer",
            cls="block font-semibold text-sm text-blue-700 hover:underline line-clamp-2 mb-3",
        ),
        # Metric
        Div(
            Span(metric_value, cls=f"text-3xl font-bold {accent_cls}"),
            Span(metric_key, cls="text-xs text-gray-500 ml-1"),
            cls="flex items-baseline gap-1 mb-2",
        ),
        # Uploader
        P(uploader, cls="text-xs text-gray-600"),
        cls="bg-white p-6 rounded-xl border border-gray-200 shadow-sm flex-1 flex flex-col",
    )


def ExtremeComparisonRow(
    title: str,
    subtitle: str,
    metric_key: str,
    left_row: dict,
    right_row: dict,
    left_cfg: dict,
    right_cfg: dict,
) -> Div:
    """
    Render a paired comparison row with two extreme cards and connector.

    Args:
        title: Section title (e.g., "🎯 Viral vs Quiet")
        subtitle: Description text
        metric_key: DataFrame column to display (e.g., "Views")
        left_row: Left video row data
        right_row: Right video row data
        left_cfg: Left card config (label, colors, etc.)
        right_cfg: Right card config

    Returns:
        Div: Rendered comparison section
    """
    return Div(
        # Header
        Div(
            H3(title, cls="text-xl font-bold text-gray-900"),
            P(subtitle, cls="text-sm text-gray-600"),
            cls="mb-6",
        ),
        # Comparison lane
        Div(
            ExtremeCard(left_row, metric_key, **left_cfg),
            ComparisonConnector(),
            ExtremeCard(right_row, metric_key, **right_cfg),
            cls="flex items-stretch gap-6",
        ),
        cls="mb-16",
    )


# -----------------------------------------------------------------------------
# Main section
# Video Extremes Section (Hybrid: Elegant Data + Strong Visual Comparison)
# -----------------------------------------------------------------------------
def VideoExtremesSection(df: list[dict[str, Any]]) -> Div:
    """
    Display paired video extremes using a comparison-first narrative.
    e.g most/least viewed, longest/shortest videos.
    """
    if df is None or len(df) == 0:
        return Div(P("No videos found in playlist.", cls="text-gray-500"))

    try:
        # ---------------------------------------------------------------------
        # Compute extremes efficiently (native Python)
        # ---------------------------------------------------------------------
        extremes = {}

        if has_column(df, "Views"):
            max_idx, min_idx = find_extreme_indices(df, "Views")
            extremes.update(
                {
                    "most_viewed": max_idx,
                    "least_viewed": min_idx,
                }
            )

        if has_column(df, "Duration"):
            max_idx, min_idx = find_extreme_indices(df, "Duration")
            extremes.update(
                {
                    "longest": max_idx,
                    "shortest": min_idx,
                }
            )
        if has_column(df, "Comments"):
            max_idx, min_idx = find_extreme_indices(df, "Comments")
            extremes.update(
                {
                    "most_engaged": max_idx,
                    "least_engaged": min_idx,
                }
            )
        if has_column(df, "Likes"):
            max_idx, min_idx = find_extreme_indices(df, "Likes")
            extremes.update(
                {
                    "most_liked": max_idx,
                    "least_liked": min_idx,
                }
            )

        rows = {key: df[idx] for key, idx in extremes.items() if idx is not None}

        # ---------------------------------------------------------------------
        # Build paired sections
        # ---------------------------------------------------------------------
        sections = []

        for pair in EXTREME_PAIRS:
            left_cfg = pair["left"]
            right_cfg = pair["right"]

            if left_cfg["key"] not in rows or right_cfg["key"] not in rows:
                continue

            sections.append(
                ExtremeComparisonRow(
                    title=pair["title"],
                    subtitle=pair["subtitle"],
                    metric_key=pair["metric_key"],
                    left_row=rows[left_cfg["key"]],
                    right_row=rows[right_cfg["key"]],
                    left_cfg=left_cfg,
                    right_cfg=right_cfg,
                )
            )

        if not sections:
            return Div(
                P("Insufficient data to display video extremes.", cls="text-gray-500"),
                cls="mt-8 p-4 bg-gray-50 rounded-lg",
            )

        # ---------------------------------------------------------------------
        # Final render
        # ---------------------------------------------------------------------
        return Div(
            H2("📊 Playlist Extremes", cls="text-2xl font-bold text-gray-900 mb-10"),
            *sections,
            cls="mt-12 pb-12 border-b border-gray-200",
        )

    except Exception as e:
        logger.exception(f"Error rendering VideoExtremesSection: {e}")
        return Div(
            P("Unable to load video extremes.", cls="text-gray-500"),
            cls="mt-8 p-4 bg-gray-50 rounded-lg",
        )
