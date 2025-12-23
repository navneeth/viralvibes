"""Table cell renderers and table-related components for the ViralVibes application."""

import logging

import polars as pl
from fasthtml.common import *
from monsterui.all import *

from constants import FLEX_COL
from utils import format_duration, format_number

logger = logging.getLogger(__name__)


# =============================================================================
# Table Cell Renderers
# =============================================================================
def thumbnail_cell(url: str, vid: str, title: str = None) -> A:
    """Render a clickable thumbnail that opens YouTube in a new tab."""
    if not url:
        # small placeholder
        return Div(
            A(
                Div("No thumbnail", cls="text-xs text-gray-400"),
                href=f"https://youtube.com/watch?v={vid}" if vid else "#",
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
            onerror="this.src='/static/favicon.jpeg'",
        ),
        href=f"https://youtube.com/watch?v={vid}",
        target="_blank",
        title=title or "",
        cls="inline-block",
    )


def title_cell(row: dict) -> Div:
    """Render a title cell with video metadata, tags, and uploader info."""
    title = row.get("Title", "Untitled")
    vid = row.get("id", "")
    uploader = row.get("Uploader", "")
    tags = row.get("Tags") or []
    tag_nodes = []
    for t in tags[:2] if isinstance(tags, (list, tuple)) else []:
        # ✅ Use Span with Tailwind classes instead of Badge
        tag_nodes.append(
            Span(
                t,
                cls="inline-block px-2 py-1 mr-1 text-xs bg-blue-100 text-blue-700 rounded-full font-medium",
            )
        )
    meta = Div(
        Div(
            A(
                title,
                href=f"https://youtube.com/watch?v={vid}",
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
    label: str,
    badge_cls: str,
    accent_cls: str,
) -> Div:
    video_id = row.get("id") or row.get("Id") or ""
    video_url = f"https://youtube.com/watch?v={video_id}"
    thumbnail = row.get("Thumbnail") or row.get("thumbnail") or "/static/favicon.jpeg"
    title = row.get("Title", "Untitled")
    uploader = row.get("Uploader", "Unknown")
    views = row.get("Views", 0)

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
        # Views (primary metric)
        Div(
            Span(
                format_number(views),
                cls=f"text-3xl font-bold {accent_cls}",
            ),
            Span("Views", cls="text-xs text-gray-500 ml-1"),
            cls="flex items-baseline gap-1 mb-2",
        ),
        # Uploader
        P(uploader, cls="text-xs text-gray-600"),
        cls="flex flex-col",
    )


# -----------------------------------------------------------------------------
# Configuration: Paired extremes (product intent lives here)
# -----------------------------------------------------------------------------
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
    video_url = f"https://youtube.com/watch?v={video_id}"
    thumbnail = row.get("Thumbnail") or row.get("thumbnail") or "/static/favicon.jpeg"
    title = row.get("Title", "Untitled")
    uploader = row.get("Uploader", "Unknown")
    value = row.get(metric_key, 0)

    metric_value = (
        format_number(value) if metric_key == "Views" else format_duration(value)
    )

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
    """One paired comparison row."""
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
def VideoExtremesSection(df: pl.DataFrame) -> Div:
    """
    Display paired video extremes using a comparison-first narrative.
    e.g most/least viewed, longest/shortest videos.
    """
    if df is None or df.is_empty():
        return Div(P("No videos found in playlist.", cls="text-gray-500"))

    try:
        # ---------------------------------------------------------------------
        # Compute extremes efficiently (single-pass Polars ops)
        # ---------------------------------------------------------------------
        extremes = {}

        if "Views" in df.columns:
            extremes.update(
                {
                    "most_viewed": df.select(pl.col("Views").arg_max()).item(),
                    "least_viewed": df.select(pl.col("Views").arg_min()).item(),
                }
            )

        if "Duration" in df.columns:
            extremes.update(
                {
                    "longest": df.select(pl.col("Duration").arg_max()).item(),
                    "shortest": df.select(pl.col("Duration").arg_min()).item(),
                }
            )

        rows = {
            key: df.row(idx, named=True)
            for key, idx in extremes.items()
            if idx is not None
        }

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
