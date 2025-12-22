"""Table cell renderers and table-related components for the ViralVibes application."""

import logging

import polars as pl
from fasthtml.common import *
from monsterui.all import *

from constants import FLEX_COL
from utils import format_duration, format_number

logger = logging.getLogger(__name__)


def thumbnail_cell(url, vid, title=None):
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
            cls="h-14 w-28 object-cover rounded-lg shadow-sm hover:opacity-90 transition",
        ),
        href=f"https://youtube.com/watch?v={vid}",
        target="_blank",
        title=title or "",
        cls="inline-block",
    )


def title_cell(row):
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


def category_emoji_cell(row):
    """Render category with emoji in table (reads from row dict)."""
    category_emoji = row.get("Category Emoji", "📹")
    category_name = row.get("CategoryName", "Unknown")

    return Div(
        Span(category_emoji, cls="text-xl mr-2"),
        Span(category_name, cls="text-sm text-gray-700"),
        cls="flex items-center gap-1",
        title=f"Category: {category_name}",
    )


def number_cell(val):
    """Render a number cell with proper formatting."""
    if val is None:
        return Div("", cls="text-right font-medium")
    try:
        numeric_val = float(val) if isinstance(val, str) else val
        return Div(format_number(numeric_val), cls="text-right font-medium")
    except (ValueError, TypeError):
        return Div(str(val), cls="text-right font-medium")


def VideoExtremesSection(df: pl.DataFrame) -> Div:
    """
    Display 4 card extremes: most/least viewed, longest/shortest videos.
    Optimized: uses arg_max/arg_min instead of sorting 4 times.
    """
    if df is None or df.is_empty():
        return Div(P("No videos found in playlist.", cls="text-gray-500"))

    try:
        # Compute extremes using Polars (safe with error handling)
        # ✅ EFFICIENT: Get indices directly without sorting
        most_viewed_idx = df.select(pl.col("Views").arg_max()).item()
        least_viewed_idx = df.select(pl.col("Views").arg_min()).item()
        longest_idx = df.select(pl.col("Duration").arg_max()).item()
        shortest_idx = df.select(pl.col("Duration").arg_min()).item()

        # Fetch rows by index
        most_viewed = df.row(most_viewed_idx, named=True)
        least_viewed = df.row(least_viewed_idx, named=True)
        longest = df.row(longest_idx, named=True)
        shortest = df.row(shortest_idx, named=True)

        extremes = [
            (
                "trending-up",
                "Most Viewed",
                most_viewed,
                f"{format_number(most_viewed.get('Views', 0))} views",
            ),
            (
                "trending-down",
                "Least Viewed",
                least_viewed,
                f"{format_number(least_viewed.get('Views', 0))} views",
            ),
            (
                "clock",
                "Longest Video",
                longest,
                f"{format_duration(longest.get('Duration', 0))}",
            ),
            (
                "zap",
                "Shortest Video",
                shortest,
                f"{format_duration(shortest.get('Duration', 0))}",
            ),
        ]

        cards = []
        for icon_name, title, row, metric in extremes:
            video_id = row.get("id") or row.get("ID") or ""
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            thumbnail = (
                row.get("Thumbnail") or row.get("thumbnail") or "/static/favicon.jpeg"
            )

            cards.append(
                Card(
                    Div(
                        UkIcon(
                            icon_name,
                            height=28,
                            width=28,
                            cls="text-red-600 flex-shrink-0",
                        ),
                        H4(title, cls="font-semibold text-gray-900"),
                        cls="flex items-center gap-3 mb-4",
                    ),
                    Img(
                        src=thumbnail,
                        alt=row.get("Title", "Video"),
                        cls="w-full h-40 object-cover rounded-lg shadow-sm",
                        loading="lazy",
                        onerror="this.src='/static/favicon.jpeg'",
                    ),
                    Div(
                        P(
                            row.get("Title", "Untitled"),
                            cls="font-medium line-clamp-2 mt-3 text-gray-800",
                        ),
                        P(metric, cls="text-sm text-red-600 font-semibold mt-1"),
                        P(
                            row.get("Uploader", "Unknown"),
                            cls="text-xs text-gray-500 mt-1",
                        ),
                        cls="px-1",
                    ),
                    footer=Div(
                        A(
                            Button(
                                UkIcon("play", width=16, height=16, cls="mr-1"),
                                "Watch",
                                href=video_url,
                                target="_blank",
                                rel="noopener noreferrer",
                                cls=(ButtonT.ghost, "w-full text-center"),
                            ),
                            href=video_url,
                            target="_blank",
                            rel="noopener noreferrer",
                            cls="no-underline",
                        ),
                        cls="mt-4",
                    ),
                    cls="shadow-lg hover:shadow-xl hover:-translate-y-1 transition-all duration-300 bg-white",
                )
            )

        return Div(
            Div(
                H2("🎯 Video Extremes", cls="text-2xl font-bold text-gray-900"),
                P(
                    "Identify your viral peaks and quiet spots",
                    cls="text-gray-600 text-sm mt-1",
                ),
                cls="mb-8",
            ),
            Grid(
                *cards,
                cols="1 sm:2 lg:4",
                gap="4 md:6",
                cls="w-full",
            ),
            cls="mt-12 pb-12 border-b border-gray-200",
        )

    except Exception as e:
        logger.exception(f"Error rendering VideoExtremesSection: {e}")
        return Div(
            P("Unable to load video extremes.", cls="text-gray-500"),
            cls="mt-8 p-4 bg-gray-50 rounded-lg",
        )
