# Standard library and typing
import logging
from typing import Dict, Optional

# Third-party libraries
import polars as pl
from fasthtml.common import *
from monsterui.all import *

# Local modules
from charts import (
    chart_bubble_engagement_vs_views,
    chart_category_performance,
    chart_comments_engagement,
    chart_duration_impact,
    chart_duration_vs_engagement,
    chart_engagement_breakdown,
    chart_engagement_ranking,
    chart_likes_per_1k_views,
    chart_performance_heatmap,
    chart_stacked_interactions,
    chart_top_performers_radar,
    chart_treemap_reach,
    chart_treemap_views,
    chart_video_radar,
    chart_views_ranking,
    chart_views_vs_likes,
    chart_views_vs_likes_enhanced,
)
from components import styled_div
from components.buttons import (
    small_badge,
)
from components.cards import (
    MetricCard,
)
from db import fetch_playlists, get_cached_playlist_stats
from utils import format_number

"""Define reusable UI components for the ViralVibes application."""
# Custom shadow styles
inset = "shadow-[0_2px_2px_rgba(255,255,255,0.5),0_3px_3px_rgba(0,0,0,0.2)]"
bnset = "shadow-[inset_0_2px_4px_rgba(255,255,255,0.1),0_4px_8px_rgba(0,0,0,0.5)]"

# Get logger instance
logger = logging.getLogger(__name__)


def CachedResultsBanner(cached_at: str) -> Div:
    """Display a banner indicating results are from cache."""
    return styled_div(
        styled_div(
            UkIcon("check-circle", cls="text-green-500 mr-2", height=20, width=20),
            Span("Instant Results from Cache", cls="font-semibold text-green-700"),
            Span(f" • Last analyzed: {cached_at}", cls="text-gray-600 text-sm ml-2"),
            cls="flex items-center",
        ),
        cls="bg-green-50 border border-green-200 rounded-lg p-4 mb-6",
    )


def AnalyticsDashboardSection(
    df,
    summary: Dict,
    playlist_name: str,
    channel_name: str,
    playlist_thumbnail: str = None,
    channel_thumbnail: str = None,
    channel_url: str = None,
    from_cache: bool = False,
    cached_at: str = None,
):
    """Create an analytics dashboard section for a playlist with enhanced header."""
    # Derive total / processed counts with a clear precedence to avoid empty/missing keys
    # precedence: explicit summary keys -> common aliases -> df height -> 0
    actual_playlist_count = (
        summary.get("actual_playlist_count")
        or summary.get("video_count")
        or summary.get("total_count")
        or (df.height if (df is not None and hasattr(df, "height")) else 0)
    )

    processed_count = (
        summary.get("processed_video_count")
        if summary.get("processed_video_count") is not None
        else (df.height if (df is not None and hasattr(df, "height")) else 0)
    )

    # Extract additional data from summary for enhanced header
    total_views = summary.get("total_views", 0)
    engagement_rate = summary.get("avg_engagement", 0)
    processed_date = summary.get("processed_date") or cached_at

    return Section(
        # Show cache banner if results are from DB
        (CachedResultsBanner(cached_at) if from_cache and cached_at else None),
        # Enhanced professional header with all DB fields
        AnalyticsHeader(
            playlist_title=playlist_name,
            channel_name=channel_name,
            total_videos=actual_playlist_count,
            processed_videos=processed_count,
            playlist_thumbnail=playlist_thumbnail,
            channel_thumbnail=channel_thumbnail,
            channel_url=channel_url,
            processed_date=processed_date,
            engagement_rate=engagement_rate,
            total_views=total_views,
        ),
        # Playlist Metrics Overview
        PlaylistMetricsOverview(df, summary),
        # Header
        Div(
            H2("📊 Playlist Analytics", cls="text-3xl font-bold text-gray-900"),
            P(
                "Dive deep into your playlist's performance, audience engagement, and content patterns.",
                cls="text-gray-500 mt-2 mb-8 text-lg",
            ),
            cls="text-center mb-12",
        ),
        # =====================================================================
        # BLOCK 1: REACH & PERFORMANCE
        # =====================================================================
        Div(
            H3(
                "👀 Reach & Performance",
                cls="text-2xl font-semibold text-gray-800 mb-4",
            ),
            P(
                "How your videos are reaching your audience - which ones drive the most views?",
                cls="text-gray-500 mb-8",
            ),
            Grid(
                chart_views_ranking(df, "views-ranking"),
                chart_treemap_reach(df, "treemap-reach"),
                cols="1 md:2",  # 1 col on mobile, 2 on desktop)
                gap="6 md:8",  # RESPONSIVE GAP
                cls="w-full",
            ),
            cls="pb-16 mb-16 border-b-2 border-gray-100",
        ),
        # =====================================================================
        # BLOCK 2: AUDIENCE ENGAGEMENT QUALITY
        # =====================================================================
        Div(
            H3(
                "💬 Engagement Quality", cls="text-2xl font-semibold text-gray-800 mb-4"
            ),
            P(
                "Beyond views - how engaged is your audience? Likes per view & comment rates.",
                cls="text-gray-500 mb-6",
            ),
            Grid(
                chart_engagement_ranking(df, "engagement-ranking"),
                chart_engagement_breakdown(df, "engagement-breakdown"),
                cols="1 md:2",  # 1 col on mobile, 2 on desktop)
                gap="6 md:8",  # RESPONSIVE GAP
                cls="w-full",
            ),
            Div(
                H4(
                    "📐 How We Measure Engagement",
                    cls="text-sm font-semibold text-gray-700 mt-8 mb-2",
                ),
                P(
                    "Engagement = (Likes + Comments) ÷ Views × 100",
                    cls="text-xs text-gray-600 bg-gray-50 p-4 rounded-lg border border-gray-200 leading-relaxed",
                ),
                cls="mt-8 bg-gray-50/50 rounded-xl p-6 shadow-inner",
            ),
            cls="mb-16 pb-12 border-b border-gray-200",
        ),
        # =====================================================================
        # BLOCK 3: CONTENT INTERACTION
        # =====================================================================
        Div(
            H3(
                "🎯 Content Interaction",
                cls="text-2xl font-semibold text-gray-800 mb-4",
            ),
            P(
                "How views convert to likes and comments - understand audience behavior.",
                cls="text-gray-500 mb-6",
            ),
            Grid(
                chart_views_vs_likes_enhanced(df, "views-vs-likes"),
                chart_comments_engagement(df, "comments-engagement"),
                cols="1 md:2",  # 1 col on mobile, 2 on desktop)
                gap="6 md:10",  # RESPONSIVE GAP
                cls="w-full",
            ),
            cls="mb-16 pb-12 border-b border-gray-200",
        ),
        # =====================================================================
        # BLOCK 5: CONTENT OPTIMIZATION
        # =====================================================================
        Div(
            H3(
                "⏱️ Content Optimization",
                cls="text-2xl font-semibold text-gray-800 mb-4",
            ),
            P(
                "Does video length affect engagement? What's the optimal duration for your audience?",
                cls="text-gray-500 mb-6",
            ),
            Grid(
                chart_duration_impact(df, "duration-impact"),
                cols="1",
                gap="8",
                cls="w-full",
            ),
            cls="mb-16 pb-12 border-b border-gray-200",
        ),
        # =====================================================================
        # BLOCK 4: PERFORMANCE QUADRANTS
        # =====================================================================
        # Div(
        #     H3(
        #         "🎯 Strategic Positioning",
        #         cls="text-2xl font-semibold text-gray-800 mb-2",
        #     ),
        #     P(
        #         "Where does your content stand? Identify high-performers and improvement opportunities.",
        #         cls="text-gray-500 mb-8 text-sm",
        #     ),
        #     Grid(
        #         # chart_performance_heatmap(df, "performance-heatmap"),
        #         chart_bubble_engagement_vs_views(df, "bubble-engagement"),
        #         cols="1 md:2",
        #         gap="6 md:10",
        #         cls="w-full",
        #     ),
        #     cls="mb-16 pb-12 border-b border-gray-200",
        # ),
        # =====================================================================
        # BLOCK 5: CONTENT FACTORS
        # =====================================================================
        # Div(
        #     H3(
        #         "⏱️ Content Optimization",
        #         cls="text-2xl font-semibold text-gray-800 mb-4",
        #     ),
        #     P(
        #         "Does video length affect engagement? What's the optimal duration for your audience?",
        #         cls="text-gray-500 mb-6",
        #     ),
        #     Grid(
        #         chart_duration_impact(df, "duration-impact"),
        #         chart_duration_vs_engagement(df, "duration-engagement"),
        #         cols="1 md:2",
        #         gap="6 md:10",
        #         cls="w-full",
        #     ),
        #     cls="mb-16 pb-12 border-b border-gray-200",
        # ),
        # =====================================================================
        # BLOCK 6: TOP PERFORMERS COMPARISON
        # =====================================================================
        Div(
            H3(
                "🏆 Top Performers",
                cls="text-2xl font-semibold text-gray-800 mb-4",
            ),
            P(
                "Compare your best videos across all key metrics (Views, Likes, Comments, Engagement).",
                cls="text-gray-500 mb-6",
            ),
            Grid(
                chart_top_performers_radar(df, "top-radar", top_n=3),  # ← Top 3 videos
                cols="1",
                gap="8",
                cls="w-full",
            ),
            cls="mb-16 pb-12 border-b border-gray-200",
        ),
        # =====================================================================
        # BLOCK 7: CATEGORY PERFORMANCE
        # =====================================================================
        (
            Div(
                H3(
                    "📁 Category Performance",
                    cls="text-2xl font-semibold text-gray-800 mb-4",
                ),
                P(
                    "How do different content categories perform? Which niches drive engagement?",
                    cls="text-gray-500 mb-6",
                ),
                Grid(
                    chart_category_performance(df, "category-performance"),
                    cols="1",
                    gap="8",
                    cls="w-full",
                ),
                cls="pb-16 mb-16 border-b-2 border-gray-100",
            )
            if (
                "CategoryName" in df.columns
                and df.select("CategoryName").n_unique() > 1  # ← DEFENSIVE CHECK
            )
            else None  # Skip if <2 categories
        ),
        # =====================================================================
        # BLOCK 4: ENGAGEMENT ANALYSIS
        # =====================================================================
        # Div(
        #     H3(
        #         "🎯 Engagement vs Reach",
        #         cls="text-2xl font-semibold text-gray-800 mb-4",
        #     ),
        #     P(
        #         "Which videos are efficient? Do more views always mean better engagement?",
        #         cls="text-gray-500 mb-6",
        #     ),
        #     Grid(
        #         chart_bubble_engagement_vs_views(df, "bubble-engagement"),
        #         cols="1",
        #         gap="8",
        #         cls="w-full",
        #     ),
        #     cls="mb-16 pb-12 border-b border-gray-200",
        # ),
        # =====================================================================
        # BLOCK 8: SENTIMENT & CONTROVERSY
        # =====================================================================
        # Group 4: Advanced Insights & Patterns
        # Div(
        #     H3(
        #         "📈 Advanced Insights & Patterns",
        #         cls="text-2xl font-semibold text-gray-800 mb-4",
        #     ),
        #     P(
        #         "Uncover deeper relationships between viewership, engagement, and controversy across your content.",
        #         cls="text-gray-500 mb-6",
        #     ),
        #     Grid(
        #         chart_scatter_likes_dislikes(
        #             df, "scatter-likes"
        #         ),  # Correlation analysis
        #         chart_bubble_engagement_vs_views(
        #             df, "bubble-engagement"
        #         ),  # Multi-dimensional analysis
        #         # chart_duration_vs_engagement(df, "duration-engagement"),
        #         chart_video_radar(df, "video-radar"),
        #         cls="grid-cols-1 md:grid-cols-2 gap-10",
        #     ),
        #     cls="mb-16",
        # ),
        # =====================================================================
        # BLOCK 6: TOP PERFORMERS COMPARISON
        # =====================================================================
        # Div(
        #     H3("🏆 Top Performers", cls="text-2xl font-semibold text-gray-800 mb-4"),
        #     P(
        #         "Compare your best videos across all key metrics.",
        #         cls="text-gray-500 mb-6",
        #     ),
        #     Grid(
        #         chart_top_performers_radar(df, "top-radar", top_n=5),
        #         cls="grid-cols-1 gap-10",
        #     ),
        #     cls="mb-16",
        # ),
        cls="mt-20 pt-12 border-t border-gray-200 space-y-12",
    )


def AnalyticsHeader(
    playlist_title: str,
    channel_name: str,
    total_videos: int,
    processed_videos: int,
    playlist_thumbnail: Optional[str] = None,
    channel_url: Optional[str] = None,
    channel_thumbnail: Optional[str] = None,
    processed_date: Optional[str] = None,
    engagement_rate: Optional[float] = None,
    total_views: Optional[int] = None,
) -> Div:
    """Create a professional header for the analytics dashboard with rich DB data."""
    video_info = f"{total_videos} videos"
    if processed_videos < total_videos:
        missing = total_videos - processed_videos
        video_info = f"{processed_videos} of {total_videos} analyzed"
        # Show why some are missing
        missing_note = f"({missing} unavailable - deleted, private, or restricted)"
    else:
        missing_note = None

    eng_display = None
    if engagement_rate is not None:
        # Normalize to 0-100 if needed
        eng_val = engagement_rate * 100 if engagement_rate <= 1 else engagement_rate
        eng_display = f"{eng_val:.1f}"

    return Div(
        # Main header content
        Div(
            # Left side: Combined playlist and channel info
            Div(
                # Playlist thumbnail (larger)
                (
                    Img(
                        src=playlist_thumbnail,
                        alt=f"{playlist_title} thumbnail",
                        cls="w-20 h-20 rounded-lg shadow-md mr-4 object-cover",
                        style="min-width: 80px; min-height: 80px;",
                    )
                    if playlist_thumbnail
                    else ""
                ),
                Div(
                    # Title and channel with thumbnail
                    Div(
                        H1(
                            playlist_title,
                            cls="text-2xl md:text-3xl font-bold text-gray-900 mb-2",
                        ),
                        # Channel info with thumbnail
                        Div(
                            (
                                Img(
                                    src=channel_thumbnail,
                                    alt=f"{channel_name} avatar",
                                    cls="w-6 h-6 rounded-full border border-gray-300",
                                )
                                if channel_thumbnail
                                else UkIcon("user", cls="w-5 h-5 text-gray-500")
                            ),
                            Span(
                                "by ",
                                (
                                    A(
                                        str(channel_name or "Unknown Channel"),
                                        href=channel_url,
                                        target="_blank",
                                        cls="text-blue-600 hover:text-blue-800 hover:underline",
                                    )
                                    if channel_url
                                    else Span(str(channel_name or "Unknown Channel"))
                                ),
                                cls="text-gray-700",
                            ),
                            Span("•", cls="text-gray-400 mx-2"),
                            Span(video_info, cls="text-gray-600"),
                            cls="flex items-center gap-2 text-sm md:text-base",
                        ),
                        # Missing note (if any)
                        (
                            P(
                                missing_note,
                                cls="text-xs text-orange-600 mt-1 italic",
                            )
                            if missing_note
                            else None
                        ),
                        # Quick stats row
                        (
                            Div(
                                # Views badge
                                (
                                    small_badge(format_number(total_views), icon="eye")
                                    if total_views
                                    else None
                                ),
                                # Engagement badge
                                (
                                    small_badge(
                                        f"{eng_display}% engagement",
                                        icon="heart",
                                        kind="info",
                                    )
                                    if engagement_rate
                                    else None
                                ),
                                # Date badge
                                (
                                    small_badge(
                                        f"Analyzed {processed_date}", icon="calendar"
                                    )
                                    if processed_date
                                    else None
                                ),
                                cls="flex flex-wrap items-center gap-2 mt-2",
                            )
                            if (total_views or engagement_rate or processed_date)
                            else None
                        ),
                    ),
                    cls="flex-1",
                ),
                cls="flex items-start" if playlist_thumbnail else "",
            ),
            # Right side: Status badge
            Div(
                Div(
                    UkIcon(
                        "check-circle",
                        cls="text-green-500 mr-2",
                        height=20,
                        width=20,
                    ),
                    Span("Analysis Complete", cls="text-sm text-green-700 font-medium"),
                    cls="flex items-center px-3 py-2 bg-green-100/40 rounded-lg border border-green-200",
                ),
                cls="hidden md:flex items-center",
            ),
            cls="flex flex-col md:flex-row md:items-start md:justify-between gap-4",
        ),
        # Solid bar background with padding
        cls="p-6 mb-8 rounded-lg shadow-md",
        style="background: linear-gradient(to right, #f0f4f8, #e2e8f0);",
    )


# ----------------------------------------------------------------------
# Helper: safe numeric aggregation
# ----------------------------------------------------------------------
def _safe_agg(df: pl.DataFrame, col: str, agg: str) -> int | float:
    """
    Return 0 (or 0.0) if the column does not exist or the aggregation fails.
    `agg` can be "sum", "mean", "max", "min".
    """
    if col not in df.columns:
        return 0
    try:
        expr = getattr(pl.col(col), agg)()
        return df.select(expr).item()
    except Exception:
        return 0


# ----------------------------------------------------------------------
# Robust PlaylistMetricsOverview
# ----------------------------------------------------------------------
def PlaylistMetricsOverview(df: pl.DataFrame, summary: Dict) -> Div:
    """
    Four (or six) instantly-useful metric cards.
    Works with the exact schema you posted **and** with the enriched
    columns that `youtube_transforms._enrich_dataframe` adds.
    """

    # ------------------------------------------------------------------
    # a) Basic counts from the summary dict (always present)
    # ------------------------------------------------------------------
    total_videos = summary.get("actual_playlist_count", 0) or df.height
    processed_videos = summary.get("processed_video_count", 0) or df.height
    total_views = summary.get("total_views", 0)

    # ------------------------------------------------------------------
    # b) Pull enriched stats if they exist – otherwise calculate on-the-fly
    # ------------------------------------------------------------------
    avg_engagement = summary.get("avg_engagement", 0.0)

    # If the enriched columns are missing, compute them ourselves
    if "Engagement Rate Raw" not in df.columns and total_videos:
        # (likes + dislikes + comments) / views
        likes = _safe_agg(df, "Likes", "sum")
        comments = _safe_agg(df, "Comments", "sum")
        views = _safe_agg(df, "Views", "sum") or 1
        avg_engagement = (likes + comments) / views
        avg_engagement_quality = comments / likes  # Engagement quality proxy

    # ------------------------------------------------------------------
    # c) Top-performer & average view count
    # ------------------------------------------------------------------
    top_views = _safe_agg(df, "Views", "max")
    avg_views = total_views / total_videos if total_videos else 0

    # ------------------------------------------------------------------
    # d) Bonus signals (HD, captions) – only if the columns exist
    # ------------------------------------------------------------------
    hd_ratio = 0.0
    caption_ratio = 0.0
    if "Definition" in df.columns and total_videos:
        hd = df.filter(pl.col("Definition") == "hd").height
        hd_ratio = hd / total_videos
    if "Caption" in df.columns and total_videos:
        caps = df.filter(pl.col("Caption") is True).height
        caption_ratio = caps / total_videos

    # ------------------------------------------------------------------
    # e) Build the four (or six) cards
    # ------------------------------------------------------------------
    cards = [
        MetricCard(
            title="Total Reach",
            value=format_number(total_views),
            subtitle=f"Across {total_videos:,} videos",
            icon="eye",
            color="blue",
        ),
        MetricCard(
            title="Engagement Rate",
            value=f"{avg_engagement:.1%}",
            subtitle="Likes + comments ÷ views",
            icon="heart",
            color="red",
        ),
        MetricCard(
            title="Top Performer",
            value=format_number(top_views),
            subtitle="Most-viewed video",
            icon="trending-up",
            color="green",
        ),
        MetricCard(
            title="Avg. Views per Video",
            value=format_number(int(avg_views)),
            subtitle="Playlist-wide average",
            icon="bar-chart",
            color="purple",
        ),
    ]

    # Optional extra cards – they disappear automatically if data is missing
    if hd_ratio > 0:
        cards.append(
            MetricCard(
                title="HD Content",
                value=f"{hd_ratio:.0%}",
                subtitle="Videos in 720p+",
                icon="film",
                color="indigo",
            )
        )
    if caption_ratio > 0:
        cards.append(
            MetricCard(
                title="Captioned",
                value=f"{caption_ratio:.0%}",
                subtitle="Videos with subtitles",
                icon="closed-caption",
                color="orange",
            )
        )
    # ==================================================================
    # NEW: Category Emoji Breakdown (replaces Controversy)
    # ==================================================================
    category_breakdown = None
    if "CategoryName" in df.columns and "Category Emoji" in df.columns:
        try:
            # Group by category and count videos
            category_data = (
                df.group_by("CategoryName")
                .agg(
                    pl.count().alias("count"),
                    pl.first("Category Emoji").alias("emoji"),  # Get emoji
                )
                .sort("count", descending=True)
                .head(5)  # Top 5 categories only
                .to_dicts()
            )

            if category_data:  # Only show if we have categories
                category_items = []
                for cat in category_data:
                    category_name = cat.get("CategoryName", "Unknown")
                    count = cat.get("count", 0)
                    emoji = cat.get("emoji", "📹")

                    category_items.append(
                        Div(
                            Span(emoji, cls="text-2xl mr-3"),
                            Span(category_name, cls="text-sm font-medium flex-grow"),
                            Span(
                                f"{count} video{'s' if count != 1 else ''}",
                                cls="text-xs text-gray-500 font-semibold",
                            ),
                            cls="flex items-center justify-between py-2.5 px-3 bg-gray-50 rounded-lg border border-gray-200 hover:bg-gray-100 transition-colors",
                        )
                    )

                category_breakdown = Div(
                    H3(
                        "📁 Content Mix", cls="text-sm font-semibold text-gray-700 mb-3"
                    ),
                    Div(*category_items, cls="space-y-2"),
                    cls="mt-6 p-4 bg-white rounded-lg border border-gray-200 shadow-sm",
                )
        except Exception as e:
            logger.debug(f"Failed to build category breakdown: {e}")
            category_breakdown = None

    # ------------------------------------------------------------------
    # f) Layout
    # ------------------------------------------------------------------
    return Div(
        # Section header
        Div(
            H2("Key Metrics", cls="text-xl font-semibold text-gray-800 mb-2"),
            P(
                "At-a-glance performance of your playlist",
                cls="text-gray-600 text-sm mb-6",
            ),
            cls="text-center",
        ),
        # Responsive grid
        Grid(
            *cards,
            cols_sm=2,  # 2 columns on phones
            cols_md=3,  # 3 columns on tablets
            cols_lg=len(cards),  # full row on desktop (4-6 cards)
            gap=4,
            cls="mb-8",
        ),
        # Category breakdown (replaces old Controversy card)
        (category_breakdown if category_breakdown else None),
        cls="mb-12",
    )
