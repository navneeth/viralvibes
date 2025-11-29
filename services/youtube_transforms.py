"""
youtube_transforms.py
---------------------
Polars-based normalization and enrichment of playlist DataFrames.
Clean, fast, UI-ready. Works with both yt-dlp and YouTube Data API backends.
"""

import logging
from typing import Any, Dict, Tuple

import polars as pl

from utils import format_duration, format_number, parse_iso_duration

logger = logging.getLogger(__name__)


# --- Normalize dataframe column names between yt-dlp and YouTube API ---
def normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize column names across backends → canonical schema."""
    if not isinstance(df, pl.DataFrame):
        logger.warning(f"Invalid DataFrame in normalize_columns: {type(df)}")
        return df
    if df.is_empty():
        logger.info("Empty DataFrame received in normalize_columns")
        return df

    # Map any known variants → canonical names
    rename_map = {
        "title": "Title",
        "videoTitle": "Title",
        "id": "id",
        "videoId": "id",
        # map any variants into canonical columns
        "view_count": "Views",
        "viewCount": "Views",
        "View Count": "Views",
        "like_count": "Likes",
        "likeCount": "Likes",
        "Like Count": "Likes",
        "dislike_count": "Dislikes",
        "dislikeCount": "Dislikes",
        "Dislike Count": "Dislikes",
        "comment_count": "Comments",
        "commentCount": "Comments",
        "Comment Count": "Comments",
        "duration_string": "Duration",
        "duration": "Duration",
        "durationSec": "Duration",
        "upload_date": "PublishedAt",
        "publishedAt": "PublishedAt",
        "channel": "Uploader",
        "channelTitle": "Uploader",
        "channel_id": "Channel ID",
        "channelId": "Channel ID",
        "thumbnail": "Thumbnail",
        "thumbnails": "Thumbnail",
        "tags": "Tags",
    }

    df = df.rename({old: new for old, new in rename_map.items() if old in df.columns})

    # Mirror legacy "X Count" names for backward compatibility
    mirrors = [
        ("Views", "View Count"),
        ("Likes", "Like Count"),
        ("Dislikes", "Dislike Count"),
        ("Comments", "Comment Count"),
    ]
    for src, mirror in mirrors:
        if src in df.columns and mirror not in df.columns:
            df = df.with_columns(pl.col(src).alias(mirror))

    # Convert ISO duration strings → seconds (if needed)
    # Handle Duration conversion more robustly
    if "Duration" in df.columns:
        duration_col = pl.col("Duration")

        # Check if Duration is already numeric (Int64)
        if df["Duration"].dtype in (pl.Int64, pl.Int32, pl.Int16, pl.Int8):
            # Already numeric, just ensure it's Int64
            df = df.with_columns(
                duration_col.cast(pl.Int64, strict=False).alias("Duration")
            )
        else:
            # Duration is string or other type - convert it
            df = df.with_columns(
                pl.when(duration_col.is_null() | (duration_col.cast(pl.Utf8) == ""))
                .then(None)
                .otherwise(
                    pl.col("Duration").map_elements(
                        lambda d: (
                            parse_iso_duration(d)
                            if isinstance(d, str) and "PT" in d
                            else d
                        ),
                        return_dtype=pl.Int64,
                    )
                )
                .cast(pl.Int64, strict=False)
                .alias("Duration")
            )

    # Ensure numeric columns are Int64
    numeric_cols = [
        "Views",
        "Likes",
        "Dislikes",
        "Comments",
        "View Count",
        "Like Count",
        "Dislike Count",
        "Comment Count",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))

    return df


def _enrich_dataframe(
    df: pl.DataFrame, actual_playlist_count: int = None
) -> Tuple[pl.DataFrame, Dict[str, Any]]:
    """Add UI-ready columns + rich stats. Never returns None."""
    if not isinstance(df, pl.DataFrame):
        logger.warning(f"Invalid DataFrame in _enrich_dataframe: {type(df)}")
        empty_stats = {
            "total_views": 0,
            "total_likes": 0,
            "total_dislikes": 0,
            "total_comments": 0,
            "avg_engagement": 0.0,
            "actual_playlist_count": actual_playlist_count or 0,
            "processed_video_count": 0,
        }
        return pl.DataFrame(), empty_stats

    if df.is_empty():
        return df, {
            "total_views": 0,
            "total_likes": 0,
            "total_dislikes": 0,
            "total_comments": 0,
            "avg_engagement": 0.0,
            "actual_playlist_count": actual_playlist_count or 0,
            "processed_video_count": 0,
        }

    # === Controversy & Engagement (raw numeric) ===
    df = df.with_columns(
        [
            (
                1
                - (pl.col("Likes") - pl.col("Dislikes")).abs()
                / (pl.col("Likes") + pl.col("Dislikes") + 1)
            )
            .fill_null(value=0.0)  # Explicit value parameter
            .alias("Controversy"),
            (
                (pl.col("Likes") + pl.col("Dislikes") + pl.col("Comments"))
                / (pl.col("Views") + 1)
            )
            .fill_null(value=0.0)  # Explicit value parameter
            .alias("Engagement Rate Raw"),
        ]
    )

    # === Human-readable date ===
    if "PublishedAt" in df.columns:
        df = df.with_columns(
            pl.col("PublishedAt")
            .str.to_datetime(strict=False)
            .dt.strftime("%b %d, %Y")
            .alias("Published Date")
        )

    # === Formatted columns (for direct UI use) ===
    df = df.with_columns(
        [
            pl.col("Views")
            .map_elements(format_number, return_dtype=pl.Utf8)
            .alias("Views Formatted"),
            pl.col("Likes")
            .map_elements(format_number, return_dtype=pl.Utf8)
            .alias("Likes Formatted"),
            pl.col("Dislikes")
            .map_elements(format_number, return_dtype=pl.Utf8)
            .alias("Dislikes Formatted"),
            pl.col("Comments")
            .map_elements(format_number, return_dtype=pl.Utf8)
            .alias("Comments Formatted"),
            pl.col("Duration")
            .map_elements(format_duration, return_dtype=pl.Utf8)
            .alias("Duration Formatted"),
            pl.col("Controversy")
            .map_elements(lambda x: f"{x:.1%}", return_dtype=pl.Utf8)
            .alias("Controversy %"),
            pl.col("Engagement Rate Raw")
            .map_elements(lambda x: f"{x:.2%}", return_dtype=pl.Utf8)
            .alias("Engagement Rate (%)"),
        ]
    )

    # === Stats dict ===
    stats = {
        "total_views": int(df["Views"].sum()),
        "total_likes": int(df["Likes"].sum()),
        "total_dislikes": int(df["Dislikes"].sum()),
        "total_comments": int(df["Comments"].sum()),
        "avg_engagement": float(df["Engagement Rate Raw"].mean()),
        "actual_playlist_count": actual_playlist_count or df.height,
        "processed_video_count": df.height,
    }

    return df, stats
