"""
youtube_transforms.py
---------------------
Polars-based normalization and enrichment of playlist DataFrames.
"""

import logging
from typing import Any, Dict, Tuple

import polars as pl

from utils import format_duration, format_number, parse_iso_duration

logger = logging.getLogger(__name__)


# --- Normalize dataframe column names between yt-dlp and YouTube API ---
def normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize dataframe column names between different backends."""
    if df is None or not isinstance(df, pl.DataFrame):
        logger.warning(f"Invalid DataFrame in normalize_columns: received {type(df)}")
        return None
    if df.is_empty():
        logger.info("Empty DataFrame received in normalize_columns")
        return None

    # Canonicalize to internal schema: "Views", "Likes", "Dislikes", "Comments"
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
        # keep canonical names if already present
        "Views": "Views",
        "Likes": "Likes",
        "Dislikes": "Dislikes",
        "Comments": "Comments",
        # other fields
        "duration_string": "Duration",
        "duration": "Duration",
        "durationSec": "Duration",
        "upload_date": "Published Date",
        "publishedAt": "Published Date",
        "channel": "Channel",
        "channelTitle": "Channel",
        "channel_id": "Channel ID",
        "channelId": "Channel ID",
        "thumbnail": "Thumbnail",
        "thumbnails": "Thumbnail",
        "tags": "Tags",
    }

    # Rename columns if present
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df = df.rename({old: new})

    # Mirror canonical columns back to legacy "... Count" names if they are missing
    mirrors = [
        ("Views", "View Count"),
        ("Likes", "Like Count"),
        ("Dislikes", "Dislike Count"),
        ("Comments", "Comment Count"),
    ]
    for src, mirror in mirrors:
        if src in df.columns and mirror not in df.columns:
            df = df.with_columns(pl.col(src).alias(mirror))

    # Normalize duration to readable string if ISO 8601
    if "Duration" in df.columns:
        df = df.with_columns(
            pl.col("Duration").map_elements(
                lambda d: (
                    parse_iso_duration(d) if isinstance(d, str) and "PT" in d else d
                )
            )
        )

    # Ensure numeric types for both canonical and legacy mirrors
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
    """Calculate summary statistics for the playlist."""
    # Check if df is a Polars DataFrame or None
    if df is None or not isinstance(df, pl.DataFrame) or df.is_empty():
        logger.warning(
            f"Invalid or empty DataFrame received in _enrich_dataframe: {type(df)}"
        )
        return None, {"total_views": 0, "processed_video_count": 0}

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
    df = df.with_columns(
        [
            (
                1
                - (pl.col("Likes") - pl.col("Dislikes")).abs()
                / (pl.col("Likes") + pl.col("Dislikes"))
            )
            .fill_nan(0.0)
            .alias("Controversy"),
            (
                (pl.col("Likes") + pl.col("Dislikes") + pl.col("Comments"))
                / pl.col("Views")
            )
            .fill_nan(0.0)
            .alias("Engagement Rate Raw"),
        ]
    )
    stats = {
        "total_views": df["Views"].sum(),
        "total_likes": df["Likes"].sum(),
        "total_dislikes": df["Dislikes"].sum(),
        "total_comments": df["Comments"].sum(),
        "avg_engagement": df["Engagement Rate Raw"].mean(),
        "actual_playlist_count": actual_playlist_count or df.height,
        "processed_video_count": df.height,
    }
    df = df.with_columns(
        [
            pl.col("Views")
            .map_elements(format_number, return_dtype=pl.String)
            .alias("Views Formatted"),
            pl.col("Likes")
            .map_elements(format_number, return_dtype=pl.String)
            .alias("Likes Formatted"),
            pl.col("Dislikes")
            .map_elements(format_number, return_dtype=pl.String)
            .alias("Dislikes Formatted"),
            pl.col("Comments")
            .map_elements(format_number, return_dtype=pl.String)
            .alias("Comments Formatted"),
            pl.col("Duration")
            .map_elements(format_duration, return_dtype=pl.String)
            .alias("Duration Formatted"),
            pl.col("Controversy")
            .map_elements(lambda x: f"{x:.2%}", return_dtype=pl.String)
            .alias("Controversy Formatted"),
            pl.col("Engagement Rate Raw")
            .map_elements(lambda x: f"{x:.2%}", return_dtype=pl.String)
            .alias("Engagement Rate Formatted"),
            pl.col("Engagement Rate Raw")
            .map_elements(lambda x: f"{x:.2%}", return_dtype=pl.String)
            .alias("Engagement Rate (%)"),
        ]
    )
    return df, stats
