# services/data_utils.py
"""
Data transformation and normalization utilities.
"""

import logging
from typing import Any, Dict, List

import polars as pl

logger = logging.getLogger(__name__)


def normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalize DataFrame column names between different data sources.

    Handles inconsistencies between yt-dlp and YouTube API naming conventions.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with standardized column names
    """
    if df.is_empty():
        return df

    # Define mapping from various possible names to standard names
    rename_map = {
        "title": "Title",
        "videoTitle": "Title",
        "id": "id",
        "videoId": "id",
        "view_count": "View Count",
        "viewCount": "View Count",
        "like_count": "Like Count",
        "likeCount": "Like Count",
        "comment_count": "Comment Count",
        "commentCount": "Comment Count",
        "dislike_count": "Dislike Count",
        "dislikeCount": "Dislike Count",
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
    for old_col, new_col in rename_map.items():
        if old_col in df.columns and new_col not in df.columns:
            df = df.rename({old_col: new_col})

    # Normalize duration to readable string if ISO 8601
    if "Duration" in df.columns:
        df = df.with_columns(
            pl.col("Duration").map_elements(
                lambda d: (
                    _parse_iso_duration(d) if isinstance(d, str) and "PT" in d else d
                ),
                return_dtype=pl.Int64,
            )
        )

    # Ensure numeric types for count columns
    numeric_cols = ["View Count", "Like Count", "Dislike Count", "Comment Count"]
    for col in numeric_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))

    return df


def _parse_iso_duration(duration_str: str) -> int:
    """
    Parse ISO 8601 duration string to seconds.

    Args:
        duration_str: ISO 8601 duration string (e.g., "PT1H23M45S")

    Returns:
        Duration in seconds
    """
    try:
        import isodate

        return int(isodate.parse_duration(duration_str).total_seconds())
    except (ImportError, Exception) as e:
        logger.warning(f"Failed to parse ISO duration '{duration_str}': {e}")
        return 0


def transform_api_df(api_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform YouTube API list of videos to chart-friendly format.

    This function is for backwards compatibility with existing code that
    expects data in a specific format.

    Args:
        api_list: List of video dictionaries from YouTube API

    Returns:
        List of transformed video dictionaries
    """
    transformed = []
    for idx, video in enumerate(api_list, start=1):
        transformed.append(
            {
                "Rank": video.get("Rank", idx),
                "id": video.get("id"),
                "Title": video.get("Title", "N/A"),
                "Views": video.get("Views", 0),
                "Likes": video.get("Likes", 0),
                "Dislikes": video.get("Dislikes", 0),
                "Comments": video.get("Comments", 0),
                "Duration": video.get("Duration", 0),
                "Uploader": video.get("Uploader", "N/A"),
                "Thumbnail": video.get("Thumbnail", ""),
                "Rating": video.get("Rating", 0.0),
                "Controversy": video.get("Controversy", 0.0),
                "Engagement Rate Raw": video.get("Engagement Rate Raw", 0.0),
            }
        )
    return transformed


def create_skeleton_dataframe(
    entries: List[Dict[str, Any]], channel_name: str
) -> pl.DataFrame:
    """
    Create a skeleton DataFrame with basic video info (for fast initial display).

    This is useful when you want to show users something quickly before
    fetching full details.

    Args:
        entries: List of video entries (minimal info)
        channel_name: Default channel name

    Returns:
        Skeleton DataFrame
    """
    skeleton_rows = [
        {
            "Rank": idx + 1,
            "id": entry.get("id"),
            "Title": entry.get("title", "N/A"),
            "Views": entry.get("view_count", 0) or 0,
            "Likes": 0,
            "Dislikes": 0,
            "Comments": 0,
            "Duration": entry.get("duration", 0) or 0,
            "Uploader": entry.get("uploader", channel_name),
            "Thumbnail": entry.get("thumbnail", ""),
            "Rating": None,
        }
        for idx, entry in enumerate(entries)
        if entry
    ]
    return pl.DataFrame(skeleton_rows)


def merge_skeleton_with_expanded(
    skeleton_df: pl.DataFrame, expanded_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Merge skeleton DataFrame with expanded data.

    Prefers expanded values where available, falls back to skeleton values.

    Args:
        skeleton_df: Basic video info
        expanded_df: Detailed video info

    Returns:
        Merged DataFrame
    """
    if skeleton_df.is_empty():
        return expanded_df
    if expanded_df.is_empty():
        return skeleton_df

    # Join on video ID
    merged = skeleton_df.join(expanded_df, on="id", how="left", suffix="_exp")

    # Prefer expanded values where available using coalesce
    for col in ["Views", "Likes", "Dislikes", "Comments", "Duration", "Rating"]:
        exp_col = f"{col}_exp"
        if exp_col in merged.columns:
            merged = merged.with_columns(
                pl.coalesce([pl.col(exp_col), pl.col(col)]).alias(col)
            )

    # Drop the "_exp" columns
    exp_cols = [c for c in merged.columns if c.endswith("_exp")]
    if exp_cols:
        merged = merged.drop(exp_cols)

    return merged
