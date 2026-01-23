# worker/jobs.py
import asyncio
from datetime import timedelta

import polars as pl

from services.youtube_service import YoutubePlaylistService
from utils import compute_dashboard_id  # ✅ ADD THIS IMPORT

# Instantiate service once
yt_service = YoutubePlaylistService()


async def process_playlist(playlist_url: str) -> dict:
    """
    Fetch and process YouTube playlist stats for a given playlist URL.
    Returns a dictionary matching the playlist_stats schema.
    """
    # Fetch full playlist data (limited to 20 videos by default)
    (
        df,
        playlist_name,
        channel_name,
        channel_thumbnail,
        summary_stats,
    ) = await yt_service.get_playlist_data(playlist_url, max_expanded=20)

    # Compute video_count and avg_duration
    video_count = len(df) if df is not None else 0
    if video_count > 0 and "Duration Raw" in df.columns:
        # Handle NaN values in duration data using Polars
        duration_series = df.select("Duration Raw").drop_nulls()
        if len(duration_series) > 0:
            avg_seconds = duration_series["Duration Raw"].mean()
            # Polars mean() returns None for empty series, so check for that
            if avg_seconds is not None:
                avg_duration = timedelta(seconds=int(avg_seconds))
            else:
                avg_duration = timedelta(seconds=0)
        else:
            avg_duration = timedelta(seconds=0)
    else:
        avg_duration = timedelta(seconds=0)

    # Compute summary stats
    engagement_rate = summary_stats.get("avg_engagement", 0.0)
    controversy_score = (
        df["Controversy Raw"].mean()
        if video_count > 0 and "Controversy Raw" in df.columns
        else 0.0
    )

    # ✅ FIX 1: Add missing required fields
    result = {
        # ✅ Core identifiers
        "playlist_url": playlist_url,  # ✅ ADD: Authoritative identifier
        "dashboard_id": compute_dashboard_id(
            playlist_url
        ),  # ✅ ADD: Computed hash for routing
        # ✅ Playlist metadata
        "title": playlist_name,  # ✅ RENAME: 'playlist_name' → 'title' (matches DB schema)
        "channel_name": channel_name,  # ✅ ADD: Missing field
        "channel_thumbnail": channel_thumbnail,  # ✅ ADD: Missing field
        # ✅ Aggregate stats
        "view_count": summary_stats.get("total_views", 0),
        "like_count": summary_stats.get("total_likes", 0),
        "dislike_count": (
            int(df["Dislike Count Raw"].sum())
            if video_count > 0 and "Dislike Count Raw" in df.columns
            else 0
        ),
        "comment_count": summary_stats.get(
            "total_comments", 0
        ),  # ✅ FIX: Use actual value from summary_stats
        "video_count": video_count,
        "processed_video_count": video_count,  # ✅ ADD: Track how many videos were actually processed
        # ✅ Computed metrics
        "avg_duration": avg_duration,
        "engagement_rate": engagement_rate,
        "controversy_score": (
            float(controversy_score) if controversy_score else 0.0
        ),  # ✅ Ensure float
        # ✅ JSON payloads
        "df_json": (
            df.write_json() if df is not None else "[]"
        ),  # ✅ ADD: DataFrame as JSON string
        "summary_stats": summary_stats,  # ✅ ADD: Keep original summary_stats dict
        # ✅ Denormalized counters (updated separately via dashboard_events)
        "share_count": 0,  # ✅ ADD: Default to 0 (incremented by event tracking)
    }

    # ✅ DEBUG: Print available columns
    if df is not None:
        print(f"🔍 DataFrame columns: {df.columns}")
        print(f"🔍 DataFrame shape: {df.shape}")
        if len(df) > 0:
            print(f"🔍 First row sample: {df.head(1)}")

    return result
