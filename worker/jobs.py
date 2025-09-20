# worker/jobs.py
import asyncio
from datetime import timedelta

import polars as pl

from youtube_service import YoutubePlaylistService

# Instantiate service once
yt_service = YoutubePlaylistService()


async def process_playlist(playlist_url: str) -> dict:
    """
    Fetch and process YouTube playlist stats for a given playlist URL.
    Returns a dictionary matching the playlist_stats schema.
    """
    # Fetch full playlist data (limited to 20 videos by default)
    df, playlist_name, channel_name, channel_thumbnail, summary_stats = (
        await yt_service.get_playlist_data(playlist_url, max_expanded=20)
    )

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

    return {
        "playlist_name": playlist_name,
        "view_count": summary_stats.get("total_views", 0),
        "like_count": summary_stats.get("total_likes", 0),
        "dislike_count": (
            int(df["Dislike Count Raw"].sum())
            if video_count > 0 and "Dislike Count Raw" in df.columns
            else 0
        ),
        "comment_count": 0,  # You can add comment count logic if needed
        "video_count": video_count,
        "avg_duration": avg_duration,
        "engagement_rate": engagement_rate,
        "controversy_score": controversy_score,
    }
