# services/playlist_loader.py

import io
import logging
from typing import Any, Dict, Optional

import polars as pl

from db import (
    get_cached_playlist_stats,
    upsert_playlist_stats,
)

logger = logging.getLogger(__name__)


def load_cached_or_stub(
    playlist_url: str,
    meter_max: int,
    user_id: Optional[str] = None,  # ✅ Add user_id parameter
) -> Dict[str, Any]:
    """
    Loads cached playlist stats if available. Otherwise returns a stub payload.
    Args:
        playlist_url (str): The YouTube playlist URL.
        initial_max (int): Initial maximum value for progress meter.
        user_id (str): constraint for proper cache lookup.
        Returns: dict: A dictionary containing playlist stats and DataFrame.

    """
    # Pass user_id to cache lookup
    cached = get_cached_playlist_stats(playlist_url, user_id=user_id, check_date=True)

    if cached:
        logger.info(f"Using cached stats for playlist {playlist_url}")

        # reconstruct df other fields from cache row
        df = pl.read_json(io.BytesIO(cached["df_json"].encode("utf-8")))
        # TODO: Move this code to worker
        # if logger.isEnabledFor(logging.DEBUG):
        # logger.info("=" * 60)
        # logger.info("DataFrame columns:")
        # for col in df.columns:
        #     if col in df.columns:
        #         sample = df[col].head(1).to_list()
        #         logger.info(f"✓ {col}: {sample}")
        #     else:
        #         logger.warning(f"✗ {col} MISSING")
        # logger.info("=" * 60)
        playlist_name = cached["title"]
        channel_name = cached.get("channel_name", "")
        channel_thumbnail = cached.get("channel_thumbnail", "")
        summary_stats = cached["summary_stats"]

        # use cached video_count if present; fall back to df.height; then preview meter_max
        total = cached.get("video_count") or df.height or meter_max
        return {
            "cached": True,
            "df": df,
            "playlist_name": playlist_name,
            "channel_name": channel_name,
            "channel_thumbnail": channel_thumbnail,
            "summary_stats": summary_stats,
            "total": total,
            "cached_stats": cached,
        }

    # ----- original stub path (identical behavior) -----
    logger.warning("No cached stats found. Using stub values until worker is enabled.")

    df = pl.DataFrame([])
    playlist_name = "Unknown Playlist"
    channel_name = "Unknown Channel"
    channel_thumbnail = ""
    summary_stats = {
        "total_views": 0,
        "total_likes": 0,
        "total_dislikes": 0,
        "total_comments": 0,
        "actual_playlist_count": 0,
        "avg_duration": None,
        "avg_engagement": 0.0,
        "avg_controversy": 0.0,
    }

    # cache stub (same behavior as before)
    stats_to_cache = {
        "playlist_url": playlist_url,
        "title": playlist_name,
        "channel_name": channel_name,
        "channel_thumbnail": channel_thumbnail,
        "view_count": summary_stats.get("total_views"),
        "like_count": summary_stats.get("total_likes"),
        "dislike_count": summary_stats.get("total_dislikes"),
        "comment_count": summary_stats.get("total_comments"),
        "video_count": summary_stats.get("actual_playlist_count", df.height),
        "processed_video_count": df.height,
        "avg_duration": (
            int(summary_stats.get("avg_duration"))
            if summary_stats.get("avg_duration") is not None
            else None
        ),
        "engagement_rate": summary_stats.get("avg_engagement"),
        "controversy_score": summary_stats.get("avg_controversy", 0),
        "summary_stats": summary_stats,
        "df_json": df.write_json(),
    }
    upsert_playlist_stats(stats_to_cache)

    return {
        "cached": False,
        "df": df,
        "playlist_name": playlist_name,
        "channel_name": channel_name,
        "channel_thumbnail": channel_thumbnail,
        "summary_stats": summary_stats,
        "total": summary_stats.get("actual_playlist_count", 0),
        "cached_stats": None,
    }


# ============================================================================
# 🆕 QUICK PREVIEW: Lightweight playlist metadata (no analysis needed)
# ============================================================================


async def get_playlist_preview(playlist_url: str) -> Optional[Dict[str, Any]]:
    """
    Get lightweight playlist preview directly from YouTube API.

    This is FAST (1 API call) and shows users info before analysis starts.
    Falls back gracefully if YouTube API is unavailable.

    Args:
        playlist_url (str): YouTube playlist URL

    Returns:
        {
            "title": str,
            "channel_name": str,
            "thumbnail": str,
            "video_count": int,
            "description": str,
            "privacy_status": str,
            "published_at": str,
            "source": "youtube_api"  # Indicates live data
        }
        OR None on failure
    """
    try:
        # Lazy import to avoid bloating main.py
        from services.youtube_service import YoutubePlaylistService

        service = YoutubePlaylistService(backends=["youtubeapi"])

        # Call the lightweight get_playlist_preview method
        (
            title,
            channel,
            thumb,
            length,
            description,
            privacy_status,
            published,
        ) = await service.backend.get_playlist_preview(playlist_url)

        # Return None if preview unavailable
        if title == "Preview unavailable":
            logger.warning(f"YouTube preview unavailable for {playlist_url}")
            return None

        logger.info(f"Got YouTube preview: {title} ({length} videos) from {channel}")

        return {
            "title": title,
            "channel_name": channel,
            "thumbnail": thumb,
            "video_count": length,
            "description": description,
            "privacy_status": privacy_status,
            "published_at": published,
            "source": "youtube_api",  # Important: indicates this is live data
        }

    except Exception as e:
        logger.error(f"Failed to get YouTube preview for {playlist_url}: {e}")
        return None
