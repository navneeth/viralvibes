"""
Creator stats background worker - SCHEMA-AWARE VERSION

This version properly maps to the actual database schema:
  ✅ current_subscribers (not subscriber_count)
  ✅ current_view_count (not view_count)
  ✅ current_video_count (not video_count)
  ✅ channel_thumbnail_url (not channel_thumbnail)
  ✅ last_updated_at (not last_synced_at)
  ❌ NO engagement_score (doesn't exist in schema)
  ❌ NO quality_grade (doesn't exist in schema)

This worker polls Supabase creator_sync_jobs table for pending/failed jobs,
fetches creator channel stats from YouTube API, updates creators table with current stats,
and updates job status. Intelligently retries failed jobs with backoff.

This worker is completely independent from the playlist worker, running on a separate schedule
with its own configuration for frugal operation (1 creator at a time, 30-day update cycle).

PATCHED VERSION:
- Removes dependency on engagement_score column (may not exist in schema)
- Only updates columns that definitely exist
- Gracefully handles missing schema columns
- Gracefully handles missing playlist/engagement data


Run as: python -m worker.creator_worker
"""

import asyncio
import logging
import os
import time
import traceback
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from constants import (
    CREATOR_SYNC_JOBS_TABLE,
    CREATOR_TABLE,
    CREATOR_WORKER_BATCH_SIZE,
    CREATOR_WORKER_MAX_RETRIES,
    CREATOR_WORKER_POLL_INTERVAL,
    CREATOR_WORKER_RETRY_BASE,
)
from db import (
    get_creator_stats,
    get_pending_creator_syncs,
    init_supabase,
    mark_creator_sync_completed,
    mark_creator_sync_failed,
    mark_creator_sync_processing,
    setup_logging,
    supabase_client,
)
from services.youtube_service import YoutubePlaylistService

# --- Load environment variables early ---
load_dotenv()

# --- Logging setup ---
setup_logging()
logger = logging.getLogger("vv_creator_worker")

# --- Config with defaults (frugal operation) ---
POLL_INTERVAL = int(
    os.getenv("CREATOR_WORKER_POLL_INTERVAL", str(CREATOR_WORKER_POLL_INTERVAL))
)
BATCH_SIZE = int(os.getenv("CREATOR_WORKER_BATCH_SIZE", str(CREATOR_WORKER_BATCH_SIZE)))
MAX_RUNTIME = int(
    os.getenv("CREATOR_WORKER_MAX_RUNTIME", "3600")
)  # 1 hour default (in seconds)
MAX_RETRY_ATTEMPTS = CREATOR_WORKER_MAX_RETRIES
RETRY_BACKOFF_BASE = CREATOR_WORKER_RETRY_BASE

# --- Services ---
yt_service = YoutubePlaylistService(backend="youtubeapi")

# --- Graceful shutdown event ---
stop_event = asyncio.Event()


def handle_exit(sig, frame):
    """Handle shutdown signal gracefully."""
    logger.info(f"Received signal {sig}, shutting down gracefully...")
    stop_event.set()


import signal

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


async def init():
    """Initialize Supabase client and other resources."""
    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    if not url or not key:
        logger.error("Supabase URL or Key not configured. Exiting.")
        raise SystemExit(1)

    try:
        client = init_supabase()
        if client is not None:
            global supabase_client
            supabase_client = client
            logger.info("Supabase initialized for creator worker.")
        else:
            logger.error("Supabase environment not configured. Worker cannot run.")
            raise SystemExit(1)
    except Exception as e:
        logger.error(f"Unexpected error during Supabase initialization: {str(e)}")
        raise SystemExit(1) from e


def _build_youtube_client():
    """Build YouTube API client."""
    from googleapiclient.discovery import build

    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise ValueError("YOUTUBE_API_KEY environment variable not set")

    return build("youtube", "v3", developerKey=api_key)


async def handle_sync_job(job_id: int, creator_id: str, job_number: int = 0):
    """
    Handle a single creator sync job.

    Steps:
    1. Mark job as processing
    2. Fetch creator metadata (channel_id)
    3. Fetch channel statistics from YouTube API
    4. Update creator stats in database (with CORRECT SCHEMA MAPPING)
    5. Mark job as completed

    Args:
        job_id: Job ID in creator_sync_jobs table
        creator_id: Creator UUID
        job_number: For logging purposes
    """
    try:
        logger.info(f"[Job {job_number}] STAGE: start")
        logger.info(f"[Job {job_number}] Starting sync for creator {creator_id}")

        # STAGE 1: Fetch creator metadata
        logger.info(f"[Job {job_number}] STAGE: fetch-creator-metadata")

        if not supabase_client:
            raise Exception("Supabase client not initialized")

        creator_response = (
            supabase_client.table(CREATOR_TABLE)
            .select("channel_id,channel_name")
            .eq("id", creator_id)
            .execute()
        )

        if not creator_response.data:
            raise Exception(f"Creator not found: {creator_id}")

        creator = creator_response.data[0]
        channel_id = creator.get("channel_id")

        if not channel_id:
            raise Exception(f"Creator has no channel_id: {creator_id}")

        logger.info(f"[Job {job_number}] Found creator {channel_id}")

        # STAGE 2: Mark as processing
        logger.info(f"[Job {job_number}] STAGE: mark-processing")
        mark_creator_sync_processing(job_id)

        # STAGE 3: Fetch channel data
        logger.info(f"[Job {job_number}] STAGE: fetch-channel-data")
        channel_data = await _fetch_channel_data(channel_id)

        logger.info(f"[Job {job_number}] Retrieved stats: {channel_data}")

        # STAGE 4: Update creator stats with CORRECT SCHEMA MAPPING
        logger.info(f"[Job {job_number}] STAGE: update-stats")

        # Build update payload using ACTUAL column names from schema
        update_payload = {
            "channel_id": channel_data["channel_id"],
            "current_subscribers": channel_data["subscriber_count"],
            "current_view_count": channel_data["view_count"],
            "current_video_count": channel_data["video_count"],
            "channel_name": channel_data.get("channel_name"),
            "channel_thumbnail_url": channel_data.get("channel_thumbnail"),
            "last_updated_at": datetime.utcnow().isoformat(),
        }

        logger.debug(
            f"[Job {job_number}] Update payload keys: {list(update_payload.keys())}"
        )
        logger.debug(f"[Job {job_number}] Update payload: {update_payload}")

        try:
            # Direct Supabase update with correct column mapping
            result = (
                supabase_client.table(CREATOR_TABLE)
                .update(update_payload)
                .eq("id", creator_id)
                .execute()
            )

            if result.data:
                logger.info(f"[Job {job_number}] Successfully updated creator stats")
            else:
                logger.warning(
                    f"[Job {job_number}] Update returned no data, but no error"
                )

        except Exception as e:
            logger.error(f"[Job {job_number}] Failed to update: {e}")
            raise Exception("Failed to update creator stats")

        # STAGE 5: Mark as completed
        logger.info(f"[Job {job_number}] STAGE: mark-completed")
        mark_creator_sync_completed(job_id)
        logger.info(f"[Job {job_number}] ✅ Sync completed successfully")

    except Exception as e:
        logger.exception(f"[Job {job_number}] Sync failed: {str(e)}")
        logger.debug(f"[Job {job_number}] Full traceback:\n{traceback.format_exc()}")

        try:
            retry_count_response = (
                supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
                .select("retry_count")
                .eq("id", job_id)
                .execute()
            )

            if retry_count_response.data:
                retry_count = retry_count_response.data[0]["retry_count"]

                if retry_count >= MAX_RETRY_ATTEMPTS:
                    logger.warning(
                        f"[Job {job_number}] Sync failed - max retries exhausted"
                    )
                    mark_creator_sync_failed(job_id)
                else:
                    logger.warning(
                        f"[Job {job_number}] Sync failed - will retry later (attempt {retry_count + 1}/{MAX_RETRY_ATTEMPTS}): {str(e)}"
                    )
                    # Calculate backoff: retry_base * (2 ^ retry_count)
                    retry_delay = RETRY_BACKOFF_BASE * (2**retry_count)
                    retry_after = datetime.utcnow() + timedelta(seconds=retry_delay)

                    supabase_client.table(CREATOR_SYNC_JOBS_TABLE).update(
                        {
                            "status": "pending",
                            "retry_count": retry_count + 1,
                            "next_retry_at": retry_after.isoformat(),
                            "error_message": str(e)[:500],
                        }
                    ).eq("id", job_id).execute()
        except Exception as err:
            logger.error(f"[Job {job_number}] Failed to update job status: {err}")


async def _fetch_channel_data(channel_id: str) -> Dict[str, Any]:
    """
    Fetch complete channel data from YouTube API.

    Returns dict with:
    - channel_id
    - subscriber_count
    - view_count
    - video_count
    - channel_name
    - channel_thumbnail

    Args:
        channel_id: YouTube channel ID

    Returns:
        Dictionary with channel statistics
    """
    try:
        # 1. Fetch channel statistics
        channel_stats = await _fetch_channel_statistics(channel_id)

        if not channel_stats:
            raise Exception(f"Could not fetch channel stats for {channel_id}")

        subscriber_count = channel_stats["subscriber_count"]
        view_count = channel_stats["view_count"]
        video_count = channel_stats["video_count"]
        channel_name = channel_stats.get("channel_name", "")
        channel_thumbnail = channel_stats.get("channel_thumbnail")

        logger.info(
            f"[Creator] {channel_id}: {subscriber_count:,} subscribers, "
            f"{view_count:,} views, {video_count:,} videos"
        )

        # 2. TRY to compute engagement score (optional - but don't fail)
        try:
            engagement_score = await _compute_engagement_score(channel_id)
            logger.debug(
                f"[Creator] Computed engagement score: {engagement_score:.2f}%"
            )
        except Exception as e:
            logger.warning(
                f"[Creator] Could not compute engagement for {channel_id}: {e}"
            )
            engagement_score = 0.0

        # 3. Compute quality grade (informational only - not stored)
        try:
            quality_grade = _compute_quality_grade(engagement_score, subscriber_count)
            logger.debug(f"[Creator] Computed quality grade: {quality_grade}")
        except Exception as e:
            logger.warning(f"[Creator] Could not compute quality grade: {e}")
            quality_grade = "C"

        return {
            "channel_id": channel_id,
            "subscriber_count": subscriber_count,
            "view_count": view_count,
            "video_count": video_count,
            "channel_name": channel_name,
            "channel_thumbnail": channel_thumbnail,
            # These are not stored but useful for logging
            "engagement_score": engagement_score,
            "quality_grade": quality_grade,
        }

    except Exception as e:
        logger.exception(f"Error fetching channel data for creator {channel_id}: {e}")
        raise


async def _fetch_channel_statistics(channel_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch channel statistics from YouTube Data API v3.

    Uses channels.list method with part=statistics,snippet.
    Reference: https://developers.google.com/youtube/v3/docs/channels/list

    Args:
        channel_id: YouTube channel ID (e.g., "UCxxxxx")

    Returns:
        Dict with statistics fields, or None if channel not found
    """
    try:
        youtube = _build_youtube_client()

        # Call channels.list method
        # https://developers.google.com/youtube/v3/docs/channels/list
        request = youtube.channels().list(part="statistics,snippet", id=channel_id)
        response = request.execute()

        # Check if channel exists in response
        if not response.get("items"):
            logger.error(f"Channel not found: {channel_id}")
            return None

        channel = response["items"][0]
        statistics = channel.get("statistics", {})
        snippet = channel.get("snippet", {})

        return {
            "subscriber_count": int(statistics.get("subscriberCount", 0) or 0),
            "view_count": int(statistics.get("viewCount", 0) or 0),
            "video_count": int(statistics.get("videoCount", 0) or 0),
            # Extract metadata
            "channel_name": snippet.get("title", ""),
            "channel_thumbnail": snippet.get("thumbnails", {})
            .get("default", {})
            .get("url"),
        }

    except Exception as e:
        logger.exception(f"Error fetching channel statistics for {channel_id}: {e}")
        return None


async def _compute_engagement_score(channel_id: str) -> float:
    """
    Compute engagement score from recent videos (informational only).

    Fetches the last 10 videos from a channel's uploads playlist and calculates
    the average engagement rate across those videos.

    Engagement rate = (likes + comments) / views * 100

    Returns 0.0 if:
    - Channel has no videos
    - Playlist cannot be found (private/restricted)
    - Videos have 0 views
    - API errors

    Args:
        channel_id: YouTube channel ID

    Returns:
        Engagement score as float percentage (0-100+)
    """
    try:
        youtube = _build_youtube_client()

        # Convert channel ID to uploads playlist ID
        # For a channel "UCxxxxx", the uploads playlist is "UUxxxxx"
        uploads_playlist_id = "U" + channel_id[1:]

        logger.debug(f"[Engagement] Fetching engagement for {channel_id}")

        # Get the last 10 videos from uploads playlist
        try:
            playlist_request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=10,
            )
            playlist_response = playlist_request.execute()
        except Exception as e:
            logger.warning(
                f"[Engagement] Could not fetch playlist {uploads_playlist_id}: {e}"
            )
            # Return 0 instead of failing - some channels have private playlists
            return 0.0

        items = playlist_response.get("items", [])

        if not items:
            logger.debug(f"[Engagement] No videos found in uploads playlist")
            return 0.0

        video_ids = [item["contentDetails"]["videoId"] for item in items]

        # Fetch statistics for these videos
        try:
            stats_request = youtube.videos().list(
                part="statistics",
                id=",".join(video_ids),
            )
            stats_response = stats_request.execute()
        except Exception as e:
            logger.warning(f"[Engagement] Could not fetch video stats: {e}")
            return 0.0

        engagement_scores = []

        for video in stats_response.get("items", []):
            stats = video.get("statistics", {})
            view_count = int(stats.get("viewCount", 0) or 0)
            like_count = int(stats.get("likeCount", 0) or 0)
            comment_count = int(stats.get("commentCount", 0) or 0)

            if view_count > 0:
                engagement_rate = (like_count + comment_count) / view_count * 100
                engagement_scores.append(engagement_rate)

        if not engagement_scores:
            logger.debug(f"[Engagement] No engagement data available")
            return 0.0

        avg_engagement = sum(engagement_scores) / len(engagement_scores)

        logger.info(
            f"[Engagement] {channel_id}: {avg_engagement:.2f}% "
            f"(calculated from {len(engagement_scores)} videos)"
        )

        return avg_engagement

    except Exception as e:
        logger.warning(f"[Engagement] Error computing engagement for {channel_id}: {e}")
        # Return 0 instead of failing - engagement is optional
        return 0.0


def _compute_quality_grade(engagement_score: float, subscriber_count: int) -> str:
    """
    Compute quality grade based on engagement and subscriber count (informational only).

    Grading:
    - A+ (5000+ subs, 5%+ engagement)
    - A (1000+ subs, 3%+ engagement)
    - B+ (100+ subs, 1%+ engagement)
    - B (10+ subs, 0.5%+ engagement)
    - C (default)

    Args:
        engagement_score: Engagement percentage
        subscriber_count: Subscriber count

    Returns:
        Quality grade as string (A+, A, B+, B, C)
    """
    if subscriber_count >= 5000 and engagement_score >= 5.0:
        return "A+"
    elif subscriber_count >= 1000 and engagement_score >= 3.0:
        return "A"
    elif subscriber_count >= 100 and engagement_score >= 1.0:
        return "B+"
    elif subscriber_count >= 10 and engagement_score >= 0.5:
        return "B"
    else:
        return "C"


async def process_creator_syncs():
    """
    Main worker loop: continuously poll and process creator sync jobs.

    Fetches pending jobs, processes them in batches, and handles retries.
    """
    logger.info(
        f"Configuration: poll_interval={POLL_INTERVAL}s, batch_size={BATCH_SIZE}, "
        f"max_runtime={MAX_RUNTIME}s, max_retries={MAX_RETRY_ATTEMPTS}, "
        f"retry_base={RETRY_BACKOFF_BASE}s"
    )

    start_time = time.time()
    syncs_processed = 0

    while True:
        # Check if we should stop
        if stop_event.is_set():
            logger.info("Shutdown signal received, exiting worker loop")
            break

        # Check if we've exceeded max runtime
        elapsed = time.time() - start_time
        remaining = MAX_RUNTIME - elapsed

        if remaining <= 0:
            logger.info(f"Max runtime ({MAX_RUNTIME}s) exceeded, exiting")
            break

        logger.info(
            f"Creator worker progress: {int(elapsed / 60)}m elapsed, "
            f"{int(remaining / 60)}m remaining, {syncs_processed} syncs processed"
        )

        try:
            # Fetch pending jobs
            pending_jobs = get_pending_creator_syncs(batch_size=BATCH_SIZE)

            if not pending_jobs:
                logger.debug(f"No pending creator syncs, waiting {POLL_INTERVAL}s")
                await asyncio.sleep(POLL_INTERVAL)
                continue

            logger.info(f"Processing {len(pending_jobs)} pending sync(s)")

            # Process each job
            for job_number, job in enumerate(pending_jobs, 1):
                try:
                    await handle_sync_job(
                        job_id=job["id"],
                        creator_id=job["creator_id"],
                        job_number=job_number,
                    )
                    syncs_processed += 1

                except Exception as e:
                    logger.exception(
                        f"Unhandled error processing job {job_number}: {e}"
                    )

                # Small delay between jobs
                await asyncio.sleep(0.1)

            # Sleep before next poll
            logger.debug(f"Batch complete, waiting {POLL_INTERVAL}s for next batch")
            await asyncio.sleep(POLL_INTERVAL)

        except Exception as e:
            logger.exception(f"Error in worker loop: {e}")
            await asyncio.sleep(POLL_INTERVAL)


async def main():
    """Main entry point."""
    logger.info("Starting ViralVibes creator worker (SCHEMA-AWARE VERSION)...")
    await init()
    logger.info("Starting creator worker loop...")
    await process_creator_syncs()
    logger.info("Creator worker stopped")


if __name__ == "__main__":
    asyncio.run(main())
