"""
Creator stats background worker: polls Supabase creator_sync_jobs table for pending/failed jobs,
fetches creator channel stats from YouTube API, updates creator_current_stats and creator_daily_stats,
and updates job status. Intelligently retries failed jobs with backoff.

This worker is completely independent from the playlist worker, running on a separate schedule
with its own configuration for frugal operation (1 creator at a time, 30-minute update cycle).

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
    CREATOR_CURRENT_STATS_TABLE,
    CREATOR_DAILY_STATS_TABLE,
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
    update_creator_stats,
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
    """Build and validate YouTube API client with consistent error handling."""
    from googleapiclient.discovery import build

    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise ValueError("YOUTUBE_API_KEY environment variable not set")

    return build("youtube", "v3", developerKey=api_key)


async def fetch_pending_syncs() -> List[Dict[str, Any]]:
    """Return list of pending creator sync jobs from creator_sync_jobs table."""
    try:
        jobs = get_pending_creator_syncs(batch_size=BATCH_SIZE)
        if jobs:
            logger.debug(f"Fetched {len(jobs)} pending creator syncs")
        return jobs
    except Exception as e:
        logger.exception(f"Failed to fetch pending creator syncs: {e}")
        return []


async def fetch_creator_channel_data(creator_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch creator channel data from YouTube API.

    Uses YouTube Data API v3 to fetch:
    - Channel statistics (subscribers, views, video count)
    - Engagement metrics from recent videos
    - Growth rates by comparing with historical data
    - Quality grade based on engagement performance

    Args:
        creator_id: YouTube channel ID (starts with UC)

    Returns:
        Dict with channel stats, or None if fetch fails
    """
    logger.info(f"[Creator] Fetching channel data for {creator_id}")

    try:
        # 1. Get channel statistics from YouTube API
        channel_stats = await _fetch_channel_statistics(creator_id)
        if not channel_stats:
            logger.error(f"Failed to fetch channel statistics for {creator_id}")
            return None

        subscriber_count = channel_stats["subscriber_count"]
        view_count = channel_stats["view_count"]
        video_count = channel_stats["video_count"]

        logger.info(
            f"[Creator] {creator_id}: {subscriber_count:,} subscribers, "
            f"{view_count:,} views, {video_count:,} videos"
        )

        # 2. Fetch recent videos to compute engagement score
        engagement_score = await _compute_engagement_score(creator_id)

        # 3. Get historical data from database for growth calculations
        growth_data = await _compute_growth_rates(
            creator_id, subscriber_count, view_count
        )

        # 4. Determine quality grade based on engagement
        quality_grade = _compute_quality_grade(engagement_score, subscriber_count)

        return {
            "subscriber_count": subscriber_count,
            "view_count": view_count,
            "video_count": video_count,
            "engagement_score": engagement_score,
            "growth_rate_7d": growth_data["growth_rate_7d"],
            "growth_rate_30d": growth_data["growth_rate_30d"],
            "quality_grade": quality_grade,
            "daily_subscriber_delta": growth_data["daily_subscriber_delta"],
            "daily_view_delta": growth_data["daily_view_delta"],
        }

    except Exception as e:
        logger.exception(f"Error fetching channel data for creator {creator_id}: {e}")
        raise


async def _fetch_channel_statistics(channel_id: str) -> Optional[Dict[str, Any]]:
    """Fetch channel statistics from YouTube Data API."""
    try:
        youtube = _build_youtube_client()

        # Request channel statistics
        response = (
            youtube.channels()
            .list(part="statistics,snippet", id=channel_id, maxResults=1)
            .execute()
        )

        if not response.get("items"):
            logger.error(f"Channel not found: {channel_id}")
            return None

        channel = response["items"][0]
        stats = channel.get("statistics", {})

        return {
            "subscriber_count": int(stats.get("subscriberCount", 0) or 0),
            "view_count": int(stats.get("viewCount", 0) or 0),
            "video_count": int(stats.get("videoCount", 0) or 0),
        }

    except Exception as e:
        logger.error(f"Failed to fetch channel statistics for {channel_id}: {e}")
        return None


async def _compute_engagement_score(channel_id: str) -> float:
    """
    Compute engagement score from recent videos.

    Engagement = (likes + comments) / views * 100
    Uses average of last 10 videos.
    """
    try:
        youtube = _build_youtube_client()

        # 1. Get channel's uploads playlist ID
        channel_resp = (
            youtube.channels()
            .list(part="contentDetails", id=channel_id, maxResults=1)
            .execute()
        )

        if not channel_resp.get("items"):
            return 0.0

        uploads_playlist_id = (
            channel_resp["items"][0]
            .get("contentDetails", {})
            .get("relatedPlaylists", {})
            .get("uploads")
        )

        if not uploads_playlist_id:
            logger.warning(f"No uploads playlist found for {channel_id}")
            return 0.0

        # 2. Get recent video IDs from uploads playlist
        playlist_resp = (
            youtube.playlistItems()
            .list(
                part="contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=10,  # Last 10 videos
            )
            .execute()
        )

        video_ids = [
            item["contentDetails"]["videoId"] for item in playlist_resp.get("items", [])
        ]

        if not video_ids:
            return 0.0

        # 3. Get video statistics
        videos_resp = (
            youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
        )

        # 4. Calculate average engagement
        engagement_scores = []
        for video in videos_resp.get("items", []):
            stats = video.get("statistics", {})
            views = int(stats.get("viewCount", 0) or 0)
            likes = int(stats.get("likeCount", 0) or 0)
            comments = int(stats.get("commentCount", 0) or 0)

            if views > 0:
                engagement = ((likes + comments) / views) * 100
                engagement_scores.append(engagement)

        if not engagement_scores:
            return 0.0

        avg_engagement = sum(engagement_scores) / len(engagement_scores)
        logger.debug(f"[Creator] {channel_id}: engagement = {avg_engagement:.2f}%")

        return round(avg_engagement, 2)

    except Exception as e:
        logger.error(f"Failed to compute engagement for {channel_id}: {e}")
        return 0.0


async def _compute_growth_rates(
    creator_id: str, current_subscribers: int, current_views: int
) -> Dict[str, Any]:
    """
    Compute growth rates by comparing with historical daily_stats.

    Returns daily deltas and 7d/30d growth rates.
    """
    try:
        # Get yesterday's stats for daily delta
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        yesterday_stats = (
            supabase_client.table(CREATOR_DAILY_STATS_TABLE)
            .select("subscriber_count, view_count")
            .eq("creator_id", creator_id)
            .eq("snapshot_date", yesterday)
            .limit(1)
            .execute()
        )

        daily_subscriber_delta = 0
        daily_view_delta = 0

        if yesterday_stats.data:
            prev_subs = yesterday_stats.data[0].get("subscriber_count", 0)
            prev_views = yesterday_stats.data[0].get("view_count", 0)
            daily_subscriber_delta = current_subscribers - prev_subs
            daily_view_delta = current_views - prev_views

        # Get 7-day-ago stats for weekly growth
        week_ago = (date.today() - timedelta(days=7)).isoformat()
        week_stats = (
            supabase_client.table(CREATOR_DAILY_STATS_TABLE)
            .select("subscriber_count")
            .eq("creator_id", creator_id)
            .eq("snapshot_date", week_ago)
            .limit(1)
            .execute()
        )

        growth_rate_7d = 0.0
        if week_stats.data:
            week_subs = week_stats.data[0].get("subscriber_count", 0)
            if week_subs > 0:
                growth_rate_7d = ((current_subscribers - week_subs) / week_subs) * 100

        # Get 30-day-ago stats for monthly growth
        month_ago = (date.today() - timedelta(days=30)).isoformat()
        month_stats = (
            supabase_client.table(CREATOR_DAILY_STATS_TABLE)
            .select("subscriber_count")
            .eq("creator_id", creator_id)
            .eq("snapshot_date", month_ago)
            .limit(1)
            .execute()
        )

        growth_rate_30d = 0.0
        if month_stats.data:
            month_subs = month_stats.data[0].get("subscriber_count", 0)
            if month_subs > 0:
                growth_rate_30d = (
                    (current_subscribers - month_subs) / month_subs
                ) * 100

        return {
            "daily_subscriber_delta": daily_subscriber_delta,
            "daily_view_delta": daily_view_delta,
            "growth_rate_7d": round(growth_rate_7d, 2),
            "growth_rate_30d": round(growth_rate_30d, 2),
        }

    except Exception as e:
        logger.error(f"Failed to compute growth rates for {creator_id}: {e}")
        return {
            "daily_subscriber_delta": 0,
            "daily_view_delta": 0,
            "growth_rate_7d": 0.0,
            "growth_rate_30d": 0.0,
        }


def _compute_quality_grade(engagement_score: float, subscriber_count: int) -> str:
    """
    Compute quality grade based on engagement score.

    Grading criteria:
    - A+: >=5% engagement
    - A: >=3% engagement
    - B+: >=1% engagement
    - B: >=0.5% engagement
    - C: <0.5% engagement
    """
    if engagement_score >= 5.0:
        return "A+"
    elif engagement_score >= 3.0:
        return "A"
    elif engagement_score >= 1.0:
        return "B+"
    elif engagement_score >= 0.5:
        return "B"
    else:
        return "C"


async def handle_sync_job(job: Dict[str, Any]) -> bool:
    """
    Process a single creator sync job.

    Returns True if successful, False if failed.
    """
    job_id = job.get("id")
    creator_id = job.get("creator_id")

    def _log_stage(name: str):
        """Log current stage for debugging."""
        logger.info(f"[Job {job_id}] STAGE: {name}")

    _log_stage("start")
    logger.info(f"[Job {job_id}] Starting sync for creator {creator_id}")

    try:
        # Get creator details
        _log_stage("fetch-creator-metadata")
        resp = (
            supabase_client.table(CREATOR_TABLE)
            .select("channel_id, channel_name")
            .eq("id", creator_id)
            .single()
            .execute()
        )

        if not resp.data:
            raise Exception("Creator not found")

        channel_id = resp.data["channel_id"]
        logger.info(f"[Job {job_id}] Found creator {channel_id}")

        # Mark as processing
        _log_stage("mark-processing")
        if not mark_creator_sync_processing(job_id):
            logger.error(f"[Job {job_id}] Failed to mark as processing")
            return False

        # Fetch creator stats from YouTube
        _log_stage("fetch-channel-data")
        stats = await fetch_creator_channel_data(channel_id)

        if not stats:
            raise Exception("Failed to fetch channel data")

        logger.info(f"[Job {job_id}] Retrieved stats: {stats}")

        # Update database
        _log_stage("update-stats")
        if not update_creator_stats(creator_id, stats, job_id):
            raise Exception("Failed to update creator stats")

        _log_stage("completed")
        logger.info(
            f"[Job {job_id}] Sync completed successfully for creator {creator_id}"
        )
        return True

    except Exception as e:
        error_msg = str(e)
        error_trace = traceback.format_exc()
        logger.exception(f"[Job {job_id}] Sync failed: {error_msg}")

        # Handle failure with retry logic
        resp = (
            supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
            .select("retry_count")
            .eq("id", job_id)
            .single()
            .execute()
        )

        retry_count = resp.data.get("retry_count", 0) if resp.data else 0

        if retry_count < MAX_RETRY_ATTEMPTS:
            logger.warning(
                f"[Job {job_id}] Sync failed - will retry later "
                f"(attempt {retry_count + 1}/{MAX_RETRY_ATTEMPTS}): {error_msg}"
            )

        # Mark as failed (with retry if applicable)
        if not mark_creator_sync_failed(job_id, error_msg):
            logger.error(f"[Job {job_id}] Failed to mark sync as failed")

        return False


async def worker_loop() -> int:
    """
    Main worker loop.

    Polls creator_sync_jobs table, processes pending syncs, and retries failed jobs.
    Runs for MAX_RUNTIME, then exits.

    Returns: Total number of syncs processed.
    """
    start_time = time.time()
    syncs_processed = 0

    logger.info("Starting creator worker loop...")
    logger.info(
        f"Configuration: poll_interval={POLL_INTERVAL}s, batch_size={BATCH_SIZE}, "
        f"max_runtime={MAX_RUNTIME // 60}m, max_retries={MAX_RETRY_ATTEMPTS}, "
        f"retry_base={RETRY_BACKOFF_BASE}s"
    )

    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time >= MAX_RUNTIME:
            logger.info(
                f"Creator worker reached max runtime ({MAX_RUNTIME // 60}m). "
                f"Processed {syncs_processed} syncs. Exiting."
            )
            break

        remaining_time = MAX_RUNTIME - elapsed_time
        if int(elapsed_time) % 300 == 0 and elapsed_time > 0:
            logger.info(
                f"Creator worker progress: {int(elapsed_time // 60)}m elapsed, "
                f"{int(remaining_time // 60)}m remaining, {syncs_processed} syncs processed"
            )

        try:
            # Fetch pending syncs
            pending_syncs = await fetch_pending_syncs()

            if not pending_syncs:
                sleep_time = min(POLL_INTERVAL, remaining_time)
                if sleep_time <= 0:
                    logger.info("No time remaining, exiting worker loop")
                    break
                logger.debug(f"No pending syncs, sleeping {sleep_time}s")
                await asyncio.sleep(sleep_time)
                continue

            logger.info(f"Processing {len(pending_syncs)} pending sync(s)")

            for job in pending_syncs:
                job_id = job.get("id")
                if not job_id:
                    logger.warning("Got job without id, skipping")
                    continue

                # Check time before processing
                if time.time() - start_time >= MAX_RUNTIME:
                    logger.info("Max runtime reached while processing jobs, stopping")
                    break

                # Process job
                success = await handle_sync_job(job)

                if success:
                    syncs_processed += 1

            # Small delay between polling cycles
            await asyncio.sleep(min(0.5, remaining_time))

        except Exception:
            logger.exception(
                "Unexpected error in creator worker loop; sleeping before retry"
            )
            error_sleep_time = min(POLL_INTERVAL, remaining_time)
            if error_sleep_time > 0:
                await asyncio.sleep(error_sleep_time)
            else:
                logger.info("No time remaining for error retry sleep, exiting")
                break

    logger.info(
        f"Creator worker loop completed. Total syncs processed: {syncs_processed}"
    )
    return syncs_processed


def main():
    """Entrypoint for running the creator worker."""
    logger.info("Starting ViralVibes creator worker (frugal mode)...")
    logger.info(
        f"Configuration: poll_interval={POLL_INTERVAL}s, batch_size={BATCH_SIZE}, "
        f"max_runtime={MAX_RUNTIME // 60}m, max_retries={MAX_RETRY_ATTEMPTS}, "
        f"retry_base={RETRY_BACKOFF_BASE}s"
    )

    try:
        asyncio.run(init())
        syncs_processed = asyncio.run(worker_loop())
        logger.info(
            f"Creator worker completed successfully. Total syncs processed: {syncs_processed}"
        )

    except KeyboardInterrupt:
        logger.info("Creator worker interrupted by user, exiting gracefully.")
    except SystemExit:
        raise
    except Exception as e:
        logger.exception(f"Creator worker failed with unexpected error: {e}")
        raise SystemExit(1) from e


if __name__ == "__main__":
    main()
