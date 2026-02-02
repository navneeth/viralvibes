"""
Creator stats background worker: polls Supabase creator_sync_jobs table for pending/failed jobs,
fetches creator channel stats from YouTube API, updates creators table with current stats,
and updates job status. Intelligently retries failed jobs with backoff.

This worker is completely independent from the playlist worker, running on a separate schedule
with its own configuration for frugal operation (1 creator at a time, 30-day update cycle).

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

        # 3. Determine quality grade based on engagement
        quality_grade = _compute_quality_grade(engagement_score, subscriber_count)

        return {
            "subscriber_count": subscriber_count,
            "view_count": view_count,
            "video_count": video_count,
            "engagement_score": engagement_score,
            "quality_grade": quality_grade,
        }

    except Exception as e:
        logger.exception(f"Error fetching channel data for creator {creator_id}: {e}")
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

        return {
            "subscriber_count": int(statistics.get("subscriberCount", 0) or 0),
            "view_count": int(statistics.get("viewCount", 0) or 0),
            "video_count": int(statistics.get("videoCount", 0) or 0),
        }

    except Exception as e:
        logger.error(f"Failed to fetch channel statistics for {channel_id}: {e}")
        return None


async def _compute_engagement_score(channel_id: str) -> float:
    """
    Compute engagement score from recent uploaded videos.

    Engagement = (likes + comments) / views * 100
    Uses average of last 10 videos from the channel's uploads playlist.

    API flow:
    1. channels.list (part=contentDetails) -> get uploads playlist ID
    2. playlistItems.list (part=contentDetails) -> get video IDs
    3. videos.list (part=statistics) -> get engagement metrics

    Reference: https://developers.google.com/youtube/v3/docs/channels/list
               https://developers.google.com/youtube/v3/docs/playlistItems/list
               https://developers.google.com/youtube/v3/docs/videos/list

    Args:
        channel_id: YouTube channel ID (e.g., "UCxxxxx")

    Returns:
        Average engagement percentage (0.0-100.0)
    """
    try:
        youtube = _build_youtube_client()

        # Step 1: Get channel's uploads playlist ID from contentDetails.relatedPlaylists.uploads
        # https://developers.google.com/youtube/v3/docs/channels/list
        channel_request = youtube.channels().list(part="contentDetails", id=channel_id)
        channel_response = channel_request.execute()

        if not channel_response.get("items"):
            logger.warning(f"Channel not found: {channel_id}")
            return 0.0

        content_details = channel_response["items"][0].get("contentDetails", {})
        related_playlists = content_details.get("relatedPlaylists", {})
        uploads_playlist_id = related_playlists.get("uploads")

        if not uploads_playlist_id:
            logger.warning(f"No uploads playlist found for channel: {channel_id}")
            return 0.0

        # Step 2: Get recent video IDs from uploads playlist
        # https://developers.google.com/youtube/v3/docs/playlistItems/list
        playlist_request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=10,  # Retrieve last 10 videos
        )
        playlist_response = playlist_request.execute()

        video_ids = [
            item["contentDetails"]["videoId"]
            for item in playlist_response.get("items", [])
        ]

        if not video_ids:
            logger.debug(f"No videos found in uploads playlist for {channel_id}")
            return 0.0

        # Step 3: Get video statistics (views, likes, comments)
        # https://developers.google.com/youtube/v3/docs/videos/list
        videos_request = youtube.videos().list(
            part="statistics", id=",".join(video_ids)
        )
        videos_response = videos_request.execute()

        # Step 4: Calculate average engagement across all videos
        engagement_scores = []
        for video in videos_response.get("items", []):
            statistics = video.get("statistics", {})
            view_count = int(statistics.get("viewCount", 0) or 0)
            like_count = int(statistics.get("likeCount", 0) or 0)
            comment_count = int(statistics.get("commentCount", 0) or 0)

            if view_count > 0:
                engagement_rate = ((like_count + comment_count) / view_count) * 100
                engagement_scores.append(engagement_rate)

        if not engagement_scores:
            logger.debug(f"No engagement data available for {channel_id}")
            return 0.0

        avg_engagement = sum(engagement_scores) / len(engagement_scores)
        logger.debug(
            f"[Creator] {channel_id}: engagement = {avg_engagement:.2f}% "
            f"(calculated from {len(engagement_scores)} videos)"
        )

        return round(avg_engagement, 2)

    except Exception as e:
        logger.error(f"Failed to compute engagement for {channel_id}: {e}")
        return 0.0


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
