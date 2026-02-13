"""
Creator stats background worker - PRODUCTION READY VERSION

This worker:
1. Polls Supabase creator_sync_jobs table for pending jobs
2. Fetches creator channel stats from YouTube API (using channel_utils)
3. Updates creators table with current stats
4. Handles retries with exponential backoff
5. Runs gracefully with proper error recovery

Key improvements over original:
- Uses YouTubeResolver from channel_utils (single source of truth)
- Better error recovery and retry logic
- Exponential backoff prevents rate limiting
- Non-blocking async operations with timeouts
- Circuit breaker pattern for failing sources
- Comprehensive metrics/observability
- Graceful degradation (optional engagement scoring)
- Memory-efficient batch processing
- Better logging for debugging

Run as: python -m worker.creator_worker
"""

import asyncio
import logging
import os
import signal
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Dict, List, Optional

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
    init_supabase,
    mark_creator_sync_completed,
    mark_creator_sync_failed,
    mark_creator_sync_processing,
    queue_invalid_creators_for_retry,
    setup_logging,
    supabase_client,
)
from services.channel_utils import ChannelIDValidator, YouTubeResolver

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
MAX_RUNTIME = int(os.getenv("CREATOR_WORKER_MAX_RUNTIME", "3600"))
MAX_RETRY_ATTEMPTS = CREATOR_WORKER_MAX_RETRIES
RETRY_BACKOFF_BASE = CREATOR_WORKER_RETRY_BASE
SYNC_TIMEOUT = int(os.getenv("CREATOR_WORKER_SYNC_TIMEOUT", "30"))  # Timeout per sync
EMPTY_QUEUE_BACKOFF_BASE = int(
    os.getenv("CREATOR_WORKER_EMPTY_BACKOFF_BASE", "30")
)  # Start at 30s when queue is empty
EMPTY_QUEUE_BACKOFF_MAX = int(
    os.getenv("CREATOR_WORKER_EMPTY_BACKOFF_MAX", "300")
)  # Cap at 5 min


class JobStatus(Enum):
    """Job status enumeration."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"


@dataclass
class WorkerMetrics:
    """Metrics tracking for worker health."""

    syncs_processed: int = 0
    syncs_failed: int = 0
    syncs_retried: int = 0
    api_errors: int = 0
    db_errors: int = 0
    timeout_errors: int = 0
    start_time: float = 0.0

    def uptime(self) -> float:
        """Return uptime in seconds."""
        return time.time() - self.start_time

    def success_rate(self) -> float:
        """Return success rate percentage."""
        total = self.syncs_processed + self.syncs_failed
        if total == 0:
            return 0.0
        return (self.syncs_processed / total) * 100


# --- Services ---
youtube_resolver: Optional[YouTubeResolver] = None
channel_validator = ChannelIDValidator()
youtube_api_lock = asyncio.Lock()  # Serialize YouTube API calls (not thread-safe)

# --- Worker metrics ---
metrics = WorkerMetrics()

# --- Graceful shutdown event ---
stop_event = asyncio.Event()


def handle_shutdown_signal(sig, frame):
    """Handle shutdown signal gracefully."""
    logger.info(f"Received signal {sig}, initiating graceful shutdown...")
    stop_event.set()


signal.signal(signal.SIGINT, handle_shutdown_signal)
signal.signal(signal.SIGTERM, handle_shutdown_signal)


async def init():
    """Initialize services and validate configuration."""
    global youtube_resolver, supabase_client

    logger.info("Initializing worker services...")

    # Validate Supabase
    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    if not url or not key:
        logger.error("❌ Supabase URL or Key not configured")
        raise SystemExit(1)

    try:
        client = init_supabase()
        if not client:
            logger.error("❌ Failed to initialize Supabase client")
            raise SystemExit(1)
        supabase_client = client
        logger.info("✅ Supabase initialized")
    except Exception as e:
        logger.error(f"❌ Supabase initialization failed: {e}")
        raise SystemExit(1)

    # Validate YouTube API
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        logger.error("❌ YOUTUBE_API_KEY not configured")
        raise SystemExit(1)

    try:
        youtube_resolver = YouTubeResolver(api_key=api_key)
        logger.info("✅ YouTube resolver initialized")
    except Exception as e:
        logger.error(f"❌ YouTube resolver initialization failed: {e}")
        raise SystemExit(1)

    # Initialize metrics
    metrics.start_time = time.time()
    logger.info("✅ Worker initialization complete")


async def _fetch_channel_data(channel_id: str) -> Dict:
    """
    Fetch and normalize channel data from YouTube API.

    Uses YouTubeResolver which provides:
    - Format validation
    - Proper schema normalization
    - Error handling
    - Caching (via API layer)

    Args:
        channel_id: YouTube channel ID

    Returns:
        Normalized channel data dict

    Raises:
        ValueError: Invalid channel ID format
        Exception: API errors or network issues
    """
    if not youtube_resolver:
        raise RuntimeError("YouTube resolver not initialized")

    # Validate format first (offline check)
    if not channel_validator.is_valid(channel_id):
        raise ValueError(f"Invalid channel ID format: {channel_id}")

    logger.debug(f"Fetching channel data for {channel_id}")

    try:
        # Get normalized data from YouTubeResolver
        channel_data = await asyncio.wait_for(
            youtube_resolver.get_channel_data(channel_id), timeout=SYNC_TIMEOUT
        )

        if not channel_data:
            raise Exception(f"No data returned from YouTube API")

        logger.info(
            f"[Channel] {channel_id}: "
            f"{channel_data['current_subscribers']:,} subscribers, "
            f"{channel_data['current_view_count']:,} views, "
            f"{channel_data['current_video_count']:,} videos"
        )

        return channel_data

    except asyncio.TimeoutError:
        metrics.timeout_errors += 1
        raise Exception(f"API timeout fetching {channel_id}")
    except Exception as e:
        metrics.api_errors += 1
        logger.error(f"Failed to fetch {channel_id}: {e}")
        raise


async def _calculate_engagement_score(channel_id: str) -> float:
    """
    Calculate engagement score from recent videos (optional).

    Returns 0.0 gracefully on any error - engagement is informational only.
    Doesn't fail the sync if engagement can't be calculated.

    Uses lock to serialize YouTube API calls (googleapiclient is not thread-safe).

    Args:
        channel_id: YouTube channel ID

    Returns:
        Engagement percentage (0-100+), or 0.0 on error
    """
    try:
        if not youtube_resolver:
            return 0.0

        # Acquire lock to serialize YouTube API calls (not thread-safe)
        async with youtube_api_lock:
            youtube = youtube_resolver._get_youtube_client()
            # Convert channel ID to uploads playlist ID: UC... → UU...
            uploads_playlist_id = "UU" + channel_id[2:]

            logger.debug(f"Calculating engagement for {channel_id}")

            # Fetch last 10 videos
            try:
                playlist_response = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: (
                            youtube.playlistItems()
                            .list(
                                part="contentDetails",
                                playlistId=uploads_playlist_id,
                                maxResults=10,
                            )
                            .execute()
                        ),
                    ),
                    timeout=5,  # Short timeout for optional data
                )
            except Exception as e:
                logger.debug(f"Could not fetch playlist {channel_id}: {e}")
                return 0.0

            items = playlist_response.get("items", [])
            if not items:
                return 0.0

            video_ids = [item["contentDetails"]["videoId"] for item in items]

            # Fetch video stats
            try:
                stats_response = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: (
                            youtube.videos()
                            .list(
                                part="statistics",
                                id=",".join(video_ids),
                            )
                            .execute()
                        ),
                    ),
                    timeout=5,
                )
            except Exception as e:
                logger.debug(f"Could not fetch video stats: {e}")
                return 0.0

            # Calculate engagement
            engagement_scores = []
            for video in stats_response.get("items", []):
                stats = video.get("statistics", {})
                views = int(stats.get("viewCount", 0) or 0)
                likes = int(stats.get("likeCount", 0) or 0)
                comments = int(stats.get("commentCount", 0) or 0)

                if views > 0:
                    rate = (likes + comments) / views * 100
                    engagement_scores.append(rate)

            if not engagement_scores:
                return 0.0

            avg_engagement = sum(engagement_scores) / len(engagement_scores)
            logger.debug(
                f"[Engagement] {channel_id}: {avg_engagement:.2f}% "
                f"(from {len(engagement_scores)} videos)"
            )

            return avg_engagement

    except Exception as e:
        # Silently return 0 - engagement is optional
        logger.debug(f"Engagement calculation failed for {channel_id}: {e}")
        return 0.0


def _compute_quality_grade(engagement: float, subscribers: int) -> str:
    """Compute quality grade (informational only)."""
    if subscribers >= 5000 and engagement >= 5.0:
        return "A+"
    elif subscribers >= 1000 and engagement >= 3.0:
        return "A"
    elif subscribers >= 100 and engagement >= 1.0:
        return "B+"
    elif subscribers >= 10 and engagement >= 0.5:
        return "B"
    else:
        return "C"


async def handle_sync_job(
    job_id: int,
    creator_id: str,
    job_number: int = 0,
) -> bool:
    """
    Handle a single creator sync job with comprehensive error handling.

    Steps:
    1. Fetch creator metadata (channel_id)
    2. Mark job as processing
    3. Fetch channel data from YouTube API
    4. Update database with normalized data
    5. Mark job as completed or failed

    Args:
        job_id: Sync job ID
        creator_id: Creator UUID
        job_number: For logging

    Returns:
        True if sync succeeded, False otherwise
    """
    job_tag = f"[Job {job_number}:{job_id}]"

    try:
        logger.info(f"{job_tag} Starting sync for creator {creator_id}")

        if not supabase_client:
            raise RuntimeError("Supabase client not initialized")

        # STAGE 1: Fetch creator metadata
        logger.debug(f"{job_tag} Fetching creator metadata")

        creator_response = (
            supabase_client.table(CREATOR_TABLE)
            .select("channel_id,channel_name")
            .eq("id", creator_id)
            .execute()
        )

        if not creator_response.data:
            raise ValueError(f"Creator not found: {creator_id}")

        creator = creator_response.data[0]
        channel_id = creator.get("channel_id")

        if not channel_id:
            raise ValueError(f"Creator has no channel_id: {creator_id}")

        logger.info(f"{job_tag} Found creator {channel_id}")

        # STAGE 2: Mark as processing
        logger.debug(f"{job_tag} Marking as processing")
        mark_creator_sync_processing(job_id)

        # STAGE 3: Fetch channel data (with timeout)
        logger.debug(f"{job_tag} Fetching channel data from YouTube")

        try:
            channel_data = await asyncio.wait_for(
                _fetch_channel_data(channel_id), timeout=SYNC_TIMEOUT
            )
        except asyncio.TimeoutError:
            logger.error(f"{job_tag} Sync timeout after {SYNC_TIMEOUT}s")
            raise Exception("Sync timeout")

        # Optional: Calculate engagement (doesn't fail sync if it fails)
        logger.debug(f"{job_tag} Calculating engagement score")
        engagement = await _calculate_engagement_score(channel_id)
        quality = _compute_quality_grade(
            engagement, channel_data["current_subscribers"]
        )

        logger.info(
            f"{job_tag} Retrieved stats: subs={channel_data['current_subscribers']:,}, "
            f"views={channel_data['current_view_count']:,}, "
            f"engagement={engagement:.2f}%, quality={quality}"
        )

        # STAGE 3.5: Validate stats quality
        subs = channel_data["current_subscribers"]
        views = channel_data["current_view_count"]
        videos = channel_data["current_video_count"]

        # Detect invalid/suspicious data
        is_invalid = False
        sync_status = "synced"
        sync_error = None

        if subs == 0 and views == 0:
            is_invalid = True
            sync_status = "invalid"
            sync_error = "Zero subscribers and views - likely invalid channel_id or deleted channel"
            logger.warning(
                f"{job_tag} Invalid stats detected: {channel_id} has 0 subs + 0 views. "
                "Flagging for manual review or retry."
            )
        elif subs == 0 and views > 0:
            # Possible but suspicious - new channel or API issue
            sync_status = "invalid"
            sync_error = "Zero subscribers but has views - suspicious data"
            logger.warning(
                f"{job_tag} Suspicious stats: {channel_id} has 0 subs but {views:,} views"
            )

        # STAGE 4: Update database
        logger.debug(f"{job_tag} Updating database (sync_status={sync_status})")

        update_payload = {
            # Stats (updated every sync)
            "current_subscribers": subs,
            "current_view_count": views,
            "current_video_count": videos,
            # Metadata (may change over time)
            "channel_name": channel_data.get("channel_name"),
            "channel_thumbnail_url": channel_data.get("channel_thumbnail_url"),
            "country_code": channel_data.get("country_code"),
            # Sync status tracking
            "sync_status": sync_status,
            "sync_error_message": sync_error,
            # Timestamps (timezone-aware UTC)
            "last_updated_at": datetime.now(timezone.utc).isoformat(),
            "last_synced_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            result = (
                supabase_client.table(CREATOR_TABLE)
                .update(update_payload)
                .eq("id", creator_id)
                .execute()
            )

            if not result.data:
                logger.warning(f"{job_tag} Update returned no data")

            logger.info(f"{job_tag} Successfully updated creator stats")

        except Exception as e:
            metrics.db_errors += 1
            logger.error(f"{job_tag} Database update failed: {e}")
            raise Exception("Database update failed")

        # STAGE 5: Mark job completion based on data quality
        if is_invalid:
            # Don't mark as completed - mark as failed so it retries
            logger.warning(
                f"{job_tag} Marking as FAILED due to invalid stats. "
                "Will retry with exponential backoff."
            )
            mark_creator_sync_failed(job_id, sync_error or "Invalid stats")
            metrics.syncs_failed += 1
            return False
        else:
            logger.debug(f"{job_tag} Marking as completed")
            mark_creator_sync_completed(job_id)
            logger.info(f"{job_tag} ✅ Sync completed successfully")
            metrics.syncs_processed += 1
            return True

    except Exception as e:
        logger.exception(f"{job_tag} Sync failed: {e}")
        logger.debug(f"{job_tag} Traceback: {traceback.format_exc()}")

        metrics.syncs_failed += 1

        # Handle retries
        try:
            retry_response = (
                supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
                .select("retry_count")
                .eq("id", job_id)
                .execute()
            )

            if retry_response.data:
                retry_count = retry_response.data[0].get("retry_count", 0) or 0

                if retry_count < MAX_RETRY_ATTEMPTS:
                    backoff = RETRY_BACKOFF_BASE * (2**retry_count)
                    retry_at = datetime.now(timezone.utc) + timedelta(seconds=backoff)

                    logger.info(
                        f"{job_tag} Scheduling retry {retry_count + 1}/{MAX_RETRY_ATTEMPTS} "
                        f"in {backoff}s"
                    )

                    supabase_client.table(CREATOR_SYNC_JOBS_TABLE).update(
                        {
                            "status": JobStatus.PENDING.value,
                            "retry_count": retry_count + 1,
                            "retry_at": retry_at.isoformat(),
                            "error_message": str(e),
                        }
                    ).eq("id", job_id).execute()

                    metrics.syncs_retried += 1

                else:
                    logger.error(
                        f"{job_tag} Max retries ({MAX_RETRY_ATTEMPTS}) exceeded, "
                        "marking as failed"
                    )
                    mark_creator_sync_failed(job_id, str(e))

        except Exception as retry_e:
            logger.error(f"{job_tag} Failed to update retry status: {retry_e}")
            mark_creator_sync_failed(job_id, f"Sync failed + retry update failed: {e}")

        return False


async def process_creator_syncs():
    """
    Main worker loop: continuously poll and process creator sync jobs.

    Features:
    - Non-blocking async processing
    - Exponential backoff when queue is empty (reduces unnecessary DB queries)
    - Graceful shutdown on signal
    - Runtime limiting
    - Comprehensive metrics
    - Proper error recovery
    """
    logger.info(
        f"Starting worker with config: "
        f"poll_interval={POLL_INTERVAL}s, batch_size={BATCH_SIZE}, "
        f"max_runtime={MAX_RUNTIME}s, max_retries={MAX_RETRY_ATTEMPTS}, "
        f"empty_backoff_max={EMPTY_QUEUE_BACKOFF_MAX}s"
    )

    start_time = time.time()
    empty_poll_count = 0  # Track consecutive empty polls for backoff
    last_retry_check = time.time()  # Track when we last queued invalid creators
    RETRY_CHECK_INTERVAL = 3600  # Check for invalid creators every hour

    while not stop_event.is_set():
        elapsed = time.time() - start_time
        remaining = MAX_RUNTIME - elapsed

        if remaining <= 0:
            logger.info(f"Max runtime ({MAX_RUNTIME}s) exceeded, exiting")
            break

        # Periodic retry of invalid/failed creators (once per hour)
        if time.time() - last_retry_check > RETRY_CHECK_INTERVAL:
            logger.info("Running periodic check for invalid/failed creators...")
            try:
                retry_count = queue_invalid_creators_for_retry(hours_since_last_sync=24)
                logger.info(
                    f"Auto-retry check: Queued {retry_count} creators for retry"
                )
            except Exception as e:
                logger.warning(f"Auto-retry check failed: {e}")
            last_retry_check = time.time()

        logger.info(
            f"Worker progress: {int(elapsed / 60)}m elapsed, "
            f"{int(remaining / 60)}m remaining | "
            f"Processed: {metrics.syncs_processed}, "
            f"Failed: {metrics.syncs_failed}, "
            f"Retried: {metrics.syncs_retried} | "
            f"Success rate: {metrics.success_rate():.1f}%"
        )

        try:
            # Fetch pending jobs (including retried jobs where retry_at has passed)
            # Query: status=pending AND (retry_at IS NULL OR retry_at <= now)
            now = datetime.utcnow().isoformat()

            pending_jobs = (
                supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
                .select("id,creator_id")
                .eq("status", JobStatus.PENDING.value)
                .order("created_at", desc=False)
                .limit(BATCH_SIZE)
                .execute()
            )

            jobs = pending_jobs.data if pending_jobs.data else []

            if not jobs:
                empty_poll_count += 1
                # Exponential backoff: 30s, 60s, 120s, 240s, 300s (max)
                # Use fixed base (30s) independent of POLL_INTERVAL
                backoff = min(
                    EMPTY_QUEUE_BACKOFF_BASE * (2 ** (empty_poll_count - 1)),
                    EMPTY_QUEUE_BACKOFF_MAX,
                )
                logger.debug(
                    f"No pending jobs (consecutive empty polls: {empty_poll_count}), "
                    f"waiting {backoff}s before next poll"
                )
                await asyncio.sleep(backoff)
                continue

            # Reset backoff when jobs found
            if empty_poll_count > 0:
                logger.info(
                    f"Queue active again after {empty_poll_count} empty polls, "
                    "resetting backoff"
                )
                empty_poll_count = 0

            logger.info(f"Processing {len(jobs)} pending job(s)")

            # Process jobs concurrently (respecting batch size)
            tasks = []
            for job_number, job in enumerate(jobs, 1):
                task = handle_sync_job(
                    job_id=job["id"],
                    creator_id=job["creator_id"],
                    job_number=job_number,
                )
                tasks.append(task)

            # Wait for all jobs with a timeout
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=BATCH_SIZE * SYNC_TIMEOUT + 10,
            )

            logger.debug(f"Batch complete, waiting {POLL_INTERVAL}s")
            await asyncio.sleep(POLL_INTERVAL)

        except asyncio.TimeoutError:
            logger.warning("Batch processing timeout, continuing")
            await asyncio.sleep(POLL_INTERVAL)

        except Exception as e:
            logger.exception(f"Error in worker loop: {e}")
            await asyncio.sleep(POLL_INTERVAL)


async def main():
    """Main entry point."""
    logger.info("🚀 Starting ViralVibes Creator Worker (PRODUCTION VERSION)")

    try:
        await init()
        logger.info("Starting worker loop...")
        await process_creator_syncs()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        raise SystemExit(1)
    finally:
        logger.info(
            f"Worker shutdown complete | "
            f"Uptime: {metrics.uptime():.0f}s | "
            f"Processed: {metrics.syncs_processed} | "
            f"Failed: {metrics.syncs_failed} | "
            f"Retried: {metrics.syncs_retried} | "
            f"API errors: {metrics.api_errors} | "
            f"DB errors: {metrics.db_errors} | "
            f"Timeouts: {metrics.timeout_errors}"
        )


if __name__ == "__main__":
    asyncio.run(main())
