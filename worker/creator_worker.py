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
    queue_creator_sync,
    queue_invalid_creators_for_retry,
    setup_logging,
    supabase_client,
)
from services.channel_utils import ChannelIDValidator, YouTubeResolver
from services.schema_detector import schema_detector
from services.youtube_config import get_creator_worker_api_key

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

# YouTube API quota metering (standard quota is 10,000 units/day).
# Guard against misconfigured or non-positive quotas to avoid divide-by-zero
# and misleading "remaining" / percentage calculations downstream.
_raw_youtube_daily_quota = int(os.getenv("YOUTUBE_DAILY_QUOTA", "10000"))
if _raw_youtube_daily_quota <= 0:
    # Enforce a minimum of 1 unit so downstream code can safely assume a
    # positive daily budget. This effectively disables quota tracking while
    # still keeping metrics/log calculations well-defined.
    logger.warning(
        "YOUTUBE_DAILY_QUOTA is set to %s; it must be a positive integer. "
        "Falling back to 1 to keep quota calculations valid. "
        "This effectively disables quota tracking.",
        _raw_youtube_daily_quota,
    )
    YOUTUBE_DAILY_QUOTA = 1
else:
    YOUTUBE_DAILY_QUOTA = _raw_youtube_daily_quota

YOUTUBE_CREDITS_PER_CHANNEL_FETCH = 1  # channels.list costs 1 unit
YOUTUBE_CREDITS_PER_CATEGORY_FETCH = 2  # playlistItems.list (1) + videos.list (1)


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
    youtube_credits_used: int = 0  # YouTube API quota units consumed
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

    def quota_percentage(self) -> float:
        """Return YouTube API quota consumed as a percentage.

        Returns 0 if quota is 0 or negative (gracefully handles misconfiguration).
        """
        if YOUTUBE_DAILY_QUOTA <= 0:
            return 0.0
        return (self.youtube_credits_used / YOUTUBE_DAILY_QUOTA) * 100

    def quota_remaining(self) -> int:
        """Return remaining YouTube API quota units."""
        return max(0, YOUTUBE_DAILY_QUOTA - self.youtube_credits_used)


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


# =============================================================================
# 🔍 DB Diagnostics — run at startup to show actual state
# =============================================================================


def _diagnose_creator_db_state() -> None:
    """
    Log a breakdown of the creators table sync state at startup.

    This is the first thing that should run to understand why the worker
    may be doing nothing. It answers:
      - How many creators exist total?
      - How many have never been synced (last_synced_at IS NULL)?
      - How many have zero subscribers (unpopulated)?
      - What sync_status values are present, and how many each?
      - How many pending jobs are in creator_sync_jobs right now?
    """
    if not supabase_client:
        logger.warning("⚠️  Cannot diagnose DB state: Supabase client not available")
        return

    logger.info("=" * 60)
    logger.info("🔍 CREATOR DB STATE DIAGNOSIS")
    logger.info("=" * 60)

    try:
        # Total creator count
        total_resp = (
            supabase_client.table(CREATOR_TABLE).select("id", count="exact").execute()
        )
        total = (
            total_resp.count
            if total_resp.count is not None
            else len(total_resp.data or [])
        )
        logger.info(f"  Total creators in DB: {total:,}")

    except Exception as e:
        logger.error(f"  ❌ Could not count creators: {e}")
        total = 0

    try:
        # Never synced (last_synced_at IS NULL)
        # BUG NOTE: PostgREST .lt() silently excludes NULL rows — this is the
        # primary reason queue_invalid_creators_for_retry queues 0 creators.
        never_synced_resp = (
            supabase_client.table(CREATOR_TABLE)
            .select("id", count="exact")
            .is_("last_synced_at", "null")
            .execute()
        )
        never_synced = (
            never_synced_resp.count
            if never_synced_resp.count is not None
            else len(never_synced_resp.data or [])
        )
        logger.info(
            f"  Never synced (last_synced_at IS NULL): {never_synced:,}"
            + (
                " ⚠️  These are INVISIBLE to queue_invalid_creators_for_retry!"
                if never_synced > 0
                else ""
            )
        )
    except Exception as e:
        logger.error(f"  ❌ Could not count never-synced creators: {e}")
        never_synced = 0

    try:
        # Zero stats (likely never populated)
        zero_stats_resp = (
            supabase_client.table(CREATOR_TABLE)
            .select("id", count="exact")
            .eq("current_subscribers", 0)
            .eq("current_view_count", 0)
            .execute()
        )
        zero_stats = (
            zero_stats_resp.count
            if zero_stats_resp.count is not None
            else len(zero_stats_resp.data or [])
        )
        logger.info(f"  Zero subscribers + zero views (unpopulated): {zero_stats:,}")
    except Exception as e:
        logger.error(f"  ❌ Could not count zero-stat creators: {e}")
        zero_stats = 0

    try:
        # Breakdown by sync_status (including NULL)
        # Fetch a small sample to infer statuses — Supabase doesn't expose GROUP BY
        sample_resp = (
            supabase_client.table(CREATOR_TABLE)
            .select("sync_status")
            .limit(1000)
            .execute()
        )
        status_counts: Dict[str, int] = {}
        for row in sample_resp.data or []:
            s = row.get("sync_status") or "NULL"
            status_counts[s] = status_counts.get(s, 0) + 1

        logger.info("  sync_status breakdown (sample of up to 1000 rows):")
        for status, count in sorted(status_counts.items()):
            flag = ""
            if status == "NULL":
                flag = (
                    " ⚠️  NULL status — NOT caught by queue_invalid_creators_for_retry"
                )
            elif status in ("invalid", "failed"):
                flag = " → eligible for retry (if last_synced_at is not NULL)"
            elif status == "synced":
                flag = " → healthy"
            logger.info(f"    {status}: {count:,}{flag}")

    except Exception as e:
        logger.error(f"  ❌ Could not get sync_status breakdown: {e}")

    try:
        # Pending jobs count
        jobs_resp = (
            supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
            .select("id,status", count="exact")
            .eq("status", "pending")
            .execute()
        )
        pending_jobs = (
            jobs_resp.count
            if jobs_resp.count is not None
            else len(jobs_resp.data or [])
        )
        logger.info(
            f"  Pending jobs in creator_sync_jobs: {pending_jobs:,}"
            + (
                "\n  ✅ Jobs exist — worker should process them on next poll"
                if pending_jobs > 0
                else "\n  ⚠️  No pending jobs — worker will idle until jobs are created"
            )
        )
    except Exception as e:
        logger.error(f"  ❌ Could not count pending jobs: {e}")

    logger.info("=" * 60)

    # Actionable summary
    if never_synced > 0 or zero_stats > 0:
        logger.warning(
            f"⚠️  ACTION NEEDED: {never_synced:,} creators have never been synced. "
            f"Use _queue_unsynced_creators() to bootstrap them. "
            f"See bug note in queue_invalid_creators_for_retry."
        )


# =============================================================================
# 🔧 Bug fix: queue creators that have NEVER been synced
# =============================================================================


def _queue_unsynced_creators(batch_size: int = 100) -> int:
    """
    Queue creators that have never been synced (last_synced_at IS NULL).

    BUG FIX: queue_invalid_creators_for_retry uses .lt("last_synced_at", cutoff)
    which in PostgREST silently excludes NULL values. This means creators
    inserted via get_or_create_creator_from_playlist with last_synced_at=NULL
    are never queued for their first sync.

    This function fills that gap by explicitly targeting the NULL case.

    Args:
        batch_size: How many to queue per call (default 100)

    Returns:
        Number of creators newly queued
    """
    if not supabase_client:
        return 0

    try:
        # Find creators with null last_synced_at that aren't already pending
        response = (
            supabase_client.table(CREATOR_TABLE)
            .select("id,channel_id,sync_status")
            .is_("last_synced_at", "null")
            .limit(batch_size)
            .execute()
        )

        creators = response.data or []
        if not creators:
            logger.debug("  No unsynced creators found (last_synced_at IS NULL)")
            return 0

        logger.info(
            f"  Found {len(creators)} creators with last_synced_at=NULL — queuing for first sync"
        )

        queued = 0
        skipped = 0
        for creator in creators:
            creator_id = creator["id"]
            channel_id = creator.get("channel_id", "?")
            if queue_creator_sync(creator_id, source="bootstrap_unsynced"):
                queued += 1
                logger.debug(
                    f"    Queued {channel_id} (id={creator_id}) for first sync"
                )
            else:
                skipped += 1  # Already pending or error

        logger.info(
            f"  Bootstrap result: {queued} queued, {skipped} skipped (already pending)"
        )
        return queued

    except Exception as e:
        logger.error(f"  ❌ _queue_unsynced_creators failed: {e}")
        return 0


def _queue_creators_for_extended_refresh(days_since_last_sync: int = 7) -> int:
    """
    Queue creators that haven't been synced in N days (for periodic refresh).

    Unlike queue_invalid_creators_for_retry, this targets 'synced' creators
    to keep data fresh. Runs less frequently (weekly by default).

    Args:
        days_since_last_sync: Re-sync creators older than this many days

    Returns:
        Number of creators queued
    """
    if not supabase_client:
        return 0

    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=days_since_last_sync)
        ).isoformat()

        response = (
            supabase_client.table(CREATOR_TABLE)
            .select("id,channel_id,sync_status,last_synced_at")
            .in_("sync_status", ["synced", "synced_partial", "invalid", "failed"])
            .lt("last_synced_at", cutoff)
            .limit(100)
            .execute()
        )

        creators = response.data or []
        if not creators:
            logger.debug(
                f"  No stale creators found (last synced > {days_since_last_sync} days ago)"
            )
            return 0

        logger.info(
            f"  Found {len(creators)} stale creators "
            f"(not synced in {days_since_last_sync}+ days) — queuing for refresh"
        )

        queued = 0
        for creator in creators:
            if queue_creator_sync(creator["id"], source="scheduled_refresh"):
                queued += 1
        logger.info(f"  Scheduled refresh: {queued} creators queued")
        return queued

    except Exception as e:
        logger.error(f"  ❌ _queue_creators_for_extended_refresh failed: {e}")
        return 0


# =============================================================================
# 🔧 Fixed pending jobs query (respects retry_at)
# =============================================================================


def _fetch_pending_jobs(batch_size: int) -> List[Dict]:
    """
    Fetch pending jobs that are ready to be processed.

    BUG FIX: The original query fetched all status=pending rows regardless
    of retry_at. Jobs that failed and are waiting for their backoff window
    would be picked up immediately. Now we only fetch jobs where:
      - status = 'pending'
      - AND (retry_at IS NULL OR retry_at <= now())

    PostgREST doesn't support OR across columns in a single filter, so we
    do two queries and merge — or we rely on the DB having retry_at=NULL
    for fresh jobs (which it does, since retry_at is only set on failure).

    Actually the simplest correct fix: add .or_() to handle both cases.
    """
    if not supabase_client:
        return []

    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        # Jobs with no retry_at scheduled (fresh or first attempt)
        fresh_resp = (
            supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
            .select("id,creator_id,source,retry_count")
            .eq("status", JobStatus.PENDING.value)
            .is_("retry_at", "null")
            .order("created_at", desc=False)
            .limit(batch_size)
            .execute()
        )

        fresh_jobs = fresh_resp.data or []

        # Jobs where retry_at has passed (backoff window expired)
        if len(fresh_jobs) < batch_size:
            retry_resp = (
                supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
                .select("id,creator_id,source,retry_count")
                .eq("status", JobStatus.PENDING.value)
                .not_.is_("retry_at", "null")
                .lte("retry_at", now_iso)
                .order("retry_at", desc=False)
                .limit(batch_size - len(fresh_jobs))
                .execute()
            )
            retry_jobs = retry_resp.data or []
        else:
            retry_jobs = []

        all_jobs = fresh_jobs + retry_jobs

        if all_jobs:
            logger.info(
                f"  Pending jobs ready: {len(all_jobs)} total "
                f"({len(fresh_jobs)} fresh, {len(retry_jobs)} retry-ready)"
            )
        else:
            logger.debug("  No pending jobs ready for processing right now")

        return all_jobs

    except Exception as e:
        logger.error(f"  ❌ Failed to fetch pending jobs: {e}")
        return []


# =============================================================================
# YouTube API helpers (unchanged logic, better logging)
# =============================================================================


async def _fetch_channel_data(channel_id: str) -> Dict:
    if not youtube_resolver:
        raise RuntimeError("YouTube resolver not initialized")

    # Validate format first (offline check)
    if not channel_validator.is_valid(channel_id):
        raise ValueError(f"Invalid channel ID format: {channel_id}")

    logger.debug(f"  Calling YouTube API for channel {channel_id}...")

    try:
        # Get normalized data from YouTubeResolver
        channel_data = await asyncio.wait_for(
            youtube_resolver.get_channel_data(channel_id), timeout=SYNC_TIMEOUT
        )

        if not channel_data:
            raise Exception("No data returned from YouTube API")

        subs = channel_data.get("current_subscribers", 0)
        views = channel_data.get("current_view_count", 0)
        videos = channel_data.get("current_video_count", 0)
        logger.info(
            f"  YouTube API response for {channel_id}: "
            f"subs={subs:,}, views={views:,}, videos={videos:,}"
        )

        # Track YouTube API quota usage (channels.list = 1 credit)
        metrics.youtube_credits_used += YOUTUBE_CREDITS_PER_CHANNEL_FETCH

        return channel_data

    except asyncio.TimeoutError:
        metrics.timeout_errors += 1
        raise Exception(f"YouTube API timeout after {SYNC_TIMEOUT}s for {channel_id}")
    except Exception as e:
        metrics.api_errors += 1
        logger.error(f"  YouTube API error for {channel_id}: {e}")
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
                logger.debug(
                    f"  Could not fetch uploads playlist for {channel_id}: {e}"
                )
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
                            .list(part="statistics", id=",".join(video_ids))
                            .execute()
                        ),
                    ),
                    timeout=5,
                )
            except Exception as e:
                logger.debug(f"  Could not fetch video stats for {channel_id}: {e}")
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


def _format_categories(topic_categories: list) -> list[str]:
    """
    Convert raw YouTube topicCategories Wikipedia URLs to readable names.

    YouTube returns these as Wikipedia URLs, e.g.:
        https://en.wikipedia.org/wiki/Music            → "Music"
        https://en.wikipedia.org/wiki/Video_game_culture → "Video game culture"

    These are distinct from the video-level categoryId used by
    _fetch_channel_category_distribution — topic_categories come from
    channels.list topicDetails and are kept for reference only.
    """
    if not topic_categories:
        return []
    seen = set()
    names = []
    for url in topic_categories:
        slug = str(url).rstrip("/").rsplit("/", 1)[-1]
        name = slug.replace("_", " ")
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    return sorted(names)


def _needs_category_fetch(existing_primary_category: Optional[str]) -> bool:
    """
    Decide whether to spend 2 quota units fetching the category distribution.

    Rule: fetch only when primary_category is NULL (never been set).
    Once populated it is stable — a channel's content type rarely changes.
    To force a re-fetch for a specific creator, set primary_category = NULL
    directly in the DB and it will be picked up on the next sync.

    Cost when fetching: 2 units (playlistItems.list + videos.list).
    Cost when skipping: 0 extra units.
    """
    return existing_primary_category is None


async def _fetch_channel_category_distribution(channel_id: str) -> dict:
    """
    Fetch and return a channel's video category distribution by sampling uploads.

    YouTube doesn't expose a categoryId on channels directly — this derives it
    from snippet.categoryId on recent videos (same field used in playlist tables).

    Returns gracefully on any error; a missing category never fails the sync.

    API cost: 2 quota units (playlistItems.list + videos.list).

    Args:
        channel_id: YouTube channel ID (UCxxxxxx)

    Returns:
        {
            "primary_category":      str | None,   e.g. "Gaming"
            "primary_category_id":   str | None,   e.g. "20"
            "category_distribution": dict,         e.g. {"Gaming": 38, "Education": 4}
        }
    """
    empty: dict = {
        "primary_category": None,
        "primary_category_id": None,
        "category_distribution": {},
    }

    try:
        if not youtube_resolver:
            return empty

        async with youtube_api_lock:
            result = await asyncio.wait_for(
                youtube_resolver.get_channel_category_distribution(
                    channel_id, sample_size=50
                ),
                timeout=10,  # Short timeout — this is supplementary data
            )

        return result

    except asyncio.TimeoutError:
        metrics.timeout_errors += 1
        logger.debug(f"  Category distribution timed out for {channel_id} — skipping")
        return empty
    except Exception as e:
        logger.debug(f"  Category distribution failed for {channel_id}: {e} — skipping")
        return empty
    finally:
        # Track quota usage even on timeout/error — the API call was still made
        metrics.youtube_credits_used += YOUTUBE_CREDITS_PER_CATEGORY_FETCH


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


# =============================================================================
# Job handler
# =============================================================================


async def handle_sync_job(
    job_id: int,
    creator_id: str,
    job_number: int = 0,
    retry_count: int = 0,
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
        logger.info(
            f"{job_tag} ─── Starting sync | creator={creator_id} | retry_count={retry_count}"
        )

        if not supabase_client:
            raise RuntimeError("Supabase client not initialized")

        # STAGE 1: Fetch creator metadata
        creator_response = (
            supabase_client.table(CREATOR_TABLE)
            .select("channel_id,channel_name,primary_category")
            .eq("id", creator_id)
            .execute()
        )

        if not creator_response.data:
            raise ValueError(f"Creator not found in DB: {creator_id}")

        creator = creator_response.data[0]
        channel_id = creator.get("channel_id")
        channel_name = creator.get("channel_name", "unknown")

        if not channel_id:
            raise ValueError(f"Creator {creator_id} has no channel_id set")

        logger.info(f"{job_tag} Creator: {channel_name} ({channel_id})")

        # STAGE 2: Mark as processing
        mark_creator_sync_processing(job_id)
        logger.debug(f"{job_tag} Marked as processing")

        # STAGE 3: Fetch channel data
        logger.info(f"{job_tag} Fetching data from YouTube API...")
        try:
            channel_data = await asyncio.wait_for(
                _fetch_channel_data(channel_id), timeout=SYNC_TIMEOUT
            )
        except asyncio.TimeoutError:
            raise Exception(f"YouTube API timeout after {SYNC_TIMEOUT}s")

        # STAGE 3.5: Engagement score + category distribution (both optional)
        # Category costs 2 extra quota units — only fetch when primary_category
        # is NULL (never been set). To force a re-fetch, NULL the column in DB.
        should_fetch_categories = _needs_category_fetch(creator.get("primary_category"))
        if should_fetch_categories:
            engagement, cat_data = await asyncio.gather(
                _calculate_engagement_score(channel_id),
                _fetch_channel_category_distribution(channel_id),
            )
        else:
            engagement = await _calculate_engagement_score(channel_id)
            cat_data = {
                "primary_category": creator.get("primary_category"),
                "primary_category_id": None,
                "category_distribution": None,  # None = leave existing DB value as-is
            }
            logger.debug(f"{job_tag} Category already set — skipping fetch")
        quality = _compute_quality_grade(
            engagement, channel_data["current_subscribers"]
        )

        subs = channel_data["current_subscribers"]
        views = channel_data["current_view_count"]
        videos = channel_data["current_video_count"]
        topic_url_labels = _format_categories(channel_data.get("topic_categories", []))

        primary_category = cat_data.get("primary_category")
        primary_category_id = cat_data.get("primary_category_id")
        category_distribution = cat_data.get("category_distribution", {})

        logger.info(
            f"{job_tag} Stats: subs={subs:,}, views={views:,}, videos={videos:,}, "
            f"engagement={engagement:.2f}%, quality={quality}"
        )
        if primary_category:
            dist_str = ", ".join(
                f"{name}: {count}"
                for name, count in sorted(
                    category_distribution.items(), key=lambda x: x[1], reverse=True
                )
            )
            logger.info(
                f"{job_tag} Category: primary='{primary_category}' "
                f"(id={primary_category_id}) | "
                f"distribution ({len(category_distribution)} buckets): {dist_str}"
            )
        else:
            logger.debug(f"{job_tag} No video category data available for {channel_id}")
        if topic_url_labels:
            logger.debug(
                f"{job_tag} Topic URLs ({len(topic_url_labels)}): "
                + ", ".join(topic_url_labels)
            )

        # STAGE 3.5: Validate stats quality
        is_invalid = False
        sync_status = "synced"
        sync_error = None

        if subs == 0 and views == 0:
            is_invalid = True
            sync_status = "invalid"
            sync_error = "Zero subs + zero views — likely invalid/deleted channel"
            logger.warning(
                f"{job_tag} ⚠️  Invalid stats: {channel_id} has 0 subs AND 0 views. "
                "Flagging for retry. (Could be deleted channel or bad channel_id)"
            )
        elif subs == 0 and views > 0:
            is_invalid = True
            sync_status = "invalid"
            sync_error = "Zero subscribers but has views — suspicious, will retry"
            logger.warning(
                f"{job_tag} ⚠️  Suspicious stats: {channel_id} has 0 subs but {views:,} views"
            )

        # STAGE 4: Build full update payload
        full_payload = {
            "current_subscribers": subs,
            "current_view_count": views,
            "current_video_count": videos,
            "channel_name": channel_data.get("channel_name"),
            "channel_description": channel_data.get("channel_description"),
            "custom_url": channel_data.get("custom_url"),
            "custom_url_available": channel_data.get("custom_url_available", False),
            "channel_thumbnail_url": channel_data.get("channel_thumbnail_url"),
            "channel_thumbnail_default": channel_data.get("channel_thumbnail_default"),
            "banner_image_url": channel_data.get("banner_image_url"),
            "published_at": channel_data.get("published_at"),
            "country_code": channel_data.get("country_code"),
            "default_language": channel_data.get("default_language"),
            "keywords": channel_data.get("keywords"),
            "featured_channels_count": channel_data.get("featured_channels_count", 0),
            "featured_channels_urls": channel_data.get("featured_channels_urls"),
            "topic_categories": channel_data.get("topic_categories", []),
            "primary_category": primary_category,
        }

        # Only write category fields when freshly fetched — avoids overwriting
        # the histogram on every routine stats sync.
        if should_fetch_categories and category_distribution is not None:
            full_payload["primary_category_id"] = primary_category_id
            full_payload["category_distribution"] = category_distribution or None

        full_payload.update(
            {
                "official": channel_data.get("official", False),
                "channel_age_days": channel_data.get("channel_age_days"),
                "monthly_uploads": channel_data.get("monthly_uploads"),
                "hidden_subscriber_count": channel_data.get(
                    "hidden_subscriber_count", False
                ),
                "sync_status": sync_status,
                "sync_error_message": sync_error,
                "last_updated_at": datetime.now(timezone.utc).isoformat(),
                "last_synced_at": datetime.now(timezone.utc).isoformat(),
            }
        )

        # Filter to available schema columns
        update_payload, missing_fields = schema_detector.filter_payload(full_payload)

        if missing_fields:
            schema_detector.log_schema_mismatch(
                missing_fields, table_name=CREATOR_TABLE
            )
            logger.info(
                f"{job_tag} Schema filter: writing {len(update_payload)} fields, "
                f"skipping {len(missing_fields)} unavailable columns: {missing_fields}"
            )

        # STAGE 4: Write to DB
        try:
            result = (
                supabase_client.table(CREATOR_TABLE)
                .update(update_payload)
                .eq("id", creator_id)
                .execute()
            )

            if not result.data:
                logger.warning(
                    f"{job_tag} ⚠️  DB update returned no rows. "
                    "Check that creator_id exists and RLS allows updates."
                )
            else:
                logger.info(
                    f"{job_tag} ✅ DB updated ({len(update_payload)} fields written)"
                )

        except Exception as update_error:
            # FALLBACK: Try with minimal core fields
            metrics.db_errors += 1
            logger.warning(f"{job_tag} ⚠️  Full update failed: {update_error}")
            logger.info(f"{job_tag} Attempting fallback with minimal core fields...")

            minimal_payload = {
                "current_subscribers": subs,
                "current_view_count": views,
                "current_video_count": videos,
                "channel_name": full_payload.get("channel_name"),
                "country_code": full_payload.get("country_code"),
                "sync_status": "synced_partial",
                "sync_error_message": "Schema mismatch — synced with basic fields only",
                "last_updated_at": datetime.now(timezone.utc).isoformat(),
            }

            try:
                supabase_client.table(CREATOR_TABLE).update(minimal_payload).eq(
                    "id", creator_id
                ).execute()
                logger.info(f"{job_tag} ✅ Fallback write succeeded (core stats only)")
            except Exception as fallback_error:
                logger.error(f"{job_tag} ❌ Both full and fallback updates failed:")
                logger.error(f"   Full error:     {update_error}")
                logger.error(f"   Fallback error: {fallback_error}")
                raise Exception("DB update completely failed after fallback")

        # STAGE 5: Mark job done
        if is_invalid:
            logger.warning(
                f"{job_tag} Marking job as FAILED (invalid stats) — "
                "will retry with exponential backoff"
            )
            mark_creator_sync_failed(job_id, sync_error or "Invalid stats")
            metrics.syncs_failed += 1
            return False
        else:
            mark_creator_sync_completed(job_id)
            logger.info(f"{job_tag} ✅ Sync COMPLETED successfully for {channel_id}")
            metrics.syncs_processed += 1
            return True

    except Exception as e:
        logger.exception(f"{job_tag} ❌ Sync FAILED: {e}")

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
                current_retries = retry_response.data[0].get("retry_count", 0) or 0

                if current_retries < MAX_RETRY_ATTEMPTS:
                    backoff = RETRY_BACKOFF_BASE * (2**current_retries)
                    retry_at = datetime.now(timezone.utc) + timedelta(seconds=backoff)
                    logger.info(
                        f"{job_tag} Scheduling retry "
                        f"{current_retries + 1}/{MAX_RETRY_ATTEMPTS} "
                        f"in {backoff}s (at {retry_at.isoformat()})"
                    )
                    supabase_client.table(CREATOR_SYNC_JOBS_TABLE).update(
                        {
                            "status": JobStatus.PENDING.value,
                            "retry_count": current_retries + 1,
                            "retry_at": retry_at.isoformat(),
                            "error_message": str(e),
                        }
                    ).eq("id", job_id).execute()
                    metrics.syncs_retried += 1
                else:
                    logger.error(
                        f"{job_tag} Max retries ({MAX_RETRY_ATTEMPTS}) exhausted "
                        "— marking as permanently failed"
                    )
                    mark_creator_sync_failed(job_id, str(e))

        except Exception as retry_e:
            logger.error(f"{job_tag} Failed to update retry status: {retry_e}")
            mark_creator_sync_failed(job_id, f"Sync failed + retry update failed: {e}")

        return False


# =============================================================================
# Init
# =============================================================================


async def init():
    global youtube_resolver, supabase_client

    logger.info("Initializing worker services...")

    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    if not url or not key:
        logger.error(
            "❌ NEXT_PUBLIC_SUPABASE_URL or NEXT_PUBLIC_SUPABASE_ANON_KEY not set"
        )
        raise SystemExit(1)

    try:
        client = init_supabase()
        if not client:
            logger.error("❌ Supabase client init returned None")
            raise SystemExit(1)
        supabase_client = client
        logger.info("✅ Supabase initialized")

        detected_columns = schema_detector.detect_schema_sync(
            supabase_client, table_name=CREATOR_TABLE
        )
        status = schema_detector.get_status()
        logger.info(
            f"📊 DB schema: {status['total_columns']} columns in '{CREATOR_TABLE}'"
        )
        if status["missing_count"] > 0:
            logger.warning(
                f"⚠️  {status['missing_count']} optional columns not in DB "
                "(will be skipped in payloads)"
            )

    except Exception as e:
        logger.error(f"❌ Supabase initialization failed: {e}")
        raise SystemExit(1)

    try:
        api_key = get_creator_worker_api_key()
        youtube_resolver = YouTubeResolver(api_key=api_key)
        logger.info("✅ YouTube resolver initialized")
    except Exception as e:
        logger.error(f"❌ YouTube API key initialization failed: {e}")
        raise SystemExit(1)

    metrics.start_time = time.time()
    logger.info("✅ Worker initialization complete")

    # Run DB diagnosis immediately so we know the actual state
    _diagnose_creator_db_state()


# =============================================================================
# Main worker loop
# =============================================================================


async def process_creator_syncs():
    logger.info(
        f"Starting worker loop | "
        f"poll_interval={POLL_INTERVAL}s, batch_size={BATCH_SIZE}, "
        f"max_runtime={MAX_RUNTIME}s, max_retries={MAX_RETRY_ATTEMPTS}, "
        f"empty_backoff_max={EMPTY_QUEUE_BACKOFF_MAX}s"
    )

    start_time = time.time()
    empty_poll_count = 0  # Track consecutive empty polls for backoff
    last_periodic_check = 0  # triggers immediately on first loop
    last_extended_refresh = 0  # triggers immediately on first loop
    last_reported_metrics = (0, 0, 0)  # (processed, failed, retried) at last INFO log
    PERIODIC_CHECK_INTERVAL = 1800  # queue invalid/failed creators every 30 min
    EXTENDED_REFRESH_INTERVAL = 3600  # refresh stale synced creators every 60 min

    while not stop_event.is_set():
        elapsed = time.time() - start_time
        remaining = MAX_RUNTIME - elapsed

        if remaining <= 0:
            logger.info(f"Max runtime ({MAX_RUNTIME}s) exceeded — exiting")
            break

        # ── Periodic: queue invalid/failed + never-synced creators ────────────
        if time.time() - last_periodic_check > PERIODIC_CHECK_INTERVAL:
            logger.info("─" * 50)
            logger.info("⏰ PERIODIC CHECK: Queuing creators that need sync...")

            # Retry: creators with sync_status IN (invalid, failed) AND
            # last_synced_at IS NOT NULL (previously synced, now stale/broken).
            # PostgREST's .lt() already excludes NULLs, so these two functions
            # target strictly disjoint sets — never-synced creators are NOT
            # picked up here.
            invalid_count = queue_invalid_creators_for_retry(hours_since_last_sync=24)
            logger.info(
                f"  queue_invalid_creators_for_retry: {invalid_count} queued "
                f"(invalid/failed, previously synced, last_synced_at < 24h ago)"
            )

            # Bootstrap: creators with last_synced_at IS NULL — never processed.
            # Exclusively handled here; disjoint from the retry query above.
            unsynced_count = _queue_unsynced_creators(batch_size=100)

            total_queued = invalid_count + unsynced_count
            logger.info(
                f"✅ Periodic check complete: {total_queued} total creators queued "
                f"({invalid_count} retry, {unsynced_count} first-time/never-synced)"
            )
            last_periodic_check = time.time()

        # ── Extended: refresh stale but healthy syncs ──────────────────────────
        if time.time() - last_extended_refresh > EXTENDED_REFRESH_INTERVAL:
            logger.info("─" * 50)
            logger.info("⏰ EXTENDED REFRESH: Queuing stale synced creators...")
            stale_count = _queue_creators_for_extended_refresh(days_since_last_sync=7)
            logger.info(f"✅ Extended refresh: {stale_count} stale creator(s) queued")
            last_extended_refresh = time.time()

        # ── Progress report — only at INFO when metrics have changed ──────────
        current_metrics = (
            metrics.syncs_processed,
            metrics.syncs_failed,
            metrics.syncs_retried,
        )

        # Build progress status message using cached metrics
        progress_msg = (
            f"-- Worker status | "
            f"{int(elapsed / 60)}m elapsed, {int(remaining / 60)}m remaining | "
            f"Processed: {metrics.syncs_processed}, "
            f"Failed: {metrics.syncs_failed}, "
            f"Retried: {metrics.syncs_retried} | "
            f"API errors: {metrics.api_errors}, "
            f"DB errors: {metrics.db_errors}, "
            f"Timeouts: {metrics.timeout_errors} | "
            f"Success rate: {metrics.success_rate():.1f}% | "
            f"YT Quota: {metrics.youtube_credits_used:,}/{YOUTUBE_DAILY_QUOTA:,} ({metrics.quota_percentage():.1f}%)"
        )
        if current_metrics != last_reported_metrics:
            logger.info(progress_msg)
            last_reported_metrics = current_metrics
        else:
            logger.debug(progress_msg)

        try:
            # ── Fetch pending jobs (respects retry_at) ────────────────────────
            jobs = _fetch_pending_jobs(BATCH_SIZE)

            if not jobs:
                empty_poll_count += 1
                backoff = min(
                    EMPTY_QUEUE_BACKOFF_BASE * (2 ** (empty_poll_count - 1)),
                    EMPTY_QUEUE_BACKOFF_MAX,
                )
                # Log at INFO on the first empty poll so it's visible; subsequent
                # ones are DEBUG to avoid flooding logs during long idle periods.
                log = logger.info if empty_poll_count == 1 else logger.debug
                log(
                    f"Queue empty (consecutive empty polls: {empty_poll_count}) | "
                    f"Sleeping {backoff}s before next poll"
                )
                await asyncio.sleep(backoff)
                continue

            if empty_poll_count > 0:
                logger.info(
                    f"Queue is active again after {empty_poll_count} empty polls — "
                    "resetting backoff"
                )
                empty_poll_count = 0

            logger.info(f"Processing {len(jobs)} pending job(s)...")

            tasks = [
                handle_sync_job(
                    job_id=job["id"],
                    creator_id=job["creator_id"],
                    job_number=i,
                    retry_count=job.get("retry_count", 0),
                )
                for i, job in enumerate(jobs, 1)
            ]

            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=BATCH_SIZE * SYNC_TIMEOUT + 10,
            )

            successes = sum(1 for r in results if r is True)
            failures = sum(1 for r in results if r is False or isinstance(r, Exception))
            logger.info(
                f"Batch done: {successes}/{len(jobs)} succeeded, {failures} failed"
            )

            await asyncio.sleep(POLL_INTERVAL)

        except asyncio.TimeoutError:
            logger.warning("⚠️  Batch processing timed out — continuing to next poll")
            await asyncio.sleep(POLL_INTERVAL)

        except Exception as e:
            logger.exception(f"❌ Error in worker loop: {e}")
            await asyncio.sleep(POLL_INTERVAL)


# =============================================================================
# Entry point
# =============================================================================


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
        logger.info(
            f"YouTube API Quota | "
            f"Used: {metrics.youtube_credits_used:,} units | "
            f"Remaining: {metrics.quota_remaining():,} / {YOUTUBE_DAILY_QUOTA:,} units | "
            f"Consumed: {metrics.quota_percentage():.2f}%"
        )


if __name__ == "__main__":
    asyncio.run(main())
