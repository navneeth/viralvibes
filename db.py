"""
Database operations module.
Handles Supabase client initialization, logging setup, and caching functionality.
"""

from __future__ import annotations

import io
import json
import logging
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Protocol, NamedTuple

from supabase import Client, create_client
from tenacity import retry, stop_after_attempt, wait_exponential

from constants import (
    CREATOR_REDISCOVERY_THRESHOLD_DAYS,
    CREATOR_SYNC_JOBS_TABLE,
    CREATOR_TABLE,
    CREATOR_UPDATE_INTERVAL_DAYS,
    CREATOR_WORKER_MAX_RETRIES,
    CREATOR_WORKER_RETRY_BASE,
    PLAYLIST_JOBS_TABLE,
    PLAYLIST_STATS_TABLE,
    SIGNUPS_TABLE,
    JobStatus,
)
from utils import compute_dashboard_id, safe_get_value

# Use a dedicated DB logger
logger = logging.getLogger("vv_db")


# ==============================================================
# 🧩 Protocol-based Dependency Injection
# ==============================================================


class SupabaseLike(Protocol):
    """Protocol to allow fake/mocked Supabase clients in tests."""

    def table(self, name: str) -> Any: ...


# Global Supabase client# Global Supabase client
supabase_client: Optional[SupabaseLike] = None


def set_supabase_client(client: SupabaseLike) -> None:
    """Dependency injection hook for tests."""
    global supabase_client
    supabase_client = client
    logger.info("[DB] Supabase client overridden")


def setup_logging():
    """Configure logging for the application.

    This function should be called at application startup.
    It configures the logging format and level.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True,
)
def init_supabase() -> Optional[Client]:
    """Initialize Supabase client with retry logic and proper error handling.

    Returns:
        Optional[Client]: Supabase client if initialization succeeds, None otherwise

    Note:
        - Retries up to 3 times with exponential backoff
        - Returns None instead of raising exceptions
        - Logs errors for debugging
    """
    global supabase_client

    # Return existing client if already initialized
    if supabase_client is not None:
        return supabase_client

    url: str = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")
    key: str = os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY", "")
    if not url or not key:
        logger.warning(
            "Missing Supabase environment variables - running without Supabase"
        )
        return None

    try:
        client = create_client(url, key)

        # Test the connection
        client.auth.get_session()

        supabase_client = client
        logger.info("Supabase client initialized successfully")
        return client

    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {e}")
        return None


def _is_empty_json(json_str: Optional[str]) -> bool:
    """Check if JSON string represents empty/null data."""
    if not json_str:
        return True

    stripped = json_str.strip()
    if stripped in ("[]", "{}", "null", '""'):
        return True

    # Additional check: try parsing and check if actually empty
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, (list, dict)) and len(parsed) == 0:
            return True
    except json.JSONDecodeError:
        pass

    return False


# --- General DB Helpers ---
def upsert_row(table: str, payload: dict, conflict_fields: List[str] = None) -> bool:
    """Inserts or updates a row in a given table.

    Args:
        table (str): The name of the table.
        payload (dict): The data to insert or update.
        conflict_fields (List[str], optional): Columns to use for conflict resolution.

    Returns:
        bool: True if the operation was successful, False otherwise.
    """
    if not supabase_client:
        logger.warning("Supabase client not available for upsert")
        return False
    try:
        query = supabase_client.table(table)
        if conflict_fields:
            logger.debug(
                f"[DB] Upserting to {table} with conflict resolution on {conflict_fields}"
            )
            query = query.upsert(payload, on_conflict=",".join(conflict_fields))
        else:
            logger.debug(f"[DB] Inserting to {table} (no conflict resolution)")
            query = query.insert(payload)
        response = query.execute()
        logger.debug(f"[DB] Upsert response for table {table}: {response.data}")
        return bool(response.data)
    except Exception as e:
        logger.exception(f"Error upserting row in {table}: {e}")
        return False


# --- Playlist Caching and Job Management ---
def get_cached_playlist_stats(
    playlist_url: str, user_id: Optional[str] = None, check_date: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Fetch the most recent cached playlist stats for a given playlist URL,
    scoped to a specific user (or anonymous if user_id is None).

    - If user_id is None: only anonymous rows (shared across all users) are considered
    - If user_id is provided: only that user's rows are considered
    - If check_date is True: restrict to today's snapshot

    Args:
        playlist_url (str): The YouTube playlist URL to check.
        check_date (bool): If True, only returns today's cache.

    Returns:
        Optional[Dict[str, Any]]: Cached stats if found, None otherwise.
    """
    if not supabase_client:
        logger.warning("Supabase client not available for caching")
        return None

    try:
        query = (
            supabase_client.table(PLAYLIST_STATS_TABLE)
            .select("*")
            .eq("playlist_url", playlist_url)
        )

        # scope cache by ownership
        if user_id is None:
            query = query.is_("user_id", None)  # anonymous cache only
        else:
            query = query.eq("user_id", user_id)  # user-specific cache only

        # Optional same-day cache restriction
        if check_date:
            query = query.eq(
                "processed_date", datetime.now(timezone.utc).date().isoformat()
            )

        response = query.order("processed_on", desc=True).limit(1).execute()

        if response.data and len(response.data) > 0:
            row = response.data[0]

            # --- Validate the integrity of the cached data ---
            df_json = row.get("df_json")
            # Check if df_json is missing, empty, or represents an empty list/object.
            if _is_empty_json(df_json):
                logger.warning(
                    f"[Cache] Found invalid/empty cache entry for {playlist_url}"
                    f"(user={user_id}). Treating as miss."
                )
                return None

            logger.info(f"[Cache] Hit for playlist: {playlist_url} (user={user_id})")

            # Deserialize JSON fields
            try:
                from utils import deserialize_dataframe

                row["df"] = deserialize_dataframe(df_json)
            except Exception as e:
                logger.error(
                    f"[Cache] Failed to deserialize DataFrame for {playlist_url}: {e}"
                )
                return None

            if row.get("summary_stats"):
                try:
                    row["summary_stats"] = json.loads(row["summary_stats"])
                except json.JSONDecodeError as e:
                    logger.error(
                        f"[Cache] Failed to parse summary_stats for {playlist_url}: {e}"
                    )
                    row["summary_stats"] = {}

            return row
        else:
            logger.info(f"[Cache] Miss for playlist: {playlist_url} (user={user_id})")
            return None

    except Exception as e:
        logger.exception(
            f"Error checking cache for playlist {playlist_url} (user={user_id}): {e}"
        )
        return None


@dataclass
class UpsertResult:
    """Result of upserting playlist stats to the database."""

    source: str  # 'cache', 'fresh', 'error'
    df_json: Optional[str] = None
    summary_stats_json: Optional[str] = None
    error: Optional[str] = None
    raw_row: Optional[Dict[str, Any]] = None

    @property
    def success(self) -> bool:
        """Check if operation was successful."""
        return self.source in ("cache", "fresh")


def upsert_playlist_stats(stats: Dict[str, Any]) -> UpsertResult:
    """
    Main entrypoint for playlist stats caching:
    - Return cached stats if they exist for today.
    - Otherwise insert fresh stats and return an UpsertResult.

    Note: This function intentionally does NOT return a Polars DataFrame.

    Required in stats:
    - playlist_url: str
    - user_id: Optional[str] (None for anonymous)
    """
    playlist_url = stats.get("playlist_url")
    if not playlist_url:
        logger.error("No playlist_url provided in stats")
        return UpsertResult(source="error", error="Missing playlist_url")

    # --- Prevent caching empty DataFrames ---
    df = stats.get("df")
    if df is None or getattr(df, "is_empty", lambda: True)():
        logger.warning(
            f"Attempted to cache empty or missing DataFrame for {playlist_url}. Aborting cache."
        )
        return UpsertResult(source="error", error="Empty DataFrame provided")

    # Extract user_id from stats (can be None)
    user_id = stats.get("user_id")

    # --- Check for existing cache ---
    cached = get_cached_playlist_stats(playlist_url, user_id=user_id, check_date=True)

    if cached:
        logger.info(
            f"[Cache] Returning cached stats for {playlist_url} (user={user_id})"
        )
        # Prefer any stored df_json present in the raw row, otherwise reserialize the in-memory df
        cached_df_json = cached.get("df_json")
        if not cached_df_json and cached.get("df") is not None:
            try:
                cached_df_json = cached["df"].write_json()
            except Exception as e:
                logger.exception(
                    f"[Cache] Failed to re-serialize df for {playlist_url}: {e}"
                )
                cached_df_json = None

        # Ensure summary_stats is returned as JSON string when possible
        summary_stats_json = cached.get("summary_stats")
        if isinstance(summary_stats_json, dict):
            try:
                summary_stats_json = json.dumps(summary_stats_json)
            except Exception as e:
                logger.exception(
                    f"[Cache] Failed to serialize summary_stats for {playlist_url}: {e}"
                )
                summary_stats_json = None

        return UpsertResult(
            source="cache",
            df_json=cached_df_json,
            summary_stats_json=summary_stats_json,
            raw_row=cached,
        )

    # --- Prepare for fresh insert ---
    try:
        df_json = df.write_json() if df is not None else None
    except Exception as e:
        logger.exception(f"Error serializing df for {playlist_url}: {e}")
        df_json = None

    try:
        summary_stats_json = json.dumps(stats.get("summary_stats", {}))
    except Exception as e:
        logger.exception(f"Error serializing summary_stats for {playlist_url}: {e}")
        summary_stats_json = None

    # If serialization failed, do not insert an incomplete row; return error result
    if not df_json or not summary_stats_json:
        err_msg = (
            f"[DB] Serialization failed for {playlist_url}. "
            f"df_json present: {bool(df_json)}, "
            f"summary_stats_json present: {bool(summary_stats_json)}"
        )
        logger.error(err_msg)
        return UpsertResult(source="error", error=err_msg)

    user_id = stats.get("user_id")

    stats_to_insert = {
        **stats,
        "user_id": user_id,
        "processed_date": datetime.now(timezone.utc).date().isoformat(),
        "df_json": df_json,
        "summary_stats": summary_stats_json,
        "dashboard_id": compute_dashboard_id(playlist_url),
    }

    # Remove Polars DataFrame before inserting
    stats_to_insert.pop("df", None)

    logger.debug(
        f"[DB] Upserting stats for {playlist_url} (user={user_id}): "
        f"keys={list(stats_to_insert.keys())}"
    )

    success = upsert_row(
        PLAYLIST_STATS_TABLE,
        stats_to_insert,
        # Conflict on (playlist_url, user_id) tuple
        conflict_fields=["playlist_url", "user_id"],
    )

    if success:
        logger.info(f"[DB] Returning fresh stats for {playlist_url} (user={user_id})")
        return UpsertResult(
            source="fresh",
            df_json=df_json,
            summary_stats_json=summary_stats_json,
        )
    else:
        logger.error(
            f"[DB] Failed to insert fresh stats for {playlist_url} (user={user_id})"
        )
        return UpsertResult(source="error", error="DB insert failed")


def fetch_playlists(
    user_id: Optional[str] = None,
    max_items: int = 10,
    randomize: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch playlists analyzed by a specific user (or anonymous).
    If randomize=True, returns a random subset (non-deterministic).
    Otherwise, returns the most recent playlists (deterministic).

    Args:
        user_id: User ID (None for anonymous playlists)
        max_items: Maximum number of playlists to return
        randomize: If True, returns random subset
    """
    if not supabase_client:
        logger.warning("Supabase client not available to fetch playlists")
        return []

    try:
        pool_size = max_items * 5 if randomize else max_items
        query = supabase_client.table(PLAYLIST_STATS_TABLE).select(
            "playlist_url, title, processed_date"
        )
        # Filter by user_id
        if user_id is None:
            query = query.is_("user_id", None)  # Anonymous playlists
        else:
            query = query.eq("user_id", user_id)  # User-specific playlists

        response = query.order("processed_date", desc=True).limit(pool_size).execute()

        seen = set()
        playlists = []
        for row in response.data:
            url = row.get("playlist_url")
            if not url or url in seen:
                continue
            seen.add(url)
            playlists.append(
                {"url": url, "title": row.get("title") or "Untitled Playlist"}
            )
            if not randomize and len(playlists) >= max_items:
                break

        if not playlists:
            return []

        if randomize:
            return random.sample(playlists, k=min(max_items, len(playlists)))
        else:
            return playlists

    except Exception as e:
        logger.error(f"Error fetching playlists for user {user_id}: {e}")
        return []


def get_latest_playlist_job(playlist_url: str) -> Optional[Dict[str, Any]]:
    """
    Returns the most recent job record for a given playlist URL.
    """
    if not supabase_client:
        logger.warning("Supabase client not available to fetch job")
        return None
    try:
        response = (
            supabase_client.table(PLAYLIST_JOBS_TABLE)
            .select("*")  # Select all columns
            .eq("playlist_url", playlist_url)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if response.data:
            return response.data[0]
        return None
    except Exception as e:
        logger.error(f"Error fetching latest job for {playlist_url}: {e}")
        return None


def get_playlist_job_status(playlist_url: str) -> Optional[str]:
    """
    Returns the status of a playlist analysis job.
    Possible statuses: 'pending', 'processing', 'complete', 'failed', or None if not submitted.
    """
    if not supabase_client:
        logger.warning("Supabase client not available to fetch job status")
        return None

    try:
        response = (
            supabase_client.table(PLAYLIST_JOBS_TABLE)
            .select("status, created_at")
            .eq("playlist_url", playlist_url)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if response.data:
            return response.data[0].get("status")
        return None
    except Exception as e:
        logger.error(f"Error fetching job status for {playlist_url}: {e}")
        return None


def get_playlist_preview_info(playlist_url: str) -> Dict[str, Any]:
    """
    Returns minimal info about a playlist for preview.
    Flattens `summary_stats` JSON into usable front-end fields.
    """
    default = {
        "title": "YouTube Playlist",
        "channel": "Unknown Channel",
        "thumbnail": "/static/favicon.jpeg",
        "video_count": 0,
        "description": "",
    }

    if not supabase_client:
        logger.warning("Supabase client not available to fetch preview info")
        return default

    try:
        response = (
            supabase_client.table(PLAYLIST_STATS_TABLE)
            .select("title, channel_name, channel_thumbnail, summary_stats")
            .eq("playlist_url", playlist_url)
            .limit(1)
            .execute()
        )

        if not response.data:
            logger.warning(f"No playlist stats found for {playlist_url}")
            return default

        data = response.data[0]
        preview_info = {
            "title": data.get("title") or default["title"],
            "channel_name": data.get("channel_name") or default["channel"],
            "thumbnail": data.get("channel_thumbnail") or default["thumbnail"],
            "video_count": 0,
            "description": "",
        }

        summary_stats_raw = data.get("summary_stats") or {}
        if summary_stats_raw:
            # Parse JSON if it's a string
            if isinstance(summary_stats_raw, str):
                try:
                    summary_stats = json.loads(summary_stats_raw)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in summary_stats for {playlist_url}")
                    summary_stats = {}
            else:
                summary_stats = summary_stats_raw

            # Fill in video_count if missing
            preview_info["video_count"] = summary_stats.get(
                "actual_playlist_count", default["video_count"]
            )
            # Optional: include other summary stats for future use
            preview_info.update(
                {
                    "total_views": summary_stats.get("total_views"),
                    "total_likes": summary_stats.get("total_likes"),
                    "total_dislikes": summary_stats.get("total_dislikes"),
                    "total_comments": summary_stats.get("total_comments"),
                    "avg_engagement": summary_stats.get("avg_engagement"),
                    "processed_video_count": summary_stats.get("processed_video_count"),
                }
            )

        logger.info(
            f"Retrieved preview info for {playlist_url}: {preview_info.get('title')}, "
            f"videos: {preview_info.get('video_count')}"
        )
        return preview_info

    except Exception as e:
        logger.error(f"Error fetching preview info for {playlist_url}: {e}")
        return {}


def submit_playlist_job(
    playlist_url: str,
    user_id: Optional[str] = None,  # ✅ Add user_id parameter
) -> bool:
    """
    Insert a new playlist analysis job into the playlist_jobs table,
    but only if one is not already pending or in progress.

    ✅ Now scoped to user_id for proper job ownership.

    Args:
        playlist_url: YouTube playlist URL
        user_id: User ID (None for anonymous jobs)

    Returns:
        bool: True if job submitted successfully
    """
    if not supabase_client:
        logger.warning("Supabase client not available to submit job")
        return False

    # Check for existing jobs scoped to user
    try:
        query = (
            supabase_client.table(PLAYLIST_JOBS_TABLE)
            .select("status, created_at")
            .eq("playlist_url", playlist_url)
            .not_.eq("status", JobStatus.COMPLETE)
            .not_.eq("status", JobStatus.FAILED)
        )

        # ✅ Scope check to user
        if user_id is None:
            query = query.is_("user_id", None)
        else:
            query = query.eq("user_id", user_id)

        response = query.order("created_at", desc=True).limit(1).execute()

        # If an unfinished job exists for this user, don't submit a new one
        if response.data:
            job_status = response.data[0].get("status")
            logger.info(
                f"Skipping job submission for {playlist_url} (user={user_id}). "
                f"Job with status '{job_status}' already exists."
            )
            return False

    except Exception as e:
        logger.error(f"Error checking for existing jobs: {e}")
        # Continue to submit the job in case of an error

    # No existing job found, so submit a new one
    payload = {
        "playlist_url": playlist_url,
        "user_id": user_id,  # ✅ Store user ownership
        "status": JobStatus.PENDING,
        "created_at": datetime.utcnow().isoformat(),
        "retry_count": 0,
    }

    # Use insert instead of upsert to avoid conflicts on non-unique columns
    success = upsert_row(PLAYLIST_JOBS_TABLE, payload)
    if success:
        logger.info(f"Submitted job for {playlist_url} (user={user_id})")
    else:
        logger.error(f"Failed to submit job for {playlist_url} (user={user_id})")

    return success


# =============================================================================
# 🎬 Creator Sync Queue Management (NEW - Added for creator infrastructure)
# =============================================================================
# Manages the creator_sync_jobs queue for background processing


def queue_invalid_creators_for_retry(
    hours_since_last_sync: int = 24, force_resync_all: bool = False
) -> int:
    """
    Queue creators with invalid/failed/partial sync status for retry.

    This function finds creators marked as 'invalid', 'failed', or 'synced_partial'
    and queues them for another sync attempt. When force_resync_all=True, it will
    also queue creators with 'synced' status to refresh all fields (useful after
    schema updates).

    Args:
        hours_since_last_sync: Only retry if last sync was N hours ago (default 24)
        force_resync_all: If True, also queue creators with 'synced' status (default False)

    Returns:
        Number of creators queued for retry
    """
    if not supabase_client:
        logger.warning("Supabase client not available")
        return 0

    try:

        cutoff_time = datetime.now(timezone.utc) - timedelta(
            hours=hours_since_last_sync
        )

        # Statuses to retry:
        # - 'invalid': Zero subs/views, likely bad channel_id
        # - 'failed': Sync errors
        # - 'synced_partial': Missing columns, partial data
        # - 'synced': (optional) Force refresh all data
        cutoff_iso = cutoff_time.isoformat()

        # Query 1: creators that previously failed and whose last sync is old enough
        failed_resp = (
            supabase_client.table(CREATOR_TABLE)
            .select("id,channel_id,sync_status,sync_error_message,last_synced_at")
            .in_("sync_status", ["invalid", "failed", "synced_partial"])
            .not_.is_("last_synced_at", "null")  # ← only rows where value exists
            .lt("last_synced_at", cutoff_iso)
            .limit(100)
            .execute()
        )

        # Query 2: creators that have NEVER been synced (last_synced_at IS NULL)
        # These were being silently excluded by the original .lt() filter.
        # Also include NULL sync_status (creators inserted without a status).
        never_synced_resp = (
            supabase_client.table(CREATOR_TABLE)
            .select("id,channel_id,sync_status,sync_error_message,last_synced_at")
            .is_("last_synced_at", "null")
            .limit(100)
            .execute()
        )

        creators = (failed_resp.data or []) + (never_synced_resp.data or [])

        # Deduplicate by id (shouldn't overlap, but be safe)
        seen_ids = set()
        unique_creators = []
        for c in creators:
            if c["id"] not in seen_ids:
                seen_ids.add(c["id"])
                unique_creators.append(c)
        creators = unique_creators

        logger.info(
            f"[queue_invalid_creators_for_retry] "
            f"Found {len(failed_resp.data or [])} failed/invalid + "
            f"{len(never_synced_resp.data or [])} never-synced creators"
        )

        queued_count = 0

        for creator in creators:
            creator_id = creator["id"]
            status = creator["sync_status"]
            error = creator.get("sync_error_message", "Unknown")

            if queue_creator_sync(creator_id, source="auto_retry"):
                queued_count += 1
                logger.info(
                    f"Queued {creator['channel_id']} for retry "
                    f"(status={status}, error={error})"
                )

        if queued_count > 0:
            logger.info(
                f"Auto-retry: Queued {queued_count} creators with invalid/failed status"
            )

        return queued_count

    except Exception as e:
        logger.exception(f"Error queuing invalid creators for retry: {e}")
        return 0


def queue_creator_sync(
    creator_id: str,
    source: str = "scheduled",
) -> bool:
    """
    Queue a creator for stats sync.

    Uses check-then-insert pattern to prevent duplicate pending jobs. This is
    NOT atomic and has a race window between check and insert. For production,
    add a unique partial index to guarantee uniqueness:

        CREATE UNIQUE INDEX idx_creator_sync_jobs_pending_unique
        ON creator_sync_jobs (creator_id)
        WHERE status = 'pending';

    With the index, concurrent enqueues will safely fail on constraint violation
    instead of creating duplicates.

    Args:
        creator_id: Creator UUID from creators table
        source: How it got queued ('discovered', 'manual', 'scheduled')

    Returns:
        True if queued successfully or already pending, False otherwise
    """
    if not supabase_client:
        logger.warning("Supabase client not available to queue creator sync")
        return False

    try:
        # Check if there's already a pending job for this creator
        # NOTE: This is not atomic - see docstring for production hardening
        existing = (
            supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
            .select("id")
            .eq("creator_id", creator_id)
            .eq("status", "pending")
            .limit(1)
            .execute()
        )

        if existing.data:
            logger.debug(
                f"Creator {creator_id} already has a pending sync job, skipping"
            )
            return True  # Already queued, consider this success

        payload = {
            "creator_id": creator_id,
            "status": "pending",
            "source": source,
            "job_type": "sync_stats",
        }

        # Plain insert (no upsert semantics - we checked for duplicates above)
        response = (
            supabase_client.table(CREATOR_SYNC_JOBS_TABLE).insert(payload).execute()
        )

        if response.data:
            logger.info(f"Queued creator {creator_id} for sync (source={source})")
            return True
        else:
            logger.error(f"Failed to queue creator {creator_id}")
            return False

    except Exception as e:
        logger.exception(f"Error queuing creator sync: {e}")
        return False


def get_pending_creator_syncs(batch_size: int = 1) -> List[Dict[str, Any]]:
    """
    Get pending creator syncs from queue.

    Args:
        batch_size: How many pending jobs to retrieve (default 1 for frugal)

    Returns:
        List of pending sync job dicts
    """
    if not supabase_client:
        logger.warning("Supabase client not available to get pending creator syncs")
        return []

    try:
        response = (
            supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
            .select("id, creator_id, source, created_at")
            .eq("status", "pending")
            .order("created_at", desc=False)
            .limit(batch_size)
            .execute()
        )

        jobs = response.data if response.data else []

        if jobs:
            logger.info(f"Found {len(jobs)} pending creator syncs")
        else:
            logger.debug("No pending creator syncs")

        return jobs

    except Exception as e:
        logger.exception(f"Error getting pending creator syncs: {e}")
        return []


def mark_creator_sync_processing(job_id: int) -> bool:
    """Mark a creator sync job as currently processing."""
    if not supabase_client:
        logger.warning("Supabase client not available")
        return False

    try:
        response = (
            supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
            .update(
                {
                    "status": "processing",
                    "started_at": datetime.utcnow().isoformat(),
                }
            )
            .eq("id", job_id)
            .execute()
        )

        return bool(response.data)

    except Exception as e:
        logger.exception(f"Error marking creator sync {job_id} as processing: {e}")
        return False


def mark_creator_sync_completed(job_id: int) -> bool:
    """Mark a creator sync job as completed."""
    if not supabase_client:
        logger.warning("Supabase client not available")
        return False

    try:
        response = (
            supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
            .update(
                {
                    "status": "completed",
                    "completed_at": datetime.utcnow().isoformat(),
                }
            )
            .eq("id", job_id)
            .execute()
        )

        return bool(response.data)

    except Exception as e:
        logger.exception(f"Error marking creator sync {job_id} as completed: {e}")
        return False


def mark_creator_sync_failed(job_id: int, error: str) -> bool:
    """Mark a creator sync job as failed with retry logic."""
    if not supabase_client:
        logger.warning("Supabase client not available")
        return False

    try:
        resp = (
            supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
            .select("retry_count")
            .eq("id", job_id)
            .single()
            .execute()
        )

        retry_count = resp.data.get("retry_count", 0) if resp.data else 0

        if retry_count >= CREATOR_WORKER_MAX_RETRIES:
            status = "failed"
            next_retry = None
            logger.warning(f"Creator sync job {job_id} exhausted retries")
        else:
            status = "pending"
            backoff_seconds = CREATOR_WORKER_RETRY_BASE * (2**retry_count)
            next_retry = (
                datetime.utcnow() + timedelta(seconds=backoff_seconds)
            ).isoformat()
            logger.info(f"Creator sync job {job_id} will retry in {backoff_seconds}s")

        update_payload = {
            "status": status,
            "retry_count": retry_count + 1,
            "error_message": error,
        }

        if next_retry:
            update_payload["next_retry_at"] = next_retry

        response = (
            supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
            .update(update_payload)
            .eq("id", job_id)
            .execute()
        )

        return bool(response.data)

    except Exception as e:
        logger.exception(f"Error marking creator sync {job_id} as failed: {e}")
        return False


def archive_permanently_failed_creators(max_retries: int = 3) -> int:
    """
    Archive creators that failed to sync after max retries.

    Args:
        max_retries: Retries before marking as archived (default 3)

    Returns:
        Number of creators archived
    """
    if not supabase_client:
        logger.warning("Supabase client not available")
        return 0

    try:
        failed_response = (
            supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
            .select("creator_id, retry_count")
            .eq("status", "failed")
            .gte("retry_count", max_retries)
            .limit(100)
            .execute()
        )

        failed_jobs = failed_response.data if failed_response.data else []
        if not failed_jobs:
            return 0

        failed_creator_ids = set(job["creator_id"] for job in failed_jobs)
        archived_count = 0

        logger.info(
            f"Found {len(failed_creator_ids)} creators with {max_retries}+ "
            f"failed sync attempts - archiving..."
        )

        for creator_id in failed_creator_ids:
            try:
                result = (
                    supabase_client.table(CREATOR_TABLE)
                    .update(
                        {
                            "sync_status": "archived",
                            "sync_error_message": f"Failed after {max_retries}+ retries",
                            "archived_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                    .eq("id", creator_id)
                    .execute()
                )

                if result.data:
                    archived_count += 1
                    logger.info(f"✅ Archived creator: {creator_id}")
            except Exception as e:
                logger.error(f"Error archiving {creator_id}: {e}")

        if archived_count > 0:
            logger.warning(f"Archived {archived_count} permanently failed creators")

        return archived_count
    except Exception as e:
        logger.exception(f"Error archiving failed creators: {e}")
        return 0


# =============================================================================
# 📊 Creator Stats Management (NEW - Added for creator infrastructure)
# =============================================================================


def update_creator_stats(
    creator_id: str,
    stats: Dict[str, Any],
    job_id: int,
) -> bool:
    """
    Update creator stats after successful sync.

    Updates the creators table with current stats from YouTube API.
    Schema fields: current_subscribers, current_view_count, current_video_count, last_updated_at
    """
    if not supabase_client:
        logger.warning("Supabase client not available to update creator stats")
        return False

    try:
        # Update creators table with current stats
        # Match actual DB schema: current_subscribers, current_view_count, current_video_count
        payload = {
            "current_subscribers": stats.get("subscriber_count", 0),
            "current_view_count": stats.get("view_count", 0),
            "current_video_count": stats.get("video_count", 0),
            "last_updated_at": datetime.utcnow().isoformat(),
            "channel_name": stats.get("channel_name"),
            "channel_thumbnail_url": stats.get("channel_thumbnail"),
            "channel_url": f"https://www.youtube.com/channel/{stats.get('channel_id')}",
        }

        # Conditionally add optional fields if they exist in stats
        if "engagement_score" in stats:
            payload["engagement_score"] = stats.get("engagement_score")
        if "quality_grade" in stats:
            payload["quality_grade"] = stats.get("quality_grade")

        response = (
            supabase_client.table(CREATOR_TABLE)
            .update(payload)
            .eq("id", creator_id)
            .execute()
        )

        if response.data:
            logger.info(f"Updated creator stats for {creator_id}")
        else:
            logger.error(f"Failed to update creator stats for {creator_id}")
            return False

        # Mark sync job complete
        if not mark_creator_sync_completed(job_id):
            logger.error(f"Failed to mark job {job_id} as completed")
            return False

        logger.info(f"Successfully updated stats for creator {creator_id}")
        return True

    except Exception as e:
        logger.exception(f"Error updating creator stats: {e}")
        return False


def get_creator_stats(creator_id: str) -> Optional[Dict[str, Any]]:
    """Get the current stats from the creators table."""
    if not supabase_client:
        logger.warning("Supabase client not available to get creator stats")
        return None

    try:
        response = (
            supabase_client.table(CREATOR_TABLE)
            .select("*")
            .eq("id", creator_id)
            .single()
            .execute()
        )

        if response.data:
            logger.info(f"Retrieved stats for creator {creator_id}")
            return response.data
        else:
            logger.debug(f"No stats found for creator {creator_id}")
            return None

    except Exception as e:
        logger.exception(f"Error getting creator stats: {e}")
        return None


def get_top_creators_by_growth(limit: int = 20) -> List[Dict[str, Any]]:
    """
    Get top creators by subscriber count.

    Note: Growth rates require historical data tables (not yet implemented).
    For now, sort by current_subscribers as a proxy.
    """
    if not supabase_client:
        logger.warning("Supabase client not available to fetch top creators")
        return []

    try:
        response = (
            supabase_client.table(CREATOR_TABLE)
            .select(
                "id, channel_id, channel_name, channel_url, "
                "channel_thumbnail_url, current_subscribers, "
                "current_view_count, current_video_count, last_updated_at"
            )
            .order("current_subscribers", desc=True)
            .limit(limit)
            .execute()
        )

        creators = response.data if response.data else []
        logger.info(f"Retrieved {len(creators)} top creators by subscriber count")
        return creators

    except Exception as e:
        logger.exception(f"Error getting top creators: {e}")
        return []


def get_top_creators_by_engagement(limit: int = 20) -> List[Dict[str, Any]]:
    """
    Get top creators by view count.

    Note: Engagement metrics require additional data (not in current schema).
    For now, sort by current_view_count as a proxy.
    """
    if not supabase_client:
        logger.warning("Supabase client not available to fetch top creators")
        return []

    try:
        response = (
            supabase_client.table(CREATOR_TABLE)
            .select(
                "id, channel_id, channel_name, channel_url, "
                "channel_thumbnail_url, current_subscribers, "
                "current_view_count, current_video_count, last_updated_at"
            )
            .order("current_view_count", desc=True)
            .limit(limit)
            .execute()
        )

        creators = response.data if response.data else []
        logger.info(f"Retrieved {len(creators)} top creators by view count")
        return creators

    except Exception as e:
        logger.exception(f"Error getting top creators: {e}")
        return []
        return creators

    except Exception as e:
        logger.exception(f"Error getting top creators by engagement: {e}")
        return []


def get_job_progress(playlist_url: str) -> Dict[str, Any]:
    """
    Fetch job progress for polling updates.

    Always returns a dictionary. If no job exists, returns default values.

    Returns:
        Dict with keys:
        - job_id: int | None (renamed from 'id')
        - status: str | None
        - progress: float (0.0-1.0, guaranteed non-null)
        - started_at: str (ISO) | None
        - error: str | None
    """
    # ✅ Define default response for "no job found" case
    default_response = {
        "job_id": None,
        "status": None,
        "progress": 0.0,
        "started_at": None,
        "error": None,
    }

    if not supabase_client:
        logger.warning("Supabase client not available to fetch job progress")
        return {**default_response, "error": "Database unavailable"}

    try:
        response = (
            supabase_client.table(PLAYLIST_JOBS_TABLE)
            .select("id, status, progress, started_at, error, retry_count")
            .eq("playlist_url", playlist_url)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )

        if not response.data:
            logger.debug(f"No job found for playlist: {playlist_url}")
            return default_response

        job = response.data[0]

        # ✅ Normalize field names
        job["job_id"] = job.pop("id", None)

        # ✅ Include retry_count
        job["retry_count"] = job.get("retry_count", 0)

        # ✅ Ensure progress is float and clamped to [0.0, 1.0]
        raw_progress = job.get("progress")

        if raw_progress is None:
            job["progress"] = 0.0
        else:
            try:
                progress = float(raw_progress)
                # Clamp to valid range
                job["progress"] = max(0.0, min(1.0, progress))
            except (TypeError, ValueError):
                logger.warning(
                    f"Invalid progress value for {playlist_url}: {raw_progress!r}. "
                    f"Defaulting to 0.0"
                )
                job["progress"] = 0.0

        return job

    except Exception as e:
        logger.exception(f"Error fetching job progress for {playlist_url}: {e}")
        return {**default_response, "error": str(e)}


def get_estimated_stats(video_count: int) -> Dict[str, Any]:
    """
    Generate rough estimates for stats based on video count.
    Used to show user "what they'll discover" during processing.

    These are intentionally conservative estimates.
    """
    # Assumptions:
    # - Average video: 50K views, 1.5K likes, 150 comments
    # - Variance by channel type (music, vlog, tutorial, etc.)

    avg_views_per_video = 50_000
    avg_likes_per_video = 1_500
    avg_comments_per_video = 150

    return {
        "estimated_total_views": video_count * avg_views_per_video,
        "estimated_total_likes": video_count * avg_likes_per_video,
        "estimated_total_comments": video_count * avg_comments_per_video,
        "note": "These are rough estimates and will be refined during processing",
    }


# =============================================================================
# Dashboard events (views / shares)
# =============================================================================


def record_dashboard_event(
    supabase: Optional[SupabaseLike] = None,
    dashboard_id: str = "",
    event_type: str = "view",
) -> None:
    """
    Record a dashboard event (view, share, etc) in the dashboard_events table.

    Args:
        supabase: Supabase client (uses global if not provided)
        dashboard_id: Dashboard ID to record event for
        event_type: Type of event ('view', 'share', etc)
    """
    client = supabase or supabase_client
    if not client:
        logger.warning("Supabase client not available to record event")
        return

    # ✅ Validate event_type
    if event_type not in ("view", "share"):
        logger.warning(f"Invalid event_type: {event_type}. Must be 'view' or 'share'")
        return

    try:
        # ✅ Insert into dashboard_events table (NOT playlist_stats)
        payload = {
            "dashboard_id": dashboard_id,
            "event_type": event_type,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        client.table("dashboard_events").insert(payload).execute()

        logger.debug(f"Recorded {event_type} event for dashboard {dashboard_id}")

    except Exception as e:
        logger.warning(f"Failed to record dashboard event: {e}")
        # Don't raise - event tracking is non-critical


def get_dashboard_event_counts(
    supabase: Optional[SupabaseLike] = None, dashboard_id: str = ""
) -> dict:
    """
    Get event counts for a dashboard by aggregating from dashboard_events table.

    Args:
        supabase: Supabase client (uses global if not provided)
        dashboard_id: Dashboard ID to fetch events for

    Returns:
        dict with event_type -> count mapping (e.g., {"view": 5, "share": 2})
    """
    client = supabase or supabase_client
    if not client:
        logger.warning("Supabase client not available to fetch event counts")
        return {"view": 0, "share": 0}

    try:
        # ✅ Query dashboard_events table (NOT playlist_stats)
        response = (
            client.table("dashboard_events")
            .select("event_type")
            .eq("dashboard_id", dashboard_id)
            .execute()
        )

        # ✅ Aggregate counts by event_type
        counts = {"view": 0, "share": 0}

        if response.data:
            for event in response.data:
                event_type = event.get("event_type")
                if event_type in counts:
                    counts[event_type] += 1

        logger.debug(f"Event counts for {dashboard_id}: {counts}")
        return counts

    except Exception as e:
        logger.warning(f"Failed to get event counts for {dashboard_id}: {e}")
        return {"view": 0, "share": 0}


def get_dashboard_stats_by_id(
    dashboard_id: str,
    user_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Fetch complete playlist stats directly by dashboard_id.

    This function is used for loading existing dashboards and does NOT filter by date,
    ensuring that dashboards created on previous days are still accessible.

    Args:
        dashboard_id: The dashboard ID (MD5 hash of playlist URL)
        user_id: Optional user_id for ownership filtering

    Returns:
        Complete stats dict with df_json, summary_stats, etc., or None if not found
    """
    if not supabase_client:
        logger.warning("Supabase client not available")
        return None

    try:
        query = (
            supabase_client.table(PLAYLIST_STATS_TABLE)
            .select("*")
            .eq("dashboard_id", dashboard_id)
        )

        # Filter by user if provided
        if user_id is not None:
            query = query.eq("user_id", user_id)

        response = query.order("processed_on", desc=True).limit(1).execute()

        if response.data and len(response.data) > 0:
            row = response.data[0]

            # Validate df_json
            df_json = row.get("df_json")
            if _is_empty_json(df_json):
                logger.warning(
                    f"[Dashboard] Found invalid/empty data for dashboard_id={dashboard_id}"
                )
                return None

            logger.info(
                f"[Dashboard] Loaded dashboard: {dashboard_id} (user={user_id})"
            )

            # Deserialize JSON fields
            if row.get("summary_stats"):
                try:
                    row["summary_stats"] = json.loads(row["summary_stats"])
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse summary_stats: {e}")
                    row["summary_stats"] = {}

            return row
        else:
            logger.warning(f"[Dashboard] Not found: {dashboard_id} (user={user_id})")
            return None

    except Exception as e:
        logger.exception(f"Failed to fetch dashboard {dashboard_id}: {e}")
        return None


def resolve_playlist_url_from_dashboard_id(
    dashboard_id: str,
    user_id: Optional[str] = None,
) -> Optional[str]:
    """
    Resolve playlist_url from dashboard_id, optionally scoped to a user.

    Note: Uses 64-bit MD5 hash. Collision probability:
    - 100K playlists: ~0.00003%
    - 1M playlists: ~0.003%
    - 10M playlists: ~0.3%

    If collision occurs, returns most recent playlist for that user (or any user if user_id=None)
    """
    if not supabase_client:
        return None

    try:
        query = (
            supabase_client.table(PLAYLIST_STATS_TABLE)
            .select("playlist_url")
            .eq("dashboard_id", dashboard_id)
        )

        # Filter by user if provided
        if user_id is not None:
            query = query.eq("user_id", user_id)

        response = query.order("processed_on", desc=True).limit(1).execute()

        return response.data[0]["playlist_url"] if response.data else None

    except Exception as e:
        logger.exception(
            f"Failed to resolve dashboard_id={dashboard_id} (user={user_id}): {e}"
        )
        return None


def get_user_dashboards(
    user_id: str, search: str = "", sort: str = "recent"
) -> list[dict]:
    """
    Get all dashboards owned by a user with optional filtering and sorting.

    Args:
        user_id: User ID from session
        search: Optional search query for title/channel (sanitized)
        sort: Sort option ('recent', 'views', 'videos', 'title')

    Returns:
        List of dashboard metadata dicts sorted according to sort parameter
    """
    if not supabase_client:
        logger.warning("Supabase client not available for get_user_dashboards")
        return []

    try:
        # Build base query
        query = (
            supabase_client.table(PLAYLIST_STATS_TABLE)
            .select(
                "dashboard_id, playlist_url, title, channel_name, "
                "channel_thumbnail, video_count, view_count, processed_on, "
                "summary_stats, processed_date"
            )
            .eq("user_id", user_id)
        )

        # ✅ SECURITY: Escape wildcards in search input
        if search:
            # Escape PostgreSQL ILIKE wildcards
            escaped_search = (
                search.replace("\\", "\\\\")  # Escape backslash first
                .replace("%", "\\%")  # Escape percent (any chars)
                .replace("_", "\\_")  # Escape underscore (single char)
            )

            search_pattern = f"%{escaped_search}%"

            query = query.or_(
                f"title.ilike.{search_pattern},channel_name.ilike.{search_pattern}"
            )

        # Apply sorting
        if sort == "views":
            query = query.order("view_count", desc=True)
        elif sort == "videos":
            query = query.order("video_count", desc=True)
        elif sort == "title":
            query = query.order("title", desc=False)
        else:  # recent
            query = query.order("processed_on", desc=True)

        # Execute query
        response = query.execute()
        dashboards = response.data or []

        logger.info(
            f"✅ Found {len(dashboards)} dashboards for user {user_id} "
            f"(search='{search}', sort='{sort}')"
        )

        return dashboards

    except Exception as e:
        logger.exception(f"Failed to fetch dashboards for user {user_id}: {e}")
        return []


def get_or_create_creator_from_playlist(
    channel_id: str,
    channel_name: str,
    channel_url: str,
    channel_thumbnail_url: str | None = None,
    user_id: str | None = None,
) -> Optional[str]:
    """
    Get or create a creator row based on YouTube channel identity.

    ✅ UPDATED: Returns creator UUID, auto-queues for sync.

    Args:
        channel_id: YouTube channel ID (e.g., "UCxxxxx")
        channel_name: Creator's channel name
        channel_url: YouTube channel URL
        channel_thumbnail_url: Channel avatar URL
        user_id: Optional user who discovered (for future ownership)

    Returns:
        Creator UUID (id) if successful, None if error
    """
    if not supabase_client:
        logger.warning("Supabase client not available for creator discovery")
        return None

    try:
        # 1️⃣ Try to fetch existing creator
        response = (
            supabase_client.table(CREATOR_TABLE)
            .select("id")
            .eq("channel_id", channel_id)
            .limit(1)
            .execute()
        )

        if response.data:
            creator_id = response.data[0]["id"]
            logger.info(f"Creator {channel_id} already exists (id={creator_id})")

            # Opportunistic metadata refresh (update basic fields only)
            # Note: Stats (country_code, video_count, subscribers) are updated by creator_worker
            try:
                update_payload = {
                    "channel_name": channel_name,
                    "channel_url": channel_url,
                    "last_seen_at": datetime.utcnow().isoformat(),
                }
                # Only update thumbnail if we have a new value (avoid overwriting with None)
                if channel_thumbnail_url:
                    update_payload["channel_thumbnail_url"] = channel_thumbnail_url

                supabase_client.table(CREATOR_TABLE).update(update_payload).eq(
                    "id", creator_id
                ).execute()
                logger.debug(
                    f"Refreshed metadata for creator {channel_id} (last_seen updated)"
                )
            except Exception as e:
                logger.debug(f"Metadata refresh failed (non-critical): {e}")

            # Queue for sync if not recently synced (intelligent deduplication)
            try:
                creator_resp = (
                    supabase_client.table(CREATOR_TABLE)
                    .select("last_synced_at")
                    .eq("id", creator_id)
                    .single()
                    .execute()
                )

                last_synced = (
                    creator_resp.data.get("last_synced_at")
                    if creator_resp.data
                    else None
                )
                should_queue = True
                # Use shorter interval for rediscovered creators vs scheduled syncs
                sync_threshold = CREATOR_REDISCOVERY_THRESHOLD_DAYS

                if last_synced:
                    last_synced_dt = datetime.fromisoformat(
                        last_synced.replace("Z", "+00:00")
                    )
                    days_since_sync = (
                        datetime.utcnow() - last_synced_dt.replace(tzinfo=None)
                    ).days
                    should_queue = days_since_sync >= sync_threshold

                    if not should_queue:
                        logger.debug(
                            f"Skipping sync queue for {channel_id} - synced {days_since_sync} days ago "
                            f"(threshold: {sync_threshold} days)"
                        )
                    else:
                        logger.info(
                            f"Re-queuing {channel_id} for sync - last synced {days_since_sync} days ago"
                        )

                if should_queue:
                    if not queue_creator_sync(creator_id, source="rediscovered"):
                        logger.warning(
                            f"Failed to queue existing creator {creator_id} for sync"
                        )

            except Exception as e:
                logger.debug(
                    f"Sync queuing check failed (non-critical): {e}", exc_info=True
                )

            return creator_id

        # 2️⃣ Create new creator
        insert_payload = {
            "channel_id": channel_id,
            "channel_name": channel_name,
            "channel_url": channel_url,
            "channel_thumbnail_url": channel_thumbnail_url,
            # Ensure these essential fields have default values
            "current_subscribers": 0,
            "current_view_count": 0,
            "current_video_count": 0,
            # Country code can be null initially (will be filled by sync worker)
        }

        insert_resp = (
            supabase_client.table(CREATOR_TABLE).insert(insert_payload).execute()
        )

        if not insert_resp.data:
            logger.error(f"Failed to insert creator {channel_id}")
            return None

        creator_id = insert_resp.data[0]["id"]
        logger.info(f"Discovered new creator {channel_id} (id={creator_id})")

        # 3️⃣ Queue for immediate sync (high priority for new creators)
        if not queue_creator_sync(creator_id, source="discovered"):
            logger.warning(f"Failed to queue creator {creator_id} for sync")
        else:
            logger.debug(f"Queued creator {creator_id} for immediate sync")

        return creator_id

    except Exception as e:
        logger.exception(f"Error getting/creating creator {channel_id}: {e}")
        return None


def add_creator_manually(
    channel_id: str,
    channel_name: str,
    channel_url: str,
) -> Optional[str]:
    """
    Manual creator addition from UI endpoint.

    Args:
        channel_id: YouTube channel ID
        channel_name: Creator's name
        channel_url: YouTube URL

    Returns:
        Creator UUID if successful
    """
    if not supabase_client:
        logger.warning("Supabase client not available for manual creator add")
        return None

    try:
        # Check if already exists
        response = (
            supabase_client.table(CREATOR_TABLE)
            .select("id")
            .eq("channel_id", channel_id)
            .limit(1)
            .execute()
        )

        if response.data:
            creator_id = response.data[0]["id"]
            logger.info(f"Creator {channel_id} already exists")
            return creator_id

        # Create new
        insert_payload = {
            "channel_id": channel_id,
            "channel_name": channel_name,
            "channel_url": channel_url,
        }

        insert_resp = (
            supabase_client.table(CREATOR_TABLE).insert(insert_payload).execute()
        )

        if not insert_resp.data:
            logger.error(f"Failed to insert creator {channel_id}")
            return None

        creator_id = insert_resp.data[0]["id"]

        # Queue for sync
        queue_creator_sync(creator_id, source="manual")

        logger.info(f"Manually added creator {channel_id} (id={creator_id})")
        return creator_id

    except Exception as e:
        logger.exception(f"Error adding creator manually: {e}")
        return None


# =============================================================================
# 📊 Creator Discovery & Listing (Frontend API)
# =============================================================================


class CreatorsResult(NamedTuple):
    """Result from get_creators with pagination metadata."""

    creators: list[dict]
    total_count: int


def get_creators(
    search: str = "",
    sort: str = "subscribers",
    grade_filter: str = "all",
    language_filter: str = "all",
    activity_filter: str = "all",
    age_filter: str = "all",
    limit: int = 50,
    offset: int = 0,
    return_count: bool = False,
) -> list[dict] | CreatorsResult:
    """
    Fetch creators for frontend display with comprehensive filtering and sorting.

    ALL heavy lifting (filtering, sorting) done by database for performance.
    This is the ONLY function frontend routes should use for creator listing.

    Args:
        search: Filter by channel name or @custom_url (case-insensitive)
        sort: Sort criteria
            - subscribers: Most subscribers (default)
            - views: Most total views
            - videos: Most video count
            - engagement: Best engagement score
            - quality: Best quality grade
            - recent: Recently updated
            - consistency: Most consistent uploads (monthly_uploads DESC)
            - newest_channel: Newest channels (published_at DESC)
            - oldest_channel: Oldest/veteran channels (published_at ASC)
        grade_filter: Filter by quality grade (all, A+, A, B+, B, C)
        language_filter: Filter by content language (all, en, ja, es, ko, zh, etc)
        activity_filter: Filter by upload frequency
            - all: All creators
            - active: Very active (>5 videos/month)
            - dormant: Dormant (<1 video/month)
        age_filter: Filter by channel age
            - all: All creators
            - new: New channels (<1 year old)
            - established: Established (1-10 years old)
            - veteran: Veteran channels (10+ years old)
        limit: Maximum number of results (default 50)
        offset: Number of results to skip (for pagination)
        return_count: If True, returns CreatorsResult with total_count

    Returns:
        List of creator dicts with _rank position added (1-based index)
        OR CreatorsResult(creators, total_count) if return_count=True

    Examples:
        # Get top 50 Japanese creators by consistency
        creators = get_creators(language_filter="ja", sort="consistency", limit=50)

        # Get active English creators
        creators = get_creators(language_filter="en", activity_filter="active")

        # Find new creators with good engagement
        creators = get_creators(age_filter="new", sort="engagement")
    """
    if not supabase_client:
        logger.warning("Supabase client not available")
        return CreatorsResult([], 0) if return_count else []

    try:
        # Build sort mapping (DB does the sorting based on this)
        sort_map = {
            "subscribers": ("current_subscribers", True),
            "views": ("current_view_count", True),
            "videos": ("current_video_count", True),
            "engagement": ("engagement_score", True),
            "quality": ("quality_grade", False),
            "recent": ("last_updated_at", True),
            "consistency": ("monthly_uploads", True),
            "newest_channel": ("published_at", True),
            "oldest_channel": ("published_at", False),
        }
        sort_field, descending = sort_map.get(sort, ("current_subscribers", True))

        # Start query - must call .select() to get a builder with filter methods
        query = supabase_client.table(CREATOR_TABLE).select(
            "*", count="exact" if return_count else None
        )

        # Filter out incomplete creators (ensure data quality)
        # Using .not_.is_() for NULL check - only works after .select() is called
        query = query.not_.is_("channel_name", "null")
        query = query.gt("current_subscribers", 0)

        # Apply search filter (also search custom_url and keywords)
        if search:
            # ✅ SECURITY: Escape wildcards in search input
            escaped_search = (
                search.replace("\\", "\\\\")  # Escape backslash first
                .replace("%", "\\%")  # Escape percent (any chars)
                .replace("_", "\\_")  # Escape underscore (single char)
            )
            search_pattern = f"%{escaped_search}%"

            query = query.or_(
                f"channel_name.ilike.{search_pattern},"
                f"custom_url.ilike.{search_pattern},"
                f"keywords.ilike.{search_pattern}"
            )

        # Apply grade filter
        valid_grades = ["A+", "A", "B+", "B", "C"]
        if grade_filter and grade_filter in valid_grades:
            query = query.eq("quality_grade", grade_filter)

        # Apply language filter
        if language_filter and language_filter != "all":
            query = query.eq("default_language", language_filter)

        # Apply activity filter
        if activity_filter and activity_filter != "all":
            if activity_filter == "active":
                query = query.gt("monthly_uploads", 5)  # > 5 videos/month
            elif activity_filter == "dormant":
                query = query.lt("monthly_uploads", 1)  # < 1 video/month

        # Apply age filter (NEW)
        if age_filter and age_filter != "all":
            if age_filter == "new":
                query = query.lt("channel_age_days", 365)  # < 1 year
            elif age_filter == "established":
                query = query.gte("channel_age_days", 365)  # >= 1 year
                query = query.lt("channel_age_days", 3650)  # < 10 years
            elif age_filter == "veteran":
                query = query.gte("channel_age_days", 3650)  # >= 10 years

        # Apply sorting, limit, and offset (DB does the work for pagination)
        query = query.order(sort_field, desc=descending).limit(limit).offset(offset)

        # Execute query (count already included in select if needed)
        response = query.execute()
        creators = response.data if response.data else []
        total_count = (getattr(response, "count", 0) or 0) if return_count else 0

        # Add ranking position (1-based index, adjusted for offset)
        for idx, creator in enumerate(creators, 1):
            creator["_rank"] = offset + idx

        # Log results
        filters_applied = []
        if search:
            filters_applied.append(f"search='{search}'")
        if grade_filter != "all":
            filters_applied.append(f"grade={grade_filter}")
        if language_filter != "all":
            filters_applied.append(f"language={language_filter}")
        if activity_filter != "all":
            filters_applied.append(f"activity={activity_filter}")
        if age_filter != "all":
            filters_applied.append(f"age={age_filter}")

        filters_str = ", ".join(filters_applied) if filters_applied else "none"
        logger.info(
            f"Retrieved {len(creators)} creators "
            f"(sort={sort}, filters=[{filters_str}], limit={limit}, offset={offset})"
            + (f", total_count={total_count}" if return_count else "")
        )

        if return_count:
            return CreatorsResult(creators, total_count)
        return creators

    except Exception as e:
        logger.exception(f"Error fetching creators: {e}")
        return CreatorsResult([], 0) if return_count else []


def calculate_creator_stats(creators: list[dict], include_all: bool = False) -> dict:
    """
    Calculate aggregate statistics from creators list for hero section.

    Marketing-focused metrics:
    - Total creators (inventory size)
    - Average engagement rate (quality indicator)
    - Growth momentum (trending creators count)
    - Quality distribution (A+/A grade percentage)

    Args:
        creators: List of creator dicts (filtered or all)
        include_all: If True, fetch ALL creators from DB for accurate totals

    Returns:
        Dict with marketing-relevant metrics
    """
    if not creators:
        return {
            "total_creators": 0,
            "avg_engagement": 0,
            "growing_creators": 0,
            "premium_creators": 0,
            "total_subscribers": 0,
            "total_videos": 0,
        }

    try:

        # If include_all=True, use database aggregation instead of loading all rows into Python
        if include_all and supabase_client:
            try:
                # TODO: Push full aggregation to database via RPC or materialized view for zero data transfer
                # Current approach fetches minimal fields but still transfers O(n) rows on every page load
                logger.debug("Calculating stats from all creators using DB aggregation")

                # Get total count (fast, no data transfer)
                count_response = (
                    supabase_client.table(CREATOR_TABLE)
                    .select("id", count="exact")
                    .eq("sync_status", "synced")
                    .limit(1)
                    .execute()
                )
                total_creators = (
                    count_response.count if hasattr(count_response, "count") else 0
                )

                # Safety valve: If creator count exceeds threshold, fall back to page-level stats
                # This prevents unbounded query cost as dataset grows beyond 5000 creators
                MAX_CREATORS_FOR_FULL_STATS = 5000
                if total_creators > MAX_CREATORS_FOR_FULL_STATS:
                    logger.warning(
                        f"Creator count ({total_creators}) exceeds limit ({MAX_CREATORS_FOR_FULL_STATS}). "
                        f"Falling back to page-level stats. Consider implementing database-side aggregation."
                    )
                    stats_source = creators
                    total_creators = len(creators)
                else:
                    # For other aggregates, fetch only necessary fields (not full creator objects)
                    # Still in-memory aggregation but lighter than full rows
                    agg_response = (
                        supabase_client.table(CREATOR_TABLE)
                        .select(
                            "engagement_score,subscribers_change_30d,quality_grade,"
                            "current_subscribers,current_video_count"
                        )
                        .eq("sync_status", "synced")
                        .execute()
                    )
                    stats_source = agg_response.data if agg_response.data else []

            except Exception as e:
                logger.warning(f"Failed to fetch aggregated stats from DB: {e}")
                stats_source = creators
                total_creators = len(creators)
        else:
            stats_source = creators
            total_creators = len(creators)

        # Calculate average engagement (walrus operator := stores value and uses it in condition)
        # This avoids calling safe_get_value twice per creator (once for check, once for list)
        engagement_scores = [
            score
            for c in stats_source
            if (score := safe_get_value(c, "engagement_score", 0)) > 0
        ]
        avg_engagement = (
            sum(engagement_scores) / len(engagement_scores) if engagement_scores else 0
        )
        has_engagement_data = len(engagement_scores) > 0

        # Count growing creators (use walrus operator to avoid duplicate calls)
        growing_creators = sum(
            1
            for c in stats_source
            if (change := safe_get_value(c, "subscribers_change_30d", 0)) > 0
        )

        # Count premium creators (use walrus operator)
        premium_creators = sum(
            1
            for c in stats_source
            if (grade := safe_get_value(c, "quality_grade", "C")) in ["A+", "A"]
        )

        # Sum for secondary metrics (use walrus operator)
        total_subscribers = sum(
            subs
            for c in stats_source
            if (subs := safe_get_value(c, "current_subscribers", 0))
        )
        total_videos = sum(
            vids
            for c in stats_source
            if (vids := safe_get_value(c, "current_video_count", 0))
        )

        # ═══════════════════════════════════════════════════════════════
        # NEW AGENCY-FOCUSED METRICS FOR REDESIGNED HERO
        # ═══════════════════════════════════════════════════════════════

        # Count unique countries for geographic diversity
        countries = set(
            country
            for c in stats_source
            if (country := safe_get_value(c, "country_code", None))
        )
        total_countries = len(countries)

        # Count unique languages for content diversity
        languages = set(
            lang
            for c in stats_source
            if (lang := safe_get_value(c, "default_language", None))
        )
        total_languages = len(languages)

        # Count unique categories for content diversity
        # topic_categories can be a list or comma-separated string
        categories = set()
        for c in stats_source:
            cats = safe_get_value(c, "topic_categories", None)
            if cats:
                if isinstance(cats, list):
                    categories.update(cats)
                elif isinstance(cats, str):
                    # Handle comma-separated strings
                    categories.update(
                        cat.strip() for cat in cats.split(",") if cat.strip()
                    )
        total_categories = len(categories)

        # Grade distribution for visual quality breakdown
        grade_counts = {}
        for c in stats_source:
            grade = safe_get_value(c, "quality_grade", "C")
            grade_counts[grade] = grade_counts.get(grade, 0) + 1

        # Verified percentage (official badge)
        verified_count = sum(
            1 for c in stats_source if safe_get_value(c, "official", False)
        )
        verified_percentage = (
            (verified_count / len(stats_source)) * 100 if stats_source else 0
        )

        # Active percentage (monthly_uploads > 5 = actively posting)
        active_count = sum(
            1 for c in stats_source if safe_get_value(c, "monthly_uploads", 0) > 5
        )
        active_percentage = (
            (active_count / len(stats_source)) * 100 if stats_source else 0
        )

        # Top 3 countries by creator count
        country_counts = {}
        for c in stats_source:
            country = safe_get_value(c, "country_code", None)
            if country:
                country_counts[country] = country_counts.get(country, 0) + 1

        top_countries = sorted(
            country_counts.items(), key=lambda x: x[1], reverse=True
        )[:3]

        # Top 5 languages by creator count
        language_counts = {}
        for c in stats_source:
            lang = safe_get_value(c, "default_language", None)
            if lang:
                language_counts[lang] = language_counts.get(lang, 0) + 1

        top_languages = sorted(
            language_counts.items(), key=lambda x: x[1], reverse=True
        )[:5]

        return {
            # Original metrics (keep for backward compatibility)
            "total_creators": int(total_creators),
            "avg_engagement": round(avg_engagement, 2),
            "has_engagement_data": has_engagement_data,
            "growing_creators": int(growing_creators),
            "premium_creators": int(premium_creators),
            "total_subscribers": int(total_subscribers),
            "total_videos": int(total_videos),
            # New agency-focused metrics
            "total_countries": int(total_countries),
            "total_languages": int(total_languages),
            "total_categories": int(total_categories),
            "grade_counts": grade_counts,
            "verified_percentage": round(verified_percentage, 1),
            "active_percentage": round(active_percentage, 1),
            "top_countries": top_countries,
            "top_languages": top_languages,
        }

    except Exception as e:
        logger.exception(f"Error calculating creator stats: {e}")
        return {
            "total_creators": 0,
            "avg_engagement": 0,
            "has_engagement_data": False,
            "growing_creators": 0,
            "premium_creators": 0,
            "total_subscribers": 0,
            "total_videos": 0,
        }
