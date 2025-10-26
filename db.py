"""
Database operations module.
Handles Supabase client initialization, logging setup, and caching functionality.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import random
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Protocol, TypedDict

import polars as pl
from supabase import Client, create_client
from tenacity import retry, stop_after_attempt, wait_exponential

from constants import PLAYLIST_JOBS_TABLE, PLAYLIST_STATS_TABLE, SIGNUPS_TABLE

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
            query = query.upsert(payload, on_conflict=",".join(conflict_fields))
        else:
            query = query.insert(payload)
        response = query.execute()
        logger.debug(f"[DB] Upsert response for table {table}: {response.data}")
        return bool(response.data)
    except Exception as e:
        logger.exception(f"Error upserting row in {table}: {e}")
        return False


# --- Playlist Caching and Job Management ---
def get_cached_playlist_stats(
    playlist_url: str, check_date: bool = False
) -> Optional[Dict[str, Any]]:
    """Return stats for a playlist if already cached in DB.

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
        if check_date:
            today = date.today().isoformat()
            query = query.eq("processed_date", today)
        response = query.limit(1).execute()

        if response.data and len(response.data) > 0:
            row = response.data[0]

            # --- FIX: Validate the integrity of the cached data ---
            df_json = row.get("df_json")
            # Check if df_json is missing, empty, or represents an empty list/object.
            if _is_empty_json(df_json):
                logger.warning(
                    f"[Cache] Found invalid/empty cache entry for {playlist_url}. Treating as miss."
                )
                return None

            logger.info(f"[Cache] Hit for playlist: {playlist_url}")
            # Deserialize JSON fields
            try:
                row["df"] = pl.read_json(io.BytesIO(df_json.encode("utf-8")))
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
            logger.info(f"[Cache] Miss for playlist: {playlist_url}")
            return None

    except Exception as e:
        logger.exception(f"Error checking cache for playlist {playlist_url}: {e}")
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

    # Check cache first
    cached = get_cached_playlist_stats(playlist_url, check_date=True)
    if cached:
        logger.info(f"[Cache] Returning cached stats for {playlist_url}")
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
            f"df_json present: {bool(df_json)}, summary_stats_json present: {bool(summary_stats_json)}"
        )
        logger.error(err_msg)
        return UpsertResult(source="error", error=err_msg)

    stats_to_insert = {
        **stats,
        "processed_date": date.today().isoformat(),
        "df_json": df_json,
        "summary_stats": summary_stats_json,
    }

    # Remove Polars DataFrame before inserting
    stats_to_insert.pop("df", None)

    logger.debug(
        f"[DB] Final stats_to_insert for {playlist_url}: {stats_to_insert.keys()}"
    )

    success = upsert_row(
        PLAYLIST_STATS_TABLE, stats_to_insert, ["playlist_url", "processed_date"]
    )

    if success:
        logger.info(f"[DB] Returning fresh stats for {playlist_url}")
        return UpsertResult(
            source="fresh",
            df_json=df_json,
            summary_stats_json=summary_stats_json,
        )
    else:
        logger.error(f"[DB] Failed to insert fresh stats for {playlist_url}")
        return UpsertResult(source="error", error="DB insert failed")


def fetch_playlists(
    max_items: int = 10, randomize: bool = False
) -> List[Dict[str, Any]]:
    """
    Fetch distinct playlists from DB.
    If randomize=True, returns a random subset (non-deterministic).
    Otherwise, returns the most recent playlists (deterministic).
    """
    if not supabase_client:
        logger.warning("Supabase client not available to fetch playlists")
        return []

    try:
        pool_size = max_items * 5 if randomize else max_items
        response = (
            supabase_client.table(PLAYLIST_STATS_TABLE)
            .select("playlist_url, title, processed_date")
            .order("processed_date", desc=True)
            .limit(pool_size)
            .execute()
        )

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
        logger.error(f"Error fetching playlists: {e}")
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
    if not supabase_client:
        logger.warning("Supabase client not available to fetch preview info")
        return {}

    try:
        response = (
            supabase_client.table(PLAYLIST_STATS_TABLE)
            .select("title, channel_thumbnail, summary_stats")
            .eq("playlist_url", playlist_url)
            .limit(1)
            .execute()
        )

        if not response.data:
            logger.warning(f"No playlist stats found for {playlist_url}")
            return {}

        data = response.data[0]
        preview_info = {
            "title": data.get("title"),
            "thumbnail": data.get("channel_thumbnail"),
            "video_count": None,
        }

        summary_stats_raw = data.get("summary_stats")
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
            preview_info["video_count"] = summary_stats.get("actual_playlist_count")
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


def submit_playlist_job(playlist_url: str) -> bool:
    """Insert a new playlist analysis job into the playlist_jobs table,
    but only if one is not already pending or in progress.
    """
    if not supabase_client:
        logger.warning("Supabase client not available to submit job")
        return False

    # Check for an existing job that is not finished
    try:
        response = (
            supabase_client.table(PLAYLIST_JOBS_TABLE)
            .select("status, created_at")
            .eq("playlist_url", playlist_url)
            .not_.eq("status", "complete")
            .not_.eq("status", "failed")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )

        # If an unfinished job exists, don't submit a new one
        if response.data:
            job_status = response.data[0].get("status")
            job_created_at = response.data[0].get("created_at")
            logger.info(
                f"Skipping job submission for {playlist_url}. A job with status '{job_status}' created at {job_created_at} is already in progress or pending."
            )
            return False

    except Exception as e:
        logger.error(f"Error checking for existing jobs: {e}")
        # Continue to submit the job in case of an error

    # No existing job found, so submit a new one
    payload = {
        "playlist_url": playlist_url,
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
        "retry_count": 0,
    }

    # Use insert instead of upsert to avoid conflicts on non-unique columns
    success = upsert_row(PLAYLIST_JOBS_TABLE, payload)
    if success:
        logger.info(f"Submitted new playlist analysis job for {playlist_url}.")
    else:
        logger.error(f"Failed to submit new job for {playlist_url}.")

    return success
