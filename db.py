"""
Database operations module.
Handles Supabase client initialization, logging setup, and caching functionality.
"""

import base64
import io
import json
import logging
import os
import random
from datetime import date, datetime
from typing import Any, Dict, Optional

import polars as pl
from supabase import Client, create_client
from tenacity import retry, stop_after_attempt, wait_exponential

# Get logger instance
logger = logging.getLogger(__name__)

# Global Supabase client
supabase_client: Optional[Client] = None


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

    try:
        url: str = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")
        key: str = os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY", "")

        if not url or not key:
            logger.warning(
                "Missing Supabase environment variables - running without Supabase"
            )
            return None

        client = create_client(url, key)

        # Test the connection
        client.auth.get_session()

        supabase_client = client
        logger.info("Supabase client initialized successfully")
        return client

    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {str(e)}")
        return None


# --- Cache-aware queries for playlist stats ---
async def get_cached_playlist_stats(playlist_url: str) -> Optional[Dict[str, Any]]:
    """Return today's stats for a playlist if already cached in DB.

    Args:
        playlist_url (str): The YouTube playlist URL to check

    Returns:
        Optional[Dict[str, Any]]: Cached stats if found, None otherwise
    """
    if not supabase_client:
        logger.warning("Supabase client not available for caching")
        return None

    try:
        today = date.today().isoformat()

        # Uncomment if we want freshness check
        # response = supabase_client.table("playlist_stats").select("*").eq(
        #     "playlist_url", playlist_url).eq("processed_date",
        #                                      today).limit(1).execute()
        response = (
            supabase_client.table("playlist_stats")
            .select("*")
            .eq("playlist_url", playlist_url)
            .limit(1)
            .execute()
        )

        if response.data and len(response.data) > 0:
            logger.info(f"Cache hit for playlist: {playlist_url}")
            row = response.data[0]

            # --- Deserialize JSON fields ---
            if row.get("df_json"):
                row["df"] = pl.read_json(io.BytesIO(row["df_json"].encode("utf-8")))
            if row.get("summary_stats"):
                row["summary_stats"] = json.loads(row["summary_stats"])

            return row
        else:
            logger.info(f"Cache miss for playlist: {playlist_url}")
            return None

    except Exception as e:
        logger.error(f"Error checking cache for playlist {playlist_url}: {e}")
        return None


async def insert_playlist_stats(stats: Dict[str, Any]) -> bool:
    """Insert new playlist stats into DB if not already present for today.

    Args:
        stats (Dict[str, Any]): Playlist statistics to insert

    Returns:
        bool: True if insert was successful, False otherwise
    """
    if not supabase_client:
        logger.warning("Supabase client not available for inserting stats")
        return False

    # Add processed_date and serialize fields
    stats_with_date = {**stats, "processed_date": date.today().isoformat()}
    # --- Ensure JSON serializable fields ---
    if "df" in stats_with_date:
        stats_with_date["df_json"] = stats_with_date["df"].write_json()
        del stats_with_date["df"]
    if "summary_stats" in stats_with_date:
        stats_with_date["summary_stats"] = json.dumps(stats_with_date["summary_stats"])

    # Use upsert_row helper
    return await upsert_row("playlist_stats", stats_with_date, ["playlist_url", "processed_date"])


async def upsert_playlist_stats(stats: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main entrypoint for playlist stats caching:
    - Return cached stats if they exist for today.
    - Otherwise insert fresh stats and return them.

    Args:
        stats (Dict[str, Any]): Playlist statistics to cache or return

    Returns:
        Dict[str, Any]: Stats with source indicator ("cache" or "fresh")
    """
    playlist_url = stats.get("playlist_url")
    if not playlist_url:
        logger.error("No playlist_url provided in stats")
        stats["source"] = "error"
        return stats

    # Check cache first
    cached = await get_cached_playlist_stats(playlist_url)
    if cached:
        cached["source"] = "cache"
        logger.info(f"Returning cached stats for playlist: {playlist_url}")
        return cached

    # No cache hit, insert fresh stats
    success = await insert_playlist_stats(stats)
    if success:
        stats["source"] = "fresh"
        logger.info(f"Returning fresh stats for playlist: {playlist_url}")
    else:
        stats["source"] = "error"
        logger.error(f"Failed to insert fresh stats for playlist: {playlist_url}")

    return stats


CACHE_TABLE = "playlist_cache"


async def get_cached_playlist(playlist_url: str):
    """Fetch cached playlist data from Supabase"""
    if not supabase_client:
        logger.warning("Supabase client not available for caching")
        return None

    res = (
        supabase_client.table(CACHE_TABLE)
        .select("*")
        .eq("playlist_url", playlist_url)
        .execute()
    )
    if not res.data:
        return None

    row = res.data[0]
    # Decode parquet -> polars df
    df_bytes = base64.b64decode(row["df_parquet"])
    df = pl.read_parquet(io.BytesIO(df_bytes))
    return (
        df,
        row["playlist_name"],
        row["channel_name"],
        row["channel_thumbnail"],
        row["summary_stats"],
    )


async def cache_playlist(
    playlist_url: str,
    df: pl.DataFrame,
    playlist_name: str,
    channel_name: str,
    channel_thumbnail: str,
    summary_stats: dict,
):
    """Store playlist df + metadata in Supabase"""
    if not supabase_client:
        logger.warning("Supabase client not available for caching")
        return False

    try:
        buf = io.BytesIO()
        df.write_parquet(buf)
        df_bytes = base64.b64encode(buf.getvalue()).decode("utf-8")

        supabase_client.table(CACHE_TABLE).upsert(
            {
                "playlist_url": playlist_url,
                "df_parquet": df_bytes,
                "playlist_name": playlist_name,
                "channel_name": channel_name,
                "channel_thumbnail": channel_thumbnail,
                "summary_stats": summary_stats,
            }
        ).execute()
        return True
    except Exception as e:
        logger.error(f"Failed to cache playlist {playlist_url}: {e}")
        return False


def fetch_playlists(max_items: int = 10, randomize: bool = False) -> list[dict]:
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
            supabase_client.table("playlist_stats")
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


async def get_playlist_job_status(playlist_url: str) -> str:
    """
    Returns the status of a playlist analysis job.
    Possible statuses: 'pending', 'processing', 'complete', 'failed', or None if not submitted.
    """
    response = supabase_client.table("playlist_jobs") \
        .select("status") \
        .eq("playlist_url", playlist_url) \
        .order("created_at", desc=True) \
        .limit(1) \
        .execute()
    if response.data and len(response.data) > 0:
        return response.data[0].get("status")
    return None


async def get_playlist_preview_info(playlist_url: str) -> dict:
    """
    Returns minimal info about a playlist if available (title, thumbnail, etc).
    """
    response = supabase_client.table("playlist_stats") \
        .select("title", "channel_thumbnail") \
        .eq("playlist_url", playlist_url) \
        .limit(1) \
        .execute()
    if response.data and len(response.data) > 0:
        return {
            "title": response.data[0].get("title"),
            "thumbnail": response.data[0].get("channel_thumbnail"),
        }
    return {}


async def submit_playlist_job(playlist_url: str) -> None:
    """Insert a new playlist analysis job into the playlist_jobs table."""
    payload = {
        "playlist_url": playlist_url,
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
    }
    # Use upsert_row helper, conflict on playlist_url and status
    await upsert_row("playlist_jobs", payload, ["playlist_url", "status"])


async def upsert_row(table: str, payload: dict, conflict_fields: list = None) -> bool:
    if not supabase_client:
        logger.warning("Supabase client not available for upsert")
        return False
    try:
        if conflict_fields:
            response = supabase_client.table(table).upsert(payload, on_conflict=",".join(conflict_fields)).execute()
        else:
            response = supabase_client.table(table).insert(payload).execute()
        return bool(response.data)
    except Exception as e:
        logger.error(f"Error upserting row in {table}: {e}")
        return False
