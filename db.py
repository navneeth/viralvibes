"""
Database operations module.
Handles Supabase client initialization, logging setup, and caching functionality.
"""
import base64
import io
import json
import logging
import os
from datetime import date
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
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')


@retry(stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1, min=4, max=10),
       reraise=True)
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
async def get_cached_playlist_stats(
        playlist_url: str) -> Optional[Dict[str, Any]]:
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

        response = supabase_client.table("playlist_stats").select("*").eq(
            "playlist_url", playlist_url).eq("processed_date",
                                             today).limit(1).execute()

        if response.data and len(response.data) > 0:
            logger.info(f"Cache hit for playlist: {playlist_url}")
            row = response.data[0]

            # --- Deserialize JSON fields ---
            if row.get("df_json"):
                row["df"] = pl.read_json(row["df_json"])
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

    try:
        # Add processed_date to stats
        stats_with_date = {**stats, "processed_date": date.today().isoformat()}

        # --- Ensure JSON serializable fields ---
        if "df" in stats_with_date:  # Polars DataFrame
            stats_with_date["df_json"] = stats_with_date["df"].write_json()
            del stats_with_date["df"]
        if "summary_stats" in stats_with_date:
            stats_with_date["summary_stats"] = json.dumps(
                stats_with_date["summary_stats"])

        # Insert with conflict handling (ON CONFLICT DO NOTHING equivalent)
        response = supabase_client.table("playlist_stats").upsert(
            stats_with_date,
            on_conflict="playlist_url,processed_date").execute()

        if response.data:
            logger.info(
                f"Successfully inserted stats for playlist: {stats.get('playlist_url')}"
            )
            return True
        else:
            logger.warning(
                f"No data returned from insert for playlist: {stats.get('playlist_url')}"
            )
            return False

    except Exception as e:
        logger.error(
            f"Error inserting stats for playlist {stats.get('playlist_url')}: {e}"
        )
        return False


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
        logger.error(
            f"Failed to insert fresh stats for playlist: {playlist_url}")

    return stats


CACHE_TABLE = "playlist_cache"


async def get_cached_playlist(playlist_url: str):
    """Fetch cached playlist data from Supabase"""
    if not supabase_client:
        logger.warning("Supabase client not available for caching")
        return None

    res = supabase_client.table(CACHE_TABLE).select("*").eq(
        "playlist_url", playlist_url).execute()
    if not res.data:
        return None

    row = res.data[0]
    # Decode parquet -> polars df
    df_bytes = base64.b64decode(row["df_parquet"])
    df = pl.read_parquet(io.BytesIO(df_bytes))
    return df, row["playlist_name"], row["channel_name"], row[
        "channel_thumbnail"], row["summary_stats"]


async def cache_playlist(playlist_url: str, df: pl.DataFrame,
                         playlist_name: str, channel_name: str,
                         channel_thumbnail: str, summary_stats: dict):
    """Store playlist df + metadata in Supabase"""
    if not supabase_client:
        logger.warning("Supabase client not available for caching")
        return False

    try:
        buf = io.BytesIO()
        df.write_parquet(buf)
        df_bytes = base64.b64encode(buf.getvalue()).decode("utf-8")

        supabase_client.table(CACHE_TABLE).upsert({
            "playlist_url":
            playlist_url,
            "df_parquet":
            df_bytes,
            "playlist_name":
            playlist_name,
            "channel_name":
            channel_name,
            "channel_thumbnail":
            channel_thumbnail,
            "summary_stats":
            summary_stats
        }).execute()
        return True
    except Exception as e:
        logger.error(f"Failed to cache playlist {playlist_url}: {e}")
        return False
