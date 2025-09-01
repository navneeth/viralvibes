"""
Render worker: polls Supabase playlist_jobs table for pending jobs,
processes playlists with YoutubePlaylistService, caches results via db.upsert_playlist_stats,
and updates job status.
"""

import asyncio
import logging
import os
import traceback
from datetime import datetime

from dotenv import load_dotenv  # <-- Add this

from db import (  # <-- Import setup_logging
    init_supabase,
    setup_logging,
    supabase_client,
    upsert_playlist_stats,
)
from youtube_service import YoutubePlaylistService

# --- Load environment variables ---
load_dotenv()  # <-- Load .env early

# --- Logging setup ---
setup_logging()  # <-- Use shared logging config
logger = logging.getLogger("vv_worker")

# --- Config ---
POLL_INTERVAL = int(os.getenv("WORKER_POLL_INTERVAL", "10"))
BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "3"))

# --- Services ---
yt_service = YoutubePlaylistService()


async def init():
    """Initialize Supabase client and other resources."""
    try:
        client = init_supabase()
        if client is not None:
            global supabase_client
            supabase_client = client
            logger.info("Supabase initialized for worker.")
        else:
            logger.error("Supabase environment not configured. Worker cannot run.")
            raise SystemExit(1)
    except Exception as e:
        logger.error(f"Unexpected error during Supabase initialization: {str(e)}")
        raise SystemExit(1) from e


async def fetch_pending_jobs():
    """Return list of pending job rows from playlist_jobs table."""
    try:
        resp = (
            supabase_client.table("playlist_jobs")
            .select("*")
            .eq("status", "pending")
            .order("created_at", desc=False)  # <-- Correct usage for ascending order
            .limit(BATCH_SIZE)
            .execute()
        )
        return resp.data or []
    except Exception as e:
        logger.exception("Failed to fetch pending jobs: %s", e)
        return []


def mark_job_status(job_id, status, meta: dict | None = None):
    """Update job status synchronously via Supabase client."""
    payload = {"status": status, "updated_at": datetime.utcnow().isoformat()}
    if meta:
        payload.update(meta)

    try:
        supabase_client.table("playlist_jobs").update(payload).eq(
            "id", job_id
        ).execute()
    except Exception:
        logger.exception("Failed to update job status for id=%s", job_id)


async def handle_job(job):
    """Process a single job dict."""
    job_id = job.get("id")
    playlist_url = job.get("playlist_url")
    logger.info("Picked job id=%s playlist=%s", job_id, playlist_url)

    mark_job_status(job_id, "processing", {"started_at": datetime.utcnow().isoformat()})

    try:
        # fetch playlist data
        (
            df,
            playlist_name,
            channel_name,
            channel_thumbnail,
            summary_stats,
        ) = await yt_service.get_playlist_data(playlist_url)

        stats_to_cache = {
            "playlist_url": playlist_url,
            "title": playlist_name,
            "channel_name": channel_name,
            "channel_thumbnail": channel_thumbnail,
            "view_count": summary_stats.get("total_views"),
            "like_count": summary_stats.get("total_likes"),
            "dislike_count": summary_stats.get("total_dislikes"),
            "comment_count": summary_stats.get("total_comments"),
            "video_count": getattr(df, "height", 0) if df is not None else 0,
            "avg_duration": summary_stats.get("avg_duration"),
            "engagement_rate": summary_stats.get("avg_engagement"),
            "controversy_score": summary_stats.get("avg_controversy", 0),
            "summary_stats": summary_stats,
            "df": df,
        }

        result = await upsert_playlist_stats(stats_to_cache)
        logger.info(
            "Upsert result source=%s for playlist=%s",
            result.get("source"),
            playlist_url,
        )

        mark_job_status(
            job_id,
            "done",
            {
                "status_message": f"done (source={result.get('source')})",
                "finished_at": datetime.utcnow().isoformat(),
                "result_source": result.get("source"),
            },
        )

    except Exception as e:
        tb = traceback.format_exc()
        logger.exception("Job %s failed: %s", job_id, e)
        mark_job_status(
            job_id,
            "failed",
            {
                "error": str(e)[:1000],
                "error_trace": tb,
                "finished_at": datetime.utcnow().isoformat(),
            },
        )


async def worker_loop():
    """Main worker loop that polls for jobs and processes them."""
    logger.info("Worker starting main loop (poll interval=%s)", POLL_INTERVAL)
    while True:
        try:
            jobs = await fetch_pending_jobs()
            if not jobs:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            for job in jobs:
                await handle_job(job)

            await asyncio.sleep(0.5)
        except Exception:
            logger.exception("Unexpected error in worker loop; sleeping before retry")
            await asyncio.sleep(max(1, POLL_INTERVAL))


def main():
    """Entrypoint for running the worker."""
    logger.info("Starting Render worker...")
    try:
        asyncio.run(init())
        asyncio.run(worker_loop())
    except KeyboardInterrupt:
        logger.info("Worker interrupted, exiting.")


if __name__ == "__main__":
    main()
