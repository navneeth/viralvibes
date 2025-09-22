"""
Render worker: polls Supabase playlist_jobs table for pending jobs,
processes playlists with YoutubePlaylistService, caches results via db.upsert_playlist_stats,
and updates job status.
"""

import asyncio
import logging
import os
import time
import traceback
from datetime import datetime

from dotenv import load_dotenv

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
MAX_RUNTIME = (
    int(os.getenv("WORKER_MAX_RUNTIME", "300")) * 60
)  # Convert minutes to seconds

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
    logger.info(f"Starting job {job_id} for playlist {playlist_url}")

    # Start a timer
    start_time = time.time()

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

        # After processing, calculate duration
        duration_s = time.time() - start_time
        duration_ms = int(duration_s * 1000)

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
                "processing_time_ms": duration_ms,  # Store the actual processing time
            },
        )

    except Exception as e:
        tb = traceback.format_exc()
        logger.exception("Job %s failed: %s", job_id, e)
        # Can still log the time of failure if needed
        duration_s = time.time() - start_time
        duration_ms = int(duration_s * 1000)
        mark_job_status(
            job_id,
            "failed",
            {
                "error": str(e)[:1000],
                "error_trace": tb,
                "finished_at": datetime.utcnow().isoformat(),
                "processing_time_ms": duration_ms,
            },
        )


async def worker_loop():
    """Main worker loop that polls for jobs and processes them."""
    start_time = time.time()
    jobs_processed = 0

    logger.info(
        "Worker starting main loop (poll interval=%ss, max runtime=%sm, batch size=%s)",
        POLL_INTERVAL,
        MAX_RUNTIME // 60,
        BATCH_SIZE,
    )
    while True:
        # Check if we've exceeded max runtime
        elapsed_time = time.time() - start_time
        if elapsed_time >= MAX_RUNTIME:
            logger.info(
                "Worker reached max runtime of %s minutes. Processed %s jobs. Exiting gracefully.",
                MAX_RUNTIME // 60,
                jobs_processed,
            )
            break

        # Check remaining time and log progress periodically
        remaining_time = MAX_RUNTIME - elapsed_time
        if int(elapsed_time) % 300 == 0 and elapsed_time > 0:  # Log every 5 minutes
            logger.info(
                "Worker progress: %s minutes elapsed, %s minutes remaining, %s jobs processed",
                int(elapsed_time // 60),
                int(remaining_time // 60),
                jobs_processed,
            )

        try:
            jobs = await fetch_pending_jobs()
            if not jobs:
                # No jobs available, sleep for poll interval or remaining time (whichever is shorter)
                sleep_time = min(POLL_INTERVAL, remaining_time)
                if sleep_time <= 0:
                    logger.info("No time remaining, exiting worker loop")
                    break
                await asyncio.sleep(sleep_time)
                continue

            for job in jobs:
                # Check time before processing each job
                if time.time() - start_time >= MAX_RUNTIME:
                    logger.info("Max runtime reached while processing jobs, stopping")
                    break  # Break from job processing loop

                await handle_job(job)
                jobs_processed += 1

            await asyncio.sleep(0.5)

        except Exception:
            logger.exception("Unexpected error in worker loop; sleeping before retry")
            # Sleep for a short duration before retrying to avoid tight error loop
            # Allow shorter sleeps when time is running out
            error_sleep_time = (
                min(1, remaining_time)
                if remaining_time < 1
                else min(POLL_INTERVAL, remaining_time)
            )
            if error_sleep_time > 0:
                await asyncio.sleep(error_sleep_time)
            else:
                logger.info("No time remaining for error retry sleep, exiting")
                break

    logger.info("Worker loop completed. Total jobs processed: %s", jobs_processed)
    return jobs_processed


def main():
    """Entrypoint for running the worker."""
    logger.info("Starting ViralVibes worker...")
    logger.info(
        "Configuration: poll_interval=%ss, batch_size=%s, max_runtime=%sm",
        POLL_INTERVAL,
        BATCH_SIZE,
        MAX_RUNTIME // 60,
    )

    try:
        # Initialize
        asyncio.run(init())
        # Run worker loop
        jobs_processed = asyncio.run(worker_loop())

        logger.info("Worker completed successfully. Jobs processed: %s", jobs_processed)

    except KeyboardInterrupt:
        logger.info("Worker interrupted by user, exiting gracefully.")
    except SystemExit:
        raise  # Re-raise SystemExit from init()
    except Exception as e:
        logger.exception("Worker failed with unexpected error: %s", e)
        raise SystemExit(1) from e


if __name__ == "__main__":
    main()
