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
from typing import Any, Dict, Optional

from dotenv import load_dotenv

from db import (
    init_supabase,
    setup_logging,
    supabase_client,
    upsert_playlist_stats,
)
from youtube_service import YouTubeBotChallengeError, YoutubePlaylistService

# --- Load environment variables early ---
load_dotenv()

# --- Logging setup ---
setup_logging()  # <-- Use shared logging config
logger = logging.getLogger("vv_worker")

# --- Config ---
POLL_INTERVAL = int(os.getenv("WORKER_POLL_INTERVAL", "10"))
BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "3"))
MAX_RUNTIME = int(os.getenv("WORKER_MAX_RUNTIME", "300")) * 60  # minutes → seconds

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
        jobs = resp.data or []
        logger.debug(f"Fetched {len(jobs)} pending jobs")
        return jobs
    except Exception as e:
        logger.exception("Failed to fetch pending jobs: %s", e)
        return []


async def mark_job_status(
    job_id: str, status: str, meta: Optional[Dict[str, Any]] = None
) -> bool:
    """Update a job's status with optional metadata."""
    if not supabase_client:
        logger.error(
            f"Cannot mark job {job_id} as {status}: Supabase client not initialized"
        )
        return False

    try:
        payload = {"status": status}
        if meta:
            payload.update(meta)

        response = (
            supabase_client.table("playlist_jobs")
            .update(payload)
            .eq("id", job_id)
            .execute()
        )

        success = bool(response.data)
        if not success:
            logger.error(f"Failed to update status for job {job_id}")
        return success

    except Exception as e:
        logger.exception(f"Error updating job {job_id} status: {e}")
        return False


async def handle_job(job):
    """Process a single job dict."""
    job_id = job.get("id")
    playlist_url = job.get("playlist_url")
    logger.info(f"[Job {job_id}] Starting for playlist {playlist_url}")

    # Start a timer
    start_time = time.time()
    await mark_job_status(
        job_id, "processing", {"started_at": datetime.utcnow().isoformat()}
    )

    try:
        # fetch playlist data
        (
            df,
            playlist_name,
            channel_name,
            channel_thumbnail,
            summary_stats,
        ) = await yt_service.get_playlist_data(playlist_url)

        # --- FIX: Add validation for empty results ---
        if df is None or df.is_empty():
            error_message = (
                "No valid videos found in the playlist or failed to process."
            )
            logger.error(f"[Job {job_id}] {error_message}")
            mark_job_status(
                job_id,
                "failed",
                {
                    "error": error_message,
                    "finished_at": datetime.utcnow().isoformat(),
                },
            )
            # Stop further execution for this job
            return

        # Ensure processed video count is in summary_stats for UI consistency
        processed_video_count = getattr(df, "height", 0) if df is not None else 0
        summary_stats["processed_video_count"] = processed_video_count

        stats_to_cache = {
            "playlist_url": playlist_url,
            "title": playlist_name,
            "channel_name": channel_name,
            "channel_thumbnail": channel_thumbnail,
            "view_count": summary_stats.get("total_views"),
            "like_count": summary_stats.get("total_likes"),
            "dislike_count": summary_stats.get("total_dislikes"),
            "comment_count": summary_stats.get("total_comments"),
            "video_count": summary_stats.get(
                "actual_playlist_count", processed_video_count
            ),
            "processed_video_count": processed_video_count,
            "avg_duration": summary_stats.get("avg_duration"),
            "engagement_rate": summary_stats.get("avg_engagement"),
            "controversy_score": summary_stats.get("avg_controversy", 0),
            "summary_stats": summary_stats,
            "df": df,
        }

        safe_payload = {
            k: v for k, v in stats_to_cache.items() if k not in ["df", "summary_stats"]
        }
        logger.info(
            f"[Job {job_id}] Prepared stats for upsert (playlist={playlist_url})"
        )
        logger.debug(f"[Job {job_id}] Upsert payload={safe_payload}")

        result = await upsert_playlist_stats(stats_to_cache)
        logger.info(f"[Job {job_id}] Upsert result source={result.get('source')}")
        logger.debug(f"[Job {job_id}] Upsert result keys={list(result.keys())}")

        # --- FIX: Fail fast if upsert returns an error ---
        if result.get("source") == "error":
            error_message = "Upsert failed due to serialization or DB error."
            logger.error(f"[Job {job_id}] {error_message}")
            mark_job_status(
                job_id,
                "failed",
                {
                    "error": error_message,
                    "finished_at": datetime.utcnow().isoformat(),
                },
            )
            return

        # --- FIX: Validate that the upsert result contains critical data ---
        df = result.get("df")
        summary_stats = result.get("summary_stats")

        if df is None or df.is_empty() or not summary_stats:
            error_message = (
                "Incomplete data returned from upsert, critical fields missing."
            )
            logger.error(f"[Job {job_id}] {error_message}")
            mark_job_status(
                job_id,
                "failed",
                {
                    "error": error_message,
                    "finished_at": datetime.utcnow().isoformat(),
                },
            )
            return

        # --- FIX: Check if analysis results are valid ---
        if result.get("df") is None or result.get("df").is_empty():
            error_message = "Analysis produced no valid data"
            logger.error(f"[Job {job_id}] {error_message}")
            await mark_job_status(
                job_id,
                "failed",
                {
                    "error": error_message,
                    "finished_at": datetime.utcnow().isoformat(),
                },
            )
            return

        if result.get("source") in ["cache", "fresh"]:
            await mark_job_status(
                job_id,
                "done",
                {
                    "status_message": f"done (source={result.get('source')})",
                    "finished_at": datetime.utcnow().isoformat(),
                    "result_source": result.get("source"),
                },
            )
        else:
            # If upsert failed, mark the job as failed
            error_message = "Failed to cache playlist stats after processing."
            logger.error(f"[Job {job_id}] {error_message}")
            await mark_job_status(
                job_id,
                "failed",
                {
                    "error": error_message,
                    "finished_at": datetime.utcnow().isoformat(),
                },
            )

    except YouTubeBotChallengeError as e:
        tb = traceback.format_exc()
        logger.error(f"[Job {job_id}] Bot challenge for playlist {playlist_url}: {e}")
        await mark_job_status(
            job_id,
            "blocked",
            {
                "error": str(e),
                "error_trace": tb,
                "finished_at": datetime.utcnow().isoformat(),
            },
        )

    except Exception as e:
        tb = traceback.format_exc()
        logger.exception("[Job %s] Unexpected error: %s", job_id, e)
        # Can still log the time of failure if needed
        await mark_job_status(
            job_id,
            "failed",
            {
                "error": str(e)[:1000],
                "error_trace": tb,
                "finished_at": datetime.utcnow().isoformat(),
            },
        )

    finally:
        elapsed = time.time() - start_time
        logger.info(f"[Job {job_id}] Completed in {elapsed:.2f}s")


async def worker_loop():
    """Main worker loop that polls for jobs and processes them."""
    start_time = time.time()
    jobs_processed = 0

    logger.info(
        "Worker starting main loop (poll_interval=%ss, max_runtime=%sm, batch_size=%s)",
        POLL_INTERVAL,
        MAX_RUNTIME // 60,
        BATCH_SIZE,
    )

    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time >= MAX_RUNTIME:
            logger.info(
                "Worker reached max runtime (%sm). Processed %s jobs. Exiting.",
                MAX_RUNTIME // 60,
                jobs_processed,
            )
            break

        # Check remaining time and log progress periodically
        remaining_time = MAX_RUNTIME - elapsed_time
        if int(elapsed_time) % 300 == 0 and elapsed_time > 0:  # Log every 5 minutes
            logger.info(
                "Worker progress: %sm elapsed, %sm remaining, %s jobs processed",
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
                job_id = job.get("id")
                if not job_id:
                    continue

                # --- FIX: Add transactional job claiming to prevent race conditions ---
                try:
                    claim_response = (
                        supabase_client.table("playlist_jobs")
                        .update(
                            {
                                "status": "processing",
                                "started_at": datetime.utcnow().isoformat(),
                            }
                        )
                        .eq("id", job_id)
                        .eq("status", "pending")  # <-- Conditional update
                        .execute()
                    )

                    # If data is empty, another worker claimed the job.
                    if not claim_response.data:
                        logger.info(
                            f"[Job {job_id}] Claim failed, another worker likely took it. Skipping."
                        )
                        continue  # Skip to the next job

                except Exception as e:
                    logger.error(
                        f"[Job {job_id}] Failed to claim job due to DB error: {e}"
                    )
                    continue  # Skip to the next job

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
