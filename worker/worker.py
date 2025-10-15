"""
Render worker: polls Supabase playlist_jobs table for pending/failed jobs,
processes playlists with YouTubeService (auto backend fallback),
caches results via db.upsert_playlist_stats, and updates job status.
"""

import asyncio
import json
import logging
import os
import random
import signal
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from socket import timeout as SocketTimeout
from ssl import SSLEOFError
from typing import Any, Dict, Optional

from dotenv import load_dotenv

# --- Local imports ---
from db import (
    get_latest_playlist_job,
    init_supabase,
    setup_logging,
    supabase_client,
    upsert_playlist_stats,
)
from services.backends.base import BotChallengeError
from services.data_utils import normalize_columns
from services.youtube_service import (
    YouTubeService,
)

# --- External optional import fallback ---
try:
    from httplib2 import ServerNotFoundError
except ImportError:
    ServerNotFoundError = Exception

# --- Load environment variables ---
load_dotenv()

# --- Logging setup ---
setup_logging()
logger = logging.getLogger("vv_worker")

# --- Config ---
POLL_INTERVAL = int(os.getenv("WORKER_POLL_INTERVAL", "30"))
BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "3"))
MAX_RUNTIME = int(os.getenv("WORKER_MAX_RUNTIME", "300")) * 60
MIN_REQUEST_DELAY = float(os.getenv("MIN_REQUEST_DELAY", "1.0"))
MAX_REQUEST_DELAY = float(os.getenv("MAX_REQUEST_DELAY", "3.0"))
BOT_CHALLENGE_BACKOFF = int(os.getenv("BOT_CHALLENGE_BACKOFF", "180"))

# --- Retry configuration ---
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
RETRY_BACKOFF_BASE = int(os.getenv("RETRY_BACKOFF_BASE", "300"))
FAILED_JOB_RETRY_AGE = int(os.getenv("FAILED_JOB_RETRY_AGE", "3600"))

# --- Initialize YouTube Service ---
yt_service = YouTubeService(primary_backend="youtubeapi", enable_fallback=True)

# --- Graceful shutdown ---
stop_event = asyncio.Event()

# --- Bot challenge tracking ---
last_bot_challenge_time = None
consecutive_bot_challenges = 0


def handle_exit(sig, frame):
    logger.info(f"Received signal {sig}, shutting down gracefully...")
    stop_event.set()


signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


# ============================================================
# Initialization
# ============================================================
async def init():
    """Initialize Supabase client and YouTube service."""
    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

    if not url or not key:
        logger.error("Supabase URL or Key not configured. Exiting.")
        raise SystemExit(1)

    try:
        client = init_supabase()
        if client:
            global supabase_client
            supabase_client = client
            logger.info("Supabase initialized for worker.")
        else:
            raise RuntimeError("Supabase environment not configured.")
    except Exception as e:
        logger.exception("Failed to initialize Supabase.")
        raise SystemExit(1) from e


# ============================================================
# Job Fetching & Marking
# ============================================================
async def fetch_pending_jobs():
    """Return list of pending job rows."""
    try:
        resp = (
            supabase_client.table("playlist_jobs")
            .select("*")
            .eq("status", "pending")
            .order("created_at", desc=False)
            .limit(BATCH_SIZE)
            .execute()
        )
        return resp.data or []
    except Exception as e:
        logger.exception("Failed to fetch pending jobs: %s", e)
        return []


async def fetch_retryable_failed_jobs():
    """Return failed jobs eligible for retry."""
    try:
        cutoff_time = (
            datetime.utcnow() - timedelta(seconds=FAILED_JOB_RETRY_AGE)
        ).isoformat()
        resp = (
            supabase_client.table("playlist_jobs")
            .select("*")
            .eq("status", "failed")
            .lt("retry_count", MAX_RETRY_ATTEMPTS)
            .lt("finished_at", cutoff_time)
            .order("finished_at", desc=False)
            .limit(BATCH_SIZE // 2)
            .execute()
        )
        return resp.data or []
    except Exception as e:
        logger.exception("Failed to fetch retryable jobs: %s", e)
        return []


async def mark_job_status(
    job_id: str, status: str, meta: Optional[Dict[str, Any]] = None
):
    """Update a job's status."""
    try:
        payload = {"status": status}
        if meta:
            payload.update(meta)
        supabase_client.table("playlist_jobs").update(payload).eq(
            "id", job_id
        ).execute()
        return True
    except Exception as e:
        logger.warning(f"Failed to mark job {job_id} as {status}: {e}")
        return False


async def increment_retry_count(job_id: str, current_count: int = 0):
    """Increment retry count."""
    try:
        supabase_client.table("playlist_jobs").update(
            {"retry_count": current_count + 1}
        ).eq("id", job_id).execute()
    except Exception as e:
        logger.warning(f"Failed to increment retry count for {job_id}: {e}")


async def update_progress(job_id: str, processed: int, total: int):
    """Update progress in Supabase."""
    try:
        progress = processed / total if total > 0 else 0
        supabase_client.table("playlist_jobs").update({"progress": progress}).eq(
            "id", job_id
        ).execute()
    except Exception as e:
        logger.warning(f"Failed to update progress for {job_id}: {e}")


# ============================================================
# Bot Challenge & Retry Handling
# ============================================================
async def check_bot_challenge_cooldown():
    """Pause worker if bot challenge recently triggered."""
    global last_bot_challenge_time, consecutive_bot_challenges
    if not last_bot_challenge_time:
        return False

    elapsed = time.time() - last_bot_challenge_time
    backoff_multiplier = min(consecutive_bot_challenges, 5)
    cooldown_time = BOT_CHALLENGE_BACKOFF * backoff_multiplier

    if elapsed < cooldown_time:
        logger.warning(
            f"Bot cooldown active ({int(cooldown_time - elapsed)}s remaining)"
        )
        return True

    consecutive_bot_challenges = 0
    last_bot_challenge_time = None
    return False


# ============================================================
# Core Job Handler
# ============================================================
async def handle_job(job: Dict[str, Any], is_retry: bool = False):
    """Process one playlist job."""
    global last_bot_challenge_time, consecutive_bot_challenges
    job_id = job.get("id")
    playlist_url = job.get("playlist_url")
    retry_count = job.get("retry_count", 0)

    logger.info(
        f"[Job {job_id}] Processing {'(retry)' if is_retry else ''} {playlist_url}"
    )
    await mark_job_status(
        job_id, "processing", {"started_at": datetime.utcnow().isoformat()}
    )

    try:
        df, title, channel, thumbnail, stats = await yt_service.get_playlist_data(
            playlist_url,
            progress_callback=lambda p, t: update_progress(job_id, p, t),
        )

        if df.is_empty():
            raise ValueError("Empty DataFrame — no valid videos processed")

        df = normalize_columns(df)
        stats["processed_video_count"] = df.height

        payload = {
            "playlist_url": playlist_url,
            "title": title,
            "channel_name": channel,
            "channel_thumbnail": thumbnail,
            "view_count": stats.get("total_views"),
            "like_count": stats.get("total_likes"),
            "dislike_count": stats.get("total_dislikes"),
            "comment_count": stats.get("total_comments"),
            "processed_video_count": df.height,
            "avg_engagement": stats.get("avg_engagement"),
            "controversy_score": stats.get("avg_controversy", 0),
            "summary_stats": stats,
            "df": df,
        }

        result = await upsert_playlist_stats(payload)
        if result.get("source") not in ("cache", "fresh"):
            raise RuntimeError(f"Unexpected upsert source: {result.get('source')}")

        await mark_job_status(
            job_id,
            "done",
            {
                "status_message": f"Completed ({result.get('source')})",
                "finished_at": datetime.utcnow().isoformat(),
                "result_source": result.get("source"),
            },
        )
        logger.info(
            f"[Job {job_id}] Completed successfully via {stats['backend_used']}"
        )

    except BotChallengeError as e:
        last_bot_challenge_time = time.time()
        consecutive_bot_challenges += 1
        logger.error(f"[Job {job_id}] Bot challenge #{consecutive_bot_challenges}: {e}")
        await mark_job_status(
            job_id,
            "blocked",
            {
                "error": str(e),
                "finished_at": datetime.utcnow().isoformat(),
                "retry_after": datetime.fromtimestamp(
                    time.time() + BOT_CHALLENGE_BACKOFF * consecutive_bot_challenges
                ).isoformat(),
            },
        )

    except (SSLEOFError, ConnectionResetError, SocketTimeout, ServerNotFoundError) as e:
        logger.warning(f"[Job {job_id}] Network error: {type(e).__name__}")
        await increment_retry_count(job_id, retry_count)
        await mark_job_status(
            job_id,
            "failed",
            {
                "error": str(e),
                "finished_at": datetime.utcnow().isoformat(),
                "retry_scheduled": retry_count + 1 < MAX_RETRY_ATTEMPTS,
            },
        )

    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"[Job {job_id}] Error: {e}")
        await increment_retry_count(job_id, retry_count)
        await mark_job_status(
            job_id,
            "failed",
            {
                "error": str(e)[:1000],
                "error_trace": tb,
                "finished_at": datetime.utcnow().isoformat(),
                "retry_scheduled": retry_count + 1 < MAX_RETRY_ATTEMPTS,
            },
        )


# ============================================================
# Main Worker Loop
# ============================================================
async def worker_loop():
    start_time = time.time()
    logger.info(
        "ViralVibes worker started (batch=%s, poll=%ss)", BATCH_SIZE, POLL_INTERVAL
    )

    while not stop_event.is_set():
        elapsed = time.time() - start_time
        if elapsed > MAX_RUNTIME:
            logger.info("Max runtime reached. Shutting down worker.")
            break

        if await check_bot_challenge_cooldown():
            await asyncio.sleep(BOT_CHALLENGE_BACKOFF)
            continue

        pending = await fetch_pending_jobs()
        retryable = await fetch_retryable_failed_jobs()
        jobs = pending + retryable

        if not jobs:
            await asyncio.sleep(POLL_INTERVAL)
            continue

        for job in jobs:
            await handle_job(job, is_retry=(job in retryable))
            await asyncio.sleep(random.uniform(MIN_REQUEST_DELAY, MAX_REQUEST_DELAY))

    logger.info("Worker exiting after %.1fs", time.time() - start_time)


# ============================================================
# Entrypoint
# ============================================================
def main():
    """Entrypoint for Render worker."""
    logger.info("Starting ViralVibes worker (YouTubeService backend)")
    try:
        asyncio.run(init())
        asyncio.run(worker_loop())
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user.")
    except Exception as e:
        logger.exception("Worker crashed: %s", e)
        raise SystemExit(1)
    finally:
        # Close YouTube backends cleanly
        try:
            asyncio.run(yt_service.close())
        except Exception:
            pass


if __name__ == "__main__":
    main()
