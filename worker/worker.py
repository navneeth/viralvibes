"""
Render worker: polls Supabase playlist_jobs table for pending/failed jobs,
processes playlists with YoutubePlaylistService, caches results via db.upsert_playlist_stats,
and updates job status. Intelligently retries failed jobs with backoff.
"""

import asyncio
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

import polars as pl
from dotenv import load_dotenv

from constants import PLAYLIST_JOBS_TABLE
from db import (
    get_latest_playlist_job,
    init_supabase,
    setup_logging,
    supabase_client,
    upsert_playlist_stats,
)
from services.youtube_service import (
    YouTubeBotChallengeError,
    YoutubePlaylistService,
)
from utils import compute_dashboard_id

try:
    from httplib2 import ServerNotFoundError
except ImportError:
    # Fallback if httplib2 is not available
    ServerNotFoundError = Exception

# --- Load environment variables early ---
load_dotenv()

# --- Logging setup ---
setup_logging()
logger = logging.getLogger("vv_worker")

# --- Config with improved defaults ---
POLL_INTERVAL = int(os.getenv("WORKER_POLL_INTERVAL", "30"))
BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "3"))  # Increased back to 3
MAX_RUNTIME = int(os.getenv("WORKER_MAX_RUNTIME", "300")) * 60
MIN_REQUEST_DELAY = float(os.getenv("MIN_REQUEST_DELAY", "1.0"))  # Reduced delay
MAX_REQUEST_DELAY = float(os.getenv("MAX_REQUEST_DELAY", "3.0"))
BOT_CHALLENGE_BACKOFF = int(os.getenv("BOT_CHALLENGE_BACKOFF", "180"))  # 3 min

# --- Retry configuration ---
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
RETRY_BACKOFF_BASE = int(os.getenv("RETRY_BACKOFF_BASE", "300"))  # 5 minutes
FAILED_JOB_RETRY_AGE = int(os.getenv("FAILED_JOB_RETRY_AGE", "3600"))  # 1 hour

# --- Services ---
yt_service = YoutubePlaylistService(backend="youtubeapi")

# --- Graceful shutdown event ---
stop_event = asyncio.Event()

# --- Bot challenge tracking ---
last_bot_challenge_time = None
consecutive_bot_challenges = 0


def handle_exit(sig, frame):
    logger.info(f"Received signal {sig}, shutting down gracefully...")
    stop_event.set()


signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


async def init():
    """Initialize Supabase client and other resources."""
    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    if not url or not key:
        logger.error("Supabase URL or Key not configured. Exiting.")
        raise SystemExit(1)

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
            supabase_client.table(PLAYLIST_JOBS_TABLE)
            .select("*")
            .eq("status", "pending")
            .order("created_at", desc=False)
            .limit(BATCH_SIZE)
            .execute()
        )
        jobs = resp.data or []
        logger.debug(f"Fetched {len(jobs)} pending jobs")
        return jobs
    except Exception as e:
        logger.exception("Failed to fetch pending jobs: %s", e)
        return []


async def fetch_retryable_failed_jobs():
    """
    Fetch failed jobs that are eligible for retry.
    Criteria:
    - Status is 'failed' (not 'blocked')
    - retry_count < MAX_RETRY_ATTEMPTS
    - Last attempt was more than FAILED_JOB_RETRY_AGE seconds ago
    """
    try:
        # Calculate cutoff time for retry eligibility
        cutoff_time = (
            datetime.utcnow() - timedelta(seconds=FAILED_JOB_RETRY_AGE)
        ).isoformat()

        resp = (
            supabase_client.table(PLAYLIST_JOBS_TABLE)
            .select("*")
            .eq("status", "failed")
            .lt("retry_count", MAX_RETRY_ATTEMPTS)
            .lt("finished_at", cutoff_time)
            .order("finished_at", desc=False)  # Oldest first
            .limit(BATCH_SIZE // 2)  # Process fewer retries per batch
            .execute()
        )
        jobs = resp.data or []
        if jobs:
            logger.info(f"Found {len(jobs)} retryable failed jobs")
        return jobs
    except Exception as e:
        logger.exception("Failed to fetch retryable jobs: %s", e)
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
            supabase_client.table(PLAYLIST_JOBS_TABLE)
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


async def attach_dashboard_to_job(job_id: str, playlist_url: str) -> str:
    """
    Compute and return dashboard_id for a job.
    The dashboard_id is persisted in playlist_stats, not in playlist_jobs.
    Returns the dashboard_id.
    """
    dashboard_id = compute_dashboard_id(playlist_url)
    logger.info(f"[Job {job_id}] Computed dashboard_id={dashboard_id}")
    return dashboard_id


async def increment_retry_count(job_id: str, current_count: int = 0):
    """Increment the retry count for a job."""
    try:
        new_count = current_count + 1
        supabase_client.table(PLAYLIST_JOBS_TABLE).update(
            {"retry_count": new_count}
        ).eq("id", job_id).execute()
        logger.info(f"[Job {job_id}] Retry count incremented to {new_count}")
    except Exception as e:
        logger.warning(f"Failed to update retry count for {job_id}: {e}")


def update_progress(job_id: str, processed: int, total: int):
    """Update job progress in database."""
    progress = processed / total if total > 0 else 0
    logger.info(f"Job {job_id}: {progress * 100:.1f}% complete")
    try:
        supabase_client.table(PLAYLIST_JOBS_TABLE).update({"progress": progress}).eq(
            "id", job_id
        ).execute()
    except Exception as e:
        logger.warning(f"Failed to update progress for {job_id}: {e}")


async def check_bot_challenge_cooldown():
    """
    Check if we're in a bot challenge cooldown period.
    Returns True if we should pause processing.
    """
    global last_bot_challenge_time, consecutive_bot_challenges

    if last_bot_challenge_time is None:
        return False

    elapsed = time.time() - last_bot_challenge_time

    # Exponential backoff based on consecutive challenges
    backoff_multiplier = min(consecutive_bot_challenges, 5)
    required_cooldown = BOT_CHALLENGE_BACKOFF * backoff_multiplier

    if elapsed < required_cooldown:
        remaining = required_cooldown - elapsed
        logger.warning(
            f"Bot challenge cooldown active: {int(remaining)}s remaining "
            f"(attempt {consecutive_bot_challenges})"
        )
        return True

    # Cooldown period has passed, reset counter
    consecutive_bot_challenges = 0
    last_bot_challenge_time = None
    logger.info("Bot challenge cooldown period ended, resuming normal operation")
    return False


async def handle_job_failure(
    job_id: str, retry_count: int, error_message: str, error_trace: str = None
) -> bool:
    """
    Handle job failure with retry logic.
    Returns True if retry is scheduled, False if max retries exhausted.
    """
    if retry_count < MAX_RETRY_ATTEMPTS:
        logger.warning(
            f"[Job {job_id}] {error_message} - will retry later "
            f"(attempt {retry_count + 1}/{MAX_RETRY_ATTEMPTS})"
        )
        await increment_retry_count(job_id, retry_count)

        meta = {
            "error": error_message,
            "finished_at": datetime.utcnow().isoformat(),
            "retry_scheduled": True,
        }
        if error_trace:
            meta["error_trace"] = error_trace

        await mark_job_status(job_id, "failed", meta)
        return True
    else:
        logger.error(f"[Job {job_id}] {error_message} - max retries exhausted")

        meta = {
            "error": f"{error_message} (max retries exhausted)",
            "finished_at": datetime.utcnow().isoformat(),
            "retry_scheduled": False,
        }
        if error_trace:
            meta["error_trace"] = error_trace

        await mark_job_status(job_id, "failed", meta)
        return False


def _result_to_mapping(result) -> Dict[str, Any]:
    """Convert UpsertResult dataclass to dict for backward compatibility."""
    if hasattr(result, "__dict__"):
        return vars(result)
    elif isinstance(result, dict):
        return result
    else:
        # Fallback: try to access as dataclass
        return {
            "source": getattr(result, "source", None),
            "df": getattr(result, "df", None),
            "df_json": getattr(result, "df_json", None),
            "summary_stats": getattr(result, "summary_stats", None),
            "summary_stats_json": getattr(result, "summary_stats_json", None),
            "error": getattr(result, "error", None),
        }


async def handle_job(job: Dict[str, Any], is_retry: bool = False):
    """Process a single job dict."""
    global last_bot_challenge_time, consecutive_bot_challenges

    job_id = job.get("id")
    playlist_url = job.get("playlist_url")
    retry_count = job.get("retry_count", 0)

    # Track current stage for precise error location
    current_stage = "start"

    def _set_stage(name: str):
        nonlocal current_stage
        current_stage = name
        logger.info(f"[Job {job_id}] STAGE: {name}")

    _set_stage("start")

    retry_label = f" (retry {retry_count}/{MAX_RETRY_ATTEMPTS})" if is_retry else ""
    logger.info(f"[Job {job_id}] Starting{retry_label} for playlist {playlist_url}")

    # Add random delay before processing
    delay = random.uniform(MIN_REQUEST_DELAY, MAX_REQUEST_DELAY)
    logger.debug(f"[Job {job_id}] Waiting {delay:.2f}s before processing")
    await asyncio.sleep(delay)

    start_time = time.time()
    _set_stage("mark-processing")
    started_ok = await mark_job_status(
        job_id, "processing", {"started_at": datetime.utcnow().isoformat()}
    )
    logger.info(f"[Job {job_id}] mark_job_status(processing) returned: {started_ok}")

    try:
        _set_stage("fetch-playlist-data")
        # Fetch playlist data

        # progress callback tolerant to different caller signatures.
        def _progress_cb(*args, **kwargs):
            """
            Tolerant progress callback that handles various argument patterns.
            Returns an awaitable Future for compatibility with async callers.
            """
            try:
                # Extract processed and total from various argument patterns
                processed = None
                total = None
                meta = {}

                if len(args) >= 3:
                    # (processed, total, meta) or (index, processed, total)
                    if isinstance(args[2], dict):
                        processed, total, meta = args[0], args[1], args[2]
                    else:
                        _, processed, total = args[0], args[1], args[2]
                elif len(args) == 2:
                    # (processed, total)
                    processed, total = args[0], args[1]
                elif len(args) == 1:
                    # Single argument could be processed count or a dict
                    if isinstance(args[0], dict):
                        meta = args[0]
                        processed = meta.get("processed") or meta.get("current", 0)
                        total = meta.get("total") or meta.get("total_items", 0)
                    else:
                        processed = args[0]
                        total = kwargs.get("total") or kwargs.get("total_items", 0)
                else:
                    # No positional args, look in kwargs
                    processed = kwargs.get("processed") or kwargs.get("progress", 0)
                    total = kwargs.get("total") or kwargs.get("total_items", 0)
                    meta = kwargs.get("meta", {})

                # Ensure numeric values
                try:
                    processed = int(float(processed or 0))
                    total = int(float(total or 0))
                except (TypeError, ValueError):
                    logger.warning(
                        f"Invalid progress values: processed={processed}, total={total}"
                    )
                    processed, total = 0, 0

                # Schedule the sync update_progress in the default executor
                loop = asyncio.get_running_loop()
                return loop.run_in_executor(
                    None, update_progress, job_id, processed, total
                )

            except Exception as e:
                logger.exception(f"[Job {job_id}] Progress callback failed: {e}")
                # Return a completed Future
                loop = asyncio.get_running_loop()
                fut = loop.create_future()
                fut.set_result(None)
                return fut

        (
            df,
            playlist_name,
            channel_name,
            channel_thumbnail,
            summary_stats,
        ) = await yt_service.get_playlist_data(
            playlist_url, progress_callback=_progress_cb
        )  # ← Async (100+ concurrent HTTP calls)
        _set_stage("fetched-playlist-data")

        logger.info(
            f"[Job {job_id}] Fetched playlist data: playlist_name={playlist_name} "
            f"channel={channel_name} df_type={type(df)}"
        )

        _set_stage("validate-df")
        # Validate results
        if df is None or not isinstance(df, pl.DataFrame) or df.is_empty():
            error_message = "Empty or invalid playlist data returned."
            logger.warning(f"[Job {job_id}] {error_message}")
            await handle_job_failure(job_id, retry_count, error_message)
            return
        _set_stage("df-validated")

        _set_stage("prepare-stats")
        # Ensure processed video count is in summary_stats
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
            "dashboard_id": compute_dashboard_id(playlist_url),
            "df": df,
        }

        safe_payload = {
            k: v for k, v in stats_to_cache.items() if k not in ["df", "summary_stats"]
        }
        logger.info(
            f"[Job {job_id}] Prepared stats for upsert (playlist={playlist_url})"
        )
        logger.debug(f"[Job {job_id}] Upsert payload keys={list(safe_payload.keys())}")

        _set_stage("upsert-to-db")

        result = upsert_playlist_stats(stats_to_cache)  # ← Sync (single DB write)
        result_map = _result_to_mapping(result)
        logger.info(
            f"[Job {job_id}] Upsert result mapping keys={list(result_map.keys())}"
        )
        _set_stage("upsert-done")

        # Detailed validation: ensure DB confirms presence of serialized payloads
        df_present = bool(result_map.get("df")) or bool(result_map.get("df_json"))
        summary_present = bool(result_map.get("summary_stats")) or bool(
            result_map.get("summary_stats_json")
        )

        if not df_present or not summary_present:
            error_message = "Upsert did not return expected data frames (df or summary_stats missing)"
            logger.warning(f"[Job {job_id}] {error_message}")
            await handle_job_failure(job_id, retry_count, error_message)
            return
        _set_stage("validate-upsert-response")

        # ✅ Success — mark job as done
        source = result_map.get("source")
        if source in ["cache", "fresh"]:
            # Reset bot challenge counter on success
            consecutive_bot_challenges = 0
            last_bot_challenge_time = None

            success_message = (
                f"Completed successfully (source={result_map.get('source')})"
            )
            if is_retry:
                success_message += f" after {retry_count} retries"

            # Compute dashboard_id for logging (don't store in jobs table)
            dashboard_id = compute_dashboard_id(playlist_url)

            await mark_job_status(
                job_id,
                "done",
                {
                    "status_message": success_message,
                    "finished_at": datetime.utcnow().isoformat(),
                    "result_source": result_map.get("source"),
                    # ❌ REMOVED: Don't store dashboard_id in jobs
                },
            )
            _set_stage("marked-done")
            logger.info(f"[Job {job_id}] {success_message}")

        elif source == "error":
            error_msg = result_map.get("error", "Unknown upsert error")
            logger.error(f"[Job {job_id}] Upsert failed: {error_msg}")
            await handle_job_failure(
                job_id, retry_count, f"DB upsert error: {error_msg}"
            )

        else:
            logger.error(f"[Job {job_id}] Unknown upsert source: {source}")
            await handle_job_failure(
                job_id, retry_count, f"Unknown upsert result: {source}"
            )

    except YouTubeBotChallengeError as e:
        # Track bot challenges for backoff
        last_bot_challenge_time = time.time()
        consecutive_bot_challenges += 1

        tb = traceback.format_exc()
        logger.error(
            f"[Job {job_id}] Bot challenge #{consecutive_bot_challenges} "
            f"for playlist {playlist_url}: {e}"
        )
        logger.warning(
            f"Entering cooldown period: {BOT_CHALLENGE_BACKOFF * consecutive_bot_challenges}s"
        )

        # Bot challenges get special handling - don't retry immediately
        await mark_job_status(
            job_id,
            "blocked",
            {
                "error": str(e),
                "error_trace": tb,
                "finished_at": datetime.utcnow().isoformat(),
                "retry_after": datetime.fromtimestamp(
                    time.time() + BOT_CHALLENGE_BACKOFF * consecutive_bot_challenges
                ).isoformat(),
            },
        )

    except (SSLEOFError, ConnectionResetError, SocketTimeout, ServerNotFoundError) as e:
        # Network/SSL errors should be retried
        logger.warning(
            f"[Job {job_id}] Network/SSL error (attempt {retry_count + 1}/{MAX_RETRY_ATTEMPTS}): {type(e).__name__}"
        )

        if retry_count < MAX_RETRY_ATTEMPTS:
            await increment_retry_count(job_id, retry_count)
            await mark_job_status(
                job_id,
                "failed",
                {
                    "error": f"Network error: {str(e)}",
                    "error_type": type(e).__name__,
                    "finished_at": datetime.utcnow().isoformat(),
                },
            )
        else:
            logger.error(f"[Job {job_id}] Network error - max retries exhausted")
            await mark_job_status(
                job_id,
                "failed",
                {
                    "error": f"Network error after {MAX_RETRY_ATTEMPTS} attempts: {str(e)}",
                    "error_type": type(e).__name__,
                    "finished_at": datetime.utcnow().isoformat(),
                },
            )

    except Exception as e:
        tb = traceback.format_exc()
        logger.exception("[Job %s] Unexpected error: %s", job_id, e)

        # Decide if we should retry
        if retry_count < MAX_RETRY_ATTEMPTS:
            logger.warning(
                f"[Job {job_id}] Will retry after {RETRY_BACKOFF_BASE}s "
                f"(attempt {retry_count + 1}/{MAX_RETRY_ATTEMPTS})"
            )
            await increment_retry_count(job_id, retry_count)
            await mark_job_status(
                job_id,
                "failed",
                {
                    "error": str(e)[:1000],
                    "error_trace": tb,
                    "finished_at": datetime.utcnow().isoformat(),
                },
            )
        else:
            logger.error(f"[Job {job_id}] Max retries exhausted")
            await mark_job_status(
                job_id,
                "failed",
                {
                    "error": f"{str(e)[:1000]} (max retries exhausted)",
                    "error_trace": tb,
                    "finished_at": datetime.utcnow().isoformat(),
                },
            )

    finally:
        elapsed = time.time() - start_time
        logger.info(f"[Job {job_id}] Completed in {elapsed:.2f}s")


async def worker_loop():
    """Main worker loop that polls for pending and failed jobs."""
    start_time = time.time()
    jobs_processed = 0
    retries_processed = 0

    logger.info(
        "Worker starting main loop (poll_interval=%ss, max_runtime=%sm, batch_size=%s, "
        "request_delay=%s-%ss, bot_backoff=%ss, max_retries=%s, retry_age=%ss)",
        POLL_INTERVAL,
        MAX_RUNTIME // 60,
        BATCH_SIZE,
        MIN_REQUEST_DELAY,
        MAX_REQUEST_DELAY,
        BOT_CHALLENGE_BACKOFF,
        MAX_RETRY_ATTEMPTS,
        FAILED_JOB_RETRY_AGE,
    )

    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time >= MAX_RUNTIME:
            logger.info(
                "Worker reached max runtime (%sm). Processed %s new jobs, %s retries. Exiting.",
                MAX_RUNTIME // 60,
                jobs_processed,
                retries_processed,
            )
            break

        remaining_time = MAX_RUNTIME - elapsed_time
        if int(elapsed_time) % 300 == 0 and elapsed_time > 0:
            logger.info(
                "Worker progress: %sm elapsed, %sm remaining, %s new jobs, %s retries processed",
                int(elapsed_time // 60),
                int(remaining_time // 60),
                jobs_processed,
                retries_processed,
            )

        try:
            # Check bot challenge cooldown
            if await check_bot_challenge_cooldown():
                backoff_multiplier = min(consecutive_bot_challenges, 5)
                cooldown_sleep = min(
                    BOT_CHALLENGE_BACKOFF * backoff_multiplier, remaining_time
                )
                if cooldown_sleep > 0:
                    await asyncio.sleep(cooldown_sleep)
                continue

            # Fetch both pending and retryable failed jobs
            pending_jobs = await fetch_pending_jobs()
            failed_jobs = await fetch_retryable_failed_jobs()

            # Combine jobs: prioritize pending over retries
            all_jobs = pending_jobs + failed_jobs

            if not all_jobs:
                sleep_time = min(POLL_INTERVAL, remaining_time)
                if sleep_time <= 0:
                    logger.info("No time remaining, exiting worker loop")
                    break
                await asyncio.sleep(sleep_time)
                continue

            for job in all_jobs:
                job_id = job.get("id")
                if not job_id:
                    continue

                # Check bot challenge cooldown before each job
                if await check_bot_challenge_cooldown():
                    logger.info(
                        "Bot challenge cooldown triggered, pausing job processing"
                    )
                    break

                # Claim job atomically
                try:
                    claim_response = (
                        supabase_client.table(PLAYLIST_JOBS_TABLE)
                        .update(
                            {
                                "status": "processing",
                                "started_at": datetime.utcnow().isoformat(),
                            }
                        )
                        .eq("id", job_id)
                        .in_(
                            "status", ["pending", "failed"]
                        )  # Allow claiming failed jobs
                        .execute()
                    )

                    if not claim_response.data:
                        logger.debug(
                            f"[Job {job_id}] Claim failed, another worker took it. Skipping."
                        )
                        continue

                except Exception as e:
                    logger.error(f"[Job {job_id}] Failed to claim job: {e}")
                    continue

                # Check time before processing
                if time.time() - start_time >= MAX_RUNTIME:
                    logger.info("Max runtime reached while processing jobs, stopping")
                    break

                # Process job (with retry flag)
                is_retry = job in failed_jobs
                await handle_job(job, is_retry=is_retry)

                if is_retry:
                    retries_processed += 1
                else:
                    jobs_processed += 1

            # Small delay between polling cycles
            await asyncio.sleep(min(0.5, remaining_time))

        except Exception:
            logger.exception("Unexpected error in worker loop; sleeping before retry")
            error_sleep_time = min(POLL_INTERVAL, remaining_time)
            if error_sleep_time > 0:
                await asyncio.sleep(error_sleep_time)
            else:
                logger.info("No time remaining for error retry sleep, exiting")
                break

    logger.info(
        "Worker loop completed. New jobs: %s, Retries: %s, Total: %s",
        jobs_processed,
        retries_processed,
        jobs_processed + retries_processed,
    )
    return jobs_processed + retries_processed


@dataclass
class JobResult:
    """Structured result returned by Worker.process_one for tests."""

    job_id: str
    status: Optional[str]
    error: Optional[str] = None
    retry_scheduled: Optional[bool] = None
    result_source: Optional[str] = None
    raw_row: Optional[Dict[str, Any]] = None


class Worker:
    """Worker facade exposing a single-iteration API for tests and orchestration."""

    def __init__(self, supabase=None, yt=None, backend="youtubeapi"):
        # Use injected clients if provided (for tests), otherwise fall back to module globals
        self.supabase = supabase or supabase_client
        # Use injected yt_service if provided; otherwise create new with specified backend
        self.yt = yt or globals().get("yt_service")
        logger.info(f"Worker initialized with backend: {backend}")

    async def process_one(
        self, job: Dict[str, Any], is_retry: bool = False
    ) -> JobResult:
        """
        Process a single job and return a deterministic JobResult.

        This calls the existing handle_job() implementation (which performs
        status updates and DB writes). After handle_job returns we fetch the
        latest job row and return a structured result for assertions in tests.
        """
        job_id = job.get("id")
        playlist_url = job.get("playlist_url")

        # Call existing handler (keeps existing behavior)
        try:
            await handle_job(job, is_retry=is_retry)
        except Exception as e:
            # If handle_job raises unexpectedly, record as failed result
            tb = traceback.format_exc()
            logger.exception(f"[Worker.process_one] handle_job raised: {e}\n{tb}")
            return JobResult(
                job_id=job_id or "",
                status="failed",
                error=str(e)[:1000],
                retry_scheduled=None,
                result_source=None,
                raw_row=None,
            )

        # Fetch the final job row from DB for a stable return value
        raw_row = None
        status = None
        error = None
        retry_scheduled = None
        result_source = None

        try:
            # Try to fetch the latest job by id first (fallback to playlist lookup)
            if self.supabase:
                resp = (
                    self.supabase.table(PLAYLIST_JOBS_TABLE)
                    .select("*")
                    .eq("id", job_id)
                    .limit(1)
                    .execute()
                )
                if resp.data:
                    raw_row = resp.data[0]
            # If not found by id, use helper to get latest job for the playlist
            if not raw_row and playlist_url:
                raw_row = get_latest_playlist_job(playlist_url)
        except Exception as e:
            logger.warning(
                f"[Worker.process_one] Failed to fetch job row for {job_id}: {e}"
            )

        if raw_row:
            status = raw_row.get("status")
            error = raw_row.get("error")
            retry_scheduled = raw_row.get("retry_scheduled")
            result_source = raw_row.get("result_source")
        else:
            logger.warning(f"[Worker.process_one] No job row found for {job_id}")

        return JobResult(
            job_id=job_id or "",
            status=status,
            error=error,
            retry_scheduled=retry_scheduled,
            result_source=result_source,
            raw_row=raw_row,
        )


def main():
    """Entrypoint for running the worker."""
    logger.info("Starting ViralVibes worker with retry support...")
    logger.info(
        "Configuration: poll_interval=%ss, batch_size=%s, max_runtime=%sm, "
        "request_delay=%s-%ss, max_retries=%s, retry_age=%ss",
        POLL_INTERVAL,
        BATCH_SIZE,
        MAX_RUNTIME // 60,
        MIN_REQUEST_DELAY,
        MAX_REQUEST_DELAY,
        MAX_RETRY_ATTEMPTS,
        FAILED_JOB_RETRY_AGE,
    )

    try:
        asyncio.run(init())
        jobs_processed = asyncio.run(worker_loop())
        logger.info(
            "Worker completed successfully. Total jobs processed: %s", jobs_processed
        )

    except KeyboardInterrupt:
        logger.info("Worker interrupted by user, exiting gracefully.")
    except SystemExit:
        raise
    except Exception as e:
        logger.exception("Worker failed with unexpected error: %s", e)
        raise SystemExit(1) from e


if __name__ == "__main__":
    main()
