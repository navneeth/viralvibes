"""
worker/render_worker.py — Render.com-compatible entrypoint for the ViralVibes creator worker.

WHY THIS FILE EXISTS
────────────────────
worker/creator_worker.py was designed for a *bash respawn loop*: it exits with
code 0 after each job so a shell wrapper can start a fresh Python process with a
clean httplib2 state (EXIT_AFTER_JOB = True, line 78).

Render's worker service type has no such loop — when the process exits the
service is marked as failed and restarts cold (with a long delay on the free
tier). That is what the dashboard showed: one job processed → EXIT_AFTER_JOB
fires → service terminates → "Application exited early while running your code."

This entrypoint solves the problem by:
  1. Calling handle_sync_job() directly, completely bypassing
     process_creator_syncs() and its three exit paths (EXIT_AFTER_JOB, empty-
     queue bail-out, MAX_RUNTIME).
  2. Replacing process-level memory isolation with periodic YouTubeResolver
     resets — the same technique used in kaggle_worker.py.
  3. Never exiting unless SIGTERM/SIGINT is received (graceful shutdown) or an
     unrecoverable fatal error occurs.

DEPLOYMENT
──────────
Place this file at:  worker/render_worker.py
Run via render.yaml: python -m worker.render_worker
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import time

# ── Step 1: load secrets before any worker import touches os.environ ──────────
# secrets_loader.py lives at the project root; accessible because
# `python -m worker.render_worker` adds the project root to sys.path.
from secrets_loader import load_secrets

_env_source = load_secrets()

# ── Step 2: logging (mirrors creator_worker.py boot sequence) ─────────────────
from db import setup_logging

setup_logging()
logger = logging.getLogger("render_worker")
logger.info("render_worker starting — secrets loaded via: %s", _env_source)

# ── Step 3: worker internals ───────────────────────────────────────────────────
# All paths below are valid when run as `python -m worker.render_worker`
# from the project root.
import worker.creator_worker as _cw
from services.channel_utils import YouTubeResolver
from services.youtube_config import get_creator_worker_api_key

# ── Tuning ────────────────────────────────────────────────────────────────────

# How often to reset the YouTubeResolver (httplib2 memory hygiene).
# Mirrors the subprocess-per-job isolation used in the bash-loop pattern.
RESOLVER_RESET_INTERVAL: int = int(os.getenv("RENDER_RESOLVER_RESET_INTERVAL", "50"))

# How long to sleep between polls when the queue is empty (seconds).
# We never exit on an empty queue — we just back off and keep polling.
IDLE_POLL_INTERVAL: int = int(os.getenv("RENDER_IDLE_POLL_INTERVAL", "60"))

# How often to run periodic maintenance (queue unsynced / stale creators).
# In seconds — default every 5 minutes.
PERIODIC_CHECK_INTERVAL: int = int(os.getenv("RENDER_PERIODIC_CHECK_INTERVAL", "300"))

# Log a warning (not exit) after this many consecutive empty polls.
EMPTY_WARN_THRESHOLD: int = int(os.getenv("RENDER_EMPTY_WARN_THRESHOLD", "10"))

# ── Graceful-shutdown wiring ──────────────────────────────────────────────────

_shutdown = asyncio.Event()


def _handle_signal(sig, _frame):
    logger.info("Received signal %s — initiating graceful shutdown", sig)
    _shutdown.set()
    _cw.stop_event.set()  # also tells the inner worker to stop if it's mid-loop


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ── Main loop ─────────────────────────────────────────────────────────────────


async def run() -> None:
    """
    Outer loop that keeps the worker alive on Render indefinitely.

    Key differences from process_creator_syncs():
    - No EXIT_AFTER_JOB bail-out
    - No empty-queue exit (backs off + keeps polling instead)
    - No MAX_RUNTIME exit
    - Periodic maintenance is time-gated (not every iteration)
    - YouTubeResolver reset every RESOLVER_RESET_INTERVAL jobs
    """
    logger.info("=" * 60)
    logger.info("🚀 ViralVibes Creator Worker — Render edition")
    logger.info("   RESOLVER_RESET_INTERVAL  : %d jobs", RESOLVER_RESET_INTERVAL)
    logger.info("   IDLE_POLL_INTERVAL        : %ds", IDLE_POLL_INTERVAL)
    logger.info("   PERIODIC_CHECK_INTERVAL   : %ds", PERIODIC_CHECK_INTERVAL)
    logger.info("=" * 60)

    # init() sets up Supabase + YouTubeResolver (same as creator_worker.main)
    await _cw.init()

    jobs_processed: int = 0
    empty_polls: int = 0
    start_time: float = time.time()
    last_periodic_check: float = 0.0  # force a check on the first iteration

    while not _shutdown.is_set():

        # ── 1. Time-gated periodic maintenance ───────────────────────────────
        if time.time() - last_periodic_check >= PERIODIC_CHECK_INTERVAL:
            logger.info("⏰ Periodic maintenance check...")
            try:
                invalid_count = _cw.queue_invalid_creators_for_retry(hours_since_last_sync=24)
                logger.info("  queue_invalid_creators_for_retry: %d queued", invalid_count)
            except Exception:
                logger.debug("queue_invalid_creators_for_retry raised (non-fatal)", exc_info=True)
            try:
                unsynced_count = _cw._queue_unsynced_creators(batch_size=500)
                logger.info("  _queue_unsynced_creators: %d queued", unsynced_count)
            except Exception:
                logger.debug("_queue_unsynced_creators raised (non-fatal)", exc_info=True)
            last_periodic_check = time.time()

        # ── 2. Periodic YouTubeResolver reset (httplib2 memory hygiene) ──────
        if jobs_processed > 0 and jobs_processed % RESOLVER_RESET_INTERVAL == 0:
            logger.info(
                "🔄 Resetting YouTubeResolver after %d jobs (httplib2 flush)",
                jobs_processed,
            )
            _cw.youtube_resolver = YouTubeResolver(api_key=get_creator_worker_api_key())

        # ── 3. Fetch next job ─────────────────────────────────────────────────
        jobs = _cw._fetch_pending_jobs(batch_size=1)

        if not jobs:
            empty_polls += 1
            if empty_polls == 1:
                logger.info("Queue empty — sleeping %ds before next poll", IDLE_POLL_INTERVAL)
            elif empty_polls % EMPTY_WARN_THRESHOLD == 0:
                logger.warning(
                    "Queue has been empty for %d consecutive polls (~%.0f min idle)",
                    empty_polls,
                    (time.time() - start_time) / 60,
                )
            await asyncio.sleep(IDLE_POLL_INTERVAL)
            continue

        if empty_polls > 0:
            logger.info("Queue active again after %d idle polls — resuming", empty_polls)
            empty_polls = 0

        job = jobs[0]

        # ── 4. Process job ────────────────────────────────────────────────────
        try:
            result = await asyncio.wait_for(
                _cw.handle_sync_job(
                    job_id=job["id"],
                    creator_id=job["creator_id"],
                    job_number=jobs_processed + 1,
                    retry_count=job.get("retry_count", 0),
                ),
                timeout=_cw.SYNC_TIMEOUT + 30,
            )
            jobs_processed += 1
            status_icon = "✅" if result else "⚠️"
            logger.info(
                "%s Job #%d done | total=%d | quota=%d/%d (%.1f%%)",
                status_icon,
                jobs_processed,
                jobs_processed,
                _cw.metrics.youtube_credits_used,
                _cw.YOUTUBE_DAILY_QUOTA,
                _cw.metrics.quota_percentage(),
            )

        except asyncio.TimeoutError:
            jobs_processed += 1
            logger.warning("⏱  Job #%d timed out — continuing", jobs_processed)

        except asyncio.CancelledError:
            logger.info("Job cancelled — shutting down")
            raise

        except Exception:
            jobs_processed += 1
            logger.exception("❌ Job #%d raised an unexpected exception", jobs_processed)
            await asyncio.sleep(5)  # brief pause to avoid hammering on persistent errors

    # ── Shutdown summary ──────────────────────────────────────────────────────
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("render_worker shutting down gracefully")
    logger.info("  Uptime         : %.0fs (%.1f min)", elapsed, elapsed / 60)
    logger.info("  Jobs processed : %d", jobs_processed)
    logger.info("  Succeeded      : %d", _cw.metrics.syncs_processed)
    logger.info("  Failed         : %d", _cw.metrics.syncs_failed)
    logger.info("  API errors     : %d", _cw.metrics.api_errors)
    logger.info(
        "  YT Quota used  : %d / %d (%.2f%%)",
        _cw.metrics.youtube_credits_used,
        _cw.YOUTUBE_DAILY_QUOTA,
        _cw.metrics.quota_percentage(),
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(run())
