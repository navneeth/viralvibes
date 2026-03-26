"""
kaggle_worker.py — Kaggle Notebook runner for the ViralVibes creator worker.

USAGE — run this entire cell once:

    %run kaggle_worker.py

Or equivalently:
    import kaggle_worker
    await kaggle_worker.run(max_jobs=500)

WHY THIS EXISTS
───────────────
The production worker (worker/creator_worker.py) uses EXIT_AFTER_JOB=True and
relies on a bash loop that respawns a fresh Python process per job. This gives
httplib2 memory isolation but means each process calls load_secrets() →
UserSecretsClient.get_secret(). Kaggle rate-limits that API — 429 after ~10
calls from the same session — making the subprocess pattern unusable.

This module solves it by:
  1. Fetching secrets ONCE into os.environ before the loop starts
  2. Running the job loop in-process (no subprocess respawning)
  3. Resetting the YouTubeResolver after every N jobs to reclaim httplib2 memory
     (poor man's process isolation without the subprocess overhead)

KAGGLE SECRETS TO ADD (Add-ons → Secrets)
──────────────────────────────────────────
  NEXT_PUBLIC_SUPABASE_URL    (required)
  SUPABASE_SERVICE_KEY        (required)
  YOUTUBE_API_KEY             (required)
  YOUTUBE_DAILY_QUOTA         (optional, default 10000)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

logger = logging.getLogger("kaggle_worker")

# ── Step 1: fetch secrets once, before any worker import ─────────────────────
# This MUST happen before importing creator_worker, because that module reads
# os.environ at import time for YOUTUBE_DAILY_QUOTA and other constants.


def _bootstrap_kaggle_secrets() -> None:
    """
    Pull secrets from Kaggle UserSecretsClient into os.environ once.
    Skips any key already present — safe to call multiple times.
    """
    try:
        from kaggle_secrets import UserSecretsClient  # type: ignore[import]
    except ImportError:
        logger.info("kaggle_secrets not available — assuming env vars already set")
        return

    client = UserSecretsClient()
    required = {
        "NEXT_PUBLIC_SUPABASE_URL": None,
        "SUPABASE_SERVICE_KEY": None,
        "YOUTUBE_API_KEY": None,
    }
    optional = {
        "YOUTUBE_DAILY_QUOTA": "10000",
        "NEXT_PUBLIC_SUPABASE_ANON_KEY": None,
    }

    missing = []
    for key in {**required, **optional}:
        if key in os.environ:
            continue  # already set — don't touch Kaggle API
        try:
            val = client.get_secret(key)
            if val:
                os.environ[key] = val
                logger.info("  ✅ %s loaded", key)
            elif key in optional and optional[key]:
                os.environ[key] = optional[key]
        except KeyError:
            if key in required:
                missing.append(key)
            elif key in optional and optional[key]:
                os.environ[key] = optional[key]

    if missing:
        raise EnvironmentError(
            f"Required Kaggle secrets not found: {missing}\n"
            "Add them under Add-ons → Secrets in your notebook."
        )

    logger.info("✅ Secrets bootstrapped into os.environ (Kaggle API called once)")


# Run immediately at import — before worker module is imported below
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
_bootstrap_kaggle_secrets()

# ── Step 2: import worker internals (safe now that env is populated) ──────────
from worker.creator_worker import (  # noqa: E402
    _fetch_pending_jobs,
    handle_sync_job,
    init,
    metrics,
    stop_event,
    youtube_resolver,
    POLL_INTERVAL,
    YOUTUBE_DAILY_QUOTA,
)
from services.youtube_config import get_creator_worker_api_key  # noqa: E402
from services.channel_utils import YouTubeResolver  # noqa: E402

# ── Kaggle-specific loop config ───────────────────────────────────────────────
_DEFAULT_MAX_JOBS = 5000
_DEFAULT_MAX_EMPTY_POLLS = 3
_RESOLVER_RESET_INTERVAL = 50  # reset YouTubeResolver every N jobs to flush
# httplib2 state — replaces process isolation

# ── Main entry point ──────────────────────────────────────────────────────────


async def run(
    max_jobs: int = _DEFAULT_MAX_JOBS,
    max_empty_polls: int = _DEFAULT_MAX_EMPTY_POLLS,
) -> None:
    """
    Run the creator worker loop in-process for Kaggle.

    Unlike the production worker, this does NOT exit after each job.
    Instead it resets the YouTubeResolver every _RESOLVER_RESET_INTERVAL jobs
    to reclaim httplib2 memory, which is the only reason production uses
    subprocess isolation.

    Args:
        max_jobs:        Stop after processing this many jobs (default 500).
        max_empty_polls: Stop after this many consecutive empty queue polls.
    """
    logger.info("🚀 Kaggle Creator Worker starting")
    logger.info(
        "   max_jobs=%d  max_empty_polls=%d  resolver_reset=%d",
        max_jobs,
        max_empty_polls,
        _RESOLVER_RESET_INTERVAL,
    )

    # Initialise Supabase + YouTube resolver (same as production init())
    await init()

    jobs_processed = 0
    empty_polls = 0
    start_time = time.time()

    while not stop_event.is_set() and jobs_processed < max_jobs:

        # ── Periodic resolver reset (httplib2 memory management) ─────────────
        if jobs_processed > 0 and jobs_processed % _RESOLVER_RESET_INTERVAL == 0:
            logger.info(
                "🔄 Resetting YouTubeResolver after %d jobs (httplib2 flush)",
                jobs_processed,
            )
            import worker.creator_worker as _cw

            _cw.youtube_resolver = YouTubeResolver(api_key=get_creator_worker_api_key())

        # ── Fetch next job ────────────────────────────────────────────────────
        jobs = _fetch_pending_jobs(batch_size=1)

        if not jobs:
            empty_polls += 1
            if empty_polls >= max_empty_polls:
                logger.info(
                    "✅ Queue empty for %d consecutive polls — stopping",
                    empty_polls,
                )
                break
            backoff = min(30 * (2 ** (empty_polls - 1)), 300)
            logger.info(
                "Queue empty (%d/%d) — sleeping %ds",
                empty_polls,
                max_empty_polls,
                backoff,
            )
            await asyncio.sleep(backoff)
            continue

        empty_polls = 0
        job = jobs[0]

        try:
            result = await asyncio.wait_for(
                handle_sync_job(
                    job_id=job["id"],
                    creator_id=job["creator_id"],
                    job_number=jobs_processed + 1,
                    retry_count=job.get("retry_count", 0),
                ),
                timeout=90,  # generous timeout — no bash loop to recover us
            )
            jobs_processed += 1
            status = "✅" if result else "⚠️"
            logger.info(
                "%s Job %d done | processed=%d quota=%d/%d (%.1f%%)",
                status,
                jobs_processed,
                jobs_processed,
                metrics.youtube_credits_used,
                YOUTUBE_DAILY_QUOTA,
                metrics.quota_percentage(),
            )
        except asyncio.TimeoutError:
            jobs_processed += 1
            logger.warning("⏱  Job %d timed out after 90s — continuing", jobs_processed)
        except Exception as e:
            jobs_processed += 1
            logger.exception("❌ Job %d raised: %s", jobs_processed, e)

    # ── Final summary ─────────────────────────────────────────────────────────
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("Kaggle worker finished")
    logger.info("  Jobs processed : %d", jobs_processed)
    logger.info("  Uptime         : %.0fs (%.1f min)", elapsed, elapsed / 60)
    logger.info("  Succeeded      : %d", metrics.syncs_processed)
    logger.info("  Failed         : %d", metrics.syncs_failed)
    logger.info("  API errors     : %d", metrics.api_errors)
    logger.info(
        "  YT Quota used  : %d / %d (%.2f%%)",
        metrics.youtube_credits_used,
        YOUTUBE_DAILY_QUOTA,
        metrics.quota_percentage(),
    )
    logger.info("=" * 60)


# ── Notebook convenience: run directly with %run kaggle_worker.py ─────────────
if __name__ == "__main__":
    asyncio.run(run())
