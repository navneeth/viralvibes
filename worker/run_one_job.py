"""
worker/run_one_job.py — Single-job runner, spawned as a subprocess by render_worker.py.

This script processes exactly one creator sync job then exits, giving the
httplib2 C library a completely fresh heap for every job. This is the same
memory isolation guarantee as the original bash respawn loop, implemented in
Python so that render_worker.py (the supervisor) never has to exit.

Usage (internal — called by render_worker.py only):
    python -m worker.run_one_job --job-id <id> --creator-id <uuid> --job-number <n>

Exit codes:
    0  — job completed successfully (or was marked failed/retried in DB)
    1  — unhandled exception before or during job execution
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

# ── Secrets + logging (must come before any worker import) ───────────────────
from secrets_loader import load_secrets

_env_source = load_secrets()

from db import setup_logging, init_supabase

setup_logging()
logger = logging.getLogger("run_one_job")

# ── Worker internals ──────────────────────────────────────────────────────────
import worker.creator_worker as _cw


# ── Entry point ───────────────────────────────────────────────────────────────


async def main(job_id: int, creator_id: str, job_number: int) -> None:
    logger.info(
        "run_one_job starting | job_id=%s creator_id=%s job_number=%d",
        job_id,
        creator_id,
        job_number,
    )

    # Initialise Supabase + YouTubeResolver for this process
    await _cw.init()

    result = await asyncio.wait_for(
        _cw.handle_sync_job(
            job_id=job_id,
            creator_id=creator_id,
            job_number=job_number,
            retry_count=0,  # retry_count is tracked in DB; supervisor re-fetches on retry
        ),
        timeout=_cw.SYNC_TIMEOUT + 30,
    )

    icon = "✅" if result else "⚠️"
    logger.info("%s run_one_job finished | job_id=%s result=%s", icon, job_id, result)

    # Log quota summary (mirrors creator_worker shutdown log)
    logger.info(
        "YouTube API Quota | Used: %d units | Remaining: %d / %d | Consumed: %.2f%%",
        _cw.metrics.youtube_credits_used,
        _cw.metrics.quota_remaining(),
        _cw.YOUTUBE_DAILY_QUOTA,
        _cw.metrics.quota_percentage(),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a single creator sync job")
    parser.add_argument("--job-id", required=True, type=int)
    parser.add_argument("--creator-id", required=True)
    parser.add_argument("--job-number", required=True, type=int)
    args = parser.parse_args()

    try:
        asyncio.run(
            main(
                job_id=args.job_id,
                creator_id=args.creator_id,
                job_number=args.job_number,
            )
        )
        sys.exit(0)
    except Exception:
        logger.exception("run_one_job failed with unhandled exception")
        sys.exit(1)
