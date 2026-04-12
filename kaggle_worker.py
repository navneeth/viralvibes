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
import gc
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("kaggle_worker")

# ── Step 1: fetch secrets once, before any worker import ─────────────────────
# This MUST happen before importing creator_worker, because that module reads
# os.environ at import time for YOUTUBE_DAILY_QUOTA and other constants.


def _bootstrap_kaggle_secrets() -> None:
    """
    Pull secrets from Kaggle UserSecretsClient into os.environ once.
    Loads YOUTUBE_API_KEY, YOUTUBE_API_KEY_2, YOUTUBE_API_KEY_3 (if present)
    so KeyPool can populate itself with all available keys.
    """
    try:
        from kaggle_secrets import UserSecretsClient  # type: ignore[import]
    except ImportError:
        logger.info("kaggle_secrets not available — assuming env vars already set")
        return

    client = UserSecretsClient()
    required = [
        "NEXT_PUBLIC_SUPABASE_URL",
        "SUPABASE_SERVICE_KEY",
        "YOUTUBE_API_KEY",
    ]
    optional = {
        "YOUTUBE_DAILY_QUOTA": "10000",
        "NEXT_PUBLIC_SUPABASE_ANON_KEY": None,
        # Extra API keys for round-robin rotation — add as many as you have
        "YOUTUBE_API_KEY_2": None,
        "YOUTUBE_API_KEY_3": None,
    }

    missing = []
    for key in required + list(optional):
        if key in os.environ:
            continue
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

    # Log which API keys were found
    keys_found = [
        k
        for k in ["YOUTUBE_API_KEY", "YOUTUBE_API_KEY_2", "YOUTUBE_API_KEY_3"]
        if os.environ.get(k)
    ]
    logger.info("✅ Secrets loaded. YouTube API keys available: %d", len(keys_found))


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


# =============================================================================
# KeyPool — round-robin YouTube API key rotation
# =============================================================================


@dataclass
class ApiKeySlot:
    """Single API key with usage tracking."""

    api_key: str
    status: str = "active"  # "active" | "exhausted"
    exhausted_at: Optional[datetime] = None
    jobs_processed: int = 0

    def exhaust(self) -> None:
        self.status = "exhausted"
        self.exhausted_at = datetime.now(timezone.utc)
        key_id = hashlib.sha256(self.api_key.encode("utf-8")).hexdigest()[:12]
        logger.warning(
            "  🔑 Key id=%s exhausted after %d jobs",
            key_id,
            self.jobs_processed,
        )

    @property
    def is_active(self) -> bool:
        return self.status == "active"


class KeyPool:
    """
    Round-robin pool of YouTube API keys.

    On QuotaExceededException:
      1. Mark current slot exhausted
      2. Advance to next active slot
      3. Swap the YouTubeResolver to the new key
      4. If no active slots remain → set all_exhausted_event

    Usage:
        pool = KeyPool.from_env()
        resolver = pool.current_resolver()
        ...
        pool.mark_exhausted_and_rotate()   # on QuotaExceededException
    """

    def __init__(self, api_keys: list[str]) -> None:
        if not api_keys:
            raise ValueError("KeyPool requires at least one API key")
        self.slots: list[ApiKeySlot] = [ApiKeySlot(k) for k in api_keys]
        self._index: int = 0
        self.all_exhausted_event: asyncio.Event = asyncio.Event()
        logger.info("KeyPool initialised with %d key(s)", len(self.slots))

    @classmethod
    def from_env(cls) -> "KeyPool":
        """
        Build pool from YOUTUBE_API_KEY, YOUTUBE_API_KEY_2, YOUTUBE_API_KEY_3.
        Any key not present in env is silently skipped.
        """
        keys = []
        for name in ["YOUTUBE_API_KEY", "YOUTUBE_API_KEY_2", "YOUTUBE_API_KEY_3"]:
            k = os.environ.get(name, "").strip()
            if k:
                keys.append(k)
        if not keys:
            raise EnvironmentError("No YOUTUBE_API_KEY found in environment")
        return cls(keys)

    @property
    def current_slot(self) -> ApiKeySlot:
        return self.slots[self._index]

    def current_resolver(self) -> "YouTubeResolver":
        return YouTubeResolver(api_key=self.current_slot.api_key)

    def record_job(self) -> None:
        self.current_slot.jobs_processed += 1

    def mark_exhausted_and_rotate(self) -> bool:
        """
        Exhaust the current slot and advance to the next active one.

        Returns:
            True  — rotated successfully, caller should swap resolver and retry
            False — all slots exhausted, caller should stop
        """
        self.current_slot.exhaust()

        # Find next active slot (wrapping around)
        for offset in range(1, len(self.slots)):
            candidate = (self._index + offset) % len(self.slots)
            if self.slots[candidate].is_active:
                self._index = candidate
                logger.info(
                    "  🔄 Rotated to key slot %d (...%s)",
                    self._index,
                    self.slots[self._index].api_key[-6:],
                )
                return True

        # No active slots remain
        logger.error(
            "  ❌ All %d API key(s) exhausted. " "Quota resets at midnight Pacific (08:00 UTC).",
            len(self.slots),
        )
        self.all_exhausted_event.set()
        return False

    def summary(self) -> str:
        parts = []
        for i, slot in enumerate(self.slots):
            marker = "►" if i == self._index else " "
            parts.append(
                f"  {marker} slot {i} [...{slot.api_key[-6:]}] "
                f"{slot.status} | jobs: {slot.jobs_processed}"
            )
        return "\n".join(parts)


# ── Kaggle-specific loop config ───────────────────────────────────────────────
_DEFAULT_MAX_JOBS = 5000
# Stop after 5 consecutive empty polls instead of 3 — prevents premature exit
# during burst gaps where the queue drains briefly between job submissions.
_DEFAULT_MAX_EMPTY_POLLS = 5
# Reset YouTubeResolver every 100 jobs instead of 50.
# GC + resolver re-init is ~1-2s of dead time; httplib2 only fragments under
# sustained load and is safe well past 50 iterations.
_RESOLVER_RESET_INTERVAL = 100

# ── Main entry point ──────────────────────────────────────────────────────────


async def run(
    max_jobs: int = _DEFAULT_MAX_JOBS,
    max_empty_polls: int = _DEFAULT_MAX_EMPTY_POLLS,
) -> None:
    """
    Run the creator worker loop in-process for Kaggle.

    Uses KeyPool for round-robin API key rotation on quota exhaustion.
    Resets YouTubeResolver every _RESOLVER_RESET_INTERVAL jobs for
    httplib2 memory management.

    Args:
        max_jobs:        Stop after processing this many jobs.
        max_empty_polls: Stop after this many consecutive empty queue polls.
    """
    import worker.creator_worker as _cw

    # ── Build key pool from env ───────────────────────────────────────────────
    key_pool = KeyPool.from_env()
    logger.info("🚀 Kaggle Creator Worker starting")
    logger.info(
        "   max_jobs=%d  max_empty_polls=%d  resolver_reset=%d  keys=%d",
        max_jobs,
        max_empty_polls,
        _RESOLVER_RESET_INTERVAL,
        len(key_pool.slots),
    )

    # ── Initialise Supabase + resolver ────────────────────────────────────────
    await init()
    _cw.youtube_resolver = key_pool.current_resolver()

    jobs_processed = 0
    empty_polls = 0
    start_time = time.time()

    while (
        not stop_event.is_set()
        and not key_pool.all_exhausted_event.is_set()
        and jobs_processed < max_jobs
    ):

        # ── Periodic resolver reset (httplib2 memory management) ─────────────
        if jobs_processed > 0 and jobs_processed % _RESOLVER_RESET_INTERVAL == 0:
            logger.info(
                "🔄 Resetting YouTubeResolver after %d jobs (httplib2 flush)", jobs_processed
            )
            _cw.youtube_resolver = None
            gc.collect()
            _cw.youtube_resolver = key_pool.current_resolver()
            logger.info("✅ Memory flushed and Resolver re-initialized")

        # ── Fetch next job ────────────────────────────────────────────────────
        jobs = _fetch_pending_jobs(batch_size=1)

        if not jobs:
            empty_polls += 1
            if empty_polls >= max_empty_polls:
                logger.info("✅ Queue empty for %d consecutive polls — stopping", empty_polls)
                break
            backoff = min(30 * (2 ** (empty_polls - 1)), 300)
            logger.info("Queue empty (%d/%d) — sleeping %ds", empty_polls, max_empty_polls, backoff)
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
                timeout=90,
            )
            jobs_processed += 1
            key_pool.record_job()
            status = "✅" if result else "⚠️"
            logger.info(
                "%s Job %d done | key_slot=%d jobs_on_key=%d quota=%d/%d (%.1f%%)",
                status,
                jobs_processed,
                key_pool._index,
                key_pool.current_slot.jobs_processed,
                metrics.youtube_credits_used,
                YOUTUBE_DAILY_QUOTA,
                metrics.quota_percentage(),
            )

        except asyncio.TimeoutError:
            jobs_processed += 1
            logger.warning("⏱  Job %d timed out after 90s — continuing", jobs_processed)

        except Exception as e:
            from services.youtube_errors import QuotaExceededException

            if isinstance(e, QuotaExceededException):
                logger.warning(
                    "⏸  Quota exhausted on key slot %d (...%s) — attempting rotation",
                    key_pool._index,
                    key_pool.current_slot.api_key[-6:],
                )
                rotated = key_pool.mark_exhausted_and_rotate()
                if rotated:
                    # Swap resolver to new key
                    _cw.youtube_resolver = None
                    gc.collect()
                    _cw.youtube_resolver = key_pool.current_resolver()
                    logger.info(
                        "  ↩️  Re-queuing job %s for retry on new key",
                        job["id"],
                    )
                    # Don't increment jobs_processed — re-fetch same job next iteration
                    # (it was marked failed with retry_at, bootstrap will re-queue it,
                    #  or we can re-fetch immediately by continuing without incrementing)
                    continue
                else:
                    logger.error("  🛑 All keys exhausted — stopping worker")
                    break
            else:
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
    logger.info("Key pool summary:\n%s", key_pool.summary())
    logger.info("=" * 60)


# ── Notebook convenience: run directly with %run kaggle_worker.py ─────────────
if __name__ == "__main__":
    asyncio.run(run())
