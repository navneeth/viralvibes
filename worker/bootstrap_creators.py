# worker/bootstrap_creators.py
"""
Bootstrap script: queues creators that need syncing into creator_sync_jobs.

Handles two disjoint populations:
  1. Never-synced   — last_synced_at IS NULL  (PostgREST .lt() silently skips these)
  2. Stale-synced   — sync_status in (synced, synced_partial, invalid, failed)
                      AND last_synced_at older than STALE_DAYS

Intentionally a thin script that delegates to the same db functions used by
creator_worker.py so the queuing logic stays in one place.

Run as:
  python -m worker.bootstrap_creators
  python -m worker.bootstrap_creators --unsynced-batch 500 --stale-days 14
"""

import argparse
import logging
import sys

from secrets_loader import load_secrets

from db import (
    init_supabase,
    queue_invalid_creators_for_retry,
    refresh_category_stats_cache,
    refresh_hero_stats_cache,
    refresh_total_categories,
    setup_logging,
)

# Reuse the two queuing functions directly from creator_worker to avoid
# duplicating logic. They depend only on supabase_client and queue_creator_sync,
# both of which are initialised below via init_supabase().
from worker.creator_worker import (
    _queue_creators_for_extended_refresh,
    _queue_unsynced_creators,
)

load_secrets()
setup_logging()
logger = logging.getLogger("vv_bootstrap")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Queue never-synced and stale creators for the creator worker.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--unsynced-batch",
        type=int,
        default=500,
        metavar="N",
        help="Max never-synced creators to queue per run (default: 500)",
    )
    parser.add_argument(
        "--stale-days",
        type=int,
        default=7,
        metavar="N",
        help="Queue synced creators not refreshed in this many days (default: 7)",
    )
    parser.add_argument(
        "--no-unsynced",
        action="store_true",
        help="Skip the never-synced pass",
    )
    parser.add_argument(
        "--no-stale",
        action="store_true",
        help="Skip the stale-refresh pass",
    )
    parser.add_argument(
        "--no-invalid",
        action="store_true",
        help="Skip re-queuing previously-synced invalid/failed creators",
    )
    parser.add_argument(
        "--invalid-batch",
        type=int,
        default=50,
        metavar="N",
        help="Max invalid/failed creators to queue (default: 50, reduced to avoid timeouts)",
    )
    parser.add_argument(
        "--no-stats",
        action="store_true",
        help="Skip category and hero stats refresh (Passes 4–5)",
    )
    parser.add_argument(
        "--no-categories",
        action="store_true",
        help="Skip total_categories recount (Pass 6). Slow (~4s); safe to skip on frequent runs.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    logger.info("▶ Creator bootstrap starting")

    client = init_supabase()
    if not client:
        logger.error("❌ Supabase init failed — check env vars")
        sys.exit(1)

    logger.info("✅ Supabase connected")
    total_queued = 0

    # ── 1. Never-synced creators ──────────────────────────────────────────────
    # PostgREST .lt() silently excludes NULLs, so creator_worker's periodic
    # check misses these. This is the primary source of the backlog.
    if not args.no_unsynced:
        logger.info(f"── Pass 1: never-synced (batch={args.unsynced_batch})")
        queued = _queue_unsynced_creators(batch_size=args.unsynced_batch)
        logger.info(f"   Queued: {queued}")
        total_queued += queued
    else:
        logger.info("── Pass 1: never-synced skipped (--no-unsynced)")

    # ── 2. Stale synced creators ──────────────────────────────────────────────
    if not args.no_stale:
        logger.info(f"── Pass 2: stale refresh (>{args.stale_days} days since sync)")
        queued = _queue_creators_for_extended_refresh(days_since_last_sync=args.stale_days)
        logger.info(f"   Queued: {queued}")
        total_queued += queued
    else:
        logger.info("── Pass 2: stale refresh skipped (--no-stale)")

    # ── 3. Previously-synced invalid/failed creators ──────────────────────────
    if not args.no_invalid:
        logger.info(
            f"── Pass 3: invalid/failed creators (>24 h since last sync, batch={args.invalid_batch})"
        )
        try:
            queued = queue_invalid_creators_for_retry(
                hours_since_last_sync=24, batch_size=args.invalid_batch
            )
            logger.info(f"   Queued: {queued}")
            total_queued += queued
        except Exception as e:
            logger.warning(f"   ⚠️  Pass 3 failed (non-fatal): {e}")
            logger.info("   Continuing with remaining passes...")
    else:
        logger.info("── Pass 3: invalid/failed skipped (--no-invalid)")

    # ── 4. Refresh category box plot stats cache ─────────────────────────────
    if not args.no_stats:
        logger.info("── Pass 4: refreshing category stats cache")
        refreshed = refresh_category_stats_cache()
        logger.info(f"   Refreshed: {refreshed} categories")
    else:
        logger.info("── Pass 4: category stats skipped (--no-stats)")

    # ── 5. Refresh hero stats materialized views ──────────────────────────────
    if not args.no_stats:
        logger.info("── Pass 5: refreshing hero stats materialized views")
        result = refresh_hero_stats_cache()
        if result["success"]:
            for view in result["materialized_views"]:
                logger.info(f"   • {view['name']}: {view['rows']} rows, {view['duration_ms']}ms")
        else:
            logger.error(f"   ❌ Hero stats refresh failed: {result['error']}")
    else:
        logger.info("── Pass 5: hero stats skipped (--no-stats)")

    # ── 6. Recount total_categories (slow jsonb scan, run infrequently) ───────
    # Separated from Pass 5 because the jsonb unnest over topic_categories
    # takes ~4s and exceeds PostgREST's statement timeout when bundled with
    # the fast materialized view refreshes. Skipped by default on frequent
    # bootstrap runs; run explicitly when creator data has changed significantly.
    if not args.no_stats and not args.no_categories:
        logger.info("── Pass 6: recounting total_categories")
        count = refresh_total_categories()
        logger.info(f"   total_categories: {count}")
    else:
        logger.info("── Pass 6: total_categories skipped (--no-categories or --no-stats)")

    logger.info(f"✅ Bootstrap complete — {total_queued} total creators queued")


if __name__ == "__main__":
    main()
