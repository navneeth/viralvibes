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
import os
import sys

from secrets_loader import load_secrets

from constants import CREATOR_TABLE
from db import (
    init_supabase,
    queue_creator_sync_bulk,
    queue_invalid_creators_for_retry,
    refresh_category_stats_cache,
    setup_logging,
    supabase_client,
)
from services.channel_utils import CATEGORY_TOPIC_IDS, YouTubeResolver
from services.youtube_config import get_creator_worker_api_key
from services.youtube_errors import QuotaExceededException

# Reuse the two queuing functions directly from creator_worker to avoid
# duplicating logic. They depend only on supabase_client and queue_creator_sync,
# both of which are initialised below via init_supabase().
from worker.creator_worker import (
    _queue_creators_for_extended_refresh,
    _queue_unsynced_creators,
)

_YT_CHANNEL_PREFIX = "https://www.youtube.com/channel/"

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
        "--no-stats",
        action="store_true",
        help="Skip category stats cache refresh (Pass 4)",
    )
    parser.add_argument(
        "--search-pages",
        type=int,
        default=2,
        metavar="N",
        help=(
            "Pages of search results per category (default: 2). "
            "Each page costs 100 quota units. "
            "2 pages × 6 categories = 1,200 units total."
        ),
    )
    parser.add_argument(
        "--no-search",
        action="store_true",
        help="Skip topic-based channel search discovery (Pass 6)",
    )
    return parser.parse_args()


# =============================================================================
# Pass 6 — topic-based channel discovery via search.list
# =============================================================================

# Categories prioritised by current creator_count desc (from DB analysis).
# Only categories that have a Freebase mapping in CATEGORY_TOPIC_IDS are used.
_DISCOVERY_CATEGORIES = [
    "Music",
    "Entertainment",
    "People & Blogs",
    "Gaming",
    "Howto & Style",
    "Education",
    "Science & Technology",
    "News & Politics",
    "Sports",
    "Film & Animation",
    "Comedy",
]


def _discover_channels_by_topic(pages_per_category: int = 2) -> tuple[int, int]:
    """
    Search YouTube for channels by Freebase topic ID and insert stubs for
    any channel IDs not already in the creators table.

    QUOTA COST: 100 units per search.list call.
      Default: 2 pages × 6 active categories = 1,200 units
      Maximum: 2 pages × 11 categories = 2,200 units

    Strategy:
      1. For each category in _DISCOVERY_CATEGORIES (if topicId known):
           a. Call search.list up to `pages_per_category` times (50 results/page)
           b. Collect returned channel IDs
      2. Bulk-check which IDs already exist in creators (chunked to 500)
      3. Insert stubs for new ones and queue via queue_creator_sync_bulk

    Args:
        pages_per_category: Number of result pages to fetch per category.

    Returns:
        (inserted, queued)
    """
    if not supabase_client:
        return 0, 0

    api_key = get_creator_worker_api_key()
    resolver = YouTubeResolver(api_key=api_key)

    all_candidate_ids: set[str] = set()
    quota_used = 0

    for category in _DISCOVERY_CATEGORIES:
        topic_id = CATEGORY_TOPIC_IDS.get(category)
        if not topic_id:
            logger.debug("   No Freebase ID for category '%s' — skipping", category)
            continue

        logger.info("   Searching category='%s' (topicId=%s)", category, topic_id)
        page_token = None

        for page in range(1, pages_per_category + 1):
            try:
                result = resolver.search_channels_by_topic(
                    topic_id=topic_id,
                    max_results=50,
                    page_token=page_token,
                    order="relevance",
                )
                quota_used += 100

                found = result["channel_ids"]
                all_candidate_ids.update(found)
                logger.info(
                    "     page %d: %d channels (total candidates so far: %d, "
                    "quota used: %d)",
                    page,
                    len(found),
                    len(all_candidate_ids),
                    quota_used,
                )

                page_token = result.get("next_page_token")
                if not page_token:
                    break  # no more pages

            except QuotaExceededException:
                logger.error(
                    "   ⏸ Quota exhausted at category='%s' page=%d — stopping search",
                    category,
                    page,
                )
                # Return whatever we have collected so far
                break

        else:
            # Inner loop completed without break — continue outer loop
            continue
        # If inner loop broke due to quota — stop outer loop too
        break

    if not all_candidate_ids:
        logger.info("   No channel IDs returned from search")
        return 0, 0

    logger.info(
        "   Search complete: %d unique candidates, %d quota units used",
        len(all_candidate_ids),
        quota_used,
    )

    # ── Filter out IDs already in creators ───────────────────────────────────
    candidate_list = list(all_candidate_ids)
    existing_ids: set[str] = set()
    chunk_size = 500

    for i in range(0, len(candidate_list), chunk_size):
        chunk = candidate_list[i : i + chunk_size]
        exist_resp = (
            supabase_client.table(CREATOR_TABLE)
            .select("channel_id")
            .in_("channel_id", chunk)
            .execute()
        )
        for row in exist_resp.data or []:
            existing_ids.add(row["channel_id"])

    new_ids = [cid for cid in candidate_list if cid not in existing_ids]

    if not new_ids:
        logger.info("   All %d candidates already in DB", len(all_candidate_ids))
        return 0, 0

    logger.info(
        "   %d new channel IDs to insert (%d already existed)",
        len(new_ids),
        len(existing_ids),
    )

    # ── Insert stubs ──────────────────────────────────────────────────────────
    stubs = [
        {
            "channel_id": cid,
            "channel_url": f"{_YT_CHANNEL_PREFIX}{cid}",
            "source": "topic_search",
        }
        for cid in new_ids
    ]

    try:
        insert_resp = supabase_client.table(CREATOR_TABLE).insert(stubs).execute()
        inserted = len(insert_resp.data or [])
    except Exception as e:
        logger.exception("   ❌ Stub insert failed: %s", e)
        return 0, 0

    if inserted == 0:
        logger.warning("   Insert returned 0 rows — possible RLS or constraint issue")
        return 0, 0

    logger.info("   Inserted %d new creator stubs", inserted)

    # ── Fetch UUIDs and queue ─────────────────────────────────────────────────
    inserted_channel_ids = [s["channel_id"] for s in stubs[:inserted]]
    id_resp = (
        supabase_client.table(CREATOR_TABLE)
        .select("id")
        .in_("channel_id", inserted_channel_ids)
        .execute()
    )
    creator_uuids = [row["id"] for row in (id_resp.data or [])]

    if not creator_uuids:
        logger.warning("   Could not retrieve UUIDs for newly inserted stubs")
        return inserted, 0

    queued, skipped = queue_creator_sync_bulk(creator_uuids, source="topic_search")
    logger.info("   Queued %d for first sync (%d already pending)", queued, skipped)
    return inserted, queued


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
        queued = _queue_creators_for_extended_refresh(
            days_since_last_sync=args.stale_days
        )
        logger.info(f"   Queued: {queued}")
        total_queued += queued
    else:
        logger.info("── Pass 2: stale refresh skipped (--no-stale)")

    # ── 3. Previously-synced invalid/failed creators ──────────────────────────
    if not args.no_invalid:
        logger.info("── Pass 3: invalid/failed creators (>24 h since last sync)")
        queued = queue_invalid_creators_for_retry(hours_since_last_sync=24)
        logger.info(f"   Queued: {queued}")
        total_queued += queued
    else:
        logger.info("── Pass 3: invalid/failed skipped (--no-invalid)")

    # ── 4. Refresh category box plot stats cache ─────────────────────────────
    if not args.no_stats:
        logger.info("── Pass 4: refreshing category stats cache")
        refreshed = refresh_category_stats_cache()
        logger.info(f"   Refreshed: {refreshed} categories")
    else:
        logger.info("── Pass 4: category stats skipped (--no-stats)")

    # ── 6. Topic-based channel discovery via search.list ────────────────────
    if not args.no_search:
        logger.info(
            f"── Pass 6: topic search discovery ({args.search_pages} pages/category, "
            f"~{args.search_pages * len(_DISCOVERY_CATEGORIES) * 100} quota units max)"
        )
        inserted, queued = _discover_channels_by_topic(
            pages_per_category=args.search_pages,
        )
        logger.info(f"   Inserted: {inserted} new stubs, queued: {queued} for sync")
        total_queued += queued
    else:
        logger.info("── Pass 6: topic search skipped (--no-search)")

    logger.info(f"✅ Bootstrap complete — {total_queued} total creators queued")


if __name__ == "__main__":
    main()
