"""Backfill migration-040 contact-signal columns for already-synced creators.

Migration 040 added nine columns to ``creators`` (``extracted_email``,
``extracted_website``, ``extracted_instagram``, ``extracted_x``,
``extracted_tiktok``, ``extracted_linkedin``, ``extracted_whatsapp``,
``contact_signals_extracted_at``, ``has_contact_info``) and the worker writes
them on each sync — but at current API quota the existing 344k synced rows
will not be revisited for months.

This CLI runs the *pure-Python* extractor (no YouTube API calls) over already-
stored ``channel_description`` / ``description`` / ``bio`` / ``keywords`` and
persists the nine columns. It is safe to run in production and resumable: rows
with a non-null ``contact_signals_extracted_at`` are skipped on subsequent
runs.

Usage
-----
    # Dry-run: extract + log yield, no DB writes
    python scripts/backfill_contact_signals.py --limit 500 --dry-run

    # Process the 10,000 highest-subscriber-count rows
    python scripts/backfill_contact_signals.py --limit 10000

    # Process everything (resumable, can be interrupted with Ctrl-C)
    python scripts/backfill_contact_signals.py

    # Force re-extraction of rows already processed
    python scripts/backfill_contact_signals.py --limit 1000 --force
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from secrets_loader import load_secrets  # noqa: E402

load_secrets()

import db  # noqa: E402
from constants import CREATOR_TABLE  # noqa: E402
from db import init_supabase, setup_logging  # noqa: E402
from services.contact_extractor import ContactExtractorService  # noqa: E402

logger = logging.getLogger("backfill_contact_signals")

# Columns required by ContactExtractorService.extract_from_creator() — fetched
# in a single SELECT to avoid per-row round-trips. ``id`` is the PK for UPDATE.
_SELECT_COLUMNS = "id,channel_description,description,bio,keywords"

# Page size for the SELECT pass. PostgREST default cap is 1000, and Supabase
# accepts at most ~1000 per request anyway. Keep at 1000 for fewer round-trips.
_PAGE_SIZE = 1000


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Stop after processing N rows (0 = no limit, process all). Default: 0.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=_PAGE_SIZE,
        help=f"Rows fetched per SELECT page. Default: {_PAGE_SIZE}.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and log yield statistics, but do not write to the database.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-process rows that already have contact_signals_extracted_at set.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable DEBUG logging for per-row detail.",
    )
    return parser.parse_args()


def _fetch_page(
    *,
    sc,
    last_id: str | None,
    page_size: int,
    force: bool,
) -> list[dict]:
    """Fetch a single page of synced creators that still need extraction.

    Uses keyset pagination on ``id`` (UUID, indexed as PK) so we do not pay the
    OFFSET penalty on very deep pages.
    """
    q = sc.table(CREATOR_TABLE).select(_SELECT_COLUMNS).eq("sync_status", "synced")
    if not force:
        q = q.is_("contact_signals_extracted_at", "null")
    if last_id is not None:
        q = q.gt("id", last_id)
    q = q.order("id", desc=False).limit(page_size)
    return q.execute().data or []


def _apply_payload(sc, creator_id: str, payload: dict) -> None:
    """Persist the 9 contact-signal columns to a single creator row."""
    sc.table(CREATOR_TABLE).update(payload).eq("id", creator_id).execute()


def main() -> int:
    args = _parse_args()
    setup_logging()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    sc = init_supabase()
    if sc is None or db.supabase_client is None:
        logger.error("Supabase client unavailable — check SUPABASE_URL / SUPABASE_SERVICE_KEY")
        return 1
    sc = db.supabase_client  # use the module-level client to share connection pool

    mode = "DRY-RUN" if args.dry_run else "WRITE"
    logger.info(
        "[backfill] starting: mode=%s force=%s page_size=%d limit=%s",
        mode,
        args.force,
        args.page_size,
        args.limit or "ALL",
    )

    started_at = time.monotonic()
    last_id: str | None = None
    processed = 0
    with_contact = 0
    with_email = 0
    db_errors = 0

    try:
        while True:
            remaining = (args.limit - processed) if args.limit else args.page_size
            if remaining <= 0:
                break
            page_size = min(args.page_size, remaining) if args.limit else args.page_size

            try:
                rows = _fetch_page(
                    sc=sc,
                    last_id=last_id,
                    page_size=page_size,
                    force=args.force,
                )
            except Exception:
                logger.exception("[backfill] fetch failed at last_id=%s — aborting", last_id)
                return 2

            if not rows:
                logger.info("[backfill] no more rows to process")
                break

            for row in rows:
                creator_id = row.get("id")
                if not creator_id:
                    continue

                payload = ContactExtractorService.build_db_update_payload(row)
                processed += 1
                if payload.get("has_contact_info"):
                    with_contact += 1
                if payload.get("extracted_email"):
                    with_email += 1

                if args.verbose:
                    logger.debug(
                        "[backfill] id=%s has_contact=%s email=%s",
                        creator_id,
                        payload.get("has_contact_info"),
                        bool(payload.get("extracted_email")),
                    )

                if not args.dry_run:
                    try:
                        _apply_payload(sc, creator_id, payload)
                    except Exception as exc:
                        # Don't abort the run on a single bad row — log and continue.
                        db_errors += 1
                        logger.warning("[backfill] update failed id=%s: %s", creator_id, exc)

                last_id = creator_id

            elapsed = time.monotonic() - started_at
            rate = processed / elapsed if elapsed > 0 else 0
            yield_pct = (100 * with_contact / processed) if processed else 0
            email_pct = (100 * with_email / processed) if processed else 0
            logger.info(
                "[backfill] processed=%d with_contact=%d (%.1f%%) with_email=%d (%.1f%%) "
                "errors=%d rate=%.0f rows/s elapsed=%.0fs",
                processed,
                with_contact,
                yield_pct,
                with_email,
                email_pct,
                db_errors,
                rate,
                elapsed,
            )
    except KeyboardInterrupt:
        logger.warning("[backfill] interrupted — progress is saved (resumable)")

    logger.info(
        "[backfill] DONE mode=%s processed=%d with_contact=%d with_email=%d errors=%d",
        mode,
        processed,
        with_contact,
        with_email,
        db_errors,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
