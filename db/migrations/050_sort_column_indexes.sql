-- Migration 050: Add partial indexes for remaining sort columns
--
-- Problem: get_creators() supports sorting by:
--   - recent:         ORDER BY last_updated_at DESC
--   - newest_channel: ORDER BY published_at DESC
--   - oldest_channel: ORDER BY published_at ASC
--
-- None of these columns had partial indexes, so every sort request fell back
-- to a full sequential scan of the creators table, causing slow responses or
-- statement timeouts (pg error 57014) on large datasets.
--
-- Fix: Add partial indexes on last_updated_at DESC and published_at DESC for
-- both sync_status = 'synced' and sync_status = 'synced_partial'.
-- PostgreSQL can then use a Merge Append of the two partial index scans to
-- satisfy ORDER BY ... LIMIT 50 in O(50 seeks) instead of O(N) sequential scan.
--
-- Note: A single published_at DESC index covers both ORDER BY published_at DESC
-- (newest_channel) and ORDER BY published_at ASC (oldest_channel) — PostgreSQL
-- can walk the DESC index backwards for the ASC sort.
--
-- Mirrors the two-index pattern from migration 045 (synced + synced_partial) so
-- the Merge Append / BitmapOr plans from prior migrations still apply.
--
-- Uses plain CREATE INDEX (not CONCURRENTLY) so it can run inside a Supabase
-- SQL editor transaction. Run during low-traffic if the creators table is large.
--
-- Prerequisites: migrations 008, 043, 045 (partial index foundations).

-- ── last_updated_at — powers sort=recent ──────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_creators_last_updated_synced
    ON public.creators (last_updated_at DESC)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX IF NOT EXISTS idx_creators_last_updated_synced_partial
    ON public.creators (last_updated_at DESC)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- ── published_at — powers sort=newest_channel and sort=oldest_channel ──────────
-- A single DESC index covers both:
--   ORDER BY published_at DESC (newest_channel) → forward scan
--   ORDER BY published_at ASC  (oldest_channel) → backward scan

CREATE INDEX IF NOT EXISTS idx_creators_published_at_synced
    ON public.creators (published_at DESC)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX IF NOT EXISTS idx_creators_published_at_synced_partial
    ON public.creators (published_at DESC)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- Verification:
-- SELECT indexname, indexdef
-- FROM pg_indexes
-- WHERE tablename = 'creators'
--   AND indexname IN (
--     'idx_creators_last_updated_synced',
--     'idx_creators_last_updated_synced_partial',
--     'idx_creators_published_at_synced',
--     'idx_creators_published_at_synced_partial'
--   );
