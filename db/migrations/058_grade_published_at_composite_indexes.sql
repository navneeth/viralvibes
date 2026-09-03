-- Migration 058: Composite indexes for grade-filtered + sort-by-published_at queries
--
-- Problem: get_creators() with quality_grade + activity + category + country
-- + ORDER BY published_at (sort=newest_channel / oldest_channel) caused a
-- statement timeout (57014).  Observed 2026-09 production log:
--
--   Sort: newest_channel, Limit: 50,
--   Filters: [grade=C, activity=dormant, age=established, country=US,
--             category=Lifestyle (sociology)]
--   → 57014 statement timeout, get_creators returned empty.
--
-- Root cause: migration 048 added (quality_grade, current_view_count DESC),
-- (quality_grade, current_subscribers DESC), (quality_grade, engagement_score
-- DESC) partial indexes but did NOT cover the published_at sort dimension.
-- Migration 050 added single-column idx_creators_published_at_synced but
-- Postgres could not combine it with the quality_grade equality without a
-- sort step, so any grade + published_at sort still hit the timeout.
--
-- Fix: composite indexes on (quality_grade, published_at DESC) let Postgres:
--   1. Seek to quality_grade = 'C' in the index
--   2. Walk index rows in published_at DESC order (no sort step)
--   3. Apply residual filters (country, category, activity, age) row-by-row
--   4. Stop after 50 matches
-- A DESC index also serves ORDER BY published_at ASC (oldest_channel) via
-- a backward scan — one index covers both sort directions.
--
-- Mirrors migration 048's two-index pattern (synced + synced_partial) so the
-- BitmapOr plan from migration 045 remains available for other queries.
--
-- Uses plain CREATE INDEX (not CONCURRENTLY) so it can run inside a Supabase
-- SQL editor transaction. Run during low-traffic if the creators table is large.
--
-- Prerequisites: migrations 008, 043, 045, 048, 050 (partial index foundations).

-- ── synced creators ───────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_creators_grade_published_synced
    ON public.creators (quality_grade, published_at DESC)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- ── synced_partial creators ───────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_creators_grade_published_synced_partial
    ON public.creators (quality_grade, published_at DESC)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- Verification (run after applying):
-- SELECT indexname, indexdef
-- FROM   pg_indexes
-- WHERE  tablename = 'creators'
--   AND  indexname LIKE 'idx_creators_grade_published_%';
--
-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT id, channel_name, quality_grade, published_at
-- FROM   creators
-- WHERE  sync_status IN ('synced', 'synced_partial')
--   AND  channel_name IS NOT NULL
--   AND  current_subscribers > 0
--   AND  quality_grade = 'C'
--   AND  country_code = 'US'
-- ORDER BY published_at DESC
-- LIMIT 50;
-- Expected: "Index Scan using idx_creators_grade_published_synced" (or Bitmap
-- Or across both partial indexes) with no separate Sort node.
