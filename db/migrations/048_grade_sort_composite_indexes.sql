-- Migration 048: Composite indexes for grade-filtered + sort-by-views queries
--
-- Problem: get_creators() with quality_grade + activity (monthly_uploads) + category
-- + ORDER BY current_view_count caused a statement timeout (57014).
--
-- Example failing query:
--   quality_grade = 'A'
--   AND monthly_uploads < 1      (activity=dormant)
--   AND primary_category ILIKE '%Jazz%'
--   ORDER BY current_view_count DESC
--   LIMIT 50
--
-- Root cause: No composite index combined the grade equality predicate with the
-- views sort column. Postgres had to:
--   1. Scan all grade=A rows via idx_creators_grade_synced (many rows)
--   2. Apply the monthly_uploads and ILIKE filters as residual conditions
--   3. Sort ALL matching rows by current_view_count (expensive)
-- The sort step on a large intermediate result caused the 57014 timeout.
--
-- Fix: composite indexes on (quality_grade, current_view_count DESC) let Postgres:
--   1. Seek to quality_grade = 'A' in the index
--   2. Walk rows in current_view_count DESC order (already sorted — no sort step)
--   3. Apply monthly_uploads < 1 and ILIKE as cheap row-level filters
--   4. Stop after 50 matches
-- This is an index-ordered scan: O(rows scanned until 50 match), not O(all matching).
--
-- Mirrors the two-index pattern from migration 045 (synced + synced_partial) so
-- the BitmapOr(idx_synced, idx_synced_partial) plan from migration 045 still works.
--
-- Uses plain CREATE INDEX (not CONCURRENTLY) so it can run inside a Supabase
-- SQL editor transaction. Run during low-traffic if the creators table is large.
--
-- Prerequisites: migrations 008, 043, 045 (partial index foundations).

-- ── synced creators ───────────────────────────────────────────────────────────

-- Grade + views sort: covers quality_grade=X ORDER BY current_view_count DESC.
-- Most impactful for the grade + activity + category + sort=views combination.
CREATE INDEX IF NOT EXISTS idx_creators_grade_views_synced
    ON public.creators (quality_grade, current_view_count DESC)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- Grade + subscribers sort: covers quality_grade=X ORDER BY current_subscribers DESC.
-- Handles the common "grade=A sort=subscribers" filter without a sort step.
CREATE INDEX IF NOT EXISTS idx_creators_grade_subs_synced
    ON public.creators (quality_grade, current_subscribers DESC)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- Grade + engagement sort.
CREATE INDEX IF NOT EXISTS idx_creators_grade_engagement_synced
    ON public.creators (quality_grade, engagement_score DESC)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- ── synced_partial creators ───────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_creators_grade_views_synced_partial
    ON public.creators (quality_grade, current_view_count DESC)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX IF NOT EXISTS idx_creators_grade_subs_synced_partial
    ON public.creators (quality_grade, current_subscribers DESC)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX IF NOT EXISTS idx_creators_grade_engagement_synced_partial
    ON public.creators (quality_grade, engagement_score DESC)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- Verification:
-- SELECT indexname, indexdef
-- FROM pg_indexes
-- WHERE tablename = 'creators'
--   AND indexname LIKE 'idx_creators_grade_%'
-- ORDER BY indexname;
