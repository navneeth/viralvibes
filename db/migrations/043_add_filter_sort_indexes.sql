-- Migration 043: Add B-tree partial indexes for filter and sort columns
--
-- Problem: get_creators() supports filtering on language, grade, activity,
-- and channel age, plus sorting by upload consistency.  None of these columns
-- had indexes, so every filtered query fell back to a full sequential scan
-- of the creators table, causing statement timeouts (pg error 57014) or
-- HTTP/2 server disconnects (RemoteProtocolError) on large datasets.
--
-- All indexes are partial, matching the same WHERE predicate as the existing
-- base listing indexes (migrations 008, 028) so the query planner can
-- combine them via BitmapAnd / BitmapOr plans rather than seq-scanning.
--
-- Note: uses plain CREATE INDEX (not CONCURRENTLY) so it can run inside a
-- transaction block (Supabase SQL editor / migration runners). Each index
-- build briefly acquires a ShareLock on the creators table; run during a
-- low-traffic window if the table is large.
--
-- Prerequisites: migration 008 (idx_creators_listing_base already exists).

-- 1. default_language — used by language_filter (eq).
--    Allows the planner to jump directly to rows for a single language
--    rather than scanning all synced creators.
CREATE INDEX IF NOT EXISTS idx_creators_language_synced
    ON public.creators (default_language)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- 2. quality_grade — used by grade_filter (eq: A+, A, B+, B, C).
CREATE INDEX IF NOT EXISTS idx_creators_grade_synced
    ON public.creators (quality_grade)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- 3. monthly_uploads — used by:
--    a) activity_filter: dormant (< 1) and active (> 5) via range predicates
--    b) consistency sort: ORDER BY monthly_uploads DESC
--    A DESC index serves the ORDER BY directly (no sort step) and also
--    supports range predicates in either direction.
CREATE INDEX IF NOT EXISTS idx_creators_monthly_uploads_synced
    ON public.creators (monthly_uploads DESC)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- 4. channel_age_days — used by age_filter (new <365, established 365-3650,
--    veteran >=3650) via range predicates.
CREATE INDEX IF NOT EXISTS idx_creators_channel_age_synced
    ON public.creators (channel_age_days)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- Verification queries (run after applying):
-- SELECT indexname, indexdef
-- FROM pg_indexes
-- WHERE tablename = 'creators'
--   AND indexname IN (
--     'idx_creators_language_synced',
--     'idx_creators_grade_synced',
--     'idx_creators_monthly_uploads_synced',
--     'idx_creators_channel_age_synced'
--   );
