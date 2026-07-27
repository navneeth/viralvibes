-- Migration 056: Composite partial index for get_new_channels()
--
-- Problem: get_new_channels() times out (57014) in production.
--
-- Query:
--   SELECT * FROM creators
--   WHERE  sync_status = 'synced'
--     AND  channel_name IS NOT NULL
--     AND  current_subscribers > 0
--     AND  channel_age_days IS NOT NULL
--     AND  channel_age_days <= 365
--   ORDER BY engagement_score DESC
--   LIMIT 20;
--
-- Root cause: young channels (channel_age_days <= 365) are a small fraction
-- of the creators table.  The existing engagement sort index
-- (idx_creators_engagement_synced, migration 008) walks rows in
-- engagement_score DESC order and must fetch each row from the heap to check
-- channel_age_days.  For a table with ~1M synced creators where only a few
-- percent are "new", this degenerates into a large heap scan before 20
-- qualifying rows are found.
--
-- Fix: composite index (engagement_score DESC, channel_age_days) so the
-- planner can evaluate the channel_age_days filter directly from the index
-- leaf — no heap visit per candidate row.  With LIMIT 20 the planner stops
-- as soon as 20 rows pass the filter; expected index entries read: ~20 to
-- ~a few hundred, not tens of thousands.
--
-- Matches the same partial predicate as the existing sort/filter indexes
-- (migrations 008, 043) so BitmapAnd / Index-Only plans remain available
-- for other queries on this table.

CREATE INDEX IF NOT EXISTS idx_creators_engagement_age_synced
    ON public.creators (engagement_score DESC, channel_age_days)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- Verification (run after applying):
-- SELECT indexname, indexdef
-- FROM   pg_indexes
-- WHERE  tablename = 'creators'
--   AND  indexname = 'idx_creators_engagement_age_synced';
--
-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT id, channel_name, engagement_score, channel_age_days
-- FROM   creators
-- WHERE  sync_status = 'synced'
--   AND  channel_name IS NOT NULL
--   AND  current_subscribers > 0
--   AND  channel_age_days IS NOT NULL
--   AND  channel_age_days <= 365
-- ORDER BY engagement_score DESC
-- LIMIT 20;
-- Expected: "Index Scan using idx_creators_engagement_age_synced"
