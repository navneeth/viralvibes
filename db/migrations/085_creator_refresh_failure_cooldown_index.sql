-- Run with psql autocommit, not inside a transaction.
-- Supports the automatic unknown-activity refresh failure cooldown lookup.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_jobs_refresh_failure_cooldown
    ON public.creator_sync_jobs (
        creator_id,
        (COALESCE(completed_at, started_at, created_at))
    )
    WHERE status = 'failed'
      AND source = 'unknown_activity_refresh';

-- Verify one valid index row with the expected keys and partial predicate:
-- SELECT i.indisvalid, i.indisready, pg_get_indexdef(i.indexrelid) AS definition
-- FROM pg_index AS i
-- WHERE i.indexrelid =
--       to_regclass('public.idx_creator_sync_jobs_refresh_failure_cooldown');
