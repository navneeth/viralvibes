-- Creator sync health metrics and supporting indexes for the admin dashboard.
-- The function returns aggregates only; it does not expose creator identifiers
-- or other row-level data.

-- creators.first_seen_at is timestamp WITHOUT time zone (stored as UTC); it is
-- converted explicitly below so the result does not depend on session TimeZone.
--
-- All index names match any copy built by hand in the SQL Editor with plain
-- CREATE INDEX, so re-running this file is a no-op for those (IF NOT EXISTS).
-- Check for INVALID leftovers after a failed CONCURRENTLY build:
--   SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
--   WHERE NOT i.indisvalid AND c.relname LIKE 'idx_creator_sync_health_%';
--
-- CREATE INDEX CONCURRENTLY must run outside a transaction block. If your
-- migration runner wraps files in a transaction (including the Supabase SQL
-- Editor), run this file from the repo root with psql's default autocommit:
--   psql "$DATABASE_URL" -f db/migrations/070_add_creator_sync_indexes.sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_health_never_synced
    ON public.creators (first_seen_at, id)
    WHERE archived_at IS NULL AND last_synced_at IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_health_stale
    ON public.creators (last_synced_at, id)
    WHERE archived_at IS NULL
      AND last_synced_at IS NOT NULL
      AND sync_status IN ('synced', 'synced_partial', 'invalid', 'failed');

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_health_active_jobs
    ON public.creator_sync_jobs (creator_id)
    WHERE status IN ('pending', 'processing');

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_health_pending_retry
    ON public.creator_sync_jobs (next_retry_at)
    WHERE status = 'pending';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_health_last_success
    ON public.creator_sync_jobs (completed_at DESC)
    WHERE status = 'completed';

CREATE OR REPLACE FUNCTION public.get_creator_sync_health(p_stale_days integer DEFAULT 7)
RETURNS TABLE (
    ready_jobs bigint,
    retry_delayed_jobs bigint,
    stale_creators_without_job bigint,
    oldest_unqueued_stale_at timestamptz,
    last_successful_sync_at timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
    WITH queue_health AS (
        SELECT
            count(*) FILTER (
                WHERE j.next_retry_at IS NULL OR j.next_retry_at <= now()
            )::bigint AS ready_jobs,
            count(*) FILTER (WHERE j.next_retry_at > now())::bigint AS retry_delayed_jobs
        FROM public.creator_sync_jobs AS j
        WHERE j.status = 'pending'
    ),
    stale_health AS (
        SELECT
            count(*)::bigint AS stale_creators_without_job,
            min(COALESCE(c.last_synced_at, c.first_seen_at AT TIME ZONE 'UTC')) AS oldest_unqueued_stale_at
        FROM public.creators AS c
        WHERE c.archived_at IS NULL
          AND (
              c.last_synced_at IS NULL
              OR (
                  c.sync_status IN ('synced', 'synced_partial', 'invalid', 'failed')
                  AND c.last_synced_at < (
                      now() - make_interval(days => GREATEST(COALESCE(p_stale_days, 7), 1))
                  )
              )
          )
          AND NOT EXISTS (
            SELECT 1
            FROM public.creator_sync_jobs AS j
            WHERE j.creator_id = c.id
              AND j.status IN ('pending', 'processing')
        )
    ),
    last_success AS (
        SELECT max(j.completed_at) AS last_successful_sync_at
        FROM public.creator_sync_jobs AS j
        WHERE j.status = 'completed'
    )
    SELECT
        queue_health.ready_jobs,
        queue_health.retry_delayed_jobs,
        stale_health.stale_creators_without_job,
        stale_health.oldest_unqueued_stale_at,
        last_success.last_successful_sync_at
    FROM queue_health
    CROSS JOIN stale_health
    CROSS JOIN last_success;
$$;

COMMENT ON FUNCTION public.get_creator_sync_health(integer) IS
    'Returns aggregate creator sync health metrics for the server-side admin dashboard.';

REVOKE ALL ON FUNCTION public.get_creator_sync_health(integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.get_creator_sync_health(integer) TO service_role;

-- Verify the function and indexes after applying this migration:
-- SELECT * FROM public.get_creator_sync_health(7);
-- SELECT indexname FROM pg_indexes
-- WHERE schemaname = 'public'
--   AND indexname LIKE 'idx_creator_sync_health_%'
-- ORDER BY indexname;
