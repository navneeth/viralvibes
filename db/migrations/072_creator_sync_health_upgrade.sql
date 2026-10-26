-- Run with psql autocommit, not inside a transaction: the indexes are concurrent.
-- Existing valid production indexes are retained. Duplicate pending creator jobs
-- must be resolved explicitly before applying this migration on other databases.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_jobs_pending_unique
    ON public.creator_sync_jobs (creator_id) WHERE status = 'pending';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_health_processing
    ON public.creator_sync_jobs (started_at, created_at) WHERE status = 'processing';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_eligible_subscribers
    ON public.creators (current_subscribers DESC, id)
    WHERE archived_at IS NULL AND sync_status IS DISTINCT FROM 'invalid';

CREATE OR REPLACE FUNCTION public.creator_sync_candidates(p_min_subscribers integer DEFAULT 10000)
RETURNS TABLE (
    creator_id uuid, current_subscribers bigint, bucket text, eligible_since timestamptz
)
LANGUAGE sql STABLE
SET search_path = pg_catalog, public, pg_temp
AS $$
    WITH base AS (
        SELECT c.id, c.current_subscribers, c.last_synced_at,
               c.first_seen_at AT TIME ZONE 'UTC' AS first_seen_at,
               CASE WHEN c.recent_upload->>'published_at' ~ '^\d{4}-\d{2}-\d{2}T'
                    THEN (c.recent_upload->>'published_at')::timestamptz
               END AS last_upload_at
        FROM public.creators AS c
        WHERE c.archived_at IS NULL
          AND c.sync_status IS DISTINCT FROM 'invalid'
          AND COALESCE(c.current_subscribers, 0) >= COALESCE(p_min_subscribers, 0)
          AND NOT EXISTS (
              SELECT 1 FROM public.creator_sync_jobs AS j
              WHERE j.creator_id = c.id AND j.status IN ('pending', 'processing')
          )
    ), classified AS (
        SELECT b.*,
               CASE WHEN b.last_synced_at IS NULL THEN '1_never_synced'
                    WHEN b.last_upload_at IS NOT NULL
                         AND b.last_synced_at - b.last_upload_at <= interval '60 days'
                         AND b.last_synced_at < now() - interval '30 days'
                        THEN '2_active_overdue'
                    WHEN b.last_upload_at IS NULL
                         AND b.last_synced_at < now() - interval '90 days'
                        THEN '3_unknown_activity'
               END AS bucket
        FROM base AS b
    )
    SELECT cl.id, cl.current_subscribers::bigint, cl.bucket,
           CASE cl.bucket WHEN '1_never_synced' THEN cl.first_seen_at
                          WHEN '2_active_overdue' THEN cl.last_synced_at + interval '30 days'
                          WHEN '3_unknown_activity' THEN cl.last_synced_at + interval '90 days'
           END
    FROM classified AS cl WHERE cl.bucket IS NOT NULL;
$$;

REVOKE ALL ON FUNCTION public.creator_sync_candidates(integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.creator_sync_candidates(integer) TO service_role;

CREATE OR REPLACE FUNCTION public.enqueue_creator_syncs(
    p_limit integer DEFAULT 1000, p_min_subscribers integer DEFAULT 10000,
    p_max_queue integer DEFAULT 3000, p_dry_run boolean DEFAULT true
)
RETURNS TABLE (bucket text, selected bigint, lowest_subs bigint, highest_subs bigint, dry_run boolean)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
#variable_conflict use_column
DECLARE
    v_pending bigint;
    v_budget bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('public.enqueue_creator_syncs', 0));
    SELECT count(*) INTO v_pending FROM public.creator_sync_jobs WHERE status = 'pending';
    v_budget := GREATEST(LEAST(COALESCE(p_limit, 0)::bigint,
                              COALESCE(p_max_queue, 0)::bigint - v_pending), 0);
    DROP TABLE IF EXISTS pg_temp._enq_pick;
    CREATE TEMP TABLE _enq_pick ON COMMIT DROP AS
    SELECT candidates.creator_id, candidates.current_subscribers, candidates.bucket,
           row_number() OVER (
               ORDER BY candidates.bucket, candidates.current_subscribers DESC NULLS LAST,
                        candidates.creator_id
           ) AS rn
    FROM public.creator_sync_candidates(p_min_subscribers) AS candidates
    ORDER BY rn LIMIT v_budget;

    IF p_dry_run IS DISTINCT FROM false THEN
        RETURN QUERY
        SELECT p.bucket, count(*)::bigint, min(p.current_subscribers)::bigint,
               max(p.current_subscribers)::bigint, true
        FROM pg_temp._enq_pick AS p GROUP BY p.bucket ORDER BY p.bucket;
    ELSE
        RETURN QUERY
        WITH ins AS (
            INSERT INTO public.creator_sync_jobs (creator_id, status, job_type, source, created_at)
            SELECT p.creator_id, 'pending', 'sync_stats', 'priority_enqueue',
                   now() + p.rn * interval '1 millisecond'
            FROM pg_temp._enq_pick AS p
            ON CONFLICT (creator_id) WHERE status = 'pending' DO NOTHING
            RETURNING creator_id
        )
        SELECT p.bucket, count(*)::bigint, min(p.current_subscribers)::bigint,
               max(p.current_subscribers)::bigint, false
        FROM pg_temp._enq_pick AS p JOIN ins ON ins.creator_id = p.creator_id
        GROUP BY p.bucket ORDER BY p.bucket;
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION public.enqueue_creator_syncs(integer, integer, integer, boolean)
    FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.enqueue_creator_syncs(integer, integer, integer, boolean)
    TO service_role;

CREATE TABLE IF NOT EXISTS public.creator_sync_coverage_snapshot (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    sampled_at timestamptz NOT NULL,
    min_subscribers integer NOT NULL,
    eligible_creators_without_job bigint NOT NULL CHECK (eligible_creators_without_job >= 0),
    oldest_eligible_since timestamptz
);
ALTER TABLE public.creator_sync_coverage_snapshot ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.creator_sync_coverage_snapshot FROM PUBLIC, anon, authenticated;
GRANT SELECT, INSERT, UPDATE ON public.creator_sync_coverage_snapshot TO service_role;

CREATE OR REPLACE FUNCTION public.refresh_creator_sync_coverage(
    p_min_subscribers integer DEFAULT 10000
)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF NOT pg_try_advisory_xact_lock(hashtextextended('public.refresh_creator_sync_coverage', 0)) THEN
        RETURN false;
    END IF;
    INSERT INTO public.creator_sync_coverage_snapshot
        (singleton, sampled_at, min_subscribers, eligible_creators_without_job, oldest_eligible_since)
    SELECT true, statement_timestamp(), COALESCE(p_min_subscribers, 0), count(*), min(eligible_since)
    FROM public.creator_sync_candidates(p_min_subscribers)
    ON CONFLICT (singleton) DO UPDATE SET
        sampled_at = EXCLUDED.sampled_at,
        min_subscribers = EXCLUDED.min_subscribers,
        eligible_creators_without_job = EXCLUDED.eligible_creators_without_job,
        oldest_eligible_since = EXCLUDED.oldest_eligible_since;
    RETURN true;
END;
$$;
REVOKE ALL ON FUNCTION public.refresh_creator_sync_coverage(integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.refresh_creator_sync_coverage(integer) TO service_role;

CREATE OR REPLACE FUNCTION public.get_creator_sync_health_v2()
RETURNS TABLE (
    ready_jobs bigint, retry_delayed_jobs bigint, processing_jobs bigint,
    oldest_ready_at timestamptz, oldest_processing_at timestamptz,
    last_successful_sync_at timestamptz,
    eligible_creators_without_job bigint, oldest_eligible_since timestamptz,
    coverage_sampled_at timestamptz, coverage_min_subscribers integer
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
    WITH pending AS (
        SELECT count(*) FILTER (WHERE next_retry_at IS NULL OR next_retry_at <= now()) AS ready,
               count(*) FILTER (WHERE next_retry_at > now()) AS delayed,
               min(GREATEST(created_at, COALESCE(next_retry_at, created_at)))
                   FILTER (WHERE next_retry_at IS NULL OR next_retry_at <= now()) AS oldest
        FROM public.creator_sync_jobs WHERE status = 'pending'
    ), processing AS (
        SELECT count(*) AS jobs, min(COALESCE(started_at, created_at)) AS oldest
        FROM public.creator_sync_jobs WHERE status = 'processing'
    )
    SELECT pending.ready, pending.delayed, processing.jobs, pending.oldest, processing.oldest,
           (SELECT max(completed_at) FROM public.creator_sync_jobs WHERE status = 'completed'),
           coverage.eligible_creators_without_job, coverage.oldest_eligible_since,
           coverage.sampled_at, coverage.min_subscribers
    FROM pending CROSS JOIN processing
    LEFT JOIN public.creator_sync_coverage_snapshot AS coverage ON coverage.singleton;
$$;
REVOKE ALL ON FUNCTION public.get_creator_sync_health_v2() FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.get_creator_sync_health_v2() TO service_role;

UPDATE public.creator_sync_jobs
SET next_retry_at = retry_at AT TIME ZONE 'UTC'
WHERE status = 'pending' AND retry_at IS NOT NULL AND next_retry_at IS NULL;

-- Keep get_creator_sync_health(integer) unchanged for older app deployments.
-- Refresh coverage from background maintenance, never from a dashboard request.
-- SELECT public.refresh_creator_sync_coverage();
-- SELECT * FROM public.get_creator_sync_health_v2();
