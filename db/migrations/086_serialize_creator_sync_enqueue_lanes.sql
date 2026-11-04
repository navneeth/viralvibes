-- Use one transaction-scoped lock for every creator-sync job insertion.
-- The trigger covers PostgREST bulk inserts; SQL enqueue RPCs take the same lock
-- before counting pending jobs and selecting their batch.
CREATE OR REPLACE FUNCTION public.lock_creator_sync_job_enqueue()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('public.enqueue_creator_syncs', 0)
    );
    RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION public.lock_creator_sync_job_enqueue()
FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.lock_creator_sync_job_enqueue()
TO service_role;

DROP TRIGGER IF EXISTS trg_creator_sync_jobs_enqueue_lock
ON public.creator_sync_jobs;
CREATE TRIGGER trg_creator_sync_jobs_enqueue_lock
BEFORE INSERT ON public.creator_sync_jobs
FOR EACH ROW EXECUTE FUNCTION public.lock_creator_sync_job_enqueue();

-- Migration 078 previously used a separate advisory key, so its count-and-pick
-- could race the generic, manual, bulk, and automatic enqueue paths.
CREATE OR REPLACE FUNCTION public.enqueue_growth_baseline_refreshes(
    p_limit           integer DEFAULT 100,
    p_min_subscribers integer DEFAULT 10000,
    p_max_queue       integer DEFAULT 250,
    p_dry_run         boolean DEFAULT true
)
RETURNS TABLE (
    bucket       text,
    selected     bigint,
    lowest_subs  bigint,
    highest_subs bigint,
    dry_run      boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
#variable_conflict use_column
DECLARE
    v_pending bigint;
    v_budget bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('public.enqueue_creator_syncs', 0)
    );

    SELECT count(*) INTO v_pending
    FROM public.creator_sync_jobs
    WHERE status = 'pending';

    v_budget := GREATEST(
        LEAST(
            COALESCE(p_limit, 0)::bigint,
            500::bigint,
            LEAST(COALESCE(p_max_queue, 0)::bigint, 1000::bigint) - v_pending
        ),
        0
    );

    DROP TABLE IF EXISTS pg_temp._growth_baseline_pick;
    CREATE TEMP TABLE _growth_baseline_pick ON COMMIT DROP AS
    SELECT
        c.id AS creator_id,
        c.current_subscribers,
        row_number() OVER (
            ORDER BY c.current_subscribers DESC NULLS LAST, c.id
        ) AS rn
    FROM public.creators AS c
    WHERE c.archived_at IS NULL
      AND c.sync_status = 'synced'
      AND c.last_synced_at IS NOT NULL
      AND COALESCE(c.current_subscribers, 0) >= COALESCE(p_min_subscribers, 0)
      AND c.prev_subscribers IS NOT NULL
      AND c.prev_subscribers > 0
      AND c.current_subscribers > c.prev_subscribers
      AND c.growth_window_days IS NULL
      AND c.prev_snapshot_at <= statement_timestamp() - interval '7 days'
      AND public.try_parse_creator_upload_timestamp(
              c.recent_upload->>'published_at'
          ) >= statement_timestamp() - interval '90 days'
      AND NOT EXISTS (
          SELECT 1
          FROM public.creator_sync_jobs AS j
          WHERE j.creator_id = c.id
            AND j.status IN ('pending', 'processing')
      )
    ORDER BY c.current_subscribers DESC NULLS LAST, c.id
    LIMIT v_budget;

    IF p_dry_run IS DISTINCT FROM false THEN
        RETURN QUERY
        SELECT 'positive_growth_missing_window'::text,
               count(*)::bigint,
               min(p.current_subscribers)::bigint,
               max(p.current_subscribers)::bigint,
               true
        FROM pg_temp._growth_baseline_pick AS p
        HAVING count(*) > 0;
    ELSE
        RETURN QUERY
        WITH inserted AS (
            INSERT INTO public.creator_sync_jobs
                (creator_id, status, job_type, source, created_at)
            SELECT p.creator_id, 'pending', 'sync_stats',
                   'growth_baseline_refresh',
                   statement_timestamp() + p.rn * interval '1 millisecond'
            FROM pg_temp._growth_baseline_pick AS p
            ON CONFLICT DO NOTHING
            RETURNING creator_id
        )
        SELECT 'positive_growth_missing_window'::text,
               count(*)::bigint,
               min(p.current_subscribers)::bigint,
               max(p.current_subscribers)::bigint,
               false
        FROM pg_temp._growth_baseline_pick AS p
        JOIN inserted ON inserted.creator_id = p.creator_id
        HAVING count(*) > 0;
    END IF;
END;
$$;

COMMENT ON FUNCTION public.enqueue_growth_baseline_refreshes(integer, integer, integer, boolean) IS
    'Dry-run-first, queue-capped refresh lane for active synced creators with positive subscriber growth and a missing valid growth window.';

REVOKE ALL ON FUNCTION public.enqueue_growth_baseline_refreshes(integer, integer, integer, boolean)
    FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.enqueue_growth_baseline_refreshes(integer, integer, integer, boolean)
    TO service_role;