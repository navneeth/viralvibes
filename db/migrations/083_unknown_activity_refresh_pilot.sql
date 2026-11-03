-- Install a dry-run-first pilot lane only; do not enqueue jobs during migration.
-- Apply mode requires the active-job unique index from migration 077.
CREATE OR REPLACE FUNCTION public.enqueue_unknown_activity_refreshes(
    p_creator_ids uuid[] DEFAULT '{}'::uuid[],
    p_dry_run boolean DEFAULT true,
    p_max_queue integer DEFAULT 1000
)
RETURNS TABLE (
    creator_id uuid,
    current_subscribers bigint,
    dry_run boolean
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
    IF COALESCE(cardinality(p_creator_ids), 0) > 5 THEN
        RAISE EXCEPTION 'Unknown-activity pilot accepts at most five creator IDs';
    END IF;
    IF COALESCE(cardinality(p_creator_ids), 0) = 0 THEN
        RETURN;
    END IF;
    IF p_dry_run IS NOT DISTINCT FROM false AND NOT EXISTS (
        SELECT 1 FROM pg_index
        WHERE indexrelid = to_regclass('public.idx_creator_sync_jobs_active_unique')
          AND indrelid = 'public.creator_sync_jobs'::regclass
          AND indisunique AND indisvalid AND indisready
    ) THEN
        RAISE EXCEPTION 'A valid active-job unique index is required before enqueueing';
    END IF;
    IF NOT pg_try_advisory_xact_lock(
        hashtextextended('public.enqueue_creator_syncs', 0)
    ) THEN
        RAISE EXCEPTION 'Creator enqueue maintenance is busy; retry the pilot later';
    END IF;

    SELECT count(*) INTO v_pending
    FROM public.creator_sync_jobs WHERE status = 'pending';
    v_budget := GREATEST(
        LEAST(5::bigint, LEAST(COALESCE(p_max_queue, 0), 1000)::bigint - v_pending),
        0
    );

    DROP TABLE IF EXISTS pg_temp._unknown_activity_pick;
    CREATE TEMP TABLE _unknown_activity_pick ON COMMIT DROP AS
    SELECT candidate.creator_id, candidate.current_subscribers
    FROM public.creator_sync_candidates(10000) AS candidate
    WHERE candidate.bucket = '3_unknown_activity'
      AND candidate.creator_id = ANY(p_creator_ids)
      AND NOT EXISTS (
          SELECT 1 FROM public.creator_sync_jobs AS job
          WHERE job.creator_id = candidate.creator_id
            AND job.status IN ('pending', 'processing')
      )
    ORDER BY candidate.current_subscribers DESC NULLS LAST, candidate.creator_id
    LIMIT v_budget;

    IF p_dry_run IS DISTINCT FROM false THEN
        RETURN QUERY
        SELECT pick.creator_id, pick.current_subscribers, true
        FROM pg_temp._unknown_activity_pick AS pick
        ORDER BY pick.current_subscribers DESC NULLS LAST, pick.creator_id;
    ELSE
        RETURN QUERY
        WITH inserted AS (
            INSERT INTO public.creator_sync_jobs
                (creator_id, status, job_type, source, created_at)
            SELECT pick.creator_id, 'pending', 'sync_stats',
                   'unknown_activity_refresh', statement_timestamp()
            FROM pg_temp._unknown_activity_pick AS pick
            ON CONFLICT DO NOTHING
            RETURNING creator_id
        )
        SELECT pick.creator_id, pick.current_subscribers, false
        FROM pg_temp._unknown_activity_pick AS pick
        JOIN inserted ON inserted.creator_id = pick.creator_id
        ORDER BY pick.current_subscribers DESC NULLS LAST, pick.creator_id;
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION public.enqueue_unknown_activity_refreshes(uuid[], boolean, integer)
FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.enqueue_unknown_activity_refreshes(uuid[], boolean, integer)
TO service_role;
