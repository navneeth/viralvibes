-- Stage 1 of the active-job uniqueness rollout.
-- Replace the existing priority enqueue RPC before adding the active-job index.
-- This function definition is safe to apply independently and does not change
-- creator_sync_jobs rows or indexes.

CREATE OR REPLACE FUNCTION public.enqueue_creator_syncs(
    p_limit integer DEFAULT 1000,
    p_min_subscribers integer DEFAULT 10000,
    p_max_queue integer DEFAULT 3000,
    p_dry_run boolean DEFAULT true
)
RETURNS TABLE (
    bucket text,
    selected bigint,
    lowest_subs bigint,
    highest_subs bigint,
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
    PERFORM pg_advisory_xact_lock(hashtextextended('public.enqueue_creator_syncs', 0));

    SELECT count(*) INTO v_pending
    FROM public.creator_sync_jobs
    WHERE status = 'pending';

    v_budget := GREATEST(
        LEAST(
            COALESCE(p_limit, 0)::bigint,
            COALESCE(p_max_queue, 0)::bigint - v_pending
        ),
        0
    );

    DROP TABLE IF EXISTS pg_temp._enq_pick;
    CREATE TEMP TABLE _enq_pick ON COMMIT DROP AS
    SELECT
        candidates.creator_id,
        candidates.current_subscribers,
        candidates.bucket,
        row_number() OVER (
            ORDER BY candidates.bucket,
                     candidates.current_subscribers DESC NULLS LAST,
                     candidates.creator_id
        ) AS rn
    FROM public.creator_sync_candidates(p_min_subscribers) AS candidates
    ORDER BY rn
    LIMIT v_budget;

    IF p_dry_run IS DISTINCT FROM false THEN
        RETURN QUERY
        SELECT p.bucket,
               count(*)::bigint,
               min(p.current_subscribers)::bigint,
               max(p.current_subscribers)::bigint,
               true
        FROM pg_temp._enq_pick AS p
        GROUP BY p.bucket
        ORDER BY p.bucket;
    ELSE
        RETURN QUERY
        WITH ins AS (
            INSERT INTO public.creator_sync_jobs
                (creator_id, status, job_type, source, created_at)
            SELECT p.creator_id,
                   'pending',
                   'sync_stats',
                   'priority_enqueue',
                   statement_timestamp() + p.rn * interval '1 millisecond'
            FROM pg_temp._enq_pick AS p
            ON CONFLICT DO NOTHING
            RETURNING creator_id
        )
        SELECT p.bucket,
               count(*)::bigint,
               min(p.current_subscribers)::bigint,
               max(p.current_subscribers)::bigint,
               false
        FROM pg_temp._enq_pick AS p
        JOIN ins ON ins.creator_id = p.creator_id
        GROUP BY p.bucket
        ORDER BY p.bucket;
    END IF;
END;
$$;

COMMENT ON FUNCTION public.enqueue_creator_syncs(integer, integer, integer, boolean) IS
    'Dry-run-first priority enqueue lane; excludes active jobs and tolerates concurrent uniqueness conflicts.';

REVOKE ALL ON FUNCTION public.enqueue_creator_syncs(integer, integer, integer, boolean)
    FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.enqueue_creator_syncs(integer, integer, integer, boolean)
    TO service_role;
