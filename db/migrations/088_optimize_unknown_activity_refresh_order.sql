-- Match the existing eligible-subscriber index order so LIMIT can stop after
-- finding the requested batch instead of sorting all eligible creators.
-- current_subscribers >= 10000 excludes NULL values, so NULLS FIRST preserves
-- the result order while matching idx_creator_sync_eligible_subscribers.
CREATE OR REPLACE FUNCTION public.enqueue_unknown_activity_refresh_batch(
    p_limit integer DEFAULT 5,
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
    IF COALESCE(p_limit, 0) > 5 THEN
        RAISE EXCEPTION 'Unknown-activity refresh batch accepts at most five jobs';
    END IF;
    IF COALESCE(p_limit, 0) <= 0 THEN
        RETURN;
    END IF;

    IF p_dry_run IS NOT DISTINCT FROM false THEN
        DROP TABLE IF EXISTS pg_temp._creator_sync_active_index_guard;
        CREATE TEMP TABLE _creator_sync_active_index_guard
            (LIKE public.creator_sync_jobs) ON COMMIT DROP;
        CREATE UNIQUE INDEX _creator_sync_expected_active_unique
            ON pg_temp._creator_sync_active_index_guard (creator_id)
            WHERE status IN ('pending', 'processing');

        IF NOT EXISTS (
            SELECT 1 FROM pg_index AS active_index
            JOIN pg_attribute AS creator_column
              ON creator_column.attrelid = active_index.indrelid
             AND creator_column.attname = 'creator_id'
             AND NOT creator_column.attisdropped
            WHERE active_index.indexrelid =
                  to_regclass('public.idx_creator_sync_jobs_active_unique')
              AND active_index.indrelid = 'public.creator_sync_jobs'::regclass
              AND active_index.indisunique AND active_index.indisvalid
              AND active_index.indisready AND active_index.indislive
              AND active_index.indimmediate
              AND active_index.indnkeyatts = 1
              AND active_index.indkey[0] = creator_column.attnum
              AND active_index.indexprs IS NULL
              AND pg_get_expr(active_index.indpred, active_index.indrelid) = (
                  SELECT pg_get_expr(expected.indpred, expected.indrelid)
                  FROM pg_index AS expected
                  WHERE expected.indexrelid =
                        'pg_temp._creator_sync_expected_active_unique'::regclass
              )
        ) THEN
            RAISE EXCEPTION 'A valid active-job unique index is required before enqueueing';
        END IF;
    END IF;

    IF NOT pg_try_advisory_xact_lock(
        hashtextextended('public.enqueue_creator_syncs', 0)
    ) THEN
        RAISE EXCEPTION 'Creator enqueue maintenance is busy; retry the refresh batch later';
    END IF;

    SELECT count(*) INTO v_pending
    FROM public.creator_sync_jobs
    WHERE status = 'pending';

    v_budget := GREATEST(
        LEAST(
            LEAST(COALESCE(p_limit, 0), 5)::bigint,
            LEAST(COALESCE(p_max_queue, 0), 1000)::bigint - v_pending
        ),
        0
    );

    DROP TABLE IF EXISTS pg_temp._unknown_activity_auto_pick;
    CREATE TEMP TABLE _unknown_activity_auto_pick ON COMMIT DROP AS
    SELECT creator.id AS creator_id,
           creator.current_subscribers::bigint AS current_subscribers,
           row_number() OVER (
               ORDER BY creator.current_subscribers DESC NULLS FIRST, creator.id
           ) AS refresh_rank
    FROM public.creators AS creator
    CROSS JOIN LATERAL (
        SELECT public.try_parse_creator_upload_timestamp(
            creator.recent_upload->>'published_at'
        ) AS last_upload_at
    ) AS upload
    WHERE creator.archived_at IS NULL
      AND creator.sync_status IS DISTINCT FROM 'invalid'
      AND creator.current_subscribers >= 10000
      AND creator.last_synced_at < now() - interval '90 days'
      AND upload.last_upload_at IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM public.creator_sync_jobs AS active_job
          WHERE active_job.creator_id = creator.id
            AND active_job.status IN ('pending', 'processing')
      )
      AND NOT EXISTS (
          SELECT 1 FROM public.creator_sync_jobs AS recent_failure
          WHERE recent_failure.creator_id = creator.id
            AND recent_failure.source = 'unknown_activity_refresh'
            AND recent_failure.status = 'failed'
            AND COALESCE(
                    recent_failure.completed_at,
                    recent_failure.started_at,
                    recent_failure.created_at
                )
                >= now() - interval '7 days'
      )
    ORDER BY creator.current_subscribers DESC NULLS FIRST, creator.id
    LIMIT v_budget;

    IF p_dry_run IS DISTINCT FROM false THEN
        RETURN QUERY
        SELECT pick.creator_id, pick.current_subscribers, true
        FROM pg_temp._unknown_activity_auto_pick AS pick
        ORDER BY pick.current_subscribers DESC NULLS FIRST, pick.creator_id;
    ELSE
        RETURN QUERY
        WITH inserted AS (
            INSERT INTO public.creator_sync_jobs
                (creator_id, status, job_type, source, created_at)
            SELECT pick.creator_id, 'pending', 'sync_stats',
                   'unknown_activity_refresh',
                   statement_timestamp() + pick.refresh_rank * interval '1 millisecond'
            FROM pg_temp._unknown_activity_auto_pick AS pick
            ON CONFLICT DO NOTHING
            RETURNING creator_id
        )
        SELECT pick.creator_id, pick.current_subscribers, false
        FROM pg_temp._unknown_activity_auto_pick AS pick
        JOIN inserted ON inserted.creator_id = pick.creator_id
        ORDER BY pick.current_subscribers DESC NULLS FIRST, pick.creator_id;
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION public.enqueue_unknown_activity_refresh_batch(integer, boolean, integer)
FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.enqueue_unknown_activity_refresh_batch(integer, boolean, integer)
TO service_role;
