BEGIN;

CREATE OR REPLACE FUNCTION public.enqueue_unknown_activity_refresh_batch_after_drain(
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
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('public.enqueue_creator_syncs', 0)
    );

    IF EXISTS (
        SELECT 1
        FROM public.creator_sync_jobs
        WHERE status IN ('pending', 'processing')
    ) THEN
        RAISE EXCEPTION 'Creator sync queue is not drained; retry automatic refresh later';
    END IF;

    RETURN QUERY
    SELECT *
    FROM public.enqueue_unknown_activity_refresh_batch(
        p_limit,
        p_dry_run,
        p_max_queue
    );
END;
$$;

COMMENT ON FUNCTION public.enqueue_unknown_activity_refresh_batch_after_drain(integer, boolean, integer) IS
    'Enqueues an automatic unknown-activity refresh batch only while the creator sync queue is drained.';

REVOKE ALL ON FUNCTION public.enqueue_unknown_activity_refresh_batch_after_drain(integer, boolean, integer)
FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.enqueue_unknown_activity_refresh_batch_after_drain(integer, boolean, integer)
TO service_role;

COMMIT;