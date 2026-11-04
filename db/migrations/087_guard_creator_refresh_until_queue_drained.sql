-- Enforce the automatic refresh drain barrier under the shared enqueue lock.
-- The worker's health check is advisory; this wrapper closes the race with a
-- regular job inserted between that check and the refresh RPC.
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
    IF NOT pg_try_advisory_xact_lock(
        hashtextextended('public.enqueue_creator_syncs', 0)
    ) THEN
        RAISE EXCEPTION 'Creator enqueue maintenance is busy; retry the refresh batch later';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.creator_sync_jobs
        WHERE status IN ('pending', 'processing')
    ) THEN
        RAISE EXCEPTION 'Automatic refresh requires the creator sync queue to be drained';
    END IF;

    RETURN QUERY
    SELECT *
    FROM public.enqueue_unknown_activity_refresh_batch(p_limit, p_dry_run, p_max_queue);
END;
$$;

REVOKE ALL ON FUNCTION public.enqueue_unknown_activity_refresh_batch_after_drain(integer, boolean, integer)
FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.enqueue_unknown_activity_refresh_batch_after_drain(integer, boolean, integer)
TO service_role;
