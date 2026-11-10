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
        RAISE EXCEPTION USING
            ERRCODE = 'VQ001',
            MESSAGE = 'Creator sync queue is not drained; retry automatic refresh later';
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

COMMENT ON FUNCTION public.enqueue_unknown_activity_refresh_batch(integer, boolean, integer) IS
    'Internal enqueue primitive; service-role callers must use enqueue_unknown_activity_refresh_batch_after_drain.';
REVOKE ALL ON FUNCTION public.enqueue_unknown_activity_refresh_batch(integer, boolean, integer)
FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.lock_creator_sync_job_enqueue()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM pg_advisory_xact_lock(
            hashtextextended('public.enqueue_creator_syncs', 0)
        );
    ELSIF OLD.status = 'failed' AND NEW.status = 'pending' THEN
        PERFORM pg_advisory_xact_lock(
            hashtextextended('public.enqueue_creator_syncs', 0)
        );
    END IF;
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
BEFORE INSERT OR UPDATE OF status ON public.creator_sync_jobs
FOR EACH ROW EXECUTE FUNCTION public.lock_creator_sync_job_enqueue();

COMMIT;
