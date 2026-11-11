BEGIN;

CREATE OR REPLACE FUNCTION public.archive_terminal_creator_syncs(
    p_max_retries integer DEFAULT 3,
    p_limit integer DEFAULT 100
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
SET statement_timeout = '5s'
AS $$
DECLARE
    v_archived integer := 0;
    v_limit integer;
BEGIN
    IF COALESCE(p_max_retries, 0) <= 0 OR COALESCE(p_limit, 0) <= 0 THEN
        RAISE EXCEPTION 'Retry threshold and archive limit must be positive';
    END IF;
    v_limit := LEAST(p_limit, 100);

    IF NOT pg_try_advisory_xact_lock(
        hashtextextended('public.enqueue_creator_syncs', 0)
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'VQ002',
            MESSAGE = 'Creator enqueue maintenance is busy; retry terminal archival later';
    END IF;

    -- archived_at is the terminal marker because the sync_status constraint
    -- does not allow an "archived" value. A newer sync supersedes an older
    -- terminal failure, and active jobs must remain queueable.
    WITH terminal_failures AS (
        SELECT
            job.creator_id,
            max(COALESCE(job.completed_at, job.started_at, job.created_at)) AS failed_at
        FROM public.creator_sync_jobs AS job
        WHERE job.creator_id IS NOT NULL
          AND job.status = 'failed'
          AND job.retry_count >= p_max_retries
        GROUP BY job.creator_id
    ), candidates AS (
        SELECT creator.id, terminal.failed_at
        FROM public.creators AS creator
        JOIN terminal_failures AS terminal ON terminal.creator_id = creator.id
        WHERE creator.archived_at IS NULL
          AND (
              creator.last_synced_at IS NULL
              OR creator.last_synced_at <= terminal.failed_at
          )
          AND NOT EXISTS (
              SELECT 1
              FROM public.creator_sync_jobs AS active_job
              WHERE active_job.creator_id = creator.id
                AND active_job.status IN ('pending', 'processing')
          )
        ORDER BY terminal.failed_at, creator.id
        LIMIT v_limit
        FOR UPDATE OF creator SKIP LOCKED
    ), archived AS (
        UPDATE public.creators AS creator
        -- Keep sync_status within its allowed values; archived_at seals the row.
        SET sync_status = 'failed',
            sync_error_message = 'Failed after maximum sync retries',
            archived_at = statement_timestamp()
        FROM candidates
        WHERE creator.id = candidates.id
          AND creator.archived_at IS NULL
          AND (
              creator.last_synced_at IS NULL
              OR creator.last_synced_at <= candidates.failed_at
          )
        RETURNING creator.id
    )
    SELECT count(*)::integer INTO v_archived FROM archived;

    RETURN v_archived;
END;
$$;

COMMENT ON FUNCTION public.archive_terminal_creator_syncs(integer, integer) IS
    'Atomically archives a bounded set of terminally failed creators, excluding recovered or active creators and serializing with enqueue operations.';

REVOKE ALL ON FUNCTION public.archive_terminal_creator_syncs(integer, integer)
FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.archive_terminal_creator_syncs(integer, integer)
TO service_role;

CREATE OR REPLACE FUNCTION public.lock_creator_sync_job_enqueue()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM pg_advisory_xact_lock(
            hashtextextended('public.enqueue_creator_syncs', 0)
        );
    ELSIF NEW.status IN ('pending', 'processing')
          AND OLD.status IS DISTINCT FROM NEW.status THEN
        PERFORM pg_advisory_xact_lock(
            hashtextextended('public.enqueue_creator_syncs', 0)
        );
    ELSE
        RETURN NEW;
    END IF;

    IF NEW.creator_id IS NOT NULL AND EXISTS (
        SELECT 1
        FROM public.creators AS creator
        WHERE creator.id = NEW.creator_id
          AND creator.archived_at IS NOT NULL
    ) THEN
        RETURN NULL;
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