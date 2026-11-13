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

    -- Match migration 092's index order instead of aggregating and sorting all
    -- failed history. Exclude archived, recovered, and active rows before LIMIT.
    WITH candidates AS MATERIALIZED (
        SELECT DISTINCT ON (job.creator_id)
            creator.id,
            COALESCE(job.completed_at, job.started_at, job.created_at) AS failed_at
        FROM public.creator_sync_jobs AS job
        JOIN public.creators AS creator ON creator.id = job.creator_id
        WHERE job.creator_id IS NOT NULL
          AND job.status = 'failed'
          AND job.retry_count >= p_max_retries
          AND creator.archived_at IS NULL
          AND (
              creator.last_synced_at IS NULL
              OR creator.last_synced_at <=
                  COALESCE(job.completed_at, job.started_at, job.created_at)
          )
          AND NOT EXISTS (
              SELECT 1 FROM public.creator_sync_jobs AS active_job
              WHERE active_job.creator_id = creator.id
                AND active_job.status IN ('pending', 'processing')
          )
        ORDER BY job.creator_id,
                 COALESCE(job.completed_at, job.started_at, job.created_at) DESC
        LIMIT v_limit
    ), locked_candidates AS MATERIALIZED (
        SELECT creator.id, candidates.failed_at
        FROM public.creators AS creator
        JOIN candidates ON candidates.id = creator.id
        FOR UPDATE OF creator SKIP LOCKED
    ), archived AS (
        UPDATE public.creators AS creator
        -- archived_at seals the row; the status constraint disallows 'archived'.
        SET sync_status = 'failed',
            sync_error_message = 'Failed after maximum sync retries',
            archived_at = statement_timestamp()
        FROM locked_candidates
        WHERE creator.id = locked_candidates.id
          AND creator.archived_at IS NULL
          AND (
              creator.last_synced_at IS NULL
              OR creator.last_synced_at <= locked_candidates.failed_at
          )
        RETURNING creator.id
    )
    SELECT count(*)::integer INTO v_archived FROM archived;

    RETURN v_archived;
END;
$$;

REVOKE ALL ON FUNCTION public.archive_terminal_creator_syncs(integer, integer)
FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.archive_terminal_creator_syncs(integer, integer)
TO service_role;

COMMIT;