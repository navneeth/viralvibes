-- Budgeted, priority-ranked enqueue of creator sync jobs.
-- Replaces reliance on the worker's in-process timers, which never fire when the
-- worker exits after one job (EXIT_AFTER_JOB) or runs under kaggle_worker.py.
--
-- Priority classes (each ranked by current_subscribers DESC):
--   1_never_synced       never synced, subscribers >= p_min_subscribers
--   2_active_overdue     was uploading (<= 60d between last upload and last sync)
--                        and last synced > 30d ago
--   3_unknown_activity   synced > 90d ago with no recorded upload date
-- Dormant creators and sync_status = 'invalid' are never enqueued.
--
-- p_dry_run defaults to TRUE: it only reports what would be inserted.
-- Safe to run repeatedly: skips creators with a pending/processing job and relies on
-- idx_creator_sync_jobs_pending_unique for races.
--
-- Usage:
--   SELECT * FROM public.enqueue_creator_syncs(1000, 10000, 3000, true);   -- preview
--   SELECT * FROM public.enqueue_creator_syncs(1000, 10000, 3000, false);  -- insert

CREATE OR REPLACE FUNCTION public.enqueue_creator_syncs(
    p_limit           integer DEFAULT 1000,
    p_min_subscribers integer DEFAULT 10000,
    p_max_queue       integer DEFAULT 3000,
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
    v_budget  bigint;
BEGIN
    SELECT count(*) INTO v_pending
    FROM public.creator_sync_jobs
    WHERE status = 'pending';

    -- Never let the queue grow past p_max_queue pending jobs.
    v_budget := GREATEST(
        LEAST(COALESCE(p_limit, 0)::bigint, COALESCE(p_max_queue, 0)::bigint - v_pending),
        0
    );

    CREATE TEMP TABLE _enq_pick ON COMMIT DROP AS
    WITH base AS (
        SELECT
            c.id,
            c.current_subscribers,
            c.last_synced_at,
            CASE WHEN c.recent_upload->>'published_at' ~ '^\d{4}-\d{2}-\d{2}T'
                 THEN (c.recent_upload->>'published_at')::timestamptz
            END AS last_upload_at
        FROM public.creators AS c
        WHERE c.archived_at IS NULL
          AND c.sync_status IS DISTINCT FROM 'invalid'
          AND COALESCE(c.current_subscribers, 0) >= COALESCE(p_min_subscribers, 0)
          AND NOT EXISTS (
              SELECT 1
              FROM public.creator_sync_jobs AS j
              WHERE j.creator_id = c.id
                AND j.status IN ('pending', 'processing')
          )
    ),
    classified AS (
        SELECT
            b.id,
            b.current_subscribers,
            CASE
                WHEN b.last_synced_at IS NULL
                    THEN '1_never_synced'
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
    SELECT
        cl.id,
        cl.current_subscribers,
        cl.bucket,
        row_number() OVER (
            ORDER BY cl.bucket, cl.current_subscribers DESC NULLS LAST, cl.id
        ) AS rn
    FROM classified AS cl
    WHERE cl.bucket IS NOT NULL
    ORDER BY rn
    LIMIT v_budget;

    IF p_dry_run THEN
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
        -- created_at is offset by rank so the worker's FIFO fetch honours priority.
        RETURN QUERY
        WITH ins AS (
            INSERT INTO public.creator_sync_jobs (creator_id, status, job_type, source, created_at)
            SELECT p.id, 'pending', 'sync_stats', 'priority_enqueue',
                   now() + p.rn * interval '1 millisecond'
            FROM pg_temp._enq_pick AS p
            ON CONFLICT (creator_id) WHERE status = 'pending' DO NOTHING
            RETURNING creator_id
        )
        SELECT p.bucket,
               count(*)::bigint,
               min(p.current_subscribers)::bigint,
               max(p.current_subscribers)::bigint,
               false
        FROM pg_temp._enq_pick AS p
        JOIN ins ON ins.creator_id = p.id
        GROUP BY p.bucket
        ORDER BY p.bucket;
    END IF;
END;
$$;

COMMENT ON FUNCTION public.enqueue_creator_syncs(integer, integer, integer, boolean) IS
    'Priority-ranked, queue-capped enqueue of creator sync jobs. Dry-run by default.';

REVOKE ALL ON FUNCTION public.enqueue_creator_syncs(integer, integer, integer, boolean)
    FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.enqueue_creator_syncs(integer, integer, integer, boolean)
    TO service_role;

-- One-time data fix: legacy retries wrote retry_at only, so the worker's fetch
-- (which reads next_retry_at) ignored their backoff. retry_at is timestamp WITHOUT
-- time zone and holds UTC wall time.
UPDATE public.creator_sync_jobs
SET next_retry_at = retry_at AT TIME ZONE 'UTC'
WHERE status = 'pending' AND retry_at IS NOT NULL AND next_retry_at IS NULL;

-- Optional scheduling, only after a dry run looks right (pg_cron runs in UTC; the
-- YouTube quota resets at 08:00 UTC):
-- SELECT cron.schedule('enqueue-creator-syncs', '0 */6 * * *',
--   $$SELECT * FROM public.enqueue_creator_syncs(750, 10000, 3000, false)$$);
