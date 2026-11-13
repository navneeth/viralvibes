BEGIN;

ALTER TABLE public.creator_sync_jobs
    ADD COLUMN IF NOT EXISTS snapshot_failed_at timestamptz;

COMMENT ON COLUMN public.creator_sync_jobs.snapshot_failed_at IS
    'Latest optional video snapshot failure for this job; does not change core sync status or retry behavior.';

CREATE OR REPLACE FUNCTION public.enqueue_video_snapshot_refreshes(
    p_limit integer DEFAULT 40,
    p_dry_run boolean DEFAULT true,
    p_max_queue integer DEFAULT 250
)
RETURNS TABLE (creator_id uuid, dry_run boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_pending integer;
    v_budget integer;
    v_now timestamptz := statement_timestamp();
BEGIN
    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 40 THEN
        RAISE EXCEPTION 'Snapshot refresh accepts one to 40 jobs';
    END IF;
    IF NOT pg_try_advisory_xact_lock(hashtextextended('public.enqueue_creator_syncs', 0)) THEN
        RAISE EXCEPTION 'Creator enqueue maintenance is busy';
    END IF;

    SELECT count(*)::integer INTO v_pending
    FROM (
        SELECT 1 FROM public.creator_sync_jobs WHERE status = 'pending' LIMIT 250
    ) AS pending;
    v_budget := GREATEST(LEAST(p_limit, LEAST(COALESCE(p_max_queue, 0), 250) - v_pending), 0);

    DROP TABLE IF EXISTS pg_temp._video_snapshot_refresh_pick;
    CREATE TEMP TABLE _video_snapshot_refresh_pick ON COMMIT DROP AS
    SELECT creator.id AS creator_id,
           row_number() OVER (
               ORDER BY creator.recent_videos_fetched_at NULLS FIRST, creator.id
           ) AS refresh_rank
    FROM public.creators AS creator
    WHERE creator.recent_videos_requested_at IS NOT NULL
      AND creator.archived_at IS NULL
      AND creator.sync_status IN ('synced', 'synced_partial')
      AND creator.channel_id ~ '^UC[A-Za-z0-9_-]{22}$'
      AND (
          creator.recent_videos IS NULL
          OR creator.recent_videos_fetched_at IS NULL
          OR creator.recent_videos_fetched_at > v_now
          OR creator.recent_videos_fetched_at <= v_now - interval '7 days'
      )
      AND NOT EXISTS (
          SELECT 1 FROM public.creator_sync_jobs AS active_job
          WHERE active_job.creator_id = creator.id
            AND active_job.status IN ('pending', 'processing')
      )
      AND NOT EXISTS (
          SELECT 1 FROM public.creator_sync_jobs AS failed_job
          WHERE failed_job.creator_id = creator.id
            AND failed_job.source = 'video_snapshot_refresh'
            AND (
                (
                    failed_job.status = 'failed'
                    AND COALESCE(failed_job.completed_at, failed_job.started_at, failed_job.created_at)
                        >= v_now - interval '7 days'
                )
                OR failed_job.snapshot_failed_at >= v_now - interval '7 days'
            )
      )
    ORDER BY creator.recent_videos_fetched_at NULLS FIRST, creator.id
    LIMIT v_budget;

    IF p_dry_run IS DISTINCT FROM false THEN
        RETURN QUERY
        SELECT pick.creator_id, true FROM pg_temp._video_snapshot_refresh_pick AS pick
        ORDER BY pick.refresh_rank;
    ELSE
        RETURN QUERY
        WITH inserted AS (
            INSERT INTO public.creator_sync_jobs (creator_id, status, job_type, source, created_at)
            SELECT pick.creator_id, 'pending', 'sync_stats', 'video_snapshot_refresh',
                   v_now + pick.refresh_rank * interval '1 millisecond'
            FROM pg_temp._video_snapshot_refresh_pick AS pick
            ORDER BY pick.refresh_rank
            ON CONFLICT DO NOTHING
            RETURNING creator_sync_jobs.creator_id
        )
        SELECT pick.creator_id, false
        FROM pg_temp._video_snapshot_refresh_pick AS pick
        JOIN inserted ON inserted.creator_id = pick.creator_id
        ORDER BY pick.refresh_rank;
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION public.enqueue_video_snapshot_refreshes(integer, boolean, integer)
FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.enqueue_video_snapshot_refreshes(integer, boolean, integer)
TO service_role;

COMMENT ON FUNCTION public.enqueue_video_snapshot_refreshes(integer, boolean, integer) IS
    'Dry-run-first weekly snapshot refresh for enrolled healthy creators; no new enrollment or admission-budget changes.';

COMMIT;
