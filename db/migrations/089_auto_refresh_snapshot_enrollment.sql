BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '10s';

CREATE TABLE IF NOT EXISTS public.video_snapshot_demand_auto_budget (
    admission_day date PRIMARY KEY,
    admissions integer NOT NULL CHECK (admissions BETWEEN 1 AND 50)
);
ALTER TABLE public.video_snapshot_demand_auto_budget ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.video_snapshot_demand_auto_budget FROM PUBLIC, anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.video_snapshot_demand_auto_budget TO service_role;

CREATE OR REPLACE FUNCTION public.request_creator_video_snapshot(p_creator_id uuid, p_user_id uuid)
RETURNS text
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_creator public.creators%ROWTYPE;
    v_job_id bigint;
    v_job_status text;
    v_now timestamptz := statement_timestamp();
    v_day date := (statement_timestamp() AT TIME ZONE 'UTC')::date;
    v_user_admissions integer;
    v_global_admissions integer;
BEGIN
    DELETE FROM public.video_snapshot_demand_budget WHERE admission_day <= v_day - 30;
    DELETE FROM public.video_snapshot_demand_auto_budget WHERE admission_day <= v_day - 30;

    IF p_user_id IS NULL OR NOT EXISTS (SELECT 1 FROM public.users WHERE id = p_user_id) THEN
        RETURN 'unauthorized';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_index
        WHERE indexrelid = to_regclass('public.idx_creator_sync_jobs_active_unique')
          AND indisvalid AND indisready AND indisunique
    ) THEN
        RETURN 'unavailable';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_index
        WHERE indexrelid = to_regclass('public.idx_creators_video_snapshot_enrollment')
          AND indisvalid AND indisready
    ) THEN RETURN 'unavailable'; END IF;
    IF NOT pg_try_advisory_xact_lock(hashtextextended('public.enqueue_creator_syncs', 0)) THEN
        RETURN 'busy';
    END IF;
    SELECT * INTO v_creator FROM public.creators WHERE id = p_creator_id FOR UPDATE;
    IF NOT FOUND OR v_creator.archived_at IS NOT NULL THEN RETURN 'not_found'; END IF;
    IF v_creator.channel_id IS NULL OR v_creator.channel_id !~ '^UC[A-Za-z0-9_-]{22}$' THEN
        RETURN 'invalid';
    END IF;
    IF v_creator.recent_videos IS NOT NULL
       AND v_creator.recent_videos_fetched_at <= v_now
       AND v_creator.recent_videos_fetched_at > v_now - interval '7 days' THEN
        RETURN 'fresh';
    END IF;
    IF v_creator.recent_videos_requested_at > v_now - interval '24 hours' THEN
        RETURN 'cooldown';
    END IF;
    SELECT id, status INTO v_job_id, v_job_status
    FROM public.creator_sync_jobs
    WHERE creator_id = p_creator_id AND status IN ('pending', 'processing')
    FOR UPDATE;
    IF v_job_status = 'processing' THEN RETURN 'busy'; END IF;
    IF v_creator.recent_videos_requested_at IS NULL
       AND (SELECT count(*) FROM (
           SELECT 1 FROM public.creators WHERE recent_videos_requested_at IS NOT NULL LIMIT 1000
       ) AS enrolled) >= 1000 THEN
        RETURN 'cohort_full';
    END IF;
    IF v_job_id IS NULL AND (SELECT count(*) FROM (
        SELECT 1 FROM public.creator_sync_jobs WHERE status = 'pending' LIMIT 250
    ) AS pending) >= 250 THEN
        RETURN 'queue_full';
    END IF;
    SELECT (
        SELECT COALESCE(sum(admissions), 0)
        FROM public.video_snapshot_demand_budget
        WHERE admission_day = v_day
    ) + COALESCE((
        SELECT admissions
        FROM public.video_snapshot_demand_auto_budget
        WHERE admission_day = v_day
    ), 0)
    INTO v_global_admissions;
    IF v_global_admissions >= 50 THEN RETURN 'daily_limit'; END IF;
    SELECT admissions INTO v_user_admissions
    FROM public.video_snapshot_demand_budget WHERE admission_day = v_day AND user_id = p_user_id;
    IF COALESCE(v_user_admissions, 0) >= 10 THEN RETURN 'user_limit'; END IF;

    UPDATE public.creators SET recent_videos_requested_at = v_now WHERE id = p_creator_id;
    IF v_job_id IS NULL THEN
        INSERT INTO public.creator_sync_jobs (creator_id, status, job_type, source, created_at)
        VALUES (p_creator_id, 'pending', 'sync_stats', 'video_snapshot_demand', v_now)
        ON CONFLICT DO NOTHING
        RETURNING id INTO v_job_id;
        IF v_job_id IS NULL THEN
            RAISE EXCEPTION 'Video snapshot job admission conflicted';
        END IF;
    END IF;
    INSERT INTO public.video_snapshot_demand_budget (admission_day, user_id, admissions)
    VALUES (v_day, p_user_id, 1)
    ON CONFLICT (admission_day, user_id) DO UPDATE
        SET admissions = public.video_snapshot_demand_budget.admissions + 1;
    RETURN 'queued';
END;
$$;
REVOKE ALL ON FUNCTION public.request_creator_video_snapshot(uuid, uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.request_creator_video_snapshot(uuid, uuid) TO service_role;

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
    v_queue_budget bigint;
    v_global_admissions bigint;
    v_enrolled_creators bigint;
    v_new_enrollment_budget bigint;
    v_day date := (statement_timestamp() AT TIME ZONE 'UTC')::date;
    v_now timestamptz := statement_timestamp();
BEGIN
    IF COALESCE(p_limit, 0) > 5 THEN
        RAISE EXCEPTION 'Unknown-activity refresh batch accepts at most five jobs';
    END IF;
    IF COALESCE(p_limit, 0) <= 0 THEN
        RETURN;
    END IF;

    IF p_dry_run IS FALSE THEN
        DELETE FROM public.video_snapshot_demand_budget WHERE admission_day <= v_day - 30;
        DELETE FROM public.video_snapshot_demand_auto_budget WHERE admission_day <= v_day - 30;
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

    v_queue_budget := GREATEST(
        LEAST(
            LEAST(COALESCE(p_limit, 0), 5)::bigint,
            LEAST(COALESCE(p_max_queue, 0), 1000)::bigint - v_pending
        ),
        0
    );
    SELECT (
        SELECT COALESCE(sum(admissions), 0)
        FROM public.video_snapshot_demand_budget
        WHERE admission_day = v_day
    ) + COALESCE((
        SELECT admissions
        FROM public.video_snapshot_demand_auto_budget
        WHERE admission_day = v_day
    ), 0)
    INTO v_global_admissions;
    SELECT count(*) INTO v_enrolled_creators
    FROM public.creators
    WHERE recent_videos_requested_at IS NOT NULL;
    v_new_enrollment_budget := GREATEST(
        LEAST(1000 - v_enrolled_creators, 50 - v_global_admissions),
        0
    );

    DROP TABLE IF EXISTS pg_temp._unknown_activity_auto_pick;
    CREATE TEMP TABLE _unknown_activity_auto_pick ON COMMIT DROP AS
    WITH candidates AS (
        SELECT creator.id AS creator_id,
               creator.current_subscribers::bigint AS current_subscribers,
               creator.recent_videos_requested_at IS NOT NULL AS enrolled,
               row_number() OVER (
                   ORDER BY creator.current_subscribers DESC NULLS LAST, creator.id
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
          AND creator.last_synced_at < v_now - interval '90 days'
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
                    ) >= v_now - interval '7 days'
          )
        ORDER BY creator.current_subscribers DESC NULLS LAST, creator.id
        LIMIT v_queue_budget
    ),
    ranked AS (
        SELECT candidate.*,
               count(*) FILTER (WHERE NOT candidate.enrolled) OVER (
                   ORDER BY candidate.refresh_rank
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS new_enrollment_rank
        FROM candidates AS candidate
    )
    SELECT creator_id, current_subscribers, enrolled, refresh_rank
    FROM ranked
    WHERE enrolled OR new_enrollment_rank <= v_new_enrollment_budget;

    IF p_dry_run IS DISTINCT FROM false THEN
        RETURN QUERY
        SELECT pick.creator_id, pick.current_subscribers, true
        FROM pg_temp._unknown_activity_auto_pick AS pick
        ORDER BY pick.current_subscribers DESC NULLS LAST, pick.creator_id;
    ELSE
        RETURN QUERY
        WITH inserted AS (
            INSERT INTO public.creator_sync_jobs
                (creator_id, status, job_type, source, created_at)
            SELECT pick.creator_id, 'pending', 'sync_stats',
                   'unknown_activity_refresh',
                   v_now + pick.refresh_rank * interval '1 millisecond'
            FROM pg_temp._unknown_activity_auto_pick AS pick
            ON CONFLICT DO NOTHING
            RETURNING creator_id
        ),
        enrolled AS (
            UPDATE public.creators AS creator
            SET recent_videos_requested_at = v_now
            FROM inserted
            WHERE creator.id = inserted.creator_id
              AND creator.recent_videos_requested_at IS NULL
            RETURNING creator.id
        ),
        budget AS (
            INSERT INTO public.video_snapshot_demand_auto_budget (admission_day, admissions)
            SELECT v_day, count(*)::integer
            FROM enrolled
            HAVING count(*) > 0
            ON CONFLICT (admission_day) DO UPDATE
                SET admissions = public.video_snapshot_demand_auto_budget.admissions
                                 + EXCLUDED.admissions
            RETURNING admission_day
        )
        SELECT pick.creator_id, pick.current_subscribers, false
        FROM pg_temp._unknown_activity_auto_pick AS pick
        JOIN inserted ON inserted.creator_id = pick.creator_id
        CROSS JOIN (SELECT count(*) FROM budget) AS budget_write
        ORDER BY pick.current_subscribers DESC NULLS LAST, pick.creator_id;
    END IF;
END;
$$;
REVOKE ALL ON FUNCTION public.enqueue_unknown_activity_refresh_batch(integer, boolean, integer)
FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.enqueue_unknown_activity_refresh_batch(integer, boolean, integer)
TO service_role;

COMMENT ON TABLE public.video_snapshot_demand_auto_budget IS
    'Daily automated snapshot enrollments, counted with profile admissions toward the shared 50/day limit.';
COMMIT;
