BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '10s';

ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS recent_videos_requested_at timestamptz;

CREATE TABLE IF NOT EXISTS public.video_snapshot_demand_budget (
    admission_day date NOT NULL,
    user_id uuid NOT NULL,
    admissions integer NOT NULL CHECK (admissions BETWEEN 1 AND 10),
    PRIMARY KEY (admission_day, user_id)
);
ALTER TABLE public.video_snapshot_demand_budget ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.video_snapshot_demand_budget FROM PUBLIC, anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.video_snapshot_demand_budget TO service_role;

COMMIT;

-- Run with psql autocommit, not --single-transaction.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_video_snapshot_enrollment
    ON public.creators (recent_videos_requested_at)
    WHERE recent_videos_requested_at IS NOT NULL;

BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '10s';

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
BEGIN
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
    SELECT COALESCE(sum(admissions), 0) INTO v_user_admissions
    FROM public.video_snapshot_demand_budget WHERE admission_day = v_day;
    IF v_user_admissions >= 50 THEN RETURN 'daily_limit'; END IF;
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
    DELETE FROM public.video_snapshot_demand_budget WHERE admission_day <= v_day - 30;
    RETURN 'queued';
END;
$$;
REVOKE ALL ON FUNCTION public.request_creator_video_snapshot(uuid, uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.request_creator_video_snapshot(uuid, uuid) TO service_role;

COMMENT ON COLUMN public.creators.recent_videos_requested_at IS
    'Persisted demand enrollment, honored by all creator workers. At most 1000 creators admitted; no automatic expiry.';
COMMIT;