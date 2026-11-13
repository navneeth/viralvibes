\set ON_ERROR_STOP on

DO $$
BEGIN
    IF to_regclass('public.creators') IS NOT NULL
       OR to_regclass('public.users') IS NOT NULL
       OR to_regclass('public.creator_sync_jobs') IS NOT NULL THEN
        RAISE EXCEPTION 'Use an empty disposable database; application tables already exist';
    END IF;
END;
$$;

CREATE TABLE public.users (id uuid PRIMARY KEY);
CREATE TABLE public.creators (
    id uuid PRIMARY KEY,
    channel_id text,
    archived_at timestamptz,
    current_subscribers bigint NOT NULL DEFAULT 0,
    last_synced_at timestamptz,
    sync_status text,
    recent_upload jsonb
);
CREATE TABLE public.creator_sync_jobs (
    id bigserial PRIMARY KEY,
    creator_id uuid REFERENCES public.creators(id),
    status text NOT NULL,
    job_type text NOT NULL,
    source text,
    created_at timestamptz DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    next_retry_at timestamptz
);
CREATE UNIQUE INDEX idx_creator_sync_jobs_active_unique ON public.creator_sync_jobs (creator_id)
WHERE status IN ('pending', 'processing');

CREATE FUNCTION public.try_parse_creator_upload_timestamp(p_value text)
RETURNS timestamptz
LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    RETURN p_value::timestamptz;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

\ir ../migrations/087_recent_video_snapshot.sql
\ir ../migrations/088_video_snapshot_demand.sql
\ir ../migrations/084_enqueue_unknown_activity_refresh_batch.sql
\ir ../migrations/089_auto_refresh_snapshot_enrollment.sql
\ir ../migrations/094_enqueue_scheduled_video_snapshots.sql

BEGIN;
INSERT INTO public.users SELECT md5('user-' || value)::uuid FROM generate_series(1, 5) value;
INSERT INTO public.creators (id, channel_id)
SELECT md5(value::text)::uuid, 'UC' || lpad(value::text, 22, '0')
FROM generate_series(1, 1001) value;
INSERT INTO public.creators (id, channel_id, current_subscribers, last_synced_at, sync_status)
VALUES
    (md5('2000')::uuid, 'UC' || lpad('2000', 22, '0'), 100000, now() - interval '100 days', 'synced'),
    (md5('2001')::uuid, 'UC' || lpad('2001', 22, '0'), 90000, now() - interval '100 days', 'synced'),
    (md5('2002')::uuid, 'UC' || lpad('2002', 22, '0'), 80000, now() - interval '100 days', 'synced');
INSERT INTO public.creator_sync_jobs (creator_id, status, job_type)
VALUES (md5('2002')::uuid, 'pending', 'sync_stats');

DO $$
DECLARE
    v_user uuid := md5('user-1')::uuid;
    v_result text;
BEGIN
    v_result := public.request_creator_video_snapshot(md5('1')::uuid, v_user);
    IF v_result <> 'queued' THEN RAISE EXCEPTION 'Expected queued, got %', v_result; END IF;
    IF NOT EXISTS (SELECT 1 FROM public.creators WHERE id = md5('1')::uuid AND recent_videos_requested_at IS NOT NULL) THEN
        RAISE EXCEPTION 'Enrollment was not persisted';
    END IF;
    INSERT INTO public.video_snapshot_demand_budget (admission_day, user_id, admissions)
    VALUES (
        (now() AT TIME ZONE 'UTC')::date - 31,
        md5('expired-budget-user')::uuid,
        1
    );
    IF public.request_creator_video_snapshot(md5('1')::uuid, v_user) <> 'cooldown' THEN
        RAISE EXCEPTION 'Creator cooldown was not enforced';
    END IF;
    IF public.request_creator_video_snapshot(md5('2')::uuid, NULL) <> 'unauthorized' THEN
        RAISE EXCEPTION 'Missing actor was admitted';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.video_snapshot_demand_budget
        WHERE user_id = md5('expired-budget-user')::uuid
    ) THEN
        RAISE EXCEPTION 'Expired budget row was not cleaned on a rejected request';
    END IF;
END;
$$;

DO $$
DECLARE
    v_selected uuid;
    v_selected_count bigint;
BEGIN
    SELECT count(*) INTO v_selected_count
    FROM public.enqueue_unknown_activity_refresh_batch(5, true, 1000);
    IF v_selected_count <> 2 THEN
        RAISE EXCEPTION 'Automatic refresh preview should select two eligible creators, got %',
            v_selected_count;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.creator_sync_jobs
        WHERE source = 'unknown_activity_refresh'
    ) OR EXISTS (
        SELECT 1 FROM public.creators
        WHERE id IN (md5('2000')::uuid, md5('2001')::uuid)
          AND recent_videos_requested_at IS NOT NULL
    ) OR EXISTS (
        SELECT 1 FROM public.creators
        WHERE id = md5('2002')::uuid
          AND recent_videos_requested_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'Automatic refresh dry run or active-job filter changed enrollment';
    END IF;

    SELECT creator_id INTO v_selected
    FROM public.enqueue_unknown_activity_refresh_batch(1, false, 1000);
    IF v_selected <> md5('2000')::uuid THEN
        RAISE EXCEPTION 'Automatic refresh did not select the highest-ranked creator';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.creator_sync_jobs
        WHERE creator_id = md5('2000')::uuid
          AND source = 'unknown_activity_refresh'
          AND status = 'pending'
    ) OR NOT EXISTS (
        SELECT 1 FROM public.creators
        WHERE id = md5('2000')::uuid
          AND recent_videos_requested_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'Queued automatic refresh was not atomically enrolled';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.creators
        WHERE id = md5('2001')::uuid
          AND recent_videos_requested_at IS NOT NULL
    ) OR EXISTS (
        SELECT 1 FROM public.creator_sync_jobs
        WHERE creator_id = md5('2001')::uuid
          AND source = 'unknown_activity_refresh'
    ) THEN
        RAISE EXCEPTION 'Unqueued automatic candidate was enrolled';
    END IF;
    IF (SELECT admissions FROM public.video_snapshot_demand_auto_budget
        WHERE admission_day = (now() AT TIME ZONE 'UTC')::date) IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION 'Automatic enrollment was not counted toward the daily limit';
    END IF;
END;
$$;

INSERT INTO public.creator_sync_jobs (creator_id, status, job_type)
VALUES (md5('2')::uuid, 'pending', 'sync_stats'), (md5('3')::uuid, 'processing', 'sync_stats');
UPDATE public.creators SET archived_at = now() WHERE id = md5('4')::uuid;
UPDATE public.creators SET recent_videos = '[]', recent_videos_fetched_at = now() WHERE id = md5('5')::uuid;
UPDATE public.creators SET channel_id = 'invalid' WHERE id = md5('6')::uuid;

DO $$
DECLARE v_user uuid := md5('user-1')::uuid;
BEGIN
    IF public.request_creator_video_snapshot(md5('2')::uuid, v_user) <> 'queued'
       OR (SELECT count(*) FROM public.creator_sync_jobs WHERE creator_id = md5('2')::uuid) <> 1 THEN
        RAISE EXCEPTION 'Pending job was not reused';
    END IF;
    IF public.request_creator_video_snapshot(md5('3')::uuid, v_user) <> 'busy' THEN
        RAISE EXCEPTION 'Processing job was not deferred';
    END IF;
    IF public.request_creator_video_snapshot(md5('4')::uuid, v_user) <> 'not_found' THEN
        RAISE EXCEPTION 'Archived creator admitted';
    END IF;
    IF public.request_creator_video_snapshot(md5('5')::uuid, v_user) <> 'fresh' THEN
        RAISE EXCEPTION 'Fresh empty snapshot refreshed';
    END IF;
    IF public.request_creator_video_snapshot(md5('6')::uuid, v_user) <> 'invalid' THEN
        RAISE EXCEPTION 'Invalid channel admitted';
    END IF;
END;
$$;

UPDATE public.video_snapshot_demand_budget SET admissions = 10 WHERE user_id = md5('user-1')::uuid;
DO $$
BEGIN
    IF public.request_creator_video_snapshot(md5('7')::uuid, md5('user-1')::uuid) <> 'user_limit' THEN
        RAISE EXCEPTION 'User admission cap not enforced';
    END IF;
END;
$$;
INSERT INTO public.video_snapshot_demand_budget
SELECT (now() AT TIME ZONE 'UTC')::date, md5('user-' || value)::uuid, 10
FROM generate_series(2, 5) value;
DO $$
BEGIN
    IF public.request_creator_video_snapshot(md5('7')::uuid, md5('user-2')::uuid) <> 'daily_limit' THEN
        RAISE EXCEPTION 'Global admission cap not enforced';
    END IF;
END;
$$;

DELETE FROM public.video_snapshot_demand_budget;
DELETE FROM public.creator_sync_jobs;
INSERT INTO public.creator_sync_jobs (creator_id, status, job_type)
SELECT md5(value::text)::uuid, 'pending', 'sync_stats' FROM generate_series(100, 349) value;
DO $$
BEGIN
    IF public.request_creator_video_snapshot(md5('7')::uuid, md5('user-1')::uuid) <> 'queue_full' THEN
        RAISE EXCEPTION 'Queue ceiling not enforced';
    END IF;
END;
$$;
DELETE FROM public.creator_sync_jobs;
INSERT INTO public.video_snapshot_demand_budget (admission_day, user_id, admissions)
SELECT (now() AT TIME ZONE 'UTC')::date, md5('user-' || value)::uuid,
       CASE WHEN value = 5 THEN 9 ELSE 10 END
FROM generate_series(1, 5) value;
UPDATE public.creators SET last_synced_at = now()
WHERE id = md5('2000')::uuid;
UPDATE public.creators SET last_synced_at = now() - interval '100 days'
WHERE id = md5('2001')::uuid;
DO $$
BEGIN
    IF (SELECT count(*) FROM public.enqueue_unknown_activity_refresh_batch(5, false, 1000)) <> 0 THEN
        RAISE EXCEPTION 'Automatic refresh ignored the shared daily admission cap';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.creators
        WHERE id = md5('2001')::uuid
          AND recent_videos_requested_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'Daily-limited automatic candidate was enrolled';
    END IF;
    IF public.request_creator_video_snapshot(md5('7')::uuid, md5('user-1')::uuid) <> 'daily_limit' THEN
        RAISE EXCEPTION 'Profile admissions did not count automatic enrollments';
    END IF;
END;
$$;
DELETE FROM public.video_snapshot_demand_budget;
UPDATE public.creators SET recent_videos_requested_at = now() - interval '2 days'
WHERE id IN (SELECT md5(value::text)::uuid FROM generate_series(1, 999) value);
UPDATE public.creators SET recent_videos_requested_at = now() - interval '2 days'
WHERE id = md5('2000')::uuid;
UPDATE public.creators
SET current_subscribers = 50000, last_synced_at = now() - interval '100 days'
WHERE id = md5('1001')::uuid;
DO $$
BEGIN
    IF public.request_creator_video_snapshot(md5('1001')::uuid, md5('user-1')::uuid) <> 'cohort_full' THEN
        RAISE EXCEPTION 'Enrollment cap not enforced';
    END IF;
    IF (SELECT count(*) FROM public.enqueue_unknown_activity_refresh_batch(5, false, 1000)) <> 0 THEN
        RAISE EXCEPTION 'Automatic refresh exceeded the enrolled cohort cap';
    END IF;
    IF has_function_privilege('anon', 'public.request_creator_video_snapshot(uuid,uuid)', 'EXECUTE')
       OR has_function_privilege('authenticated', 'public.request_creator_video_snapshot(uuid,uuid)', 'EXECUTE') THEN
        RAISE EXCEPTION 'Client roles can execute admission RPC';
    END IF;
    IF has_function_privilege('anon', 'public.enqueue_unknown_activity_refresh_batch(integer,boolean,integer)', 'EXECUTE')
       OR has_function_privilege('authenticated', 'public.enqueue_unknown_activity_refresh_batch(integer,boolean,integer)', 'EXECUTE') THEN
        RAISE EXCEPTION 'Client roles can execute automatic refresh RPC';
    END IF;
    IF public.creator_video_snapshot_is_enrolled(md5('1')::uuid) IS NOT TRUE THEN
        RAISE EXCEPTION 'Enrollment fallback did not identify enrolled creator';
    END IF;
    IF has_function_privilege('anon', 'public.creator_video_snapshot_is_enrolled(uuid)', 'EXECUTE')
       OR has_function_privilege('authenticated', 'public.creator_video_snapshot_is_enrolled(uuid)', 'EXECUTE') THEN
        RAISE EXCEPTION 'Client roles can execute enrollment fallback RPC';
    END IF;
END;
$$;
ROLLBACK;

BEGIN;
INSERT INTO public.creators (
    id, channel_id, sync_status, recent_videos_requested_at,
    recent_videos, recent_videos_fetched_at
)
SELECT md5('snapshot-' || value)::uuid, 'UC' || lpad(value::text, 22, '0'),
       'synced', now() - interval '8 days', '[]'::jsonb, now() - interval '8 days'
FROM generate_series(1, 13) value;
UPDATE public.creators SET recent_videos = NULL, recent_videos_fetched_at = now()
WHERE id = md5('snapshot-2')::uuid;
UPDATE public.creators SET recent_videos_fetched_at = NULL WHERE id = md5('snapshot-3')::uuid;
UPDATE public.creators SET recent_videos_fetched_at = now() WHERE id = md5('snapshot-4')::uuid;
UPDATE public.creators SET recent_videos_requested_at = NULL WHERE id = md5('snapshot-5')::uuid;
UPDATE public.creators SET archived_at = now() WHERE id = md5('snapshot-6')::uuid;
UPDATE public.creators SET channel_id = 'invalid' WHERE id = md5('snapshot-7')::uuid;
UPDATE public.creators SET sync_status = 'invalid_channel' WHERE id = md5('snapshot-8')::uuid;
UPDATE public.creators SET sync_status = 'synced_partial' WHERE id = md5('snapshot-13')::uuid;
INSERT INTO public.creator_sync_jobs (creator_id, status, job_type, next_retry_at)
VALUES
    (md5('snapshot-9')::uuid, 'pending', 'sync_stats', now() + interval '1 day'),
    (md5('snapshot-10')::uuid, 'processing', 'sync_stats', NULL);
INSERT INTO public.creator_sync_jobs (creator_id, status, job_type, source, completed_at)
VALUES
    (md5('snapshot-11')::uuid, 'failed', 'sync_stats', 'video_snapshot_refresh', now()),
    (md5('snapshot-12')::uuid, 'failed', 'sync_stats', 'video_snapshot_refresh', now() - interval '8 days');
INSERT INTO public.video_snapshot_demand_budget (admission_day, user_id, admissions)
VALUES ((now() AT TIME ZONE 'UTC')::date, md5('snapshot-user')::uuid, 10);
INSERT INTO public.video_snapshot_demand_auto_budget (admission_day, admissions)
VALUES ((now() AT TIME ZONE 'UTC')::date, 40);
CREATE TEMP TABLE snapshot_enrollment_before ON COMMIT DROP AS
SELECT id, recent_videos_requested_at FROM public.creators;

DO $$
DECLARE
    v_count bigint;
BEGIN
    SELECT count(*) INTO v_count FROM public.enqueue_video_snapshot_refreshes();
    IF v_count <> 5 THEN RAISE EXCEPTION 'Expected five due enrolled creators, got %', v_count; END IF;
    IF (SELECT count(*) FROM public.enqueue_video_snapshot_refreshes(25, NULL)) <> 5
       OR EXISTS (SELECT 1 FROM public.enqueue_video_snapshot_refreshes() WHERE NOT dry_run)
       OR EXISTS (SELECT 1 FROM public.creator_sync_jobs WHERE source = 'video_snapshot_refresh' AND status = 'pending') THEN
        RAISE EXCEPTION 'Repeated or NULL dry-run inserted jobs';
    END IF;
    IF (SELECT count(*) FROM public.enqueue_video_snapshot_refreshes(25, true, 1)) <> 0
       OR (SELECT count(*) FROM public.enqueue_video_snapshot_refreshes(25, true, 3)) <> 2 THEN
        RAISE EXCEPTION 'Pending queue budget was not respected';
    END IF;
    SELECT count(*) INTO v_count FROM public.enqueue_video_snapshot_refreshes(2, false);
    IF v_count <> 2 THEN RAISE EXCEPTION 'Batch limit failed'; END IF;
    SELECT count(*) INTO v_count FROM public.enqueue_video_snapshot_refreshes(25, false);
    IF v_count <> 3 THEN RAISE EXCEPTION 'Remaining eligible batch failed'; END IF;
    SELECT count(*) INTO v_count FROM public.enqueue_video_snapshot_refreshes(25, false);
    IF v_count <> 0 THEN RAISE EXCEPTION 'Active-job deduplication failed'; END IF;
    IF EXISTS (
        SELECT 1 FROM public.creator_sync_jobs
        WHERE source = 'video_snapshot_refresh' AND status = 'pending'
          AND (job_type <> 'sync_stats' OR creator_id NOT IN (
              md5('snapshot-1')::uuid, md5('snapshot-2')::uuid, md5('snapshot-3')::uuid,
              md5('snapshot-12')::uuid, md5('snapshot-13')::uuid
          ))
    ) THEN
        RAISE EXCEPTION 'Refresh selected a fresh, unenrolled, unhealthy, active, or cooled-down creator';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.creators AS creator JOIN snapshot_enrollment_before AS before USING (id)
        WHERE creator.recent_videos_requested_at IS DISTINCT FROM before.recent_videos_requested_at
    ) OR (SELECT sum(admissions) FROM public.video_snapshot_demand_budget) <> 10
      OR (SELECT sum(admissions) FROM public.video_snapshot_demand_auto_budget) <> 40 THEN
        RAISE EXCEPTION 'Recurring refresh changed enrollment or admission budgets';
    END IF;
    IF has_function_privilege('anon', 'public.enqueue_video_snapshot_refreshes(integer,boolean,integer)', 'EXECUTE')
       OR has_function_privilege('authenticated', 'public.enqueue_video_snapshot_refreshes(integer,boolean,integer)', 'EXECUTE')
       OR NOT has_function_privilege('service_role', 'public.enqueue_video_snapshot_refreshes(integer,boolean,integer)', 'EXECUTE') THEN
        RAISE EXCEPTION 'Scheduled refresh grants are incorrect';
    END IF;
END;
$$;

DELETE FROM public.creator_sync_jobs WHERE source = 'video_snapshot_refresh' AND status = 'pending';
INSERT INTO public.creators (id, channel_id)
SELECT md5('snapshot-' || value)::uuid, 'UC' || lpad(value::text, 22, '0')
FROM generate_series(100, 348) value;
INSERT INTO public.creator_sync_jobs (creator_id, status, job_type)
SELECT md5('snapshot-' || value)::uuid, 'pending', 'sync_stats'
FROM generate_series(100, 348) value;
DO $$
BEGIN
    IF (SELECT count(*) FROM public.enqueue_video_snapshot_refreshes(25, false, 1000)) <> 0 THEN
        RAISE EXCEPTION 'Caller bypassed the hard 250-pending queue cap';
    END IF;
END;
$$;
ROLLBACK;
