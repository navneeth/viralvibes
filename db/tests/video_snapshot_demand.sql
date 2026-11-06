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
    archived_at timestamptz
);
CREATE TABLE public.creator_sync_jobs (
    id bigserial PRIMARY KEY,
    creator_id uuid REFERENCES public.creators(id),
    status text NOT NULL,
    job_type text NOT NULL,
    source text,
    created_at timestamptz DEFAULT now()
);
CREATE UNIQUE INDEX idx_creator_sync_jobs_active_unique ON public.creator_sync_jobs (creator_id)
WHERE status IN ('pending', 'processing');

\ir ../migrations/087_recent_video_snapshot.sql
\ir ../migrations/088_video_snapshot_demand.sql

BEGIN;
INSERT INTO public.users SELECT md5('user-' || value)::uuid FROM generate_series(1, 5) value;
INSERT INTO public.creators (id, channel_id)
SELECT md5(value::text)::uuid, 'UC' || lpad(value::text, 22, '0')
FROM generate_series(1, 1001) value;

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
UPDATE public.creators SET recent_videos_requested_at = now() - interval '2 days'
WHERE id IN (SELECT md5(value::text)::uuid FROM generate_series(1, 1000) value);
DO $$
BEGIN
    IF public.request_creator_video_snapshot(md5('1001')::uuid, md5('user-1')::uuid) <> 'cohort_full' THEN
        RAISE EXCEPTION 'Enrollment cap not enforced';
    END IF;
    IF has_function_privilege('anon', 'public.request_creator_video_snapshot(uuid,uuid)', 'EXECUTE')
       OR has_function_privilege('authenticated', 'public.request_creator_video_snapshot(uuid,uuid)', 'EXECUTE') THEN
        RAISE EXCEPTION 'Client roles can execute admission RPC';
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
