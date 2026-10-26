-- Disposable database only. Existing application tables cause this script to stop.
-- psql "$TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -f db/tests/creator_sync_health.sql
\set ON_ERROR_STOP on
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN CREATE ROLE anon; END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
        CREATE ROLE authenticated;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
        CREATE ROLE service_role;
    END IF;
END $$;
CREATE TABLE public.creators (
    id uuid PRIMARY KEY, current_subscribers bigint, last_synced_at timestamptz,
    first_seen_at timestamp, archived_at timestamptz, sync_status text, recent_upload jsonb
);
CREATE TABLE public.creator_sync_jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    creator_id uuid REFERENCES public.creators(id), status text, job_type text, source text,
    created_at timestamptz DEFAULT now(), started_at timestamptz, completed_at timestamptz,
    retry_at timestamp, next_retry_at timestamptz
);
INSERT INTO public.creators (id, current_subscribers, first_seen_at, sync_status)
SELECT lpad(number::text, 32, '0')::uuid, 20000,
       (now() - interval '20 days') AT TIME ZONE 'UTC', 'synced'
FROM generate_series(1, 10) AS number;
UPDATE public.creators SET current_subscribers = 30000
WHERE id = lpad('2', 32, '0')::uuid;
UPDATE public.creators SET last_synced_at = now() - interval '40 days',
    recent_upload = jsonb_build_object('published_at', now() - interval '50 days')
WHERE id = lpad('3', 32, '0')::uuid;
UPDATE public.creators SET last_synced_at = now() - interval '100 days'
WHERE id IN (lpad('4', 32, '0')::uuid, lpad('5', 32, '0')::uuid);
UPDATE public.creators SET recent_upload = jsonb_build_object('published_at', now() - interval '200 days')
WHERE id = lpad('5', 32, '0')::uuid;
UPDATE public.creators SET sync_status = 'invalid' WHERE id = lpad('6', 32, '0')::uuid;
UPDATE public.creators SET archived_at = now() WHERE id = lpad('7', 32, '0')::uuid;
UPDATE public.creators SET current_subscribers = 9999 WHERE id = lpad('8', 32, '0')::uuid;
INSERT INTO public.creator_sync_jobs (creator_id, status, retry_at)
VALUES (lpad('9', 32, '0')::uuid, 'pending', (now() + interval '1 day') AT TIME ZONE 'UTC');
INSERT INTO public.creator_sync_jobs (creator_id, status, started_at)
VALUES (lpad('10', 32, '0')::uuid, 'processing', now() - interval '1 hour');
\ir ../migrations/070_add_creator_sync_indexes.sql
\ir ../migrations/071_enqueue_creator_syncs.sql
\ir ../migrations/072_creator_sync_health_upgrade.sql
BEGIN;
SET LOCAL TIME ZONE 'Asia/Tokyo';
DO $$
DECLARE total bigint; health record; first_time timestamptz; second_time timestamptz;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.creator_sync_jobs
                   WHERE retry_at AT TIME ZONE 'UTC' = next_retry_at) THEN
        RAISE EXCEPTION 'Legacy retry backfill failed';
    END IF;
    SELECT * INTO health FROM public.get_creator_sync_health_v2();
    IF health.ready_jobs <> 0 OR health.retry_delayed_jobs <> 1 OR health.processing_jobs <> 1
       OR health.coverage_sampled_at IS NOT NULL THEN
        RAISE EXCEPTION 'Initial queue health or missing snapshot incorrect';
    END IF;
    PERFORM public.refresh_creator_sync_coverage();
    SELECT * INTO health FROM public.get_creator_sync_health_v2();
    IF health.eligible_creators_without_job <> 4 OR health.coverage_min_subscribers <> 10000 THEN
        RAISE EXCEPTION 'Coverage does not follow eligibility policy';
    END IF;
    SELECT sum(selected) INTO total FROM public.enqueue_creator_syncs(2, 10000, 3, true);
    IF total <> 2 THEN RAISE EXCEPTION 'Dry run selection incorrect'; END IF;
    SELECT sum(selected) INTO total FROM public.enqueue_creator_syncs(2, 10000, 3, true);
    IF total <> 2 THEN RAISE EXCEPTION 'Repeated dry run failed'; END IF;
    IF (SELECT count(*) FROM public.creator_sync_jobs WHERE status = 'pending') <> 1 THEN
        RAISE EXCEPTION 'Dry run wrote jobs';
    END IF;
    PERFORM public.enqueue_creator_syncs(2, 10000, 3, NULL);
    IF (SELECT count(*) FROM public.creator_sync_jobs WHERE status = 'pending') <> 1 THEN
        RAISE EXCEPTION 'NULL dry run enabled writes';
    END IF;
    SELECT sum(selected) INTO total FROM public.enqueue_creator_syncs(2, 10000, 3, false);
    IF total <> 2 THEN RAISE EXCEPTION 'Real enqueue failed'; END IF;
    SELECT created_at INTO first_time FROM public.creator_sync_jobs
        WHERE creator_id = lpad('2', 32, '0')::uuid;
    SELECT created_at INTO second_time FROM public.creator_sync_jobs
        WHERE creator_id = lpad('1', 32, '0')::uuid;
    IF first_time IS NULL OR second_time IS NULL OR first_time >= second_time THEN
        RAISE EXCEPTION 'Priority ordering changed';
    END IF;
    PERFORM public.enqueue_creator_syncs(100, 10000, 3, false);
    IF (SELECT count(*) FROM public.creator_sync_jobs WHERE status = 'pending') <> 3 THEN
        RAISE EXCEPTION 'Queue cap or duplicate prevention failed';
    END IF;
    PERFORM public.refresh_creator_sync_coverage();
    SELECT * INTO health FROM public.get_creator_sync_health_v2();
    IF health.ready_jobs <> 2 OR health.retry_delayed_jobs <> 1
       OR health.eligible_creators_without_job <> 2 THEN
        RAISE EXCEPTION 'Updated coverage or runnable counts incorrect';
    END IF;
    UPDATE public.creator_sync_jobs SET next_retry_at = now() - interval '1 hour',
        created_at = now() - interval '10 days' WHERE status = 'pending';
    SELECT * INTO health FROM public.get_creator_sync_health_v2();
    IF health.oldest_ready_at <> now() - interval '1 hour' THEN
        RAISE EXCEPTION 'Retry wait incorrectly includes backoff time';
    END IF;
    IF has_function_privilege('anon', 'public.get_creator_sync_health_v2()', 'EXECUTE')
       OR has_function_privilege('authenticated', 'public.refresh_creator_sync_coverage(integer)', 'EXECUTE')
       OR NOT has_function_privilege('service_role', 'public.get_creator_sync_health_v2()', 'EXECUTE') THEN
        RAISE EXCEPTION 'Service-role permissions incorrect';
    END IF;
END $$;
ROLLBACK;