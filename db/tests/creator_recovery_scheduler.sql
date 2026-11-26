\set ON_ERROR_STOP on
BEGIN;
DO $$ BEGIN
    IF to_regclass('public.creators') IS NOT NULL THEN
        RAISE EXCEPTION 'Disposable empty database required';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN CREATE ROLE anon; END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN CREATE ROLE authenticated; END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN CREATE ROLE service_role; END IF;
END $$;
CREATE TABLE public.creators (
    id uuid PRIMARY KEY, channel_id text, current_subscribers bigint,
    channel_name text DEFAULT 'Creator', country_code text DEFAULT 'GB',
    sync_status text, last_synced_at timestamptz, first_seen_at timestamp,
    archived_at timestamptz
);
CREATE TABLE public.creator_sync_jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    creator_id uuid REFERENCES public.creators(id), status text, source text, job_type text,
    created_at timestamptz DEFAULT now(), started_at timestamptz, completed_at timestamptz,
    retry_count integer DEFAULT 0
);
CREATE UNIQUE INDEX recovery_active_unique ON public.creator_sync_jobs(creator_id)
WHERE status IN ('pending', 'processing');
CREATE TABLE public.creator_sync_coverage_snapshot (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), sampled_at timestamptz,
    min_subscribers integer, eligible_creators_without_job bigint,
    oldest_eligible_since timestamptz
);
COMMIT;
\ir ../migrations/097_creator_recovery_scheduler.sql
\ir ../migrations/098_country_listing_eligibility.sql
\ir ../migrations/099_country_listing_order_index.sql
\ir ../migrations/100_creator_tiered_sync_cadence.sql
BEGIN;
INSERT INTO public.creators(id, channel_id, current_subscribers, sync_status, first_seen_at, last_synced_at)
SELECT lpad(number::text, 32, '0')::uuid, 'UC' || lpad(number::text, 22, '0'),
       100000000 - number * 1000, 'pending', now() - interval '200 days', now() - interval '150 days'
FROM generate_series(1, 13) number;
UPDATE public.creators SET last_synced_at = NULL WHERE id = lpad('2', 32, '0')::uuid;
UPDATE public.creators SET sync_status = 'synced_partial' WHERE id = lpad('3', 32, '0')::uuid;
UPDATE public.creators SET sync_status = 'synced' WHERE id IN (lpad('4', 32, '0')::uuid, lpad('5', 32, '0')::uuid);
UPDATE public.creators SET first_seen_at = now() - interval '400 days',
    last_synced_at = now() - interval '390 days', current_subscribers = 10000
WHERE id = lpad('12', 32, '0')::uuid;
INSERT INTO public.creator_sync_jobs(creator_id, status)
VALUES (lpad('1', 32, '0')::uuid, 'processing');
UPDATE public.creators SET archived_at = now() WHERE id = lpad('6', 32, '0')::uuid;
UPDATE public.creators SET sync_status = 'invalid' WHERE id = lpad('7', 32, '0')::uuid;
UPDATE public.creators SET channel_id = 'bad-id' WHERE id = lpad('8', 32, '0')::uuid;
UPDATE public.creators SET sync_status = 'synced_partial', sync_completion_attempts = 3
WHERE id = lpad('9', 32, '0')::uuid;
UPDATE public.creators SET last_synced_at = now() WHERE id = lpad('10', 32, '0')::uuid;
INSERT INTO public.creator_sync_jobs(creator_id, status, source, job_type, retry_count, completed_at)
VALUES (lpad('11', 32, '0')::uuid, 'failed', 'priority_enqueue', 'sync_stats', 3, now());
INSERT INTO public.creator_sync_jobs(creator_id, status, source, job_type, retry_count, completed_at)
VALUES (lpad('13', 32, '0')::uuid, 'failed', 'video_snapshot_refresh', 'sync_stats', 3, now());
DO $$
DECLARE total bigint; jobs_before bigint; health record;
BEGIN
    IF (SELECT count(*) FROM public.creator_recovery_candidates()) <> 6 THEN
        RAISE EXCEPTION 'Candidate filtering failed';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.creator_recovery_candidates()
        WHERE creator_id = lpad('13', 32, '0')::uuid
    ) THEN
        RAISE EXCEPTION 'Optional snapshot failure incorrectly blocked creator recovery';
    END IF;
    INSERT INTO public.creators(id, channel_id, current_subscribers, sync_status, first_seen_at, last_synced_at)
    VALUES
        (lpad('14', 32, '0')::uuid, 'UC' || lpad('14', 22, '0'), 5000,
         'synced', now() - interval '700 days', now() - interval '400 days'),
        (lpad('15', 32, '0')::uuid, 'UC' || lpad('15', 22, '0'), 5000,
         'synced', now() - interval '700 days', now() - interval '600 days'),
        (lpad('16', 32, '0')::uuid, 'UC' || lpad('16', 22, '0'), 50000,
         'synced', now() - interval '300 days', now() - interval '29 days'),
        (lpad('17', 32, '0')::uuid, 'UC' || lpad('17', 22, '0'), 50000,
         'synced', now() - interval '300 days', now() - interval '31 days'),
        (lpad('18', 32, '0')::uuid, 'UC' || lpad('18', 22, '0'), 1000000,
         'synced', now() - interval '100 days', now() - interval '29 days'),
        (lpad('19', 32, '0')::uuid, 'UC' || lpad('19', 22, '0'), 1000000,
         'synced', now() - interval '100 days', now() - interval '31 days'),
        (lpad('20', 32, '0')::uuid, 'UC' || lpad('20', 22, '0'), 10000000,
         'synced', now() - interval '30 days', now() - interval '13 days'),
        (lpad('21', 32, '0')::uuid, 'UC' || lpad('21', 22, '0'), 10000000,
         'synced', now() - interval '30 days', now() - interval '15 days');
    IF EXISTS (SELECT 1 FROM public.creator_recovery_candidates(0)
               WHERE creator_id = lpad('14', 32, '0')::uuid)
       OR NOT EXISTS (SELECT 1 FROM public.creator_recovery_candidates(0)
                      WHERE creator_id = lpad('15', 32, '0')::uuid)
       OR EXISTS (SELECT 1 FROM public.creator_recovery_candidates(0)
                  WHERE creator_id IN (lpad('16', 32, '0')::uuid, lpad('18', 32, '0')::uuid,
                                       lpad('20', 32, '0')::uuid))
       OR (SELECT count(*) FROM public.creator_recovery_candidates(0)
           WHERE creator_id IN (lpad('17', 32, '0')::uuid, lpad('19', 32, '0')::uuid,
                                lpad('21', 32, '0')::uuid)) <> 3 THEN
        RAISE EXCEPTION 'Subscriber-tier refresh intervals were not enforced';
    END IF;
    PERFORM public.refresh_creator_sync_coverage(10000);
    IF (SELECT eligible_creators_without_job FROM public.creator_sync_coverage_snapshot) <> 9
       OR EXISTS (SELECT 1 FROM public.creator_recovery_candidates(10000)
                  WHERE creator_id = lpad('15', 32, '0')::uuid) THEN
        RAISE EXCEPTION 'Coverage does not follow the active scheduler policy';
    END IF;
    PERFORM public.refresh_creator_sync_coverage(0);
    IF (SELECT eligible_creators_without_job FROM public.creator_sync_coverage_snapshot) <> 10
       OR (SELECT min_subscribers FROM public.creator_sync_coverage_snapshot) <> 0 THEN
        RAISE EXCEPTION 'All-creator coverage omitted eligible sub-10K creators';
    END IF;
    SELECT count(*) INTO jobs_before FROM public.creator_sync_jobs;
    SELECT sum(selected) INTO total FROM public.enqueue_creator_syncs(4, 10000, 1000, true);
    IF total <> 4 THEN
        RAISE EXCEPTION 'Dry run picked the wrong count: expected 4, got %', total;
    END IF;
    IF (SELECT count(*) FROM public.creator_sync_jobs) <> jobs_before THEN
        RAISE EXCEPTION 'Dry run modified creator_sync_jobs';
    END IF;
    PERFORM public.enqueue_creator_syncs(4, 10000, 1000, NULL);
    IF (SELECT count(*) FROM public.creator_sync_jobs) <> jobs_before THEN
        RAISE EXCEPTION 'NULL preview modified creator_sync_jobs';
    END IF;
    PERFORM public.enqueue_creator_syncs(4, 10000, 1000, false);
    IF NOT EXISTS (SELECT 1 FROM public.creator_sync_jobs WHERE creator_id = lpad('2', 32, '0')::uuid)
       OR NOT EXISTS (SELECT 1 FROM public.creator_sync_jobs WHERE creator_id = lpad('3', 32, '0')::uuid)
         OR NOT EXISTS (SELECT 1 FROM public.creator_sync_jobs WHERE creator_id = lpad('17', 32, '0')::uuid)
       OR NOT EXISTS (SELECT 1 FROM public.creator_sync_jobs WHERE creator_id = lpad('12', 32, '0')::uuid) THEN
        RAISE EXCEPTION 'Recovery, high-impact refresh, or oldest-age allocation failed';
    END IF;
    PERFORM public.enqueue_creator_syncs(700, 10000, 5, false);
    IF (SELECT count(*) FROM public.creator_sync_jobs WHERE status IN ('pending', 'processing')) <> 5 THEN
        RAISE EXCEPTION 'Active queue cap failed';
    END IF;
    UPDATE public.creators SET last_synced_at = now() WHERE id = lpad('3', 32, '0')::uuid;
    UPDATE public.creators SET last_synced_at = now() + interval '1 second' WHERE id = lpad('3', 32, '0')::uuid;
    UPDATE public.creators SET last_synced_at = now() + interval '2 seconds' WHERE id = lpad('3', 32, '0')::uuid;
    IF (SELECT sync_completion_attempts FROM public.creators WHERE id = lpad('3', 32, '0')::uuid) <> 3 THEN
        RAISE EXCEPTION 'Partial attempt tracking failed';
    END IF;
    SELECT * INTO health FROM public.get_creator_recovery_health();
    IF health.exhausted_partial <> 2 OR health.overdue_incomplete = 0 THEN
        RAISE EXCEPTION 'Incomplete health did not retain original age or exhausted attempts';
    END IF;
    UPDATE public.creators SET sync_status = 'synced' WHERE id = lpad('3', 32, '0')::uuid;
    IF EXISTS (SELECT 1 FROM public.creators WHERE id = lpad('3', 32, '0')::uuid
               AND (sync_completion_attempts <> 0 OR sync_incomplete_since IS NOT NULL)) THEN
        RAISE EXCEPTION 'Successful completion did not clear recovery state';
    END IF;
    IF has_function_privilege('anon', 'public.get_creator_recovery_health(integer)', 'EXECUTE')
       OR has_function_privilege('authenticated', 'public.enqueue_creator_syncs(integer,integer,integer,boolean)', 'EXECUTE') THEN
        RAISE EXCEPTION 'Recovery functions must remain service-role only';
    END IF;
END $$;
ROLLBACK;
BEGIN;
INSERT INTO public.creators(id, channel_id, current_subscribers, sync_status, first_seen_at, last_synced_at)
VALUES
    (lpad('22', 32, '0')::uuid, 'UC' || lpad('22', 22, '0'), NULL, 'synced', now() - interval '1000 days', now() - interval '600 days'),
    (lpad('23', 32, '0')::uuid, 'UC' || lpad('23', 22, '0'), 1000000, 'synced', now() - interval '100 days', now() - interval '46 days'),
    (lpad('24', 32, '0')::uuid, 'UC' || lpad('24', 22, '0'), 50000, 'synced', now() - interval '300 days', now() - interval '181 days'),
    (lpad('25', 32, '0')::uuid, 'UC' || lpad('25', 22, '0'), 10000000, 'synced', now() - interval '1100 days', now() - interval '1000 days'),
    (lpad('26', 32, '0')::uuid, 'UC' || lpad('26', 22, '0'), 100, 'pending', now() - interval '100 days', now() - interval '90 days'),
    (lpad('28', 32, '0')::uuid, 'UC' || lpad('28', 22, '0'), 10000000, 'synced', now() - interval '1100 days', now() - interval '800 days');
DO $$
BEGIN
    PERFORM public.enqueue_creator_syncs(5, 0, 1000, false);
    IF NOT EXISTS (SELECT 1 FROM public.creator_sync_jobs WHERE creator_id = lpad('22', 32, '0')::uuid) THEN
        RAISE EXCEPTION 'Unknown subscriber count was excluded from the under-10K reservation';
    END IF;
END $$;
ROLLBACK;
BEGIN;
INSERT INTO public.creators(id, channel_id, current_subscribers, sync_status, first_seen_at, last_synced_at)
VALUES
    (lpad('30', 32, '0')::uuid, 'UC' || lpad('30', 22, '0'), 10000, 'synced', now() - interval '100 days', now() - interval '29 days'),
    (lpad('31', 32, '0')::uuid, 'UC' || lpad('31', 22, '0'), 10000, 'synced', now() - interval '100 days', now() - interval '31 days');
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.creator_recovery_candidates(0) WHERE creator_id = lpad('30', 32, '0')::uuid)
       OR NOT EXISTS (SELECT 1 FROM public.creator_recovery_candidates(0) WHERE creator_id = lpad('31', 32, '0')::uuid) THEN
        RAISE EXCEPTION '10K creators must use the 30-day refresh interval';
    END IF;
END $$;
ROLLBACK;
BEGIN;
INSERT INTO public.creators(id, current_subscribers, sync_status, archived_at, country_code)
VALUES
    (lpad('1', 32, '0')::uuid, 100, 'synced', NULL, 'GB'),
    (lpad('2', 32, '0')::uuid, 100, 'synced_partial', NULL, 'GB'),
    (lpad('3', 32, '0')::uuid, 200, 'pending', NULL, 'GB'),
    (lpad('4', 32, '0')::uuid, 300, 'synced', now(), 'GB'),
    (lpad('5', 32, '0')::uuid, 400, 'synced', NULL, NULL);
DO $$
DECLARE
    oversized_rejected boolean := false;
BEGIN
    IF NOT (
        SELECT proc.proretset AND proc.prorettype = 'public.creators'::regtype
        FROM pg_proc AS proc
        WHERE proc.oid = 'public.get_country_preview_creators(text[],integer)'::regprocedure
    ) THEN
        RAISE EXCEPTION 'Country preview RPC return type changed incompatibly';
    END IF;
    IF (SELECT count(*) FROM public.country_list_creators) <> 2
       OR (SELECT count(*) FROM public.get_country_preview_creators(ARRAY['GB'], 5)) <> 2
       OR (SELECT creator_count FROM public.get_top_countries_with_counts(10) WHERE country_code = 'GB') <> 2 THEN
        RAISE EXCEPTION 'Country lists, previews, and counts disagree';
    END IF;
    BEGIN
        PERFORM public.get_country_preview_creators(
            ARRAY(SELECT lpad(n::text, 2, 'A') FROM generate_series(1, 101) n), 5
        );
    EXCEPTION WHEN OTHERS THEN
        oversized_rejected := true;
    END;
    IF NOT oversized_rejected THEN
        RAISE EXCEPTION 'Country preview function accepted an oversized request';
    END IF;
    IF NOT has_function_privilege('anon', 'public.get_country_preview_creators(text[],integer)', 'EXECUTE')
       OR NOT has_function_privilege('authenticated', 'public.get_country_preview_creators(text[],integer)', 'EXECUTE')
       OR NOT has_function_privilege('service_role', 'public.get_country_preview_creators(text[],integer)', 'EXECUTE') THEN
        RAISE EXCEPTION 'Country preview RPC is missing an intended role grant';
    END IF;
    IF public.get_country_creator_rank(lpad('1', 32, '0')::uuid) <> 1
       OR public.get_country_creator_rank(lpad('2', 32, '0')::uuid) <> 2
       OR public.get_country_creator_rank(lpad('3', 32, '0')::uuid) IS NOT NULL
       OR public.get_country_creator_rank(lpad('4', 32, '0')::uuid) IS NOT NULL
       OR public.get_country_creator_rank(lpad('5', 32, '0')::uuid) IS NOT NULL THEN
        RAISE EXCEPTION 'Country ranks do not match list eligibility or deterministic tie order';
    END IF;
END $$;
ROLLBACK;
