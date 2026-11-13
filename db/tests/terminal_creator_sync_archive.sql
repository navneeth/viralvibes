-- Disposable database after migrations through 093.
-- Requires four non-archived creators with canonical channel IDs and no jobs.
\set ON_ERROR_STOP on
BEGIN;
DO $$
DECLARE
    fixture_ids uuid[];
    recovered_id uuid;
    active_id uuid;
    terminal_id uuid;
    later_terminal_id uuid;
    old_failure_at timestamptz := statement_timestamp() - interval '2 days';
    recovered_at timestamptz := statement_timestamp() - interval '1 hour';
    archived_count integer;
    inserted_count bigint;
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.creator_sync_jobs
        WHERE status = 'failed' AND retry_count >= 3
    ) THEN
        RAISE EXCEPTION 'Test requires no preexisting terminal failed jobs';
    END IF;

    SELECT array_agg(id) INTO fixture_ids
    FROM (
        SELECT creator.id
        FROM public.creators AS creator
        WHERE creator.archived_at IS NULL
          AND creator.channel_id ~ '^UC[A-Za-z0-9_-]{22}$'
          AND NOT EXISTS (
              SELECT 1 FROM public.creator_sync_jobs AS job
              WHERE job.creator_id = creator.id
          )
        ORDER BY creator.id
        LIMIT 4
    ) AS fixtures;
    IF COALESCE(cardinality(fixture_ids), 0) < 4 THEN
        RAISE EXCEPTION 'Four unused creator rows are required';
    END IF;

    recovered_id := fixture_ids[1];
    active_id := fixture_ids[2];
    terminal_id := fixture_ids[3];
    later_terminal_id := fixture_ids[4];

    UPDATE public.creators
    SET archived_at = NULL, sync_status = 'failed',
        last_synced_at = old_failure_at - interval '1 day'
    WHERE id IN (recovered_id, active_id, terminal_id, later_terminal_id);

    INSERT INTO public.creator_sync_jobs
        (creator_id, status, job_type, source, retry_count,
         created_at, started_at, completed_at)
    VALUES
        (recovered_id, 'failed', 'sync_stats', 'terminal_archive_test', 4,
         old_failure_at, old_failure_at, old_failure_at),
        (active_id, 'failed', 'sync_stats', 'terminal_archive_test', 4,
         old_failure_at, old_failure_at, old_failure_at),
        (terminal_id, 'failed', 'sync_stats', 'terminal_archive_test', 4,
         old_failure_at, old_failure_at, old_failure_at),
        (later_terminal_id, 'failed', 'sync_stats', 'terminal_archive_test', 4,
         old_failure_at, old_failure_at, old_failure_at);

    INSERT INTO public.creator_sync_jobs
        (creator_id, status, job_type, source, created_at, completed_at)
    VALUES (recovered_id, 'completed', 'sync_stats', 'terminal_archive_test',
            recovered_at, recovered_at);
    UPDATE public.creators SET last_synced_at = recovered_at WHERE id = recovered_id;

    INSERT INTO public.creator_sync_jobs (creator_id, status, job_type, source)
    VALUES (active_id, 'pending', 'sync_stats', 'terminal_archive_test');

    SELECT public.archive_terminal_creator_syncs(3, 1) INTO archived_count;
    IF archived_count <> 1 THEN
        RAISE EXCEPTION 'Expected one terminal creator archived, got %', archived_count;
    END IF;
    SELECT public.archive_terminal_creator_syncs(3, 1) INTO archived_count;
    IF archived_count <> 1 THEN
        RAISE EXCEPTION 'Archived history blocked the next batch, got %', archived_count;
    END IF;
    IF (SELECT archived_at FROM public.creators WHERE id = recovered_id) IS NOT NULL THEN
        RAISE EXCEPTION 'Creator with a newer successful sync was archived';
    END IF;
    IF (SELECT archived_at FROM public.creators WHERE id = active_id) IS NOT NULL THEN
        RAISE EXCEPTION 'Creator with an active job was archived';
    END IF;
    IF (SELECT archived_at FROM public.creators WHERE id = terminal_id) IS NULL THEN
        RAISE EXCEPTION 'Terminally failed creator was not archived';
    END IF;
    IF (SELECT archived_at FROM public.creators WHERE id = later_terminal_id) IS NULL THEN
        RAISE EXCEPTION 'Later terminal creator was not archived';
    END IF;

    UPDATE public.creator_sync_jobs
    SET status = 'processing'
    WHERE creator_id = active_id AND status = 'pending';
    SELECT public.archive_terminal_creator_syncs(3, 100) INTO archived_count;
    IF archived_count <> 0 THEN
        RAISE EXCEPTION 'Processing creator should remain active, got % archived', archived_count;
    END IF;
    IF (SELECT archived_at FROM public.creators WHERE id = active_id) IS NOT NULL THEN
        RAISE EXCEPTION 'Creator with a processing job was archived';
    END IF;

    WITH attempted AS (
        INSERT INTO public.creator_sync_jobs (creator_id, status, job_type, source)
        VALUES (terminal_id, 'pending', 'sync_stats', 'terminal_archive_test')
        RETURNING id
    )
    SELECT count(*) INTO inserted_count FROM attempted;
    IF inserted_count <> 0 THEN
        RAISE EXCEPTION 'Enqueue trigger accepted a job for an archived creator';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_creator_sync_jobs_enqueue_lock'
          AND tgrelid = 'public.creator_sync_jobs'::regclass
          AND pg_get_triggerdef(oid) ILIKE '%UPDATE OF status%'
          AND tgenabled <> 'D'
    ) THEN
        RAISE EXCEPTION 'Archived creator enqueue guard trigger is missing or disabled';
    END IF;

    IF NOT has_function_privilege(
        'service_role',
        'public.archive_terminal_creator_syncs(integer,integer)',
        'EXECUTE'
    ) OR has_function_privilege(
        'anon',
        'public.archive_terminal_creator_syncs(integer,integer)',
        'EXECUTE'
    ) OR has_function_privilege(
        'authenticated',
        'public.archive_terminal_creator_syncs(integer,integer)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'Terminal archive RPC permissions are incorrect';
    END IF;
END $$;
ROLLBACK;
