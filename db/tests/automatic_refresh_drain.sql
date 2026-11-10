-- Disposable database with the application schema and migrations 089-090 applied.
-- Requires an empty active queue and a creator without a recent refresh failure.
\set ON_ERROR_STOP on
BEGIN;
DO $$
DECLARE
    test_creator_id uuid;
    test_job_id bigint;
    preview_count bigint;
    preview_creator_id uuid;
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.creator_sync_jobs
        WHERE status IN ('pending', 'processing')
    ) THEN
        RAISE EXCEPTION 'The drain guard test requires an initially empty active queue';
    END IF;

    SELECT creator.id INTO test_creator_id
    FROM public.creators AS creator
    WHERE NOT EXISTS (
        SELECT 1
        FROM public.creator_sync_jobs AS job
        WHERE job.creator_id = creator.id
          AND job.source = 'unknown_activity_refresh'
          AND job.status = 'failed'
          AND COALESCE(job.completed_at, job.started_at, job.created_at)
              >= now() - interval '7 days'
    )
    LIMIT 1;
    IF test_creator_id IS NULL THEN
        RAISE EXCEPTION 'A creator without a recent refresh failure is required';
    END IF;

    UPDATE public.creators
    SET archived_at = NULL,
        sync_status = 'synced',
        current_subscribers = GREATEST(COALESCE(current_subscribers, 0), 10000),
        last_synced_at = now() - interval '91 days',
        recent_upload = NULL,
        recent_videos_requested_at = COALESCE(recent_videos_requested_at, now())
    WHERE id = test_creator_id;

    INSERT INTO public.creator_sync_jobs (creator_id, status, job_type, source)
    VALUES (test_creator_id, 'pending', 'sync_stats', 'drain_guard_test')
    RETURNING id INTO test_job_id;

    BEGIN
        PERFORM public.enqueue_unknown_activity_refresh_batch_after_drain(0, true, 1000);
        RAISE EXCEPTION 'Expected pending jobs to block automatic refresh';
    EXCEPTION WHEN SQLSTATE 'VQ001' THEN NULL;
    END;

    UPDATE public.creator_sync_jobs SET status = 'processing' WHERE id = test_job_id;
    BEGIN
        PERFORM public.enqueue_unknown_activity_refresh_batch_after_drain(0, true, 1000);
        RAISE EXCEPTION 'Expected processing jobs to block automatic refresh';
    EXCEPTION WHEN SQLSTATE 'VQ001' THEN NULL;
    END;

    UPDATE public.creator_sync_jobs SET status = 'failed' WHERE id = test_job_id;
    UPDATE public.creator_sync_jobs SET status = 'pending' WHERE id = test_job_id;
    BEGIN
        PERFORM public.enqueue_unknown_activity_refresh_batch_after_drain(0, true, 1000);
        RAISE EXCEPTION 'Expected rescued jobs to block automatic refresh';
    EXCEPTION WHEN SQLSTATE 'VQ001' THEN NULL;
    END;

    DELETE FROM public.creator_sync_jobs WHERE id = test_job_id;
    CREATE TEMP TABLE _drain_guard_preview ON COMMIT DROP AS
    SELECT * FROM public.enqueue_unknown_activity_refresh_batch_after_drain(1, true, 1000);
    SELECT count(*), (SELECT creator_id FROM pg_temp._drain_guard_preview LIMIT 1)
    INTO preview_count, preview_creator_id
    FROM pg_temp._drain_guard_preview;
    IF preview_count <> 1 OR preview_creator_id <> test_creator_id THEN
        RAISE EXCEPTION 'Drained wrapper preview returned %, expected creator %',
            preview_creator_id, test_creator_id;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_creator_sync_jobs_enqueue_lock'
          AND tgrelid = 'public.creator_sync_jobs'::regclass
          AND pg_get_triggerdef(oid) ILIKE '%UPDATE OF status%'
          AND tgenabled <> 'D'
    ) THEN
        RAISE EXCEPTION 'Failed-to-pending enqueue lock trigger is missing or disabled';
    END IF;

    IF has_function_privilege(
        'anon',
        'public.enqueue_unknown_activity_refresh_batch_after_drain(integer,boolean,integer)',
        'EXECUTE'
    ) OR has_function_privilege(
        'authenticated',
        'public.enqueue_unknown_activity_refresh_batch_after_drain(integer,boolean,integer)',
        'EXECUTE'
    ) OR NOT has_function_privilege(
        'service_role',
        'public.enqueue_unknown_activity_refresh_batch_after_drain(integer,boolean,integer)',
        'EXECUTE'
    ) OR has_function_privilege(
        'service_role',
        'public.enqueue_unknown_activity_refresh_batch(integer,boolean,integer)',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'Automatic refresh drain guard permissions are incorrect';
    END IF;
END $$;
ROLLBACK;
