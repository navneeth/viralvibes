-- Disposable database with the application schema and migrations 089-090 applied.
-- Requires at least one creator without an active sync job.
\set ON_ERROR_STOP on
BEGIN;
DO $$
DECLARE
    test_creator_id uuid;
    test_job_id bigint;
BEGIN
    SELECT creator.id INTO test_creator_id
    FROM public.creators AS creator
    WHERE NOT EXISTS (
        SELECT 1
        FROM public.creator_sync_jobs AS job
        WHERE job.creator_id = creator.id
          AND job.status IN ('pending', 'processing')
    )
    LIMIT 1;
    IF test_creator_id IS NULL THEN
        RAISE EXCEPTION 'A creator without an active sync job is required';
    END IF;

    INSERT INTO public.creator_sync_jobs (creator_id, status, job_type, source)
    VALUES (test_creator_id, 'pending', 'sync_stats', 'drain_guard_test')
    RETURNING id INTO test_job_id;

    BEGIN
        PERFORM public.enqueue_unknown_activity_refresh_batch_after_drain(0, true, 1000);
        RAISE EXCEPTION 'Expected pending jobs to block automatic refresh';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM <> 'Creator sync queue is not drained; retry automatic refresh later' THEN
            RAISE;
        END IF;
    END;

    UPDATE public.creator_sync_jobs SET status = 'processing' WHERE id = test_job_id;
    BEGIN
        PERFORM public.enqueue_unknown_activity_refresh_batch_after_drain(0, true, 1000);
        RAISE EXCEPTION 'Expected processing jobs to block automatic refresh';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM <> 'Creator sync queue is not drained; retry automatic refresh later' THEN
            RAISE;
        END IF;
    END;

    DELETE FROM public.creator_sync_jobs WHERE id = test_job_id;
    PERFORM public.enqueue_unknown_activity_refresh_batch_after_drain(0, true, 1000);

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
    ) THEN
        RAISE EXCEPTION 'Automatic refresh drain guard permissions are incorrect';
    END IF;
END $$;
ROLLBACK;