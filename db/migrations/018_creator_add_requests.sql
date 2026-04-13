-- Migration 018: Creator add-by-handle/ID request queue
--
-- Adds two columns to creator_sync_jobs so that the web frontend can submit
-- a raw @handle or UCxxxxxxxx channel ID without calling the YouTube API.
-- The worker resolves the input, upserts the creator, then converts the job
-- to a normal sync_stats job.
--
-- New job_type: 'resolve_and_add'
--   - creator_id  : NULL until the worker resolves the input
--   - input_query : raw user input — "@MrBeast" or "UCxxxxxxxxxxxxxxxxxx"
--   - requested_by: users.id of the user who submitted the request
--
-- Run once in the Supabase SQL editor.

-- 1. Add input_query column — stores "@handle" or "UCxxxxxxxx"
ALTER TABLE public.creator_sync_jobs
    ADD COLUMN IF NOT EXISTS input_query   text;

-- 2. Add requested_by column — FK to users; nullable for system-queued jobs
ALTER TABLE public.creator_sync_jobs
    ADD COLUMN IF NOT EXISTS requested_by  uuid REFERENCES public.users(id) ON DELETE SET NULL;

-- 3. Allow creator_id to be NULL so resolve_and_add jobs can be inserted
--    before the creator row exists.
--    (creator_id was NOT NULL if enforced at DB level — check and drop if so)
ALTER TABLE public.creator_sync_jobs
    ALTER COLUMN creator_id DROP NOT NULL;

-- 4. Partial unique index: one pending resolve_and_add per input_query.
--    Prevents the same handle being submitted twice while a job is running.
CREATE UNIQUE INDEX IF NOT EXISTS idx_creator_sync_jobs_pending_resolve
    ON public.creator_sync_jobs (input_query)
    WHERE status = 'pending' AND job_type = 'resolve_and_add';

-- 5. Index to let the worker efficiently poll for resolve_and_add jobs
CREATE INDEX IF NOT EXISTS idx_creator_sync_jobs_resolve_and_add
    ON public.creator_sync_jobs (status, job_type, created_at)
    WHERE job_type = 'resolve_and_add';

-- 6. Index for looking up requests by the submitting user (rate-limit check)
CREATE INDEX IF NOT EXISTS idx_creator_sync_jobs_requested_by
    ON public.creator_sync_jobs (requested_by, created_at)
    WHERE requested_by IS NOT NULL;
