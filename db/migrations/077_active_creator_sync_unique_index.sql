-- Stage 2 of the active-job uniqueness rollout.
--
-- Preflight immediately before running:
--   SELECT creator_id, count(*) AS active_jobs
--   FROM public.creator_sync_jobs
--   WHERE status IN ('pending', 'processing')
--   GROUP BY creator_id
--   HAVING count(*) > 1;
-- Continue only when this returns no rows.
--
-- Pause all creator sync workers while this concurrent index is built. This
-- allows new site requests to queue pending work (the existing pending-only
-- index still protects it) without a worker changing pending rows to processing
-- during the index build. Resume workers only after the index is verified valid.
--
-- Run this statement by itself, outside a transaction. Do not use a migration
-- runner that wraps the file in a transaction.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_jobs_active_unique
    ON public.creator_sync_jobs (creator_id)
    WHERE status IN ('pending', 'processing');

-- Verify before continuing to stage 3. Both values must be true:
-- SELECT i.indisready, i.indisvalid
-- FROM pg_index AS i
-- JOIN pg_class AS c ON c.oid = i.indexrelid
-- WHERE c.relname = 'idx_creator_sync_jobs_active_unique';
