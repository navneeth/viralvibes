-- Run this statement separately with psql autocommit, not in a transaction.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_terminal_lookup
ON public.creator_sync_jobs (
    creator_id,
    (COALESCE(completed_at, started_at, created_at)) DESC
)
INCLUDE (retry_count)
WHERE status = 'failed' AND creator_id IS NOT NULL;

-- IF NOT EXISTS does not repair an invalid index left by an interrupted build.
SELECT indexrelid::regclass AS index_name, indisvalid, indisready
FROM pg_index
WHERE indexrelid = 'public.idx_creator_sync_terminal_lookup'::regclass;
