-- Migration 064: Add a combined browseable index for the Recent sort.
--
-- The /creators query filters sync_status IN ('synced', 'synced_partial') and
-- orders by last_updated_at DESC. The status-specific indexes in migration 050
-- do not provide one ordered scan across both statuses. Production EXPLAIN
-- plans and Vercel logs showed sequential scans, full sorts, and timeouts on
-- this first-page sort.
--
-- Run outside a transaction. CREATE INDEX CONCURRENTLY avoids blocking normal
-- reads and writes while PostgreSQL builds the index. Supabase SQL Editor may
-- wrap statements in a transaction; run this migration with psql through a
-- direct or session-pooler connection instead.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_last_updated_browseable
    ON public.creators (last_updated_at DESC)
    WHERE (sync_status = 'synced' OR sync_status = 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- Verification: the unfiltered first page should use this index and avoid a
-- full-table scan and sort.
-- EXPLAIN (COSTS, VERBOSE)
-- SELECT id, last_updated_at
-- FROM public.creators
-- WHERE sync_status IN ('synced', 'synced_partial')
--   AND channel_name IS NOT NULL
--   AND current_subscribers > 0
-- ORDER BY last_updated_at DESC
-- LIMIT 50;
