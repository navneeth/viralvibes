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

-- A canceled concurrent build can leave an invalid index entry. Remove only
-- that invalid entry before CREATE so rerunning this psql script preserves a
-- healthy index instead of rebuilding it. \gexec executes the generated DROP
-- as a separate statement, which is required for DROP INDEX CONCURRENTLY.
SELECT format('DROP INDEX CONCURRENTLY %I.%I;', index_namespace.nspname, index_relation.relname)
FROM pg_index AS index_state
JOIN pg_class AS index_relation
  ON index_relation.oid = index_state.indexrelid
JOIN pg_namespace AS index_namespace
  ON index_namespace.oid = index_relation.relnamespace
WHERE index_state.indrelid = 'public.creators'::regclass
  AND index_relation.relname = 'idx_creators_last_updated_browseable'
  AND (NOT index_state.indisvalid OR NOT index_state.indisready)
\gexec

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_last_updated_browseable
  ON public.creators (last_updated_at DESC NULLS LAST)
  WHERE sync_status IN ('synced', 'synced_partial')
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
-- ORDER BY last_updated_at DESC NULLS LAST
-- LIMIT 50;
