-- Migration 032: Fix timeout in creator_sync_jobs polling with OR query
--
-- Problem: _fetch_pending_jobs() uses a single OR query:
--   WHERE status='pending' AND (next_retry_at IS NULL OR next_retry_at <= now())
--   ORDER BY created_at ASC
--
-- The partial indexes from migration 026 (designed for separate queries) don't
-- cover this OR predicate efficiently. PostgreSQL must consider both branches,
-- leading to full table scans and timeouts when the table has 800K+ rows.
--
-- Solution: Create a covering index that satisfies the entire query without
-- accessing the base table. This index includes all SELECT columns so
-- PostgreSQL can do an "index-only scan" (very fast).

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_jobs_pending_or_query
    ON public.creator_sync_jobs (created_at ASC)
    INCLUDE (id, creator_id, source, retry_count, job_type, next_retry_at)
    WHERE status = 'pending';

-- This index:
-- 1. Is partial (only includes rows where status='pending') → small and fast
-- 2. Orders by created_at to support ORDER BY created_at ASC
-- 3. Includes all SELECT columns for index-only scans (no heap lookup needed)
-- 4. Covers both OR branches: next_retry_at IS NULL and next_retry_at <= now()
--
-- Expected query plan:
--   Index Only Scan using idx_creator_sync_jobs_pending_or_query
--     Index Cond: (created_at > now() - interval '...')  [if needed]
--     Heap Fetches: 0
--
-- Verification:
-- SELECT indexname, indexdef FROM pg_indexes
-- WHERE indexname = 'idx_creator_sync_jobs_pending_or_query';
