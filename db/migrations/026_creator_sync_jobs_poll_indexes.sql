-- Migration 026: Add polling indexes to creator_sync_jobs
-- Purpose: Fix progressive throughput decline in the Kaggle worker
--
-- Root cause: _fetch_pending_jobs() runs 2 queries per job:
--   1. WHERE status='pending' AND retry_at IS NULL  ORDER BY created_at ASC LIMIT 1
--   2. WHERE status='pending' AND retry_at IS NOT NULL AND retry_at <= now() ORDER BY retry_at ASC LIMIT 1
--
-- With 800K+ rows and no covering index, both queries perform full sequential
-- scans. Each scan takes longer as the table grows, causing jobs/hour to
-- decline progressively (280/hr → 70/hr over 4 days observed Apr 25–29 2026).
--
-- Fix: two partial indexes that cover exactly the predicate + ORDER BY of each
-- query. PostgreSQL can satisfy each query with a tiny index scan rather than
-- reading all 800K rows.

-- ── Index 1: fresh pending jobs (no retry scheduled) ─────────────────────────
-- Covers: WHERE status = 'pending' AND retry_at IS NULL ORDER BY created_at ASC
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_jobs_pending_fresh
    ON public.creator_sync_jobs (created_at ASC)
    WHERE status = 'pending' AND retry_at IS NULL;

-- ── Index 2: retry-ready jobs (backoff window expired) ────────────────────────
-- Covers: WHERE status = 'pending' AND retry_at IS NOT NULL AND retry_at <= now()
--         ORDER BY retry_at ASC
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creator_sync_jobs_pending_retry
    ON public.creator_sync_jobs (retry_at ASC)
    WHERE status = 'pending' AND retry_at IS NOT NULL;

-- (No explicit ANALYZE; rely on autovacuum or run manually if needed)

-- ── Verification ─────────────────────────────────────────────────────────────
-- Run after applying to confirm both indexes exist:
--
-- SELECT indexname, indexdef
-- FROM pg_indexes
-- WHERE tablename = 'creator_sync_jobs'
--   AND indexname LIKE 'idx_creator_sync_jobs_pending%'
-- ORDER BY indexname;
