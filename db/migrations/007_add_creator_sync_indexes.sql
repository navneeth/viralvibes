-- Migration 006: Add performance indexes for creator sync queries
-- Purpose: Fix statement timeout errors in bootstrap_creators.py Pass 3
-- Issue: Query on (sync_status, last_synced_at) was doing full table scans
--
-- This migration adds composite indexes to speed up:
-- 1. Finding invalid/failed creators for retry
-- 2. Finding never-synced creators
-- 3. Finding stale creators needing refresh

-- ═══════════════════════════════════════════════════════════════════════════
-- Index 1: Composite index for invalid/failed creator queries
-- ═══════════════════════════════════════════════════════════════════════════
-- Supports queries like:
--   WHERE sync_status IN ('invalid', 'failed', 'synced_partial')
--     AND last_synced_at IS NOT NULL
--     AND last_synced_at < '2026-03-22'
--
-- Note: PostgreSQL can use this index for the .in_() filter + date comparison
CREATE INDEX IF NOT EXISTS idx_creators_sync_status_last_synced
ON creators (sync_status, last_synced_at DESC)
WHERE last_synced_at IS NOT NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- Index 2: Partial index for never-synced creators
-- ═══════════════════════════════════════════════════════════════════════════
-- Supports queries like:
--   WHERE last_synced_at IS NULL
--
-- This is a partial index (only includes NULL rows), making it very small
-- and fast to scan. Perfect for the "never-synced" use case.
CREATE INDEX IF NOT EXISTS idx_creators_never_synced
ON creators (id)
WHERE last_synced_at IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- Index 3: Index for stale creator refresh queries
-- ═══════════════════════════════════════════════════════════════════════════
-- Supports queries like:
--   WHERE sync_status = 'synced'
--     AND last_synced_at < '2026-03-15'
--
-- Used by _queue_creators_for_extended_refresh()
CREATE INDEX IF NOT EXISTS idx_creators_synced_last_synced
ON creators (last_synced_at DESC)
WHERE sync_status = 'synced';

-- ═══════════════════════════════════════════════════════════════════════════
-- Verification
-- ═══════════════════════════════════════════════════════════════════════════
-- After applying this migration, you can verify the indexes with:
--
-- SELECT
--   schemaname,
--   tablename,
--   indexname,
--   indexdef
-- FROM pg_indexes
-- WHERE tablename = 'creators'
--   AND indexname LIKE 'idx_creators_%'
-- ORDER BY indexname;
--
-- Expected output:
-- - idx_creators_sync_status_last_synced
-- - idx_creators_never_synced
-- - idx_creators_synced_last_synced
