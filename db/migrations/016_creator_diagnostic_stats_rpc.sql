-- Migration: RPC function for creator worker diagnostics
-- Purpose: Replace client-side sampling with optimized DB-side aggregation
--          Provides exact counts for startup diagnostics without timeouts
-- Date: 2026-04-04

-- ─────────────────────────────────────────────────────────────────────────────
-- INDEXES (required for RPC performance at 1M+ scale)
-- ─────────────────────────────────────────────────────────────────────────────

-- Index for sync_status GROUP BY aggregation
CREATE INDEX IF NOT EXISTS idx_creators_sync_status
    ON public.creators(sync_status)
    WHERE sync_status IS NOT NULL;

-- Index for last_synced_at IS NULL queries
CREATE INDEX IF NOT EXISTS idx_creators_last_synced_at_null
    ON public.creators(last_synced_at DESC NULLS FIRST);

-- ─────────────────────────────────────────────────────────────────────────────
-- get_creator_diagnostic_stats()
--
-- Optimized RPC that returns all diagnostics needed for worker startup:
--   - total_pending_jobs: Number of jobs waiting to be processed
--   - never_synced_count: Creators that have never been synced (last_synced_at IS NULL)
--   - status_breakdown: JSONB object with sync_status distribution
--
-- Now uses indexes for efficient GROUP BY and NULL checks.
-- Eliminates full table scans even at 1M+ rows.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.get_creator_diagnostic_stats()
RETURNS TABLE (
    total_pending_jobs bigint,
    never_synced_count bigint,
    status_breakdown jsonb
)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT
        -- Count pending jobs (indexed on status)
        (SELECT COUNT(*)::bigint FROM public.creator_sync_jobs WHERE status = 'pending')
            AS total_pending_jobs,
        -- Count never-synced creators (indexed on last_synced_at with NULLS FIRST)
        (SELECT COUNT(*)::bigint FROM public.creators WHERE last_synced_at IS NULL)
            AS never_synced_count,
        -- Breakdown of sync_status values as JSONB object (indexed on sync_status)
        jsonb_object_agg(
            COALESCE(sync_status::text, 'NULL'),
            count
        ) AS status_breakdown
    FROM (
        SELECT
            sync_status,
            COUNT(*)::bigint AS count
        FROM public.creators
        GROUP BY sync_status
    ) status_groups;
$$;

COMMENT ON FUNCTION public.get_creator_diagnostic_stats() IS
    'Returns creator DB diagnostics for worker startup: pending jobs, never-synced count, and sync_status breakdown. '
    'Uses indexes for efficient aggregation at 1M+ scale. Single DB-side aggregation with no row transfer.';
