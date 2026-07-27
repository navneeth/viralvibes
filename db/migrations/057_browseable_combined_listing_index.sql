-- Migration 057: Unified partial index for browseable creator listings.
--
-- Context: get_creators() uses sync_status IN ('synced', 'synced_partial') for all
-- browse queries. Migrations 008 and 045 each created a partial index for one status:
--   idx_creators_listing_base          WHERE sync_status = 'synced'         ...
--   idx_creators_listing_base_partial  WHERE sync_status = 'synced_partial' ...
--
-- PostgreSQL satisfies IN ('synced', 'synced_partial') via
--   BitmapOr(idx_creators_listing_base, idx_creators_listing_base_partial)
-- A bitmap scan does NOT maintain sort order. The planner therefore does a full
-- Bitmap Heap Scan across all ~800K browseable creators and then sorts before
-- applying LIMIT 50 — O(N) work regardless of the requested page size.
--
-- Unfiltered default browse (no search, all filters = "all"):
--   routes/creators.py now skips count=exact for this path and uses hero_stats
--   for the pagination count. The BitmapOr sort issue still adds latency for the
--   LIMIT 50 data fetch itself.
--
-- Filtered/searched pages:
--   These still issue count=exact (Prefer: count=exact via PostgREST), which
--   forces a COUNT(*) of every matching row. On large tables this exhausts the
--   statement timeout (57014) before returning. The combined index below fixes
--   this by allowing a single IndexScan (sorted, stops at LIMIT 50) and an
--   Index Only Scan for COUNT(*) with no heap access beyond the returned rows.
--
-- The per-status indexes from migrations 008 and 045 are retained; they remain
-- the best choice for single-status filtered queries (e.g. /lists queries that
-- filter on sync_status = 'synced' directly).
--
-- NOTE: Plain CREATE INDEX (no CONCURRENTLY) is used here because the Supabase
-- SQL editor runs inside an implicit transaction block that is incompatible with
-- CONCURRENTLY. Plain CREATE INDEX takes a ShareUpdateExclusiveLock — reads are
-- unaffected but writes (INSERT/UPDATE/DELETE) on the creators table are blocked
-- for the duration of the build. Run during a low-traffic window.

CREATE INDEX IF NOT EXISTS idx_creators_listing_browseable
    ON public.creators (current_subscribers DESC)
    WHERE (sync_status = 'synced' OR sync_status = 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- ── Mirror indexes for the other default-sort columns ────────────────────────
-- These cover the same IN() timeout for /creators?sort=views, sort=engagement, etc.
-- Add them one by one if those sort options also show 57014 timeouts.

CREATE INDEX IF NOT EXISTS idx_creators_views_browseable
    ON public.creators (current_view_count DESC)
    WHERE (sync_status = 'synced' OR sync_status = 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX IF NOT EXISTS idx_creators_engagement_browseable
    ON public.creators (engagement_score DESC)
    WHERE (sync_status = 'synced' OR sync_status = 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX IF NOT EXISTS idx_creators_monthly_uploads_browseable
    ON public.creators (monthly_uploads DESC)
    WHERE (sync_status = 'synced' OR sync_status = 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;
