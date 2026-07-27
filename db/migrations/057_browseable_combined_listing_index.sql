-- Migration 057: Unified partial index for the default /creators browse.
--
-- Problem: The default /creators page (no filters, sort=subscribers, return_count=True)
-- issues a query equivalent to:
--
--   SELECT *, COUNT(*) OVER()
--   FROM creators
--   WHERE sync_status IN ('synced', 'synced_partial')
--     AND channel_name IS NOT NULL
--     AND current_subscribers > 0
--   ORDER BY current_subscribers DESC
--   LIMIT 50;
--
-- Migrations 008 and 045 each created a partial index for one status value:
--   idx_creators_listing_base          WHERE sync_status = 'synced'         ...
--   idx_creators_listing_base_partial  WHERE sync_status = 'synced_partial' ...
--
-- PostgreSQL satisfies IN ('synced', 'synced_partial') via
--   BitmapOr(idx_creators_listing_base, idx_creators_listing_base_partial)
-- A bitmap scan does NOT maintain sort order.  The planner therefore does a
-- full Bitmap Heap Scan across all ~800K browseable creators and then sorts
-- before applying LIMIT 50.  The Prefer: count=exact header added by PostgREST
-- compounds this: it forces a COUNT(*) of every matching row.  Together these
-- exhaust the statement timeout (error 57014) on the production table.
--
-- Fix: a single combined partial index whose WHERE predicate mirrors the
-- IN() condition lets PostgreSQL choose a plain IndexScan (sorted order
-- maintained → stops at LIMIT 50) and an Index Only Scan for COUNT(*).
-- Neither operation needs to visit heap pages beyond the 50 rows returned.
--
-- The per-status indexes from migrations 008 and 045 are retained; they
-- remain the best choice for single-status filtered queries (e.g. the /lists
-- queries that still use sync_status = 'synced' directly).
--
-- NOTE: CONCURRENTLY is omitted — the Supabase SQL editor wraps statements in
-- an implicit transaction block, which is incompatible with CONCURRENTLY.
-- The index build will take a brief ShareLock on the table while it runs.

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
