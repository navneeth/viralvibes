-- =============================================================================
-- Migration 015: Add sync_status = 'synced' filter to mv_lists_meta
-- =============================================================================
-- Problem: After migration 014 removed the total_categories JSONB subquery,
--          refresh_mv_lists_meta still times out at ~5 s because the view
--          scans all 684K creators (channel_name NOT NULL + subscribers > 0).
--          mv_hero_stats filters by sync_status = 'synced' and refreshes in
--          ~120ms over ~58K rows.
--
-- Fix: align mv_lists_meta's WHERE clause with mv_hero_stats so both views
--      scan the same ~58K-row synced population.
--
-- Correctness: unsync'd creators have no country_code / default_language
--              populated yet, so they add no signal to the DISTINCT counts
--              anyway. The counts stay accurate.
-- =============================================================================

BEGIN;

DROP MATERIALIZED VIEW IF EXISTS public.mv_lists_meta;

CREATE MATERIALIZED VIEW public.mv_lists_meta AS
SELECT
    1                                                                             AS id,
    count(*)                                                                      AS total_creators,
    count(DISTINCT country_code) FILTER (WHERE country_code IS NOT NULL)          AS total_countries,
    count(DISTINCT default_language) FILTER (WHERE default_language IS NOT NULL)  AS total_languages
FROM public.creators
WHERE
    channel_name IS NOT NULL
    AND current_subscribers > 0
    AND sync_status = 'synced';

CREATE UNIQUE INDEX idx_mv_lists_meta_id ON public.mv_lists_meta (id);

COMMENT ON MATERIALIZED VIEW public.mv_lists_meta IS
'Pre-computed list-page counts (creators / countries / languages) for synced creators. '
'total_categories is stored in app_stats. '
'Refreshed by bootstrap_creators.py Pass 5 (refresh_mv_lists_meta RPC).';

COMMIT;
