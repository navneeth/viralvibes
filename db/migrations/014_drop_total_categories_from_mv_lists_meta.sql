-- =============================================================================
-- Migration 014: Remove total_categories from mv_lists_meta
-- =============================================================================
-- Root cause: migration 013 moved total_categories into app_stats and updated
--             get_lists_meta() to read from there, but mv_lists_meta was never
--             rebuilt. The view still carries the expensive jsonb unnest subquery,
--             causing refresh_mv_lists_meta to time out (~4s > PostgREST limit).
--
-- Fix: recreate mv_lists_meta with only total_creators / total_countries /
--      total_languages. The refresh becomes a fast COUNT + COUNT DISTINCT scan
--      with no jsonb work at all. get_lists_meta() already sources
--      total_categories from app_stats (via migration 013) — no further changes
--      to that function are needed.
-- =============================================================================

BEGIN;

-- Recreate the view without the total_categories subquery.
-- We keep the id = 1 surrogate column so the unique index (required for
-- REFRESH CONCURRENTLY) can be recreated unchanged.
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
    AND current_subscribers > 0;

CREATE UNIQUE INDEX idx_mv_lists_meta_id ON public.mv_lists_meta (id);

COMMENT ON MATERIALIZED VIEW public.mv_lists_meta IS
'Pre-computed list-page counts (creators / countries / languages). '
'total_categories is stored in app_stats and refreshed by refresh_total_categories(). '
'Refreshed by bootstrap_creators.py Pass 5 (refresh_mv_lists_meta RPC).';

COMMIT;
