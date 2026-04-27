-- =============================================================================
-- Migration 025: Fix refresh_mv_hero_stats statement timeout
-- =============================================================================
-- Problem: Supabase's DB-level statement_timeout fires before
--          REFRESH MATERIALIZED VIEW completes on the mv_hero_stats view,
--          returning HTTP 500 / code 57014 during bootstrap Pass 5.
--
-- Fix: Add SET LOCAL statement_timeout = 0 inside the function body.
--      SET LOCAL resets automatically when the function exits — no global
--      side effects. This pattern was already used in migration 011.
--
-- Safe to re-run: CREATE OR REPLACE is idempotent.
-- =============================================================================

CREATE OR REPLACE FUNCTION public.refresh_mv_hero_stats()
RETURNS TABLE(materialized_view text, rows_refreshed bigint, refresh_duration_ms bigint)
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    start_time TIMESTAMPTZ;
    end_time   TIMESTAMPTZ;
    row_count  BIGINT;
BEGIN
    -- Disable statement timeout for this session only.
    -- Supabase's DB-level statement_timeout fires before REFRESH MATERIALIZED VIEW
    -- completes on large tables. SET LOCAL is safe — it resets when the function exits.
    SET LOCAL statement_timeout = 0;

    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW public.mv_hero_stats;
    end_time := clock_timestamp();
    SELECT total_creators INTO row_count FROM public.mv_hero_stats;
    RETURN QUERY SELECT
        'mv_hero_stats'::TEXT,
        row_count,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::BIGINT;
END;
$function$;
