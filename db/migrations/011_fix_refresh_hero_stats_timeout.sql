-- =============================================================================
-- Fix: refresh_hero_stats_cache hits Supabase's PostgREST statement timeout
--      (default 3s on free tier) because CONCURRENTLY refresh on a large
--      table takes longer than that.
--
-- Fix: SET LOCAL statement_timeout = 0 at the top of the function disables
--      the timeout for the duration of that call only. This is safe because:
--      - The function is SECURITY DEFINER (runs as the owner, not the caller)
--      - SET LOCAL is scoped to the current transaction — it resets automatically
--        when the function returns, so it cannot leak to other connections
-- =============================================================================

CREATE OR REPLACE FUNCTION public.refresh_hero_stats_cache()
RETURNS TABLE(materialized_view text, rows_refreshed bigint, refresh_duration_ms bigint)
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    start_time TIMESTAMPTZ;
    end_time   TIMESTAMPTZ;
    hero_count  BIGINT;
    lists_count BIGINT;
BEGIN
    -- Disable statement timeout for this call only.
    -- SET LOCAL resets automatically when the function returns.
    SET LOCAL statement_timeout = 0;

    -- Refresh hero stats
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_hero_stats;
    end_time := clock_timestamp();
    SELECT total_creators INTO hero_count FROM public.mv_hero_stats;
    RETURN QUERY SELECT
        'mv_hero_stats'::TEXT,
        hero_count,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::BIGINT;

    -- Refresh lists meta
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_lists_meta;
    end_time := clock_timestamp();
    SELECT total_creators INTO lists_count FROM public.mv_lists_meta;
    RETURN QUERY SELECT
        'mv_lists_meta'::TEXT,
        lists_count,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::BIGINT;
END;
$function$;

-- =============================================================================
-- Fix: refresh_hero_stats_cache statement timeout
--
-- Root cause: Supabase enforces statement_timeout at the role level in their
--             PostgREST infrastructure. SET LOCAL inside a function body
--             cannot override it — the timeout fires before the refresh
--             completes (~3s on free tier).
--
-- Fix: drop CONCURRENTLY from both REFRESH calls.
--
-- Why this is safe: both views are single-row aggregates. A non-concurrent
-- refresh takes an exclusive lock for the duration of the query, but on a
-- single-row view that is microseconds — invisible to any reader.
-- The unique indexes added by the previous migration are harmless; leave them.
-- =============================================================================

CREATE OR REPLACE FUNCTION public.refresh_hero_stats_cache()
RETURNS TABLE(materialized_view text, rows_refreshed bigint, refresh_duration_ms bigint)
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    start_time  TIMESTAMPTZ;
    end_time    TIMESTAMPTZ;
    hero_count  BIGINT;
    lists_count BIGINT;
BEGIN
    -- Refresh hero stats (no CONCURRENTLY — single-row view, lock is ~microseconds)
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW public.mv_hero_stats;
    end_time := clock_timestamp();
    SELECT total_creators INTO hero_count FROM public.mv_hero_stats;
    RETURN QUERY SELECT
        'mv_hero_stats'::TEXT,
        hero_count,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::BIGINT;

    -- Refresh lists meta
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW public.mv_lists_meta;
    end_time := clock_timestamp();
    SELECT total_creators INTO lists_count FROM public.mv_lists_meta;
    RETURN QUERY SELECT
        'mv_lists_meta'::TEXT,
        lists_count,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::BIGINT;
END;
$function$;
