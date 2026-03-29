-- topic_categories is stored as text, cast to jsonb in the index expression
-- to match the ::jsonb cast used in the mv_lists_meta query.
CREATE INDEX IF NOT EXISTS idx_creators_topic_categories_gin
    ON public.creators USING GIN ((topic_categories::jsonb) jsonb_path_ops)
    WHERE topic_categories IS NOT NULL;

-- =============================================================================
-- Fix: split refresh_hero_stats_cache into two separate RPCs so that
-- mv_lists_meta's ~8s refresh can't kill mv_hero_stats in the same call.
-- (GIN index on topic_categories already applied separately.)
-- =============================================================================

-- ── 1. Dedicated RPC for mv_hero_stats only ──────────────────────────────────
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


-- ── 2. Dedicated RPC for mv_lists_meta only ───────────────────────────────────
CREATE OR REPLACE FUNCTION public.refresh_mv_lists_meta()
RETURNS TABLE(materialized_view text, rows_refreshed bigint, refresh_duration_ms bigint)
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    start_time TIMESTAMPTZ;
    end_time   TIMESTAMPTZ;
    row_count  BIGINT;
BEGIN
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW public.mv_lists_meta;
    end_time := clock_timestamp();
    SELECT total_creators INTO row_count FROM public.mv_lists_meta;
    RETURN QUERY SELECT
        'mv_lists_meta'::TEXT,
        row_count,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::BIGINT;
END;
$function$;


-- ── 3. Keep original function as a wrapper (SQL editor / manual use) ──────────
CREATE OR REPLACE FUNCTION public.refresh_hero_stats_cache()
RETURNS TABLE(materialized_view text, rows_refreshed bigint, refresh_duration_ms bigint)
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
BEGIN
    RETURN QUERY SELECT * FROM public.refresh_mv_hero_stats();
    RETURN QUERY SELECT * FROM public.refresh_mv_lists_meta();
END;
$function$;
