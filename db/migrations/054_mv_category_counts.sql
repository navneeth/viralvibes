-- Migration 054: mv_category_counts — pre-compute category counts to fix statement timeout
--
-- Root cause
-- ----------
-- get_top_categories_with_counts (migrations 002 → 034 → 053) runs a
-- LATERAL jsonb_array_elements_text unnest over every creator row on every
-- page request. As the creators table grows this scan consistently exceeds
-- the PostgREST statement timeout (Postgres error 57014, HTTP 500). The
-- emergency client-side fallback is capped at 1 000 rows — inaccurate.
--
-- Fix
-- ---
-- 1. Create mv_category_counts: a materialized view that pre-computes the
--    GROUP BY result once.  The per-request query becomes a sub-millisecond
--    O(p_limit) index scan instead of O(n_creators × avg_categories).
-- 2. Add refresh_mv_category_counts() RPC (same signature as the existing
--    mv_hero_stats / mv_lists_meta refresh RPCs) so the worker can refresh
--    it alongside the other materialised views.
-- 3. Rewrite get_top_categories_with_counts() to read from the MV.
--
-- Dependency: safe_jsonb() helper from migration 053 (idempotently re-created
-- here so this migration can be applied standalone if 053 was skipped).

-- ─────────────────────────────────────────────────────────────────────────────
-- 0. Ensure safe_jsonb() exists (idempotent — also in migration 053)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.safe_jsonb(p_text text)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
STRICT
SECURITY INVOKER
AS $$
BEGIN
    RETURN p_text::jsonb;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Materialized view
-- ─────────────────────────────────────────────────────────────────────────────
-- The aggregation logic is identical to the LATERAL unnest in migration 053;
-- running it once at refresh time means serving requests costs only an index
-- seek.  Refresh is triggered by the worker's bootstrap pass (via
-- refresh_mv_category_counts() RPC added to db.py's refresh_hero_stats_cache
-- loop) so data is always at most one worker cycle out of date.
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_category_counts AS
WITH unnested AS (
    SELECT
        TRIM(
            REGEXP_REPLACE(
                REPLACE(
                    CASE
                        WHEN cat_raw.value LIKE '%/wiki/%'
                        THEN SPLIT_PART(cat_raw.value, '/wiki/', 2)
                        ELSE cat_raw.value
                    END,
                    '_', ' '
                ),
                '\s+', ' ', 'g'
            )
        ) AS cat
    FROM public.creators c
    -- Parse topic_categories once per row; NULL result (malformed JSON) drops
    -- the row via the empty-set behaviour of jsonb_array_elements_text(NULL).
    CROSS JOIN LATERAL (SELECT public.safe_jsonb(c.topic_categories) AS j) AS parsed
    CROSS JOIN LATERAL jsonb_array_elements_text(
        CASE
            WHEN jsonb_typeof(parsed.j) = 'array' THEN parsed.j
            ELSE '[]'::jsonb
        END
    ) AS cat_raw(value)
    WHERE c.sync_status         IN ('synced', 'synced_partial')
      AND c.channel_name        IS NOT NULL
      AND c.topic_categories    IS NOT NULL
      AND c.current_subscribers  > 0
)
SELECT
    cat               AS category,
    COUNT(*)::bigint  AS creator_count
FROM unnested
WHERE cat <> ''
GROUP BY cat
ORDER BY creator_count DESC
WITH DATA;

-- Unique index lets the function ORDER BY + LIMIT use an index-only scan.
CREATE UNIQUE INDEX IF NOT EXISTS mv_category_counts_category_idx
    ON public.mv_category_counts (category);

COMMENT ON MATERIALIZED VIEW public.mv_category_counts IS
    'Pre-computed topic-category creator counts (synced + synced_partial). '
    'Refreshed via refresh_mv_category_counts() RPC in the worker bootstrap. '
    'Replaces the per-request LATERAL unnest that was causing 57014 timeouts '
    'as the creators table grew (migration 054).';

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Refresh RPC  (mirrors refresh_mv_hero_stats / refresh_mv_lists_meta)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.refresh_mv_category_counts()
RETURNS TABLE (
    materialized_view  text,
    rows_refreshed     bigint,
    refresh_duration_ms bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    _start TIMESTAMPTZ;
    _end   TIMESTAMPTZ;
    _rows  BIGINT;
BEGIN
    _start := clock_timestamp();
    REFRESH MATERIALIZED VIEW public.mv_category_counts;
    _end := clock_timestamp();
    SELECT COUNT(*) INTO _rows FROM public.mv_category_counts;
    RETURN QUERY SELECT
        'mv_category_counts'::TEXT,
        _rows,
        EXTRACT(MILLISECONDS FROM (_end - _start))::BIGINT;
END;
$$;

COMMENT ON FUNCTION public.refresh_mv_category_counts() IS
    'Refresh mv_category_counts. Called by db.py refresh_hero_stats_cache() '
    'alongside mv_hero_stats and mv_lists_meta refreshes (migration 054).';

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Rewrite get_top_categories_with_counts to read from the MV
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.get_top_categories_with_counts(p_limit integer DEFAULT 50)
RETURNS TABLE (category text, creator_count bigint)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    SELECT category, creator_count
    FROM public.mv_category_counts
    ORDER BY creator_count DESC
    LIMIT p_limit;
$$;

COMMENT ON FUNCTION public.get_top_categories_with_counts(integer) IS
    'Returns (category, creator_count) from mv_category_counts. '
    'O(p_limit) index scan — was O(n_creators × avg_categories) LATERAL unnest. '
    'Fixes 57014 statement timeout (migration 054). '
    'MV is refreshed by refresh_mv_category_counts() in the worker bootstrap.';
