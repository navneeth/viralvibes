-- Migration: Shared stats RPCs for /creators and /lists pages
-- Purpose: (1) Add get_top_languages_with_counts() so language stats are
--              DB-accurate on both pages.
--          (2) Extend get_creator_hero_stats() with total_countries and
--              total_languages so the creators page hero shows the same
--              counts as the /lists page without a separate scan.
-- Date: 2026-03-12

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. get_top_languages_with_counts(p_limit)
--    Returns top content languages ranked by creator count.
--    Mirrors get_top_countries_with_counts() from migration 002.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.get_top_languages_with_counts(p_limit integer DEFAULT 10)
RETURNS TABLE (language_code text, creator_count bigint)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT
        c.default_language      AS language_code,
        COUNT(*)::bigint        AS creator_count
    FROM public.creators c
    WHERE c.channel_name        IS NOT NULL
      AND c.default_language    IS NOT NULL
      AND c.current_subscribers  > 0
    GROUP BY c.default_language
    ORDER BY creator_count DESC
    LIMIT p_limit;
$$;

COMMENT ON FUNCTION public.get_top_languages_with_counts(integer) IS
    'Returns (language_code, creator_count) ranked by creator count. '
    'Used by the /creators filter bar. Zero row-transfer aggregation.';


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. get_creator_hero_stats()  (DROP + CREATE — adds two new OUT columns)
--    PostgreSQL requires DROP when the return-type signature changes.
--    Now also returns total_countries and total_languages so the creators
--    page hero is driven by the same DB-side aggregation as /lists.
--
--    Previous columns preserved unchanged:
--        total_creators, avg_engagement, growing_creators, premium_creators
-- ─────────────────────────────────────────────────────────────────────────────
DROP FUNCTION IF EXISTS public.get_creator_hero_stats();

CREATE OR REPLACE FUNCTION public.get_creator_hero_stats()
RETURNS TABLE (
    total_creators   bigint,
    avg_engagement   numeric,
    growing_creators bigint,
    premium_creators bigint,
    total_countries  bigint,
    total_languages  bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT
        COUNT(*)                                                AS total_creators,
        ROUND(COALESCE(AVG(engagement_score), 0)::numeric, 2) AS avg_engagement,
        COUNT(*) FILTER (
            WHERE subscribers_change_30d > 0
        )                                                       AS growing_creators,
        COUNT(*) FILTER (
            WHERE quality_grade IN ('A+', 'A')
        )                                                       AS premium_creators,
        COUNT(DISTINCT country_code)                           AS total_countries,
        COUNT(DISTINCT default_language)                       AS total_languages
    FROM public.creators
    WHERE channel_name        IS NOT NULL
      AND current_subscribers  > 0;
$$;

COMMENT ON FUNCTION public.get_creator_hero_stats() IS
    'Global aggregate stats for the /creators hero section. '
    'Returns a single row with exact DB-side counts — zero row transfer. '
    'total_countries and total_languages added in migration 003 so the '
    'creators and lists pages share the same source of truth.';


-- ─────────────────────────────────────────────────────────────────────────────
-- Verification queries
-- ─────────────────────────────────────────────────────────────────────────────
-- SELECT * FROM public.get_top_languages_with_counts(10);
-- SELECT * FROM public.get_creator_hero_stats();
