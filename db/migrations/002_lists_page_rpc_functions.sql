-- Migration: RPC functions for /lists page aggregations
-- Purpose: Replace three client-side 50k-row scans with zero-transfer DB aggregations.
--          Mirrors the pattern used by get_creator_hero_stats() on the creators page.
-- Date: 2026-03-11

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. get_top_countries_with_counts(p_limit)
--    Returns top countries ranked by creator count.
--    Replaces the Python client-side GROUP BY over 50k rows.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.get_top_countries_with_counts(p_limit integer DEFAULT 50)
RETURNS TABLE (country_code text, creator_count bigint)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT
        c.country_code,
        COUNT(*)::bigint AS creator_count
    FROM public.creators c
    WHERE c.channel_name IS NOT NULL
      AND c.country_code  IS NOT NULL
      AND c.current_subscribers > 0
    GROUP BY c.country_code
    ORDER BY creator_count DESC
    LIMIT p_limit;
$$;

COMMENT ON FUNCTION public.get_top_countries_with_counts(integer) IS
    'Returns (country_code, creator_count) ranked by creator count. '
    'Used by the /lists By Country tab. Zero row-transfer aggregation.';


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. get_top_categories_with_counts(p_limit)
--    Unnests the topic_categories JSONB/text-array column, normalises each
--    value (trim + collapse underscores/spaces), then groups and counts.
--    Replaces the Python client-side unnest + normalize_category_name loop.
--
--    Normalisation matches utils/core.py normalize_category_name():
--      1. TRIM whitespace
--      2. Replace underscores with spaces
--      3. Collapse runs of whitespace to a single space
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.get_top_categories_with_counts(p_limit integer DEFAULT 50)
RETURNS TABLE (category text, creator_count bigint)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    WITH unnested AS (
        -- topic_categories is stored as a JSONB array or text array.
        -- CASE handles both storage formats gracefully.
        SELECT
            TRIM(
                REGEXP_REPLACE(
                    REPLACE(cat_raw.value, '_', ' '),
                    '\s+', ' ', 'g'
                )
            ) AS cat
        FROM public.creators c,
        LATERAL (
            SELECT value
            FROM jsonb_array_elements_text(
                CASE
                    -- Already a JSON array stored as jsonb
                    WHEN jsonb_typeof(c.topic_categories::jsonb) = 'array'
                        THEN c.topic_categories::jsonb
                    ELSE '[]'::jsonb
                END
            )
        ) AS cat_raw
        WHERE c.channel_name        IS NOT NULL
          AND c.topic_categories    IS NOT NULL
          AND c.current_subscribers  > 0
    )
    SELECT
        cat                  AS category,
        COUNT(*)::bigint     AS creator_count
    FROM unnested
    WHERE cat <> ''
    GROUP BY cat
    ORDER BY creator_count DESC
    LIMIT p_limit;
$$;

COMMENT ON FUNCTION public.get_top_categories_with_counts(integer) IS
    'Returns (category, creator_count) after unnesting and normalising topic_categories. '
    'Normalisation mirrors utils/core.py normalize_category_name(). '
    'Used by the /lists By Category tab. Zero row-transfer aggregation.';


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. get_lists_meta()
--    Returns a single row with the three badge counts used on /lists.
--    total_creators  – exact COUNT(*)
--    total_countries – COUNT(DISTINCT country_code)
--    total_categories – COUNT(DISTINCT normalised category)
--    All three are exact DB-side aggregates; no row transfer needed.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.get_lists_meta()
RETURNS TABLE (
    total_creators   bigint,
    total_countries  bigint,
    total_categories bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT
        COUNT(*)                            AS total_creators,
        COUNT(DISTINCT c.country_code)      AS total_countries,
        (
            -- Distinct normalised categories via a subquery to reuse the same
            -- unnest + normalise logic without duplicating the WHERE clause.
            SELECT COUNT(DISTINCT
                TRIM(
                    REGEXP_REPLACE(
                        REPLACE(cat_raw.value, '_', ' '),
                        '\s+', ' ', 'g'
                    )
                )
            )
            FROM public.creators c2,
            LATERAL (
                SELECT value
                FROM jsonb_array_elements_text(
                    CASE
                        WHEN jsonb_typeof(c2.topic_categories::jsonb) = 'array'
                            THEN c2.topic_categories::jsonb
                        ELSE '[]'::jsonb
                    END
                )
            ) AS cat_raw
            WHERE c2.channel_name        IS NOT NULL
              AND c2.topic_categories    IS NOT NULL
              AND c2.current_subscribers  > 0
              AND TRIM(cat_raw.value)    <> ''
        )                                   AS total_categories
    FROM public.creators c
    WHERE c.channel_name        IS NOT NULL
      AND c.current_subscribers  > 0;
$$;

COMMENT ON FUNCTION public.get_lists_meta() IS
    'Returns a single row: (total_creators, total_countries, total_categories). '
    'All values are exact DB-side aggregates. Used by the /lists page route for '
    'tab badges and load-more counts. Zero row-transfer.';


-- ─────────────────────────────────────────────────────────────────────────────
-- Verification queries — run after applying to confirm functions work
-- ─────────────────────────────────────────────────────────────────────────────
-- SELECT * FROM public.get_top_countries_with_counts(10);
-- SELECT * FROM public.get_top_categories_with_counts(10);
-- SELECT * FROM public.get_lists_meta();
