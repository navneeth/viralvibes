-- Migration 034: Add sync_status = 'synced' filter to aggregation RPCs
--
-- Root cause of repeated RPC 500s and downstream 50k-row fallback scans:
--
--   get_top_categories_with_counts  (migration 002) — JSONB unnest over all ~816k
--     rows including pending/failed/invalid; no index can help.
--   get_top_languages_with_counts   (migration 003) — GROUP BY over all rows.
--   get_lists_meta                  (migration 002) — outer count + inner categories
--     subquery both missing sync filter; total_creators was overcounting.
--
-- Migration 027 fixed get_top_countries_with_counts but the others were missed.
--
-- Fix: add AND c.sync_status = 'synced' to the WHERE clause of every aggregate
-- that previously only filtered on channel_name IS NOT NULL / current_subscribers > 0.
-- This lets the partial GIN indexes (028, 033) and the idx_creators_category_synced
-- index (020) service these queries as index scans rather than full seq scans.


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. get_top_categories_with_counts
--    Root-cause fix: restrict JSONB unnest to synced creators only.
--    The LATERAL unnest over the full table is the primary timeout driver.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.get_top_categories_with_counts(p_limit integer DEFAULT 50)
RETURNS TABLE (category text, creator_count bigint)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    WITH unnested AS (
        -- topic_categories is stored as a JSONB array.
        -- Restricted to synced creators to avoid full-table JSONB unnest.
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
                    WHEN jsonb_typeof(c.topic_categories::jsonb) = 'array'
                        THEN c.topic_categories::jsonb
                    ELSE '[]'::jsonb
                END
            )
        ) AS cat_raw
        WHERE c.sync_status         = 'synced'      -- ← added (was missing in migration 002)
          AND c.channel_name        IS NOT NULL
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
    'Restricted to sync_status=synced (migration 034 fix). '
    'Used by the /lists By Category tab. Zero row-transfer aggregation.';


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. get_top_languages_with_counts
--    Add sync_status filter to match get_top_countries_with_counts (migration 027).
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
    WHERE c.sync_status         = 'synced'          -- ← added (was missing in migration 003)
      AND c.channel_name        IS NOT NULL
      AND c.default_language    IS NOT NULL
      AND c.current_subscribers  > 0
    GROUP BY c.default_language
    ORDER BY creator_count DESC
    LIMIT p_limit;
$$;

COMMENT ON FUNCTION public.get_top_languages_with_counts(integer) IS
    'Returns (language_code, creator_count) ranked by creator count. '
    'Restricted to sync_status=synced (migration 034 fix). '
    'Used by the /creators filter bar. Zero row-transfer aggregation.';


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. get_lists_meta
--    Add sync_status filter to outer query (total_creators was overcounting
--    pending/failed rows) and to the inner total_categories subquery.
--    total_languages column was added in migration 003; preserved here.
-- ─────────────────────────────────────────────────────────────────────────────
DROP FUNCTION IF EXISTS public.get_lists_meta();

CREATE OR REPLACE FUNCTION public.get_lists_meta()
RETURNS TABLE (
    total_creators   bigint,
    total_countries  bigint,
    total_categories bigint,
    total_languages  bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT
        COUNT(*)                                AS total_creators,
        COUNT(DISTINCT c.country_code)          AS total_countries,
        (
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
            WHERE c2.sync_status        = 'synced'  -- ← added
              AND c2.channel_name        IS NOT NULL
              AND c2.topic_categories    IS NOT NULL
              AND c2.current_subscribers  > 0
              AND TRIM(cat_raw.value)    <> ''
        )                                       AS total_categories,
        COUNT(DISTINCT c.default_language)      AS total_languages
    FROM public.creators c
    WHERE c.sync_status         = 'synced'          -- ← added (was missing in migration 002)
      AND c.channel_name        IS NOT NULL
      AND c.current_subscribers  > 0;
$$;

COMMENT ON FUNCTION public.get_lists_meta() IS
    'Returns a single row: (total_creators, total_countries, total_categories, total_languages). '
    'All values are exact DB-side aggregates restricted to sync_status=synced (migration 034 fix). '
    'total_languages added in migration 003; preserved here. '
    'Used by the /lists page route for tab badges and load-more counts. Zero row-transfer.';


-- ─────────────────────────────────────────────────────────────────────────────
-- Verification queries — run after applying to confirm functions work
-- ─────────────────────────────────────────────────────────────────────────────
-- SELECT * FROM public.get_top_categories_with_counts(10);
-- SELECT * FROM public.get_top_languages_with_counts(10);
-- SELECT * FROM public.get_lists_meta();
