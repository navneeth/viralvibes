-- Migration 004: Backfill dirty topic_categories rows + defense-in-depth RPC fix
-- Date: 2026-03-19
--
-- Root cause:
--   channel_utils.normalize_channel() passes the raw YouTube API array
--   (topicCategories — a list of Wikipedia URLs) through to the worker.
--   _format_categories() in creator_worker.py correctly strips the URL prefix
--   on every sync, but rows synced by an older version of the worker still
--   hold raw URLs like "https://en.wikipedia.org/wiki/Music".
--   Because the DB is largely stable after the initial import and full
--   re-syncs are infrequent, this dirt persists indefinitely.
--
-- Strategy:
--   1. PRIMARY FIX — one-time UPDATE to clean existing dirty rows.
--      Targets only rows whose topic_categories JSON contains a '/wiki/' URL.
--      After this runs the data is permanently clean; the worker keeps it clean.
--   2. SECONDARY FIX — update the two RPCs to apply the same stripping at
--      query time (defense-in-depth against any future dirty rows).


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. ONE-TIME DATA BACKFILL
--    Re-normalise topic_categories in-place for rows containing raw Wikipedia URLs.
--    Only touches rows that actually need cleaning (LIKE '%/wiki/%' guard).
--    After this migration the worker's _format_categories() keeps new writes clean.
-- ─────────────────────────────────────────────────────────────────────────────
UPDATE public.creators
SET topic_categories = (
    SELECT COALESCE(
        jsonb_agg(
            TRIM(
                REGEXP_REPLACE(
                    REPLACE(
                        CASE
                            WHEN value LIKE '%/wiki/%'
                            THEN SPLIT_PART(value, '/wiki/', 2)
                            ELSE value
                        END,
                        '_', ' '
                    ),
                    '\s+', ' ', 'g'
                )
            )
        ),
        '[]'::jsonb  -- jsonb_agg returns NULL on empty input; keep [] not NULL
    )
    FROM jsonb_array_elements_text(
        -- Guard against scalar/object values — mirrors the RPC pattern.
        -- Non-array shapes are treated as empty so the row is skipped cleanly.
        CASE
            WHEN jsonb_typeof(topic_categories::jsonb) = 'array'
            THEN topic_categories::jsonb
            ELSE '[]'::jsonb
        END
    ) AS elems(value)
    WHERE TRIM(value) <> ''
)
WHERE
    topic_categories IS NOT NULL
    AND topic_categories::text LIKE '%/wiki/%';

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. get_top_categories_with_counts — add /wiki/ prefix stripping (defense-in-depth)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.get_top_categories_with_counts(p_limit integer DEFAULT 50)
RETURNS TABLE (category text, creator_count bigint)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
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
    'Normalisation: strip Wikipedia /wiki/ URL prefix, replace underscores with spaces, '
    'collapse whitespace, trim. Mirrors utils/core.py normalize_category_name(). '
    'Used by the /lists By Category tab and /lists/categories explorer. '
    'Zero row-transfer aggregation.';


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. get_lists_meta — apply the same /wiki/ stripping (defense-in-depth)
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
SET search_path = pg_catalog, public
AS $$
    SELECT
        COUNT(*)                            AS total_creators,
        COUNT(DISTINCT c.country_code)      AS total_countries,
        (
            -- Post-normalization filter (cat <> '') matches get_top_categories_with_counts
            -- so values that normalise to empty (e.g. pure underscores) are excluded here too.
            SELECT COUNT(DISTINCT cat)
            FROM (
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
            ) AS normalised
            WHERE cat <> ''
        )                                   AS total_categories
    FROM public.creators c
    WHERE c.channel_name        IS NOT NULL
      AND c.current_subscribers  > 0;
$$;

COMMENT ON FUNCTION public.get_lists_meta() IS
    'Returns a single row: (total_creators, total_countries, total_categories). '
    'Category normalisation now strips Wikipedia /wiki/ URL prefixes, matching '
    'utils/core.py normalize_category_name(). '
    'All values are exact DB-side aggregates. Zero row-transfer.';


-- ─────────────────────────────────────────────────────────────────────────────
-- Verification
-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Confirm all dirty rows were cleaned (should return 0):
--    SELECT COUNT(*) FROM public.creators WHERE topic_categories::text LIKE '%/wiki/%';
--
-- 2. Confirm duplicates have collapsed (Music 3034 + 2861 → ~5895 in one row):
--    SELECT * FROM public.get_top_categories_with_counts(20);
--
-- 3. Check updated total_categories count:
--    SELECT * FROM public.get_lists_meta();
