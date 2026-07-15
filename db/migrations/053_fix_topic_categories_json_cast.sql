-- Migration 053: Fix get_top_categories_with_counts crash on malformed topic_categories JSON
--
-- Root cause
-- ----------
-- topic_categories is a TEXT column. Some rows contain malformed JSON (e.g. Python-style
-- single-quoted lists written by older sync jobs: ['Music', 'Sports'] instead of
-- ["Music", "Sports"]). The bare `topic_categories::jsonb` cast used in every version of
-- get_top_categories_with_counts (migrations 002, 004, 034) throws error 22P02
-- (invalid input syntax for type json) when it encounters a malformed row.
--
-- PostgREST maps 22P02 to HTTP 400 and tries to return a JSON error body. When the error
-- detail contains the malformed bytes Cloudflare's proxy intercepts the upstream 400 and
-- substitutes its own HTML error page:
--   <html><head><title>400 Bad Request</title></head><body>...<center>cloudflare</center>...
-- postgrest-py then fails to parse HTML as JSON and raises:
--   "JSON could not be generated"  (pydantic ValidationError for APIErrorFromJSON)
-- The fallback activates and scans up to 1 000 rows client-side — an inaccurate result.
--
-- Secondary bug (migration 045 drift)
-- ------------------------------------
-- Migration 034 added sync_status = 'synced' to restrict the JSONB unnest. Migration 045
-- expanded BROWSEABLE_SYNC_STATUSES to ('synced', 'synced_partial'). The RPC was never
-- updated, so synced_partial creators (a meaningful share of the DB) are silently
-- excluded from category counts even though the creators endpoint shows both statuses.
--
-- Fix
-- ---
-- 1. Add safe_jsonb(text) → jsonb: catches 22P02 and returns NULL instead of raising.
--    IMMUTABLE + STRICT so Postgres can inline / short-circuit on NULL inputs.
-- 2. Rewrite get_top_categories_with_counts:
--    a. Pre-parse each row's topic_categories via safe_jsonb() in a lateral subquery so
--       the cast runs once per row, not twice (avoids the double-call in the CASE expr).
--    b. Replace sync_status = 'synced' with sync_status IN ('synced', 'synced_partial'),
--       aligning with constants.BROWSEABLE_SYNC_STATUSES.
--    c. Retain all existing normalisation logic (wiki-prefix strip, underscore→space,
--       whitespace collapse) so downstream projection onto YOUTUBE_TOPIC_CATEGORY_LABELS
--       is unaffected.

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Safe TEXT → JSONB cast helper
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

COMMENT ON FUNCTION public.safe_jsonb(text) IS
    'Cast text to jsonb, returning NULL on any cast error (including 22P02 invalid JSON) '
    'instead of raising. Used by RPCs that read the topic_categories TEXT column to prevent '
    'a single malformed row from aborting the entire aggregation query (migration 053).';

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Rewrite get_top_categories_with_counts
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
        FROM public.creators c
        -- Parse topic_categories once per row via safe_jsonb() so the cast
        -- runs exactly once and any malformed value becomes NULL (skipped below).
        CROSS JOIN LATERAL (
            SELECT public.safe_jsonb(c.topic_categories) AS j
        ) AS parsed
        -- Unnest the JSON array; CROSS JOIN drops rows where j IS NULL (malformed)
        -- because jsonb_array_elements_text(NULL) returns an empty set.
        CROSS JOIN LATERAL jsonb_array_elements_text(
            CASE
                WHEN jsonb_typeof(parsed.j) = 'array' THEN parsed.j
                ELSE '[]'::jsonb
            END
        ) AS cat_raw(value)
        WHERE c.sync_status         IN ('synced', 'synced_partial')   -- was = 'synced' only
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
    'Normalisation: strip Wikipedia /wiki/ URL prefix, replace underscores with spaces, '
    'collapse whitespace, trim. Mirrors utils/core.py normalize_category_name(). '
    'Uses safe_jsonb() to skip rows with malformed JSON (22P02 crash fix, migration 053). '
    'Includes synced_partial creators (sync_status drift fix, migration 053). '
    'Used by the /lists By Category tab and /lists/categories explorer.';

-- Verify the helper is callable (will raise if there is a syntax error above).
DO $$ BEGIN
    PERFORM public.safe_jsonb('["test"]');
    PERFORM public.safe_jsonb('not valid json');
    PERFORM public.safe_jsonb(NULL);
END $$;
