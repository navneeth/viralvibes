-- Migration 036: Make creator search exact-handle lookups and broad text search index-backed.
--
-- Context
-- -------
-- /creators?search=asianometry timed out with Postgres error 57014 because
-- get_creators() builds a multi-column OR ILIKE query:
--
--   channel_name     ILIKE '%term%'
--   OR custom_url    ILIKE '%term%'
--   OR primary_category ILIKE '%term%'
--   OR keywords      ILIKE '%term%'
--
-- The app also normalizes handles to "mrbeast", while production custom_url
-- values are mostly stored as "@MrBeast" / "@mrbeast".
--
-- Trade-off: Transactional Migration
-- ------------------------------------
-- This migration runs inside a transaction (required by Supabase and many
-- migration runners). Transactional DDL requires removing CONCURRENTLY from
-- index creation, which means the `creators` table will be briefly locked
-- (read-write) during each index build. For the 810K-row creators table,
-- each GIN index build takes roughly 2-10 seconds. This is acceptable during
-- off-peak deployment; for high-availability environments, run indexes outside
-- business hours or use a separate non-transactional migration tool if available.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Exact-handle lookup used by public.find_creator_by_normalized_handle().
CREATE INDEX IF NOT EXISTS idx_creators_custom_url_normalized_expr
ON public.creators (LOWER(LTRIM(custom_url, '@')))
WHERE custom_url IS NOT NULL
  AND custom_url <> '';

-- Broad /creators search indexes. The partial WHERE mirrors db.get_creators()
-- base filters so Postgres can use these indexes for the current query shape.
CREATE INDEX IF NOT EXISTS idx_creators_channel_name_search_trgm
ON public.creators USING GIN (channel_name gin_trgm_ops)
WHERE sync_status IN ('synced', 'pending')
  AND channel_name IS NOT NULL
  AND current_subscribers > 0;

CREATE INDEX IF NOT EXISTS idx_creators_custom_url_search_trgm
ON public.creators USING GIN (custom_url gin_trgm_ops)
WHERE sync_status IN ('synced', 'pending')
  AND channel_name IS NOT NULL
  AND current_subscribers > 0
  AND custom_url IS NOT NULL
  AND custom_url <> '';

CREATE INDEX IF NOT EXISTS idx_creators_primary_category_search_trgm
ON public.creators USING GIN (primary_category gin_trgm_ops)
WHERE sync_status IN ('synced', 'pending')
  AND channel_name IS NOT NULL
  AND current_subscribers > 0
  AND primary_category IS NOT NULL
  AND primary_category <> '';

CREATE INDEX IF NOT EXISTS idx_creators_keywords_search_trgm
ON public.creators USING GIN (keywords gin_trgm_ops)
WHERE sync_status IN ('synced', 'pending')
  AND channel_name IS NOT NULL
  AND current_subscribers > 0
  AND keywords IS NOT NULL
  AND keywords <> '';

CREATE OR REPLACE FUNCTION public.find_creator_by_normalized_handle(p_handle text)
RETURNS SETOF public.creators
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT c.*
  FROM public.creators c
  WHERE c.custom_url IS NOT NULL
    AND c.custom_url <> ''
    AND LOWER(LTRIM(c.custom_url, '@')) = LOWER(LTRIM(p_handle, '@'))
  ORDER BY c.current_subscribers DESC NULLS LAST
  LIMIT 1;
$$;

COMMENT ON FUNCTION public.find_creator_by_normalized_handle(text) IS
  'Exact creator lookup by lowercased YouTube handle, ignoring a leading @ in both input and stored custom_url.';
