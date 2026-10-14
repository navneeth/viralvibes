-- Migration 065: Index-backed ranked search across creator profile text.
--
-- The ranked RPC filters synced_partial creators, but the legacy trigram
-- indexes were built for older sync_status predicates. Those partial indexes
-- cannot cover the current RPC result set. Replace indexes one at a time so
-- temporary disk usage is bounded to roughly one old/new index pair.
--
-- Run this script with psql outside a transaction (CREATE INDEX CONCURRENTLY
-- and \gexec are required). Deploy the application after the migration.

-- Remove invalid remnants from any canceled concurrent build before retrying.
SELECT format('DROP INDEX CONCURRENTLY %I.%I;', index_namespace.nspname, index_relation.relname)
FROM pg_index AS index_state
JOIN pg_class AS index_relation ON index_relation.oid = index_state.indexrelid
JOIN pg_namespace AS index_namespace ON index_namespace.oid = index_relation.relnamespace
WHERE index_state.indrelid = 'public.creators'::regclass
  AND index_relation.relname IN (
      'idx_creators_channel_name_browseable_trgm',
      'idx_creators_custom_url_browseable_trgm',
      'idx_creators_primary_category_browseable_trgm',
      'idx_creators_keywords_browseable_trgm',
      'idx_creators_channel_description_browseable_trgm',
      'idx_creators_topic_categories_browseable_trgm'
  )
  AND (NOT index_state.indisvalid OR NOT index_state.indisready)
\gexec

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_channel_name_browseable_trgm
    ON public.creators USING GIN (channel_name gin_trgm_ops)
    WHERE sync_status IN ('synced', 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_creators_channel_name_search_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_custom_url_browseable_trgm
    ON public.creators USING GIN (custom_url gin_trgm_ops)
    WHERE sync_status IN ('synced', 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_creators_custom_url_search_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_primary_category_browseable_trgm
    ON public.creators USING GIN (primary_category gin_trgm_ops)
    WHERE sync_status IN ('synced', 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_creators_primary_category_search_trgm;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_creators_primary_category_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_keywords_browseable_trgm
    ON public.creators USING GIN (keywords gin_trgm_ops)
    WHERE sync_status IN ('synced', 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_creators_keywords_search_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_channel_description_browseable_trgm
    ON public.creators USING GIN (channel_description gin_trgm_ops)
    WHERE sync_status IN ('synced', 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_creators_channel_description_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_topic_categories_browseable_trgm
    ON public.creators USING GIN (topic_categories gin_trgm_ops)
    WHERE sync_status IN ('synced', 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_creators_topic_categories_trgm;

CREATE OR REPLACE FUNCTION public.search_creators_ranked(
    p_search text,
    p_sort text DEFAULT 'subscribers',
    p_limit integer DEFAULT 50,
    p_offset integer DEFAULT 0
)
RETURNS TABLE (
    creator jsonb,
    total_count bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    WITH params AS (
        SELECT
            LOWER(LTRIM(COALESCE(p_search, ''), '@')) AS q,
            (
                '%' ||
                REPLACE(
                    REPLACE(
                        REPLACE(LOWER(LTRIM(COALESCE(p_search, ''), '@')), '\', '\\'),
                        '%',
                        '\%'
                    ),
                    '_',
                    '\_'
                ) ||
                '%'
            ) AS q_pattern,
            GREATEST(0, LEAST(COALESCE(p_limit, 50), 100)) AS lim,
            GREATEST(0, COALESCE(p_offset, 0)) AS off,
            COALESCE(p_sort, 'subscribers') AS sort_key
    ),
    matched AS (
        SELECT
            c.*,
            CASE
                WHEN LOWER(LTRIM(c.custom_url, '@')) = p.q THEN 0
                WHEN LOWER(c.channel_name) = p.q THEN 1
                WHEN LOWER(LTRIM(c.custom_url, '@')) LIKE p.q || '%' THEN 2
                WHEN LOWER(c.channel_name) LIKE p.q || '%' THEN 3
                WHEN c.channel_name ILIKE p.q_pattern ESCAPE '\' THEN 4
                WHEN c.custom_url ILIKE p.q_pattern ESCAPE '\' THEN 5
                WHEN c.primary_category ILIKE p.q_pattern ESCAPE '\' THEN 6
                WHEN c.keywords ILIKE p.q_pattern ESCAPE '\' THEN 7
                WHEN c.channel_description ILIKE p.q_pattern ESCAPE '\' THEN 8
                WHEN c.topic_categories ILIKE p.q_pattern ESCAPE '\' THEN 9
                ELSE 10
            END AS match_rank
        FROM public.creators c
        CROSS JOIN params p
        WHERE p.q <> ''
          AND c.channel_name IS NOT NULL
          AND c.current_subscribers > 0
          AND c.sync_status IN ('synced', 'synced_partial')
          AND (
              c.channel_name ILIKE p.q_pattern ESCAPE '\'
              OR c.custom_url ILIKE p.q_pattern ESCAPE '\'
              OR c.primary_category ILIKE p.q_pattern ESCAPE '\'
              OR c.keywords ILIKE p.q_pattern ESCAPE '\'
              OR c.channel_description ILIKE p.q_pattern ESCAPE '\'
              OR c.topic_categories ILIKE p.q_pattern ESCAPE '\'
          )
    ),
    counted AS (
        SELECT
            matched.*,
            COUNT(*) OVER () AS total_rows
        FROM matched
    )
    SELECT
        TO_JSONB(counted) - 'match_rank' - 'total_rows' AS creator,
        counted.total_rows::bigint AS total_count
    FROM counted
    CROSS JOIN params p
    ORDER BY
        counted.match_rank ASC,
        CASE WHEN p.sort_key = 'views' THEN counted.current_view_count END DESC NULLS LAST,
        CASE WHEN p.sort_key = 'videos' THEN counted.current_video_count END DESC NULLS LAST,
        CASE WHEN p.sort_key = 'engagement' THEN counted.engagement_score END DESC NULLS LAST,
        CASE WHEN p.sort_key = 'quality' THEN
            CASE counted.quality_grade WHEN 'A+' THEN 1 WHEN 'A' THEN 2 WHEN 'B+' THEN 3 WHEN 'B' THEN 4 WHEN 'C' THEN 5 ELSE 99 END
        END ASC NULLS LAST,
        CASE WHEN p.sort_key = 'recent' THEN counted.last_updated_at END DESC NULLS LAST,
        CASE WHEN p.sort_key = 'consistency' THEN counted.monthly_uploads END DESC NULLS LAST,
        CASE WHEN p.sort_key = 'newest_channel' THEN counted.published_at END DESC NULLS LAST,
        CASE WHEN p.sort_key = 'oldest_channel' THEN counted.published_at END ASC NULLS LAST,
        counted.current_subscribers DESC NULLS LAST
    LIMIT (SELECT lim FROM params)
    OFFSET (SELECT off FROM params);
$$;

COMMENT ON FUNCTION public.search_creators_ranked(text, text, integer, integer) IS
    'Ranked creator search across handles, names, categories, topics, profile descriptions, and keywords. Exact matches rank first; substring predicates are backed by browseable-row trigram indexes.';
