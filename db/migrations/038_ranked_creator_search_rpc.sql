-- Migration 038: Ranked creator search RPC for /creators.
--
-- This is the long-term replacement for the PostgREST OR-ILIKE query used by
-- db.get_creators(search=...). It keeps ranking logic in Postgres, allows exact
-- handle/name matches to beat keyword noise, and pairs with the trigram indexes
-- from migration 036.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

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
                ELSE 99
            END AS match_rank
        FROM public.creators c
        CROSS JOIN params p
        WHERE p.q <> ''
          AND c.channel_name IS NOT NULL
          AND c.current_subscribers > 0
          AND c.sync_status IN ('synced', 'pending')
          AND (
              LOWER(LTRIM(c.custom_url, '@')) = p.q
              OR LOWER(c.channel_name) = p.q
              OR LOWER(LTRIM(c.custom_url, '@')) LIKE p.q || '%'
              OR LOWER(c.channel_name) LIKE p.q || '%'
              OR c.channel_name ILIKE p.q_pattern ESCAPE '\'
              OR c.custom_url ILIKE p.q_pattern ESCAPE '\'
              OR c.primary_category ILIKE p.q_pattern ESCAPE '\'
              OR c.keywords ILIKE p.q_pattern ESCAPE '\'
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
        CASE WHEN p.sort_key = 'quality' THEN counted.quality_grade END ASC NULLS LAST,
        CASE WHEN p.sort_key = 'recent' THEN counted.last_updated_at END DESC NULLS LAST,
        CASE WHEN p.sort_key = 'consistency' THEN counted.monthly_uploads END DESC NULLS LAST,
        CASE WHEN p.sort_key = 'newest_channel' THEN counted.published_at END DESC NULLS LAST,
        CASE WHEN p.sort_key = 'oldest_channel' THEN counted.published_at END ASC NULLS LAST,
        counted.current_subscribers DESC NULLS LAST
    LIMIT (SELECT lim FROM params)
    OFFSET (SELECT off FROM params);
$$;

COMMENT ON FUNCTION public.search_creators_ranked(text, text, integer, integer) IS
    'Ranked creator search for /creators. Prioritizes exact handle/name matches before broad trigram-backed text matches.';
