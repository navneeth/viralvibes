-- Migration 051: Fix search_creators_ranked RPC to include synced_partial creators
--
-- Problem: Migration 045 changed the browseable sync status set from
-- ('synced', 'pending') to ('synced', 'synced_partial'). The
-- search_creators_ranked RPC (migration 038) was not updated at the same time
-- and still filters for sync_status IN ('synced', 'pending'). This means:
--
--   1. creators with sync_status = 'synced_partial' (a significant subset with
--      basic channel data) are silently excluded from all search results.
--   2. creators with sync_status = 'pending' (not browseable) may incorrectly
--      appear in search results if any such rows exist.
--
-- Fix: Replace ('synced', 'pending') with ('synced', 'synced_partial') in the
-- WHERE clause, aligning the RPC with constants.BROWSEABLE_SYNC_STATUSES.
--
-- Quality sort: uses inline CASE WHEN for grade rank so this migration runs
-- independently of migration 052 (which adds the quality_grade_rank column).
-- After migration 052 is applied the planner still evaluates the same logic,
-- just from the generated column in the heap rather than recalculating.
--
-- Uses CREATE OR REPLACE FUNCTION — sufficient because the signature
-- (argument names/types/defaults) is unchanged. No DROP is needed.

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
          AND c.sync_status IN ('synced', 'synced_partial')   -- was ('synced', 'pending')
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
    'Ranked creator search for /creators. Prioritizes exact handle/name matches before broad trigram-backed text matches. Includes both synced and synced_partial creators.';
