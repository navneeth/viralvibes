-- Migration 047: Keyword search RPC for creator discovery
--
-- Provides full-text search over transcript_keywords added in migration 046.
-- Uses the idx_creators_transcript_keywords_fts GIN index (tsvector over kw strings).
--
-- "keyboard" matches creators with keywords like "budget mechanical keyboard",
-- "mechanical switches", "custom keyboards" — stemming handled by Postgres.
--
-- Called from the Supabase client:
--   sb.rpc("search_creators_by_keyword", {
--       "search_query": "mechanical keyboard",
--       "p_category":   "Science & Technology",   -- optional
--       "p_min_subs":   100000,                   -- optional
--       "p_limit":      50,
--   }).execute()

CREATE OR REPLACE FUNCTION public.search_creators_by_keyword(
    search_query  TEXT,
    p_category    TEXT    DEFAULT NULL,
    p_min_subs    BIGINT  DEFAULT NULL,
    p_max_subs    BIGINT  DEFAULT NULL,
    p_limit       INT     DEFAULT 50,
    p_offset      INT     DEFAULT 0
)
RETURNS TABLE (
    id                          UUID,
    channel_name                TEXT,
    channel_id                  TEXT,
    custom_url                  TEXT,
    primary_category            TEXT,
    current_subscribers         BIGINT,
    engagement_score            NUMERIC,
    transcript_keywords         JSONB,
    transcript_keywords_updated_at TIMESTAMPTZ,
    total_count                 BIGINT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    WITH matched AS (
        SELECT
            c.id,
            c.channel_name,
            c.channel_id,
            c.custom_url,
            c.primary_category,
            c.current_subscribers,
            c.engagement_score,
            c.transcript_keywords,
            c.transcript_keywords_updated_at,
            -- Rank by relevance score: sum of keyword scores for matching phrases
            -- so a creator whose top keyword IS the search term ranks above one
            -- where it appears as a low-weight bigram.
            (
                SELECT COALESCE(SUM((kw_item->>'score')::NUMERIC), 0)
                FROM jsonb_array_elements(c.transcript_keywords) AS kw_item
                WHERE to_tsvector('english', kw_item->>'kw')
                      @@ plainto_tsquery('english', search_query)
            ) AS relevance
        FROM public.creators c
        WHERE
            c.sync_status = 'synced'
            AND c.transcript_keywords IS NOT NULL
            -- FTS match — uses idx_creators_transcript_keywords_fts
            AND jsonb_to_tsvector('english', c.transcript_keywords, '["string"]')
                @@ plainto_tsquery('english', search_query)
            -- Optional filters
            AND (p_category IS NULL OR c.primary_category = p_category)
            AND (p_min_subs IS NULL OR c.current_subscribers >= p_min_subs)
            AND (p_max_subs IS NULL OR c.current_subscribers <= p_max_subs)
    ),
    counted AS (
        SELECT *, COUNT(*) OVER () AS total_count
        FROM matched
        ORDER BY relevance DESC, current_subscribers DESC
        LIMIT  GREATEST(1, LEAST(COALESCE(p_limit, 50), 200))
        OFFSET GREATEST(0, COALESCE(p_offset, 0))
    )
    SELECT
        id, channel_name, channel_id, custom_url,
        primary_category, current_subscribers, engagement_score,
        transcript_keywords, transcript_keywords_updated_at,
        total_count
    FROM counted;
$$;

-- Grant execute to the anon and authenticated roles used by Supabase client
GRANT EXECUTE ON FUNCTION public.search_creators_by_keyword(
    TEXT, TEXT, BIGINT, BIGINT, INT, INT
) TO anon, authenticated;

COMMENT ON FUNCTION public.search_creators_by_keyword IS
    'Full-text search over creator transcript keywords extracted by KeyBERT. '
    '"keyboard" matches creators with keywords like "budget mechanical keyboard", '
    '"mechanical switches", etc. Supports optional category and subscriber filters. '
    'Returns results ranked by keyword relevance score then subscriber count.';
