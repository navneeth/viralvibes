-- =============================================================================
-- Migration: fix REFRESH MATERIALIZED VIEW CONCURRENTLY on mv_hero_stats
--            and mv_lists_meta.
--
-- Root cause: both views are single-row aggregates with no unique column.
--             CONCURRENTLY requires at least one unique index on the view.
-- Fix: recreate each view with a surrogate `id = 1` column, then index it.
--      The RPC function (refresh_hero_stats_cache) is unchanged.
-- =============================================================================

BEGIN;

-- =============================================================================
-- 1. mv_hero_stats
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS public.mv_hero_stats;

CREATE MATERIALIZED VIEW public.mv_hero_stats AS
SELECT
    1                                                                          AS id,
    count(*)                                                                   AS total_creators,
    round(COALESCE(avg(engagement_score), 0::numeric), 2)                     AS avg_engagement,
    CASE
        WHEN count(*) FILTER (WHERE engagement_score IS NOT NULL) > 0 THEN true
        ELSE false
    END                                                                        AS has_engagement_data,
    count(*) FILTER (WHERE subscribers_change_30d > 0)                        AS growing_creators,
    count(*) FILTER (
        WHERE quality_grade::text = ANY (
            ARRAY['A+'::character varying, 'A'::character varying]::text[]
        )
    )                                                                          AS premium_creators,
    count(DISTINCT country_code)                                               AS total_countries,
    count(DISTINCT default_language)                                           AS total_languages
FROM public.creators
WHERE
    channel_name IS NOT NULL
    AND current_subscribers > 0
    AND sync_status::text = 'synced';

CREATE UNIQUE INDEX idx_mv_hero_stats_id ON public.mv_hero_stats (id);


-- =============================================================================
-- 2. mv_lists_meta
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS public.mv_lists_meta;

CREATE MATERIALIZED VIEW public.mv_lists_meta AS
SELECT
    1                                                                          AS id,
    count(*)                                                                   AS total_creators,
    count(DISTINCT country_code) FILTER (WHERE country_code IS NOT NULL)      AS total_countries,
    count(DISTINCT default_language) FILTER (WHERE default_language IS NOT NULL) AS total_languages,
    (
        SELECT count(DISTINCT category.value)
        FROM public.creators creators_1,
             LATERAL jsonb_array_elements_text(
                 CASE
                     WHEN jsonb_typeof(creators_1.topic_categories::jsonb) = 'array'::text
                         THEN creators_1.topic_categories::jsonb
                     ELSE '[]'::jsonb
                 END
             ) category(value)
        WHERE
            creators_1.channel_name IS NOT NULL
            AND creators_1.current_subscribers > 0
            AND creators_1.topic_categories IS NOT NULL
    )                                                                          AS total_categories
FROM public.creators
WHERE
    channel_name IS NOT NULL
    AND current_subscribers > 0;

CREATE UNIQUE INDEX idx_mv_lists_meta_id ON public.mv_lists_meta (id);


COMMIT;
