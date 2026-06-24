-- Migration 044: Add total_db_creators to mv_hero_stats
--
-- Context: The /creators page hero section wants to display the full size of
-- the creators database (all sync_status values) to showcase scale, while
-- total_creators in mv_hero_stats only counts sync_status='synced' rows and
-- is used as the browseable pool denominator for filtered searches.
--
-- Problem: A live HEAD request (SELECT id FROM creators for COUNT) on the
-- 800K-row table was added to db.py to get this number, defeating the purpose
-- of the materialized view caching system (see migrations 009 and 025).
--
-- Fix: Add total_db_creators = COUNT(*) to mv_hero_stats so it is pre-computed
-- alongside total_creators and costs nothing to read on page load.
-- The existing refresh_mv_hero_stats() function automatically picks up the new
-- column on the next worker Pass 5 refresh cycle.
--
-- Note: total_db_creators counts every row in the creators table with no
-- filter — it will include pending, failed, invalid, and synced_partial rows.
-- This is intentional: the goal is to showcase the full dataset size.

-- 1. Recreate mv_hero_stats with the new column.
--    REFRESH MATERIALIZED VIEW cannot add columns; we must DROP and recreate.
DROP MATERIALIZED VIEW IF EXISTS public.mv_hero_stats CASCADE;

CREATE MATERIALIZED VIEW public.mv_hero_stats AS
SELECT
    -- Synced, browseable pool (filter denominator for /creators page)
    COUNT(*) FILTER (
        WHERE channel_name IS NOT NULL
          AND current_subscribers > 0
          AND sync_status = 'synced'
    ) AS total_creators,
    -- Full DB size (all sync_status values) for hero marketing headline
    COUNT(*) AS total_db_creators,
    ROUND(
        COALESCE(
            AVG(engagement_score) FILTER (
                WHERE channel_name IS NOT NULL
                  AND current_subscribers > 0
                  AND sync_status = 'synced'
            ),
            0
        )::numeric,
        2
    ) AS avg_engagement,
    CASE
        WHEN COUNT(*) FILTER (
            WHERE engagement_score IS NOT NULL
              AND sync_status = 'synced'
        ) > 0 THEN TRUE
        ELSE FALSE
    END AS has_engagement_data,
    COUNT(*) FILTER (
        WHERE subscribers_change_30d > 0
          AND sync_status = 'synced'
    ) AS growing_creators,
    COUNT(*) FILTER (
        WHERE quality_grade IN ('A+', 'A')
          AND sync_status = 'synced'
    ) AS premium_creators
FROM public.creators;

-- Recreate unique index (required for CONCURRENTLY refresh)
CREATE UNIQUE INDEX mv_hero_stats_unique_idx ON public.mv_hero_stats ((1));

COMMENT ON MATERIALIZED VIEW public.mv_hero_stats IS
'Pre-computed aggregate stats for /creators hero section. '
'total_creators = synced only (filter denominator). '
'total_db_creators = all rows (hero headline). '
'Refreshed by bootstrap_creators.py Pass 5 via refresh_mv_hero_stats().';


-- 2. Update get_creator_hero_stats() RPC to return the new column.
DROP FUNCTION IF EXISTS public.get_creator_hero_stats() CASCADE;

CREATE OR REPLACE FUNCTION public.get_creator_hero_stats()
RETURNS TABLE (
    total_creators      BIGINT,
    total_db_creators   BIGINT,
    avg_engagement      NUMERIC,
    has_engagement_data BOOLEAN,
    growing_creators    BIGINT,
    premium_creators    BIGINT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT
        total_creators,
        total_db_creators,
        avg_engagement,
        has_engagement_data,
        growing_creators,
        premium_creators
    FROM public.mv_hero_stats
    LIMIT 1;
$$;

COMMENT ON FUNCTION public.get_creator_hero_stats() IS
'Reads pre-computed hero stats from mv_hero_stats materialized view (~1ms). '
'total_db_creators added in migration 044 to surface full DB size in hero headline.';


-- 3. Trigger an immediate refresh so the new column is populated.
--    refresh_mv_hero_stats() sets statement_timeout=0 internally (migration 025).
SELECT * FROM public.refresh_mv_hero_stats();
