-- =============================================================================
-- Migration 007: Materialized Views for Hero Stats Performance
-- =============================================================================
-- Problem: With 1M+ creators, aggregate queries timeout on page load
-- Solution: Pre-compute stats in materialized views, refresh periodically
--
-- Performance impact:
--   BEFORE: Full table scan on every request (~5-30 seconds with 1M rows)
--   AFTER:  Single row read from materialized view (~1-5ms)
--
-- Refresh strategy: Called by worker/bootstrap_creators.py (Pass 5)
-- =============================================================================

-- Drop existing RPC if it exists (will be recreated to use materialized view)
DROP FUNCTION IF EXISTS get_creator_hero_stats();
DROP FUNCTION IF EXISTS get_lists_meta();

-- =============================================================================
-- 1. Hero Stats Materialized View (for /creators page hero section)
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hero_stats AS
SELECT
    COUNT(*) AS total_creators,
    ROUND(COALESCE(AVG(engagement_score), 0)::numeric, 2) AS avg_engagement,
    CASE
        WHEN COUNT(*) FILTER (WHERE engagement_score IS NOT NULL) > 0 THEN TRUE
        ELSE FALSE
    END AS has_engagement_data,
    COUNT(*) FILTER (WHERE subscribers_change_30d > 0) AS growing_creators,
    COUNT(*) FILTER (WHERE quality_grade IN ('A+', 'A')) AS premium_creators
FROM creators
WHERE channel_name IS NOT NULL
  AND current_subscribers > 0
  AND sync_status = 'synced';

-- Create unique index to enable CONCURRENTLY refresh (prevents locking)
CREATE UNIQUE INDEX IF NOT EXISTS mv_hero_stats_unique_idx ON mv_hero_stats ((1));

COMMENT ON MATERIALIZED VIEW mv_hero_stats IS
'Pre-computed aggregate stats for /creators hero section. Refreshed by bootstrap_creators.py Pass 5.';


-- =============================================================================
-- 2. Lists Meta Materialized View (for /lists page tab badges)
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_lists_meta AS
SELECT
    COUNT(*) AS total_creators,
    COUNT(DISTINCT country_code) FILTER (WHERE country_code IS NOT NULL) AS total_countries,
    COUNT(DISTINCT default_language) FILTER (WHERE default_language IS NOT NULL) AS total_languages,
    -- Category count requires unnesting topic_categories JSONB array
    (
        SELECT COUNT(DISTINCT category)
        FROM creators,
        LATERAL jsonb_array_elements_text(
            CASE
                WHEN jsonb_typeof(topic_categories::jsonb) = 'array'
                THEN topic_categories::jsonb
                ELSE '[]'::jsonb
            END
        ) AS category
        WHERE channel_name IS NOT NULL
          AND current_subscribers > 0
          AND topic_categories IS NOT NULL
    ) AS total_categories
FROM creators
WHERE channel_name IS NOT NULL
  AND current_subscribers > 0;

-- Create unique index to enable CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS mv_lists_meta_unique_idx ON mv_lists_meta ((1));

COMMENT ON MATERIALIZED VIEW mv_lists_meta IS
'Pre-computed counts for /lists page tab badges and load-more limits. Refreshed by bootstrap_creators.py Pass 5.';


-- =============================================================================
-- 3. Updated RPC Functions (read from materialized views)
-- =============================================================================

CREATE OR REPLACE FUNCTION get_creator_hero_stats()
RETURNS TABLE (
    total_creators BIGINT,
    avg_engagement NUMERIC,
    has_engagement_data BOOLEAN,
    growing_creators BIGINT,
    premium_creators BIGINT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT
        total_creators,
        avg_engagement,
        has_engagement_data,
        growing_creators,
        premium_creators
    FROM mv_hero_stats
    LIMIT 1;
$$;

COMMENT ON FUNCTION get_creator_hero_stats IS
'Reads pre-computed hero stats from mv_hero_stats materialized view. ~1ms response time.';


CREATE OR REPLACE FUNCTION get_lists_meta()
RETURNS TABLE (
    total_creators BIGINT,
    total_countries BIGINT,
    total_languages BIGINT,
    total_categories BIGINT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT
        total_creators,
        total_countries,
        total_languages,
        total_categories
    FROM mv_lists_meta
    LIMIT 1;
$$;

COMMENT ON FUNCTION get_lists_meta IS
'Reads pre-computed list metadata from mv_lists_meta materialized view. ~1ms response time.';


-- =============================================================================
-- 4. Refresh Helper Function (called by Python worker)
-- =============================================================================

CREATE OR REPLACE FUNCTION refresh_hero_stats_cache()
RETURNS TABLE (
    materialized_view TEXT,
    rows_refreshed BIGINT,
    refresh_duration_ms BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
    hero_count BIGINT;
    lists_count BIGINT;
BEGIN
    -- Refresh hero stats (CONCURRENTLY prevents table locking)
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_hero_stats;
    end_time := clock_timestamp();

    SELECT total_creators INTO hero_count FROM mv_hero_stats;

    RETURN QUERY SELECT
        'mv_hero_stats'::TEXT,
        hero_count,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::BIGINT;

    -- Refresh lists meta
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_lists_meta;
    end_time := clock_timestamp();

    SELECT total_creators INTO lists_count FROM mv_lists_meta;

    RETURN QUERY SELECT
        'mv_lists_meta'::TEXT,
        lists_count,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::BIGINT;
END;
$$;

COMMENT ON FUNCTION refresh_hero_stats_cache IS
'Refreshes both materialized views concurrently (non-blocking). Called by bootstrap_creators.py Pass 5.';


-- =============================================================================
-- 5. Initial Population
-- =============================================================================

-- Populate materialized views immediately after creation
REFRESH MATERIALIZED VIEW mv_hero_stats;
REFRESH MATERIALIZED VIEW mv_lists_meta;


-- =============================================================================
-- Usage Notes
-- =============================================================================
--
-- Manual refresh (if needed):
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_hero_stats;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_lists_meta;
--
-- Or use the helper function:
--   SELECT * FROM refresh_hero_stats_cache();
--
-- Automated refresh:
--   Python: db.refresh_hero_stats_cache() in worker/bootstrap_creators.py
--
-- Check freshness:
--   SELECT * FROM mv_hero_stats;
--   SELECT * FROM mv_lists_meta;
--
-- =============================================================================
