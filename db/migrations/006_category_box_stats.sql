-- Migration 006: category box plot stats
-- Run each block separately in Supabase SQL Editor.

-- ── Block 1: Index ────────────────────────────────────────────────────────────
-- DROP CONCURRENTLY: Supabase SQL Editor wraps statements in a transaction;
-- CONCURRENTLY is forbidden inside a transaction block.
-- The brief table lock is acceptable at current write volumes.
CREATE INDEX IF NOT EXISTS idx_creators_category_synced
ON creators (primary_category)
WHERE sync_status = 'synced' AND current_subscribers > 0;


-- ── Block 2: RPC function ─────────────────────────────────────────────────────
-- count is hoisted to the top level — it is the same value for every metric
-- (total synced creators with subscribers > 0 in this category), so repeating
-- it inside each metric object would be redundant.
--
-- Returned shape:
--   {
--     "count": 142,
--     "subscribers":     {min, p25, median, p75, max},
--     "views":           {min, p25, median, p75, max},
--     "engagement":      {min, p25, median, p75, max},
--     "monthly_uploads": {min, p25, median, p75, max}
--   }
CREATE OR REPLACE FUNCTION get_category_box_stats(p_category text)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  SELECT jsonb_build_object(
    'count', COUNT(*),
    'subscribers', jsonb_build_object(
      'min',    MIN(current_subscribers),
      'p25',    percentile_cont(0.25) WITHIN GROUP (ORDER BY current_subscribers),
      'median', percentile_cont(0.50) WITHIN GROUP (ORDER BY current_subscribers),
      'p75',    percentile_cont(0.75) WITHIN GROUP (ORDER BY current_subscribers),
      'max',    MAX(current_subscribers)
    ),
    'views', jsonb_build_object(
      'min',    MIN(current_view_count),
      'p25',    percentile_cont(0.25) WITHIN GROUP (ORDER BY current_view_count),
      'median', percentile_cont(0.50) WITHIN GROUP (ORDER BY current_view_count),
      'p75',    percentile_cont(0.75) WITHIN GROUP (ORDER BY current_view_count),
      'max',    MAX(current_view_count)
    ),
    'engagement', jsonb_build_object(
      'min',    MIN(engagement_score),
      'p25',    percentile_cont(0.25) WITHIN GROUP (ORDER BY engagement_score),
      'median', percentile_cont(0.50) WITHIN GROUP (ORDER BY engagement_score),
      'p75',    percentile_cont(0.75) WITHIN GROUP (ORDER BY engagement_score),
      'max',    MAX(engagement_score)
    ),
    'monthly_uploads', jsonb_build_object(
      'min',    MIN(monthly_uploads),
      'p25',    percentile_cont(0.25) WITHIN GROUP (ORDER BY monthly_uploads),
      'median', percentile_cont(0.50) WITHIN GROUP (ORDER BY monthly_uploads),
      'p75',    percentile_cont(0.75) WITHIN GROUP (ORDER BY monthly_uploads),
      'max',    MAX(monthly_uploads)
    )
  )
  FROM creators
  WHERE primary_category = p_category
    AND sync_status = 'synced'
    AND current_subscribers > 0;
$$;


-- ── Block 3: Cache table ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS category_stats_cache (
  category      text PRIMARY KEY,
  stats_json    jsonb        NOT NULL,
  creator_count int          NOT NULL DEFAULT 0,
  refreshed_at  timestamptz  NOT NULL DEFAULT now()
);

COMMENT ON TABLE category_stats_cache IS
  'Pre-aggregated box plot stats per category. '
  'Written by bootstrap_creators.py (Pass 4). '
  'Read by the web app via get_cached_category_box_stats().';


-- ── Verify ────────────────────────────────────────────────────────────────────
-- SELECT indexname FROM pg_indexes WHERE tablename = 'creators';
-- SELECT get_category_box_stats('Technology');
-- SELECT * FROM category_stats_cache LIMIT 1;
