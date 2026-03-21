-- 1a. Index (CONCURRENTLY = no table lock on production)
CREATE INDEX IF NOT EXISTS idx_creators_category_synced
ON creators (primary_category)
WHERE sync_status = 'synced' AND current_subscribers > 0;


-- 1b. RPC — called by the worker, not by the web app
CREATE OR REPLACE FUNCTION get_category_box_stats(p_category text)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  SELECT jsonb_build_object(
    'subscribers', jsonb_build_object(
      'min',    MIN(current_subscribers),
      'p25',    percentile_cont(0.25) WITHIN GROUP (ORDER BY current_subscribers),
      'median', percentile_cont(0.50) WITHIN GROUP (ORDER BY current_subscribers),
      'p75',    percentile_cont(0.75) WITHIN GROUP (ORDER BY current_subscribers),
      'max',    MAX(current_subscribers),
      'count',  COUNT(*)
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


-- 1c. Cache table — written by worker, read by web app
CREATE TABLE IF NOT EXISTS category_stats_cache (
  category      text PRIMARY KEY,
  stats_json    jsonb        NOT NULL,
  creator_count int          NOT NULL DEFAULT 0,
  refreshed_at  timestamptz  NOT NULL DEFAULT now()
);

COMMENT ON TABLE category_stats_cache IS
  'Pre-aggregated box plot stats per category. Populated by bootstrap_creators.py. '
  'Web app reads this; never calls get_category_box_stats() RPC directly.';
