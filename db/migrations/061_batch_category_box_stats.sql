-- Migration 061: batched category box-plot stats
--
-- Purpose
-- -------
-- Replaces the N+1 pattern in db.refresh_category_stats_cache() where each
-- bootstrap Pass 4 ran ~274 sequential calls to get_category_box_stats(text)
-- from migration 006, each taking ~8.1 s p50 per pg_stat_statements evidence
-- (274 x 8.1 s = ~37 min of DB CPU per pass, 5.7 M shared blocks per call).
--
-- A single GROUP BY primary_category does the same math in one heap pass over
-- the partial index idx_creators_category_synced (migration 006), so the
-- ordered-set aggregate work amortises across every category at once.  Wall
-- clock for the whole pass is expected to drop from tens of minutes to
-- tens of seconds.
--
-- Per-row jsonb shape is IDENTICAL to what get_category_box_stats() returns
-- for a single category, so the cache read path
-- (db.get_cached_category_box_stats) and every downstream view stay unchanged.
--
-- The single-category RPC from migration 006 is intentionally NOT dropped.
-- Future single-category refresh needs (admin tooling, one-off warmups) can
-- still call it.
--
-- Language flags: STABLE (matches migration 006).  No SECURITY DEFINER \u2014
-- SECURITY DEFINER forces PARALLEL RESTRICTED per pg docs, and the caller
-- (service role via PostgREST RPC) already has SELECT on public.creators.

CREATE OR REPLACE FUNCTION public.get_all_category_box_stats()
RETURNS TABLE (category text, stats_json jsonb)
LANGUAGE sql
STABLE
AS $$
    SELECT
        primary_category AS category,
        jsonb_build_object(
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
        ) AS stats_json
    FROM public.creators
    WHERE sync_status = 'synced'
      AND current_subscribers > 0
      AND primary_category IS NOT NULL
    GROUP BY primary_category;
$$;

COMMENT ON FUNCTION public.get_all_category_box_stats() IS
    'Batched replacement for the N+1 get_category_box_stats() loop in '
    'refresh_category_stats_cache().  Aggregates box-plot percentile stats '
    'for every synced primary_category in one heap pass over '
    'idx_creators_category_synced (migration 006).  Per-row stats_json shape '
    'is identical to the single-category RPC, so the cache table and read '
    'path do not change.';

-- Verify:
-- SELECT category, (stats_json->>'count')::int AS creator_count
-- FROM get_all_category_box_stats()
-- ORDER BY (stats_json->>'count')::int DESC
-- LIMIT 5;
