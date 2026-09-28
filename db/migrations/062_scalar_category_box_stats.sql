-- Migration 062: return batched category stats as one scalar JSON value
--
-- PostgREST applies its configured row cap to table-returning RPCs. The
-- previous get_all_category_box_stats() returned one row per category, so a
-- sufficiently large taxonomy could be silently truncated before the worker
-- cached it. This preserves the one-call grouped calculation while returning
-- one scalar JSON object containing every category and server-side duration.
--
-- DROP is required because PostgreSQL does not allow changing a function's
-- return type with CREATE OR REPLACE.

DROP FUNCTION IF EXISTS public.get_all_category_box_stats();

CREATE FUNCTION public.get_all_category_box_stats()
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    _started_at  timestamptz := clock_timestamp();
    _categories  jsonb;
BEGIN
    SELECT COALESCE(jsonb_object_agg(category, stats_json), '{}'::jsonb)
    INTO _categories
    FROM (
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
        GROUP BY primary_category
    ) AS category_stats;

    RETURN jsonb_build_object(
        'categories', _categories,
        'duration_ms', GREATEST(
            0,
            FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - _started_at)) * 1000)::bigint
        )
    );
END;
$$;

COMMENT ON FUNCTION public.get_all_category_box_stats() IS
    'Returns one scalar JSON object containing box-plot percentile stats for '
    'every synced primary_category, avoiding the PostgREST row cap. The object '
    'includes categories and server-side duration_ms for production timing.';
