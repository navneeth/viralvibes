-- The populated hero-stats view is a single-row aggregate. A plain-column
-- unique index qualifies for concurrent refresh; the old expression index does not.
-- Install the function only: this migration does not execute a refresh.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '10s';

CREATE UNIQUE INDEX IF NOT EXISTS mv_hero_stats_total_creators_unique_idx
ON public.mv_hero_stats (total_creators);

CREATE OR REPLACE FUNCTION public.refresh_mv_hero_stats()
RETURNS TABLE (
    materialized_view text,
    rows_refreshed bigint,
    refresh_duration_ms bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    start_time timestamptz;
    end_time timestamptz;
    row_count bigint;
BEGIN
    start_time := clock_timestamp();

    REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_hero_stats;

    end_time := clock_timestamp();
    SELECT total_creators INTO row_count
    FROM public.mv_hero_stats;

    RETURN QUERY SELECT
        'mv_hero_stats'::text,
        row_count,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::bigint;
END;
$function$;

COMMIT;

SELECT indexrelid::regclass AS index_name,
       indisunique,
       indisvalid,
       indisready,
       indpred IS NULL AS covers_all_rows,
       indexprs IS NULL AS uses_plain_columns
FROM pg_index
WHERE indrelid = 'public.mv_hero_stats'::regclass;
