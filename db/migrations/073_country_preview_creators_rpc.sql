-- Fetch a bounded top-N preview for each country without a global row cap.
-- Keep the population aligned with get_top_countries_with_counts() and
-- get_country_creators().
CREATE OR REPLACE FUNCTION public.get_country_preview_creators(
    p_country_codes text[],
    p_limit_per_country integer DEFAULT 5
)
RETURNS SETOF public.creators
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
    WITH ranked AS (
        SELECT
            c.id,
            c.country_code,
            c.current_subscribers,
            ROW_NUMBER() OVER (
                PARTITION BY c.country_code
                ORDER BY c.current_subscribers DESC, c.id
            ) AS country_rank
        FROM public.creators c
        WHERE c.country_code = ANY(p_country_codes)
          AND c.sync_status = 'synced'
          AND c.channel_name IS NOT NULL
          AND c.current_subscribers > 0
    )
    SELECT c.*
    FROM ranked r
    JOIN public.creators c ON c.id = r.id
    WHERE r.country_rank <= GREATEST(COALESCE(p_limit_per_country, 0), 0)
    ORDER BY r.country_code, r.current_subscribers DESC, r.id;
$$;

COMMENT ON FUNCTION public.get_country_preview_creators(text[], integer) IS
    'Returns top synced creators per requested country for /lists preview cards.';
