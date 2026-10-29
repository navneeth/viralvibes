-- Read only the requested top-N creators from each country's existing
-- country/subscriber index. The original window query ranked every eligible
-- creator in the requested countries before returning the small preview set.
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
    SELECT c.*
    FROM (
        SELECT DISTINCT country_code
        FROM unnest(COALESCE(p_country_codes, ARRAY[]::text[])) AS codes(country_code)
    ) requested
    CROSS JOIN LATERAL (
        SELECT creator.*
        FROM public.creators creator
        WHERE creator.country_code = requested.country_code
          AND creator.sync_status = 'synced'
          AND creator.channel_name IS NOT NULL
          AND creator.current_subscribers > 0
        ORDER BY creator.current_subscribers DESC, creator.id
        LIMIT GREATEST(COALESCE(p_limit_per_country, 0), 0)
    ) c
    ORDER BY c.country_code, c.current_subscribers DESC, c.id;
$$;

COMMENT ON FUNCTION public.get_country_preview_creators(text[], integer) IS
    'Returns top synced creators per requested country for /lists preview cards.';
