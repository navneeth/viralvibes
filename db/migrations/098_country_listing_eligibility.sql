-- This migration creates a view for eligible creators based on specific criteria and provides functions to retrieve preview creators, top countries, and creator ranks.
BEGIN;

CREATE OR REPLACE VIEW public.country_list_creators
WITH (security_invoker = true)
AS
SELECT creator.* FROM public.creators AS creator
WHERE creator.archived_at IS NULL
  AND creator.sync_status IN ('synced', 'synced_partial')
  AND creator.country_code ~ '^[A-Z]{2}$'
  AND creator.channel_name IS NOT NULL
  AND btrim(creator.channel_name) <> ''
  AND creator.current_subscribers > 0;

REVOKE ALL ON public.country_list_creators FROM PUBLIC;
GRANT SELECT ON public.country_list_creators TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_country_preview_creators(
    p_country_codes text[], p_limit_per_country integer DEFAULT 5
)
-- Keep the original row type so existing callers of this public RPC remain
-- compatible. PostgreSQL rejects changing SETOF public.creators to RETURNS TABLE
-- with CREATE OR REPLACE; a new versioned RPC is required for a narrower shape.
RETURNS SETOF public.creators
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF cardinality(COALESCE(p_country_codes, ARRAY[]::text[])) > 100 THEN
        RAISE EXCEPTION 'At most 100 country codes may be requested';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM unnest(COALESCE(p_country_codes, ARRAY[]::text[])) requested(country_code)
        WHERE requested.country_code IS NULL
           OR requested.country_code !~ '^[A-Z]{2}$'
    ) THEN
        RAISE EXCEPTION 'Country codes must be two uppercase letters';
    END IF;

    RETURN QUERY
    SELECT listed.*
    FROM (
        SELECT DISTINCT country_code
        FROM unnest(COALESCE(p_country_codes, ARRAY[]::text[])) requested(country_code)
    ) requested
    CROSS JOIN LATERAL (
        SELECT preview.* FROM public.country_list_creators AS preview
        WHERE preview.country_code = requested.country_code
        ORDER BY preview.current_subscribers DESC, preview.id
        LIMIT GREATEST(LEAST(COALESCE(p_limit_per_country, 0), 100), 0)
    ) listed
    ORDER BY listed.country_code, listed.current_subscribers DESC, listed.id;
END;
$$;

REVOKE ALL ON FUNCTION public.get_country_preview_creators(text[], integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_country_preview_creators(text[], integer)
TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_top_countries_with_counts(p_limit integer DEFAULT 10)
RETURNS TABLE (country_code text, creator_count bigint)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
    SELECT listed.country_code, count(*)::bigint
    FROM public.country_list_creators AS listed
    GROUP BY listed.country_code
    ORDER BY count(*) DESC, listed.country_code
    LIMIT GREATEST(LEAST(COALESCE(p_limit, 0), 500), 0);
$$;

CREATE OR REPLACE FUNCTION public.get_country_creator_rank(p_creator_id uuid)
RETURNS bigint
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
    SELECT 1 + (
        SELECT count(*) FROM public.country_list_creators AS peer
        WHERE peer.country_code = target.country_code
          AND (peer.current_subscribers > target.current_subscribers
               OR (peer.current_subscribers = target.current_subscribers AND peer.id < target.id))
    )
    FROM public.country_list_creators AS target WHERE target.id = p_creator_id;
$$;

REVOKE ALL ON FUNCTION public.get_country_creator_rank(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.get_country_creator_rank(uuid)
TO anon, authenticated, service_role;

COMMIT;
