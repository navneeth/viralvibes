-- Migration 037: Restore fast get_lists_meta() cached-table lookup.
--
-- Migration 034 accidentally reintroduced live aggregation into get_lists_meta(),
-- including a DISTINCT category unnest over creators. On a large Supabase table
-- this can exceed the PostgREST statement timeout and force the app into a
-- degraded 1,000-row fallback scan.
--
-- The intended architecture from migrations 013-015 is:
--   - mv_lists_meta: precomputed total_creators / total_countries / total_languages
--   - app_stats('total_categories'): separately refreshed exact category count
--   - get_lists_meta(): cheap read from those cached tables

CREATE OR REPLACE FUNCTION public.get_lists_meta()
RETURNS TABLE (
    total_creators   bigint,
    total_countries  bigint,
    total_categories bigint,
    total_languages  bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    SELECT
        COALESCE(m.total_creators, 0)::bigint    AS total_creators,
        COALESCE(m.total_countries, 0)::bigint   AS total_countries,
        COALESCE(s.value, 0)::bigint             AS total_categories,
        COALESCE(m.total_languages, 0)::bigint   AS total_languages
    FROM public.mv_lists_meta m
    LEFT JOIN public.app_stats s
      ON s.key = 'total_categories'
    LIMIT 1;
$$;

COMMENT ON FUNCTION public.get_lists_meta() IS
    'Fast cached list metadata lookup. Reads mv_lists_meta plus app_stats(total_categories); does not scan creators.';
