-- Keep language-group counts aligned with mv_lists_meta and the metadata fallback.
-- This also repairs installations where the earlier sync-status RPC migration
-- was not applied before deploying the application.

CREATE OR REPLACE FUNCTION public.get_top_languages_with_counts(p_limit integer DEFAULT 10)
RETURNS TABLE (language_code text, creator_count bigint)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT
        c.default_language AS language_code,
        COUNT(*)::bigint AS creator_count
    FROM public.creators c
    WHERE c.sync_status = 'synced'
      AND c.channel_name IS NOT NULL
      AND c.default_language IS NOT NULL
      AND c.current_subscribers > 0
    GROUP BY c.default_language
    ORDER BY creator_count DESC
    LIMIT p_limit;
$$;

COMMENT ON FUNCTION public.get_top_languages_with_counts(integer) IS
    'Returns language counts for synced creators, aligned with /lists metadata.';
