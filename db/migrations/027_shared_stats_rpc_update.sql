-- 1. Drop the old function signature to resolve the "parameter defaults" error
DROP FUNCTION IF EXISTS public.get_top_countries_with_counts(integer);

-- 2. Create the optimized version aligned with Migration 003 and db_lists.py logic
CREATE OR REPLACE FUNCTION public.get_top_countries_with_counts(p_limit integer DEFAULT 10)
RETURNS TABLE (country_code text, creator_count bigint)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT
        c.country_code,
        COUNT(*)::bigint AS creator_count
    FROM public.creators c
    WHERE c.channel_name IS NOT NULL
      AND c.country_code IS NOT NULL
      AND c.sync_status = 'synced'      -- Crucial: Matches db_lists.py fallback logic
      AND c.current_subscribers > 0     -- Crucial: Matches db_lists.py fallback logic
    GROUP BY c.country_code
    ORDER BY creator_count DESC
    LIMIT p_limit;
$$;

-- 3. Add a comment for clarity
COMMENT ON FUNCTION public.get_top_countries_with_counts(integer) IS
    'Returns top countries by synced creator count. Optimized to avoid timeouts.';
