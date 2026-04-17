-- Migration 020: RPC for distinct synced categories
--
-- Fixes refresh_category_stats_cache() in db.py which called:
--
--   .table(CREATOR_TABLE)
--   .select("primary_category")
--   .eq("sync_status", "synced")
--   .gt("current_subscribers", 0)
--   .not_.is_("primary_category", None)
--   .execute()
--
-- PostgREST applies a server-side row limit (default 1,000) to all table
-- queries with no explicit limit.  With 117k+ qualifying rows the first 1,000
-- returned happened to share a single primary_category, so Pass 4 only ever
-- refreshed ONE category instead of all ~62.
--
-- This RPC returns the distinct set in a single DB-side aggregation.
-- It uses the partial index idx_creators_category_synced (added in migration 006)
-- so the scan is O(distinct categories), not O(total rows).

CREATE OR REPLACE FUNCTION public.get_distinct_synced_categories()
RETURNS TABLE (primary_category text)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
    SELECT DISTINCT primary_category
    FROM public.creators
    WHERE sync_status = 'synced'
      AND current_subscribers > 0
      AND primary_category IS NOT NULL
    ORDER BY primary_category;
$$;

COMMENT ON FUNCTION public.get_distinct_synced_categories() IS
    'Returns the distinct set of primary_category values for synced creators with '
    'subscribers > 0.  Used by refresh_category_stats_cache() to drive the '
    'per-category box-plot RPC loop.  Uses idx_creators_category_synced for '
    'efficient O(distinct categories) execution at any table size.';
