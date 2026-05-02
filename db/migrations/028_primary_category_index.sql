-- Migration 028: Fix /creators category filter statement timeout
--
-- Problem
-- -------
-- get_creators() filtered categories with:
--   query.ilike("topic_categories", "%Howto%Style%")
-- topic_categories is a raw JSON text column (500-5 000 bytes per row).
-- The leading-wildcard pattern forces a full sequential scan of 1M+ rows.
-- The existing GIN index (idx_creators_topic_categories_gin) is jsonb_path_ops
-- and only accelerates @> containment queries — it does nothing for ilike.
-- Result: statement timeout (error code 57014).
--
-- Fix
-- ---
-- primary_category is a clean, normalized, single-value column (10-30 bytes)
-- populated by the worker for every synced creator.
-- A pg_trgm GIN partial index on it supports arbitrary ilike patterns
-- (including leading wildcards) as an index scan instead of a seq scan.
--
-- The accompanying db.py change switches the filter target from
--   topic_categories   →   primary_category
-- and collapses the multi-word wildcard pattern to a single %term%.
--
-- Step 1: enable pg_trgm (safe to run even if already enabled; no-op in Supabase)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Step 2: GIN trigram partial index on primary_category
-- Partial WHERE mirrors get_creators() base filter so only live, synced rows
-- are indexed (far fewer rows to maintain; index stays compact).
CREATE INDEX IF NOT EXISTS idx_creators_primary_category_trgm
    ON public.creators USING GIN (primary_category gin_trgm_ops)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- Verification — run after applying:
--
-- SELECT indexname, indexdef
-- FROM pg_indexes
-- WHERE tablename = 'creators'
--   AND indexname = 'idx_creators_primary_category_trgm';
