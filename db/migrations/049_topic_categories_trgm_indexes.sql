-- Migration 049: GIN trigram index on topic_categories for fast ilike
--
-- Problem
-- -------
-- topic_categories is a text column storing JSON arrays as strings
-- (e.g. '["Volleyball", "Sports"]').  The PostgREST containment operator
-- (cs.) requires jsonb; on a text column it always fails with:
--   42883: operator does not exist: text @> unknown
--
-- The ilike fallback (e.g. '%Volleyball%') works correctly but requires
-- a full table scan → 57014 statement timeout on large categories.
--
-- Fix
-- ---
-- pg_trgm is pre-installed in Supabase.  A GIN trigram index enables
-- fast case-insensitive substring matching, turning the full-table scan
-- into an index seek.
--
-- Note: CONCURRENTLY is not supported inside a transaction block (e.g. the
-- Supabase SQL editor).  Run without it; the build will briefly lock the
-- table for writes but completes quickly on typical dataset sizes.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_creators_topic_categories_trgm
    ON creators
    USING GIN (topic_categories gin_trgm_ops)
    WHERE topic_categories IS NOT NULL;
