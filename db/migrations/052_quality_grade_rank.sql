-- Migration 052: Add quality_grade_rank generated column for correct quality sort
--
-- Problem: quality_grade stores text values 'A+', 'A', 'B+', 'B', 'C'.
-- Alphabetical ordering of these values does not match quality tier order:
--   ASC alphabetical: A, A+, B, B+, C   (A before A+ — wrong, A+ is best tier)
--   DESC alphabetical: C, B+, B, A+, A  (C first — wrong, C is lowest tier)
--
-- The correct order for "best quality first" is: A+, A, B+, B, C
-- (matching the grade_options display order on the /creators page).
--
-- Fix: Add a stored generated column quality_grade_rank (INT2) that maps each
-- grade text value to a sort-safe integer:
--   A+ → 1 (best)
--   A  → 2
--   B+ → 3
--   B  → 4
--   C  → 5
--   NULL / other → 99 (ungraded creators sort last)
--
-- Then ORDER BY quality_grade_rank ASC gives A+ first, C last.
-- Add partial indexes matching the existing browseable partial index pattern
-- (migrations 008, 043, 045) so the planner can use Merge Append.
--
-- db.py sort_map is updated in the same PR to use quality_grade_rank.
--
-- Prerequisites: migrations 008, 043, 045 (partial index foundations).

-- Add the generated column (PostgreSQL 12+ syntax, supported by Supabase)
ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS quality_grade_rank INT2
    GENERATED ALWAYS AS (
        CASE quality_grade
            WHEN 'A+' THEN 1
            WHEN 'A'  THEN 2
            WHEN 'B+' THEN 3
            WHEN 'B'  THEN 4
            WHEN 'C'  THEN 5
            ELSE 99
        END
    ) STORED;

-- Partial index for synced creators sorted by quality rank
CREATE INDEX IF NOT EXISTS idx_creators_quality_rank_synced
    ON public.creators (quality_grade_rank ASC)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- Partial index for synced_partial creators
CREATE INDEX IF NOT EXISTS idx_creators_quality_rank_synced_partial
    ON public.creators (quality_grade_rank ASC)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- Verification:
-- SELECT quality_grade, quality_grade_rank, COUNT(*)
-- FROM public.creators
-- WHERE sync_status = 'synced'
-- GROUP BY quality_grade, quality_grade_rank
-- ORDER BY quality_grade_rank;
