-- Migration 052: Add quality_grade_rank column for correct quality sort
--
-- Problem: quality_grade stores text values 'A+', 'A', 'B+', 'B', 'C'.
-- Alphabetical ordering of these values does not match quality tier order:
--   ASC alphabetical: A, A+, B, B+, C   (A before A+ — wrong, A+ is best tier)
--   DESC alphabetical: C, B+, B, A+, A  (C first — wrong, C is lowest tier)
--
-- The correct order for "best quality first" is: A+, A, B+, B, C
-- (matching the grade_options display order on the /creators page).
--
-- Fix: Add quality_grade_rank (INT2) that maps each grade to a sort-safe integer:
--   A+ → 1 (best)
--   A  → 2
--   B+ → 3
--   B  → 4
--   C  → 5
--   NULL / other → 99 (ungraded creators sort last)
--
-- Then ORDER BY quality_grade_rank ASC gives A+ first, C last.
--
-- Implementation note: GENERATED ALWAYS AS STORED causes a full table rewrite
-- which times out on large tables via the Supabase SQL editor. Instead we use:
--   1. ADD COLUMN (instant — no rewrite)
--   2. BEFORE INSERT/UPDATE trigger to keep new rows in sync going forward
--   3. Partial indexes for the ORDER BY query plans
--
-- Out-of-band backfill required (not part of this migration):
--   After applying this file, populate existing NULL rows by running the
--   following statement in batches until 0 rows are affected:
--
--   UPDATE public.creators
--   SET quality_grade_rank = CASE quality_grade
--       WHEN 'A+' THEN 1 WHEN 'A'  THEN 2
--       WHEN 'B+' THEN 3 WHEN 'B'  THEN 4
--       WHEN 'C'  THEN 5 ELSE 99
--   END
--   WHERE id IN (
--       SELECT id FROM public.creators
--       WHERE quality_grade_rank IS NULL
--       LIMIT 5000
--   );
--
--   Verify with: SELECT COUNT(*) FROM public.creators WHERE quality_grade_rank IS NULL;
--
-- db.py sort_map uses quality_grade_rank.
--
-- Prerequisites: migrations 008, 043, 045 (partial index foundations).

-- Step 1: Add plain nullable column (instant metadata change, no table rewrite)
ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS quality_grade_rank INT2;

-- Step 2: Trigger to keep quality_grade_rank in sync on every INSERT/UPDATE.
CREATE OR REPLACE FUNCTION public.sync_quality_grade_rank()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.quality_grade_rank := CASE NEW.quality_grade
        WHEN 'A+' THEN 1 WHEN 'A'  THEN 2
        WHEN 'B+' THEN 3 WHEN 'B'  THEN 4
        WHEN 'C'  THEN 5 ELSE 99
    END;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_sync_quality_grade_rank
BEFORE INSERT OR UPDATE OF quality_grade ON public.creators
FOR EACH ROW EXECUTE FUNCTION public.sync_quality_grade_rank();

-- Step 3: Partial indexes for the ORDER BY query plans.

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

-- Verification query:
-- SELECT quality_grade, quality_grade_rank, COUNT(*)
-- FROM public.creators
-- WHERE sync_status = 'synced'
-- GROUP BY quality_grade, quality_grade_rank
-- ORDER BY quality_grade_rank;
