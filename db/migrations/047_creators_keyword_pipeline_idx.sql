-- 047_creators_keyword_pipeline_idx.sql
-- Partial index to support the _fetch_eligible_ids() query in
-- notebooks/creator_keyword_poc.ipynb without statement timeout (57014).
--
-- The pipeline query pattern (Cell 5):
--   SELECT id, default_language, engagement_score, current_subscribers
--   FROM   creators
--   WHERE  sync_status = 'synced'
--     AND  transcript_keywords_updated_at IS NULL
--   ORDER BY id ASC
--   LIMIT 1000
--
-- Without this index Postgres performs a sequential table scan, which
-- exceeds Supabase's statement timeout on tables with > ~50K rows.
-- With this index Postgres uses a partial index range scan — O(log n)
-- per page regardless of table size, typically < 50ms.
--
-- Note: CONCURRENTLY is omitted so this can run inside the Supabase SQL
-- editor (transaction block). The table is briefly locked for writes
-- during index build — run during low-traffic hours if needed.
--
-- After applying this migration, the notebook's Python-side filters
-- (engagement_score, default_language, current_subscribers) can optionally
-- be moved back into the Supabase query since the seq-scan risk is gone.
-- The Python-side approach is also fine to keep — it is slightly less
-- selective at the DB level but has no correctness impact.

CREATE INDEX IF NOT EXISTS idx_creators_keyword_pipeline
    ON public.creators (id)
    WHERE sync_status = 'synced'
      AND transcript_keywords_updated_at IS NULL;

COMMENT ON INDEX public.idx_creators_keyword_pipeline IS
    'Partial index for the Kaggle keyword extraction pipeline. '
    'Supports: SELECT id … WHERE sync_status=''synced'' AND transcript_keywords_updated_at IS NULL ORDER BY id. '
    'Eliminates statement timeout (57014) in notebooks/creator_keyword_poc.ipynb Cell 5. '
    'Applied via migration 047.';
