-- Migration 046: Add transcript_keywords to creators table
-- Stores KeyBERT-extracted keyword profiles from video transcripts.
-- Computed offline via Kaggle (notebooks/creator_keyword_poc.ipynb).
-- The notebook runs on a schedule, picking a random sample of creators each
-- run so every niche is covered evenly (not just high-engagement creators).
--
-- Format: JSONB array of {kw: text, score: float}, e.g.:
--   [{"kw": "budget mechanical keyboard", "score": 0.7812}, ...]
--
-- Two use cases drive the index design:
--   1. Profile display   — simple SELECT, no index needed.
--   2. Keyword search    — user types "keyboards", should match
--                          "budget mechanical keyboard", "mechanical switches", etc.
--                          Requires full-text search (stemming, partial match).
--                          RPC wrapper in migration 047.

-- ── Column: keyword payload ───────────────────────────────────────────────────
ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS transcript_keywords JSONB DEFAULT NULL;

COMMENT ON COLUMN public.creators.transcript_keywords IS
    'Top keyword phrases extracted via KeyBERT from recent video transcripts. '
    'Array of {kw: text, score: float} sorted by relevance score desc. '
    'Computed offline in Kaggle (notebooks/creator_keyword_poc.ipynb). '
    'Covers mid-tier creators (50K–2M subs). Null = not yet processed.';

-- ── Column: refresh timestamp ─────────────────────────────────────────────────
-- Tracks when keywords were last computed. The Kaggle pipeline (Cell 15 in the
-- notebook) sets this on every write-back. Once all creators are processed the
-- scheduler re-runs on creators with the oldest updated_at so keywords stay fresh.
ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS transcript_keywords_updated_at TIMESTAMPTZ DEFAULT NULL;

COMMENT ON COLUMN public.creators.transcript_keywords_updated_at IS
    'Timestamp of the most recent keyword extraction write-back from Kaggle. '
    'Used by the pipeline to schedule refresh runs on stale creators. '
    'NULL = never processed.';

-- ── Index 1: Exact containment @> (programmatic / tag-click filtering) ────────
-- Use case: "find all creators whose keywords include 'mechanical keyboards'".
-- jsonb_path_ops variant is ~30% smaller — supports @> only, which is all we need.
-- Query:    WHERE transcript_keywords @> '[{"kw": "mechanical keyboards"}]'
CREATE INDEX IF NOT EXISTS idx_creators_transcript_keywords_gin
    ON public.creators USING gin (transcript_keywords jsonb_path_ops)
    WHERE transcript_keywords IS NOT NULL;

-- ── Index 2: Full-text search @@ (user-facing search box) ────────────────────
-- jsonb_to_tsvector extracts the "kw" string values and builds an English-stemmed
-- tsvector. "keyboard" matches "budget mechanical keyboard", "mechanical switches".
-- The search_creators_by_keyword() RPC in migration 047 uses this index.
-- Query:    WHERE jsonb_to_tsvector('english', transcript_keywords, '["string"]')
--                 @@ plainto_tsquery('english', 'mechanical keyboard')
CREATE INDEX IF NOT EXISTS idx_creators_transcript_keywords_fts
    ON public.creators
    USING gin (jsonb_to_tsvector('english', transcript_keywords, '["string"]'))
    WHERE transcript_keywords IS NOT NULL;

-- ── Index 3: Refresh scheduling ───────────────────────────────────────────────
-- The Kaggle pipeline orders refresh runs by oldest updated_at so no creator
-- stays stale indefinitely. NULLS FIRST puts unprocessed creators at the front.
CREATE INDEX IF NOT EXISTS idx_creators_transcript_keywords_updated_at
    ON public.creators (transcript_keywords_updated_at ASC NULLS FIRST)
    WHERE sync_status = 'synced'
      AND current_subscribers >= 50000
      AND current_subscribers <= 2000000;
