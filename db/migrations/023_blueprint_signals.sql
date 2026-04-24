-- Migration 023: Blueprint signals cache columns
-- Purpose: Store per-creator heavy signals computed by the worker (Phase 2).
--          Phase 1 leaves these NULL; the scorer gracefully stubs them.
-- Date: 2026-04-24

ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS blueprint_signals     jsonb,
    ADD COLUMN IF NOT EXISTS blueprint_computed_at timestamptz;

COMMENT ON COLUMN public.creators.blueprint_signals IS
    'Worker-computed heavy signals for Growth Blueprint scoring: '
    '{shorts_ratio, caption_coverage, avg_duration_sec, tag_discovery_pct}. '
    'NULL = not yet computed; scorer uses safe stubs.';

COMMENT ON COLUMN public.creators.blueprint_computed_at IS
    'Timestamp of last blueprint_signals computation. '
    'NULL = never computed. Worker uses this to prioritise stale rows.';

-- Index lets the worker efficiently find the stalest rows to refresh first.
CREATE INDEX IF NOT EXISTS idx_creators_blueprint_stale
    ON public.creators (blueprint_computed_at ASC NULLS FIRST)
    WHERE sync_status = 'synced';
