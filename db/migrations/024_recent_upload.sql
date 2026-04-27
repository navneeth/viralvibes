-- Migration 024: Recent upload cache column
-- Purpose: Store the most recent video metadata fetched by the worker.
--          Powers the "Latest Upload" card on creator profiles without
--          additional per-request API calls.
-- Date: 2025-07-11

ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS recent_upload jsonb;

COMMENT ON COLUMN public.creators.recent_upload IS
    'Cached metadata for the most recently published video: '
    '{video_id, title, thumbnail_url, published_at, view_count, duration_sec, is_short}. '
    'NULL = not yet fetched. Updated on each worker sync.';

-- Index lets the API quickly filter creators that have a recent upload
CREATE INDEX IF NOT EXISTS idx_creators_recent_upload_not_null
    ON public.creators (id)
    WHERE recent_upload IS NOT NULL AND sync_status = 'synced';
