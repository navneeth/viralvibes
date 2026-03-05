-- Migration: Add columns for 30-day growth tracking
-- Purpose: Enable proper calculation of subscribers_change_30d, views_change_30d, videos_change_30d
-- Date: 2026-03-05

-- Add snapshot columns to track previous values for delta calculation
ALTER TABLE public.creators
ADD COLUMN IF NOT EXISTS prev_subscribers bigint,
ADD COLUMN IF NOT EXISTS prev_view_count bigint,
ADD COLUMN IF NOT EXISTS prev_video_count integer,
ADD COLUMN IF NOT EXISTS prev_snapshot_at timestamp with time zone;

-- Add comments for documentation
COMMENT ON COLUMN public.creators.prev_subscribers IS 'Subscriber count at last 30-day snapshot (for calculating subscribers_change_30d)';
COMMENT ON COLUMN public.creators.prev_view_count IS 'View count at last 30-day snapshot (for calculating views_change_30d)';
COMMENT ON COLUMN public.creators.prev_video_count IS 'Video count at last 30-day snapshot (for calculating videos_change_30d)';
COMMENT ON COLUMN public.creators.prev_snapshot_at IS 'Timestamp of last snapshot taken (updates every ~30 days)';

-- Update existing change columns to allow NULL (indicates "tracking in progress")
COMMENT ON COLUMN public.creators.subscribers_change_30d IS 'Net change in subscribers over last 30 days (NULL = tracking initializing, requires 7+ day baseline)';
COMMENT ON COLUMN public.creators.views_change_30d IS 'Net change in views over last 30 days (NULL = tracking initializing)';
COMMENT ON COLUMN public.creators.videos_change_30d IS 'Net change in videos over last 30 days (NULL = tracking initializing)';

-- Initialize snapshot values for existing creators (one-time bootstrap)
-- This sets the baseline so next sync can calculate deltas
-- Note: We intentionally set prev_snapshot_at to NOW() so the baseline needs to mature
-- before growth metrics appear (prevents showing misleading 0% for new tracking)
UPDATE public.creators
SET
    prev_subscribers = current_subscribers,
    prev_view_count = current_view_count,
    prev_video_count = current_video_count,
    prev_snapshot_at = NOW(),
    -- Set changes to NULL to indicate "tracking in progress"
    subscribers_change_30d = NULL,
    views_change_30d = NULL,
    videos_change_30d = NULL
WHERE prev_snapshot_at IS NULL
  AND current_subscribers IS NOT NULL;

-- For creators synced more than 7 days ago, we can estimate initial growth
-- by comparing current stats to previous values (if available)
UPDATE public.creators
SET
    subscribers_change_30d = COALESCE(current_subscribers - prev_subscribers, 0),
    views_change_30d = COALESCE(current_view_count - prev_view_count, 0),
    videos_change_30d = COALESCE(current_video_count - prev_video_count, 0)
WHERE prev_snapshot_at IS NOT NULL
  AND prev_snapshot_at < NOW() - INTERVAL '7 days'
  AND subscribers_change_30d IS NULL;

-- Verify the migration
DO $$
DECLARE
    total_creators INTEGER;
    initialized_creators INTEGER;
    tracking_creators INTEGER;
    active_growth_creators INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_creators FROM public.creators;
    SELECT COUNT(*) INTO initialized_creators FROM public.creators WHERE prev_snapshot_at IS NOT NULL;
    SELECT COUNT(*) INTO tracking_creators FROM public.creators WHERE prev_snapshot_at IS NOT NULL AND subscribers_change_30d IS NULL;
    SELECT COUNT(*) INTO active_growth_creators FROM public.creators WHERE subscribers_change_30d IS NOT NULL;

    RAISE NOTICE '✅ Migration complete:';
    RAISE NOTICE '   - % creators total', total_creators;
    RAISE NOTICE '   - % with initialized snapshots', initialized_creators;
    RAISE NOTICE '   - % tracking in progress (will show "Initializing..." badge)', tracking_creators;
    RAISE NOTICE '   - % with active growth metrics', active_growth_creators;
END $$;
