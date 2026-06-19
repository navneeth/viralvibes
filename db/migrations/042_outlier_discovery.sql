-- Migration 042: Outlier discovery / viral pattern identification
-- Purpose: Cache recent-upload median views and videos that clear 3x baseline.
-- Data source: worker creator sync via YouTube Data API:
--   channels.list(part=contentDetails) -> relatedPlaylists.uploads
--   playlistItems.list(part=contentDetails, maxResults=50)
--   videos.list(part=statistics,snippet,contentDetails)

ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS uploads_playlist_id text,
    ADD COLUMN IF NOT EXISTS recent_views_median bigint,
    ADD COLUMN IF NOT EXISTS recent_video_sample_size integer,
    ADD COLUMN IF NOT EXISTS outlier_count integer,
    ADD COLUMN IF NOT EXISTS outlier_videos jsonb;

COMMENT ON COLUMN public.creators.uploads_playlist_id IS
    'YouTube uploads playlist ID from channels.list contentDetails.relatedPlaylists.uploads.';

COMMENT ON COLUMN public.creators.recent_views_median IS
    'Median view count across the latest public uploads sample, normally the latest 50 videos.';

COMMENT ON COLUMN public.creators.recent_video_sample_size IS
    'Number of recent videos included in the outlier baseline sample.';

COMMENT ON COLUMN public.creators.outlier_count IS
    'Count of sampled recent videos where view_count is greater than 3x recent_views_median.';

COMMENT ON COLUMN public.creators.outlier_videos IS
    'Top outlier video snapshots: video_id, title, thumbnail_url, published_at, view_count, like_count, view_multiplier, is_short, duration_sec.';

CREATE INDEX IF NOT EXISTS idx_creators_outlier_count
    ON public.creators (outlier_count)
    WHERE outlier_count IS NOT NULL AND outlier_count > 0;
