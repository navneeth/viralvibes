-- Migration 031: Tier 1 recent performance & brand safety columns
-- Zero additional API quota cost — all data is derived from calls already made:
--   avg_views_10 / avg_likes_10 / avg_comments_10 / avg_days_between_uploads
--     ← from playlistItems.list + videos.list (already fetched for engagement_score)
--   is_made_for_kids / has_long_upload_status
--     ← from channels.list status part (added to existing channels.list call)

ALTER TABLE creators
  ADD COLUMN IF NOT EXISTS avg_views_10          INTEGER,
  ADD COLUMN IF NOT EXISTS avg_likes_10          INTEGER,
  ADD COLUMN IF NOT EXISTS avg_comments_10       INTEGER,
  ADD COLUMN IF NOT EXISTS avg_days_between_uploads NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS is_made_for_kids      BOOLEAN,
  ADD COLUMN IF NOT EXISTS has_long_upload_status BOOLEAN;

-- Index for reach-based sorting (avg_views_10 / current_subscribers)
CREATE INDEX IF NOT EXISTS idx_creators_avg_views_10
  ON creators (avg_views_10)
  WHERE avg_views_10 IS NOT NULL;

-- Index for brand safety filtering
CREATE INDEX IF NOT EXISTS idx_creators_is_made_for_kids
  ON creators (is_made_for_kids)
  WHERE is_made_for_kids = TRUE;
