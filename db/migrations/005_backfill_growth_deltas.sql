-- Migration 005: Backfill 30-day growth deltas for mature-snapshot creators
-- Date: 2026-03-19
--
-- Context:
--   Migration 001 set prev_snapshot_at = NOW() for all existing creators and
--   set subscribers_change_30d = NULL ("tracking initializing").
--   The worker correctly writes deltas only when days_since_snapshot >= 7.
--   However, the worker processes 1 job per run and with 15K+ creators it
--   takes time to cycle through everyone. The result:
--
--   Query results before this migration:
--     total: 15,516  |  change_null: 15,515  |  change_zero: 1
--     snapshot_lt_7d:  9,546  (too fresh — worker correctly writes NULL)
--     snapshot_7_30d:  5,969  (mature enough — but worker hasn't reached them)
--     snapshot_gt_30d: 0
--
-- Fix:
--   For the 5,969 creators whose snapshot is 7–30 days old and whose change
--   columns are still NULL, compute the delta directly as:
--       current_* - prev_*
--   This is identical to what the worker would write on its next sync.
--   COALESCE guards the rare case where prev_* is NULL despite snapshot_at
--   being set (shouldn't happen, but keeps the UPDATE safe).
--
-- After this migration:
--   - 5,969 creators get real deltas immediately (many will be 0 or near-0,
--     which is accurate — snapshot and current values are 7–30 days apart)
--   - 9,546 creators with fresh snapshots keep NULL → show "Tracking initializing"
--   - The worker continues to refresh and update deltas on each sync cycle


UPDATE public.creators
SET
    subscribers_change_30d = current_subscribers - COALESCE(prev_subscribers, current_subscribers),
    views_change_30d       = current_view_count  - COALESCE(prev_view_count,  current_view_count),
    videos_change_30d      = current_video_count - COALESCE(prev_video_count, current_video_count)
WHERE
    prev_snapshot_at IS NOT NULL
    AND prev_snapshot_at < NOW() - INTERVAL '7 days'
    AND prev_snapshot_at >= NOW() - INTERVAL '30 days'
    AND subscribers_change_30d IS NULL
    AND views_change_30d IS NULL
    AND videos_change_30d IS NULL
    AND current_subscribers > 0;


-- ─────────────────────────────────────────────────────────────────────────────
-- Verification
-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Confirm 5,969 rows were updated:
--    SELECT COUNT(*) FROM public.creators
--    WHERE prev_snapshot_at < NOW() - INTERVAL '7 days'
--      AND prev_snapshot_at >= NOW() - INTERVAL '30 days'
--      AND subscribers_change_30d IS NOT NULL;
--    → should be ~5,969
--
-- 2. Distribution of results:
--    SELECT
--        COUNT(*) FILTER (WHERE subscribers_change_30d IS NULL)  AS still_null,
--        COUNT(*) FILTER (WHERE subscribers_change_30d = 0)      AS zero_growth,
--        COUNT(*) FILTER (WHERE subscribers_change_30d > 0)      AS growing,
--        COUNT(*) FILTER (WHERE subscribers_change_30d < 0)      AS declining
--    FROM public.creators WHERE current_subscribers > 0;
--
-- 3. Remaining NULL rows should all have fresh snapshots (< 7 days old):
--    SELECT COUNT(*) FROM public.creators
--    WHERE subscribers_change_30d IS NULL
--      AND prev_snapshot_at < NOW() - INTERVAL '7 days';
--    → should be 0
