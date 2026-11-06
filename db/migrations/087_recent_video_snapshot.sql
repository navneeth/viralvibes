-- Bounded metadata snapshot for the latest-12 Video Deep Dive pilot.
-- No backfill, image storage, new queue, indexes, or client grants.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '10s';

ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS recent_videos jsonb,
    ADD COLUMN IF NOT EXISTS recent_videos_fetched_at timestamptz;

DO $$
BEGIN
    ALTER TABLE public.creators
        ADD CONSTRAINT creators_recent_videos_bounded CHECK (
            recent_videos IS NULL OR
            CASE WHEN jsonb_typeof(recent_videos) = 'array' THEN
                jsonb_array_length(recent_videos) <= 12
                AND octet_length(recent_videos::text) <= 32768
            ELSE false END
        ) NOT VALID;
EXCEPTION WHEN duplicate_object THEN
    NULL;
END;
$$;

COMMENT ON COLUMN public.creators.recent_videos IS
    'Latest 12 accessible videos, newest first; metadata only. NULL means not collected; '
    '[] means a successful empty sample. Maximum serialized size 32 KiB. '
    'Populate only for the worker pilot allowlist; replaces rather than appends.';
COMMENT ON COLUMN public.creators.recent_videos_fetched_at IS
    'UTC time of the last successful recent-video snapshot fetch; failure preserves cached data.';

COMMIT;

-- Verification (read only):
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'creators'
  AND column_name IN ('recent_videos', 'recent_videos_fetched_at');