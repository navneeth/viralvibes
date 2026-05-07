-- Migration 033: Add pg_trgm GIN index on channel_description for safe ILIKE search
--
-- context: channel_description was excluded from the creator search OR filter because
-- a full sequential ILIKE scan on the long-text column caused 57014 statement timeouts
-- (same root cause as primary_category before migration 028).
--
-- After this index is created, re-add channel_description.ilike.{pattern} to the
-- OR filter in get_creators() in db.py.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_creators_channel_description_trgm
    ON public.creators USING GIN (channel_description gin_trgm_ops)
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;
