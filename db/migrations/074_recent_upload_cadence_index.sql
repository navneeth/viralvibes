-- Support the /lists Most Active query, which ranks synced creators by their
-- average interval across up to 50 recent uploads and filters on recent activity.
-- Run outside a transaction because CREATE INDEX CONCURRENTLY cannot run inside one.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_recent_cadence_synced
    ON public.creators (
        avg_days_between_uploads ASC,
        (recent_upload ->> 'published_at') DESC
    )
    WHERE sync_status = 'synced'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0
      AND avg_days_between_uploads > 0.01;