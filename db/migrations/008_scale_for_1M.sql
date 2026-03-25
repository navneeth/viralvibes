-- Statement 1: Core partial index covering the three mandatory base filters.
-- Eliminates the full-table scan on every page load and COUNT query.
CREATE INDEX IF NOT EXISTS idx_creators_listing_base
ON creators (current_subscribers DESC)
WHERE sync_status = 'synced'
  AND channel_name IS NOT NULL
  AND current_subscribers > 0;

-- Statement 2: Supports ORDER BY on other sort fields within synced rows.
CREATE INDEX IF NOT EXISTS idx_creators_views_synced
ON creators (current_view_count DESC)
WHERE sync_status = 'synced' AND channel_name IS NOT NULL AND current_subscribers > 0;

-- Statement 3: Supports engagement and quality sort.
CREATE INDEX IF NOT EXISTS idx_creators_engagement_synced
ON creators (engagement_score DESC)
WHERE sync_status = 'synced' AND channel_name IS NOT NULL AND current_subscribers > 0;

-- Statement 4: Supports country filter (used in ilike query).
CREATE INDEX IF NOT EXISTS idx_creators_country_synced
ON creators (country_code)
WHERE sync_status = 'synced' AND channel_name IS NOT NULL AND current_subscribers > 0;

EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*)
FROM creators
WHERE sync_status = 'synced'
  AND channel_name IS NOT NULL
  AND current_subscribers > 0;
