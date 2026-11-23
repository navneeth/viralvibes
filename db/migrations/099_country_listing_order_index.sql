--- This migration creates an index on the creators table to optimize queries that filter by country_code, current_subscribers, and id, while also considering specific conditions for archived_at, sync_status, channel_name, and current_subscribers. The index is created concurrently to avoid locking the table during the operation.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_country_list_order
ON public.creators (country_code, current_subscribers DESC, id)
WHERE archived_at IS NULL
  AND sync_status IN ('synced', 'synced_partial')
  AND channel_name IS NOT NULL
  AND current_subscribers > 0
