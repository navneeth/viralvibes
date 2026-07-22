-- Speed up /lists/country/{country_code} detail pages.
--
-- The page filters by country_code, restricts to synced browseable rows, and
-- orders by current_subscribers DESC.  A country-only partial index can still
-- leave large countries such as US/IN sorting many rows before LIMIT applies;
-- this composite index lets Postgres read the ranking order directly.

CREATE INDEX IF NOT EXISTS idx_creators_country_subscribers_synced
ON public.creators (country_code, current_subscribers DESC)
WHERE sync_status = 'synced'
  AND channel_name IS NOT NULL
  AND current_subscribers > 0;
