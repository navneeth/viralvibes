-- Migration 035: DB housekeeping — autovacuum tuning, duplicate index removal,
--               missing FK index
--
-- Based on DB log analysis: creators (~816k rows) and creator_sync_jobs (~814k rows)
-- show high dead tuple counts and infrequent vacuum cadence. The default
-- autovacuum scale factors (20% for vacuum, 10% for analyze) mean vacuum
-- doesn't trigger until 163k dead tuples accumulate on creators. At our write
-- volume (worker syncing hundreds of creators per hour) this causes planner
-- statistics to lag and query runtimes to become inconsistent.
--
-- NOTE: These are table-level storage parameters, applied with ALTER TABLE.
-- They take effect at the next autovacuum cycle without requiring a restart.
-- Run VACUUM ANALYZE manually after applying if you want immediate effect:
--   VACUUM ANALYZE public.creators;
--   VACUUM ANALYZE public.creator_sync_jobs;


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Autovacuum tuning on hot tables
--    Default scale_factor = 0.20 (vacuum when 20% of rows are dead).
--    At 816k rows that means ~163k dead tuples before vacuum fires.
--    Reduce to 0.01 (1%) so vacuum triggers at ~8k dead tuples instead.
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE public.creators SET (
    autovacuum_vacuum_scale_factor   = 0.01,  -- vacuum at 1% dead rows (default 0.20)
    autovacuum_analyze_scale_factor  = 0.005, -- analyze at 0.5% changed rows (default 0.10)
    autovacuum_vacuum_cost_delay     = 2      -- reduce throttle (ms); default 20
);

ALTER TABLE public.creator_sync_jobs SET (
    autovacuum_vacuum_scale_factor   = 0.01,
    autovacuum_analyze_scale_factor  = 0.005,
    autovacuum_vacuum_cost_delay     = 2
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Drop superseded pending-job indexes from migration 026
--
-- Migration 026 created two partial indexes to cover two separate polling
-- queries (null-retry branch and non-null-retry branch separately).
-- Migration 032 replaced both with one covering index
-- (idx_creator_sync_jobs_pending_or_query) that handles the combined OR
-- predicate used by the current _fetch_pending_jobs() implementation.
--
-- The two old indexes now only add write overhead with no read benefit.
-- ─────────────────────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS public.idx_creator_sync_jobs_pending_fresh;
DROP INDEX IF EXISTS public.idx_creator_sync_jobs_pending_retry;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Missing FK index on admin_users.granted_by
--
-- Migration 023 defines:
--   granted_by uuid REFERENCES public.users(id) ON DELETE SET NULL
-- without a supporting index. PostgreSQL does not auto-create indexes for FK
-- columns (only for PK/UNIQUE constraints). This means any FK integrity check
-- or join on granted_by requires a seq scan on admin_users.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_admin_users_granted_by
    ON public.admin_users (granted_by)
    WHERE granted_by IS NOT NULL;  -- partial: most rows will have a granter


-- ─────────────────────────────────────────────────────────────────────────────
-- Verification queries
-- ─────────────────────────────────────────────────────────────────────────────
-- Check autovacuum settings applied:
-- SELECT relname, reloptions
-- FROM pg_class
-- WHERE relname IN ('creators', 'creator_sync_jobs');
--
-- Confirm old indexes are gone:
-- SELECT indexname FROM pg_indexes
-- WHERE indexname IN (
--     'idx_creator_sync_jobs_pending_fresh',
--     'idx_creator_sync_jobs_pending_retry'
-- );
--
-- Confirm new FK index exists:
-- SELECT indexname FROM pg_indexes WHERE indexname = 'idx_admin_users_granted_by';
