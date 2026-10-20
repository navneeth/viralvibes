-- Migration 067: Record search-performance fixes applied by hand to production.
--
-- WHY THIS EXISTS
-- These changes were applied directly in the Supabase dashboard on 2026-10-17/18
-- while diagnosing a database CPU of ~87% and ~7 statement timeouts per minute.
-- Production already has all of them, so on production this migration is a
-- no-op. A fresh or restored database needs it to match.
--
-- BEFORE / AFTER (measured)
--   Browse sort queries, ORDER BY ... NULLS LAST LIMIT 50 (no search term):
--     last_updated_at   31 s     -> 0.3 ms
--     current_view_count 11.2 s  -> 2 ms
--     engagement_score   7.3 s   -> plan cost 12.85 (was ~202,000)
--     monthly_uploads, published_at DESC, current_video_count: same pattern
--   Supabase CPU 87% -> 5-6%. Statement timeouts ~735 / 96 min -> ~1 / 3 h.
--   pg_cron jobs 8, 9, 14, 25 went from mostly failing to 100% succeeded.
--
-- ROOT CAUSES
--   1. Existing sort indexes were DESC (NULLs first). The app orders
--      DESC NULLS LAST, so Postgres could not use them and seq-scanned and sorted
--      ~500K rows per request.
--   2. The `postgres` role had statement_timeout = 8s. Four cron refreshes of
--      materialized views need ~8-15 s and were cancelled. SET LOCAL
--      statement_timeout = 0 inside the cron functions had no effect.
--   3. Jobs 26 and 27 duplicated jobs 23 and 24.
--
-- HOW TO RUN
--   psql, direct connection (port 5432), outside a transaction:
--     CREATE INDEX CONCURRENTLY and \gexec both require it.
--   The Supabase SQL editor wraps statements in a transaction, so it cannot run
--   this file as written. In production these indexes were built there WITHOUT
--   CONCURRENTLY, which briefly blocked writes.
--   Check the repo for the next free migration number before committing:
--   066 was taken by 066_align_language_count_population.sql.

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- 1. Browse sort indexes (match ORDER BY <col> DESC NULLS LAST with the
--    browseable-row filter the app and the ranked RPC both use)
-- ---------------------------------------------------------------------------

-- Remove invalid remnants from any canceled concurrent build before retrying.
SELECT format('DROP INDEX CONCURRENTLY %I.%I;', n.nspname, c.relname)
FROM pg_index AS i
JOIN pg_class AS c ON c.oid = i.indexrelid
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE i.indrelid = 'public.creators'::regclass
  AND c.relname IN (
      'idx_creators_browse_recent',
      'idx_creators_browse_engagement',
      'idx_creators_browse_views',
      'idx_creators_browse_monthly_uploads',
      'idx_creators_browse_published_desc',
      'idx_creators_browse_videos'
  )
  AND (NOT i.indisvalid OR NOT i.indisready)
\gexec

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_browse_recent
    ON public.creators (last_updated_at DESC NULLS LAST)
    WHERE sync_status IN ('synced', 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_browse_engagement
    ON public.creators (engagement_score DESC NULLS LAST)
    WHERE sync_status IN ('synced', 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_browse_views
    ON public.creators (current_view_count DESC NULLS LAST)
    WHERE sync_status IN ('synced', 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_browse_monthly_uploads
    ON public.creators (monthly_uploads DESC NULLS LAST)
    WHERE sync_status IN ('synced', 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_browse_published_desc
    ON public.creators (published_at DESC NULLS LAST)
    WHERE sync_status IN ('synced', 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_creators_browse_videos
    ON public.creators (current_video_count DESC NULLS LAST)
    WHERE sync_status IN ('synced', 'synced_partial')
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- Not created on purpose: published_at ASC NULLS LAST ("oldest channel").
-- idx_creators_browseable_published_at (DESC) already serves it via a backward
-- index scan. Keep that index.

-- ---------------------------------------------------------------------------
-- 2. Statement timeout for the postgres role (used by pg_cron and the
--    dashboard SQL editor). Was 8s; four materialized-view refreshes need
--    ~8-15 s. Applies to NEW sessions only.
--    Revert: ALTER ROLE postgres SET statement_timeout = '8s';
--    (RESET would remove the setting rather than restore the old 8s.)
--    Not affected: anon / authenticated / service_role (PostgREST) timeouts.
-- ---------------------------------------------------------------------------
ALTER ROLE postgres SET statement_timeout = '2min';

-- ---------------------------------------------------------------------------
-- 3. Remove duplicate pg_cron jobs (same schedule and command as a lower
--    jobid). In production this removed jobs 26 and 27. No-op when no
--    duplicates exist or pg_cron is not installed.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        PERFORM cron.unschedule(j.jobid)
        FROM cron.job AS j
        WHERE EXISTS (
            SELECT 1 FROM cron.job AS k
            WHERE k.command = j.command
              AND k.schedule = j.schedule
              AND k.jobid < j.jobid
        );
    END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- 4. creator_peers autovacuum. ~946K rows of ~2 KB jsonb; ~18% dead tuples and
--    no autovacuum since 2026-09-30 because the default 20% threshold was
--    not reached. Vacuum sooner; includes its TOAST table.
--    Confirm with: SELECT reloptions FROM pg_class WHERE relname = 'creator_peers';
-- ---------------------------------------------------------------------------
ALTER TABLE public.creator_peers SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.02,
    toast.autovacuum_vacuum_scale_factor = 0.02
);

-- ---------------------------------------------------------------------------
-- NOT APPLIED YET (decide separately; review before enabling)
-- ---------------------------------------------------------------------------
-- a) Exact duplicate indexes on creators (safe to drop):
--      DROP INDEX CONCURRENTLY IF EXISTS public.idx_creators_browseable_last_updated_at;
--      DROP INDEX CONCURRENTLY IF EXISTS public.idx_creators_channel_id;
--
-- b) Zero-scan indexes (~770 MB) to review after at least a week of stats.
--    Pipeline indexes show zero when the pipeline is paused; check first:
--      idx_creators_transcript_keywords_gin / _fts (204 MB; check code use)
--      idx_creators_*_synced and grade_* variants
--      idx_creators_topic_categories_gin (never usable: column is text)
--
-- c) Search function rewrite for search_creators_ranked:
--      - add a unique tie-breaker (counted.id) as the last ORDER BY term
--      - escape % and _ in the prefix LIKE ranks (ESCAPE '\')
--      - build JSON only for the 50 returned rows instead of every match
--
-- d) Numeric overflow (22003), about one failed write every few hours.
--    Likely columns: engagement_score numeric(5,2) (max stored 950.00) or
--    avg_days_between_uploads numeric(6,2) (max stored 5902.66). Clamp in the
--    pipeline before writing; log channel_id and raw value to find the source.
--
-- e) mv_hero_stats refreshes without CONCURRENTLY (blocks readers while it runs).
--
-- Non-SQL changes made alongside this (live in the app repo / Vercel):
--   public/robots.txt: disallow meta-externalagent and Shapbot; discourage
--   /creators search and sort URLs. vercel.json: "regions": ["sin1"]
--   (Supabase is in ap-southeast-1). db_lists.py: removed a PostgREST
--   containment filter on a text column (text @> unknown errors).

-- ---------------------------------------------------------------------------
-- Verification (run after the migration)
-- ---------------------------------------------------------------------------
-- SELECT c.relname, i.indisvalid, i.indisready
--   FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
--   WHERE i.indrelid = 'public.creators'::regclass AND c.relname LIKE 'idx_creators_browse_%';
-- SELECT rolconfig FROM pg_roles WHERE rolname = 'postgres';
-- SELECT jobid, schedule, left(command, 60) FROM cron.job ORDER BY jobid;
