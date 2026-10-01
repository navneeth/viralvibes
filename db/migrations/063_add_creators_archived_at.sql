-- Migration 063: add creators.archived_at (ghost column fix)
--
-- Problem
-- -------
-- Application code already assumes `creators.archived_at` exists:
--
--   db.queue_invalid_creators_for_retry  — .is_("archived_at", "null") in two branches
--   db.archive_permanently_failed_creators — writes archived_at in the UPDATE payload
--   worker/creator_worker.py              — .is_("archived_at", "null") in stale-check
--
-- No tracked migration ever created the column, and db/README.md's schema
-- doc does not mention it either.  In production PostgreSQL returns 42703
-- ("column archived_at does not exist") on every call that touches these
-- paths; our broad try/except catches the error, returns a safe default (0
-- rows), and the bootstrap pass moves on.  Side effects:
--
--   * Permanently-failed creators are RE-QUEUED every bootstrap pass because
--     the "exclude archived" guard never fires.
--   * archive_permanently_failed_creators has never persisted a single
--     archived_at timestamp in production — the whole UPDATE 42703s out.
--
-- Same failure pattern as the thumbnail_url / bio ghost-column incident
-- fixed in an earlier hotfix PR.
--
-- Fix
-- ---
-- Add the column (idempotent — ADD COLUMN IF NOT EXISTS covers the case
-- where someone already added it via Supabase Studio without checking in a
-- migration).  ADD COLUMN with a nullable column and no default is a
-- metadata-only operation on PostgreSQL 11+ — instant regardless of table
-- size, no table rewrite.
--
-- No index is created in this migration.  The hot path uses
-- ``WHERE archived_at IS NULL`` (archive-exclusion filter) which is already
-- served by combining with existing ``sync_status`` indexes; a partial
-- index on ``WHERE archived_at IS NOT NULL`` would only help the
-- inverse-direction scan (operator audits), which has no production caller
-- yet.  A pre-deploy attempt to create it inside the Supabase SQL editor
-- timed out at 57014 even on an all-NULL column, because plain
-- CREATE INDEX seq-scans the full creators table to prove emptiness.  When
-- a real audit caller lands, add the index via CREATE INDEX CONCURRENTLY
-- from a worker shell (CONCURRENTLY cannot run in a transaction block, so
-- it needs the ``\set AUTOCOMMIT on`` psql escape, not the dashboard SQL
-- editor).
--
-- Backfill is deliberately omitted; see Block 2 below for the rationale and
-- the going-forward sealing contract via archive_permanently_failed_creators.

-- ── Block 1: Column ──────────────────────────────────────────────────────────
ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS archived_at timestamptz;

COMMENT ON COLUMN public.creators.archived_at IS
    'Terminal state marker set by db.archive_permanently_failed_creators when '
    'a creator exceeds the per-job retry cap.  Non-null means "do not retry"; '
    'queue_invalid_creators_for_retry and worker stale-check paths filter '
    'these rows out via .is_(archived_at, null).';

-- ── Block 2: no backfill ─────────────────────────────────────────────────────
-- An earlier version of this migration included an optional
-- ``UPDATE creators SET archived_at = ... WHERE id IN (SELECT ... FROM
-- creator_sync_jobs WHERE status='failed' AND retry_count >= 3)``
-- backfill.  Pre-deploy testing showed the subquery timed out with
-- 57014 even when the result set was empty — ``creator_sync_jobs`` lacks
-- a covering index on ``(status, retry_count)``, so the planner cannot
-- prove the IN subquery cheaply.
--
-- We rely on the going-forward sealing path instead: any currently-stuck
-- "should be archived" creator gets re-queued ONCE on the next bootstrap
-- pass, fails to sync, bumps its retry_count, and then
-- ``db.archive_permanently_failed_creators`` seals it with a single-row
-- UPDATE by primary key — no subquery, no planner headache.  The backlog
-- self-heals within one bootstrap cycle after this migration applies.

-- ── Verify ───────────────────────────────────────────────────────────────────
-- Expect the column to exist:
-- SELECT column_name, data_type
-- FROM   information_schema.columns
-- WHERE  table_schema = 'public' AND table_name = 'creators'
--   AND  column_name  = 'archived_at';
--
-- Expect all rows to be NULL immediately after migration:
-- SELECT COUNT(*) FILTER (WHERE archived_at IS NOT NULL) AS archived,
--        COUNT(*)                                        AS total
-- FROM   public.creators;
