-- Migration 045: Add partial indexes so synced_partial creators can be included
-- in the /creators browse page and ranking pages without forcing full table scans.
--
-- Context: get_creators() was restricted to sync_status = 'synced' because the
-- existing partial indexes (migrations 008, 028, 043) are all defined with:
--   WHERE sync_status = 'synced' AND channel_name IS NOT NULL AND current_subscribers > 0
-- Using IN ('synced', 'synced_partial') would disqualify those indexes on a single-index
-- query plan. With BOTH sets of partial indexes present, PostgreSQL can satisfy the
-- combined condition via BitmapOr(IndexScan_synced, IndexScan_partial), which is
-- efficient even for the full sort + filter + count queries.
--
-- synced_partial creators have basic fields populated (channel_name, current_subscribers,
-- current_view_count) but may be missing engagement_score, quality_grade, and recent
-- performance columns. They are browseable and should appear on ranking pages.
-- The 1,073 synced_partial rows (~0.1% of the browseable pool) are unlikely to
-- cause any index bloat concern.
--
-- Mirrors the exact set of indexes from migrations 008, 028, and 043 but with
-- sync_status = 'synced_partial' in the WHERE predicate.

-- ── Core listing index (mirrors idx_creators_listing_base from migration 008) ──
CREATE INDEX IF NOT EXISTS idx_creators_listing_base_partial
    ON public.creators (current_subscribers DESC)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- ── Sort indexes (mirror migration 008) ──────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_creators_views_synced_partial
    ON public.creators (current_view_count DESC)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX IF NOT EXISTS idx_creators_engagement_synced_partial
    ON public.creators (engagement_score DESC)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- ── Country filter index (mirrors migration 008) ──────────────────────────────
CREATE INDEX IF NOT EXISTS idx_creators_country_synced_partial
    ON public.creators (country_code)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- ── Primary category index (mirrors migration 028) ───────────────────────────
-- Note: pg_trgm GIN index from 028 covers synced only; for synced_partial the
-- planner will use this B-tree index for equality predicates.  The trgm GIN is
-- still used for text-search ilike on synced rows via BitmapOr.
CREATE INDEX IF NOT EXISTS idx_creators_primary_category_partial
    ON public.creators (primary_category)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- ── Filter/sort column indexes (mirror migration 043) ────────────────────────
CREATE INDEX IF NOT EXISTS idx_creators_language_synced_partial
    ON public.creators (default_language)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX IF NOT EXISTS idx_creators_grade_synced_partial
    ON public.creators (quality_grade)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX IF NOT EXISTS idx_creators_monthly_uploads_synced_partial
    ON public.creators (monthly_uploads DESC)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

CREATE INDEX IF NOT EXISTS idx_creators_channel_age_synced_partial
    ON public.creators (channel_age_days)
    WHERE sync_status = 'synced_partial'
      AND channel_name IS NOT NULL
      AND current_subscribers > 0;

-- ── Verification ─────────────────────────────────────────────────────────────
-- SELECT indexname, indexdef
-- FROM pg_indexes
-- WHERE tablename = 'creators'
--   AND indexname LIKE '%_partial'
-- ORDER BY indexname;
