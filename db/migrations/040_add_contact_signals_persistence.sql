-- Migration 040: Persist contact signals to creators table
-- Purpose: Enable fast filtering by contact availability without regex extraction on request
-- Extracted contacts: email, Instagram, X/Twitter, TikTok, LinkedIn, website
-- Strategy: Extract during worker sync, store in DB, denormalize with has_contact_info boolean for indexing

-- ═════════════════════════════════════════════════════════════════════════════
-- ADD CONTACT SIGNAL COLUMNS
-- ═════════════════════════════════════════════════════════════════════════════

ALTER TABLE public.creators
  ADD COLUMN IF NOT EXISTS extracted_email                  TEXT,
  ADD COLUMN IF NOT EXISTS extracted_website                TEXT,
  ADD COLUMN IF NOT EXISTS extracted_instagram              TEXT,
  ADD COLUMN IF NOT EXISTS extracted_x                      TEXT,
  ADD COLUMN IF NOT EXISTS extracted_tiktok                 TEXT,
  ADD COLUMN IF NOT EXISTS extracted_linkedin               TEXT,
  ADD COLUMN IF NOT EXISTS extracted_whatsapp               TEXT,
  ADD COLUMN IF NOT EXISTS contact_signals_extracted_at     TIMESTAMP WITH TIME ZONE,
  ADD COLUMN IF NOT EXISTS has_contact_info                 BOOLEAN DEFAULT FALSE;

COMMENT ON COLUMN public.creators.extracted_email IS 'First email address found in channel_description, bio, keywords (deduped, extracted once per sync)';
COMMENT ON COLUMN public.creators.extracted_website IS 'First personal website URL found (e.g., patreon.com, linktree.com, custom domain)';
COMMENT ON COLUMN public.creators.extracted_instagram IS 'Instagram profile URL extracted from bio/keywords (e.g., https://instagram.com/username)';
COMMENT ON COLUMN public.creators.extracted_x IS 'Twitter/X profile URL extracted from bio/keywords';
COMMENT ON COLUMN public.creators.extracted_tiktok IS 'TikTok profile URL extracted from bio/keywords';
COMMENT ON COLUMN public.creators.extracted_linkedin IS 'LinkedIn profile URL extracted from bio/keywords';
COMMENT ON COLUMN public.creators.extracted_whatsapp IS 'WhatsApp Business contact (reserved for future use)';
COMMENT ON COLUMN public.creators.contact_signals_extracted_at IS 'Timestamp of last successful extraction (used to track freshness, no caching limit)';
COMMENT ON COLUMN public.creators.has_contact_info IS 'Denormalized boolean: TRUE if any extracted contact field is non-null (enables fast WHERE filtering)';

-- ═════════════════════════════════════════════════════════════════════════════
-- CREATE INDEXES FOR FAST FILTERING
-- ═════════════════════════════════════════════════════════════════════════════

-- Index on has_contact_info for fast "all creators with any contact" queries
CREATE INDEX IF NOT EXISTS idx_creators_has_contact_info
  ON public.creators(has_contact_info)
  WHERE has_contact_info = TRUE;

-- Index on extracted_email for fast email filtering (main v1 use case)
CREATE INDEX IF NOT EXISTS idx_creators_extracted_email
  ON public.creators(extracted_email)
  WHERE extracted_email IS NOT NULL;

-- Combined index for admin export query: has_contact_info + sync_status
-- Optimizes: SELECT * FROM creators WHERE has_contact_info=TRUE AND sync_status='synced'
CREATE INDEX IF NOT EXISTS idx_creators_contact_sync_status
  ON public.creators(has_contact_info, sync_status, id)
  WHERE has_contact_info = TRUE;

-- ═════════════════════════════════════════════════════════════════════════════
-- BACKFILL INITIALIZATION
-- ═════════════════════════════════════════════════════════════════════════════
-- No backfill needed. NULL contact_signals_extracted_at is the correct initial
-- state: it means "worker has never run extraction for this row", which is
-- exactly what the worker should act on. Setting it to NOW() would incorrectly
-- signal that extraction already ran, potentially causing the worker to skip rows.

-- ═════════════════════════════════════════════════════════════════════════════
-- MIGRATION VERIFICATION
-- ═════════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
    total_creators INTEGER;
    columns_added INTEGER;
    indexes_created INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_creators FROM public.creators;

    SELECT COUNT(*) INTO columns_added FROM information_schema.columns
    WHERE table_name = 'creators'
    AND column_name IN (
        'extracted_email', 'extracted_website', 'extracted_instagram',
        'extracted_x', 'extracted_tiktok', 'extracted_linkedin',
        'extracted_whatsapp', 'contact_signals_extracted_at', 'has_contact_info'
    );

    SELECT COUNT(*) INTO indexes_created FROM pg_indexes
    WHERE tablename = 'creators'
    AND indexname LIKE 'idx_creators_%contact%';

    RAISE NOTICE '✅ Migration 040 complete:';
    RAISE NOTICE '   - % creators present', total_creators;
    RAISE NOTICE '   - % new columns added', columns_added;
    RAISE NOTICE '   - % indexes created for filtering', indexes_created;
    RAISE NOTICE '   - Next: Worker will extract contacts on next sync job';
    RAISE NOTICE '   - Admin export ready: GET /admin/outreach/export';
END $$;
