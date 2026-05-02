-- Migration 030: Add missing current_period_end column to subscriptions
--
-- Problem
-- -------
-- Migration 017 defined current_period_end in the CREATE TABLE statement,
-- but the table was created in the live DB without it (likely from an earlier
-- schema version).  Migration 022 later added other missing columns
-- (stripe_subscription_id, stripe_price_id, interval, created_at, updated_at)
-- but did not include current_period_end.
--
-- As a result:
--   - get_user_plan()      → APIError 42703 (column does not exist)
--   - upsert_subscription() → would also fail on next Stripe webhook

ALTER TABLE public.subscriptions
    ADD COLUMN IF NOT EXISTS current_period_end timestamptz;
