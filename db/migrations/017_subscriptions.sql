-- Migration 017: Stripe billing support
-- Adds stripe_customer_id to the users table and creates the subscriptions table.
-- Run once in the Supabase SQL editor.

-- 1. Extend users table with a Stripe customer reference
-- UNIQUE ensures one Stripe customer per user; PostgreSQL automatically
-- creates an index on this column, which the webhook handler uses for lookups.
ALTER TABLE public.users
    ADD COLUMN IF NOT EXISTS stripe_customer_id text UNIQUE;

-- 2. Subscriptions table
--    One row per user; status mirrors Stripe subscription status values:
--    trialing | active | past_due | canceled | unpaid | incomplete | incomplete_expired
CREATE TABLE IF NOT EXISTS public.subscriptions (
    id                     uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id                uuid        NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    -- stripe_customer_id lives on public.users — look it up via user_id to avoid drift
    stripe_subscription_id text        UNIQUE,
    stripe_price_id        text,
    plan                   text        NOT NULL DEFAULT 'free',
    interval               text,                         -- 'month' | 'year' | NULL for free
    status                 text        NOT NULL DEFAULT 'inactive',
    current_period_end     timestamptz,
    created_at             timestamptz DEFAULT now(),
    updated_at             timestamptz DEFAULT now()
);

-- 3. Lookup index for user→subscription queries
CREATE INDEX IF NOT EXISTS idx_subscriptions_user_id
    ON public.subscriptions(user_id);

-- 4. Enforce at most one active/trialing subscription per user
CREATE UNIQUE INDEX IF NOT EXISTS idx_subscriptions_active_user
    ON public.subscriptions(user_id)
    WHERE status IN ('active', 'trialing');

-- 5. Auto-update updated_at on every write
CREATE OR REPLACE FUNCTION public.set_subscriptions_updated_at()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_subscriptions_updated_at
    BEFORE UPDATE ON public.subscriptions
    FOR EACH ROW EXECUTE FUNCTION public.set_subscriptions_updated_at();
