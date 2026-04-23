-- Migration 022: Enhance subscriptions table with Stripe integration
-- Adds missing columns to align with Stripe webhook handling and payment logic.
-- Run once in the Supabase SQL editor.

-- Add missing columns to subscriptions table
ALTER TABLE public.subscriptions
ADD COLUMN IF NOT EXISTS stripe_subscription_id text UNIQUE,
ADD COLUMN IF NOT EXISTS stripe_price_id text,
ADD COLUMN IF NOT EXISTS interval text,
ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now(),
ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();

-- Update existing rows with default timestamps if NULL
UPDATE public.subscriptions
SET created_at = now(), updated_at = now()
WHERE created_at IS NULL;

-- Add auto-update trigger for updated_at if not already present
CREATE OR REPLACE FUNCTION public.set_subscriptions_updated_at()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_subscriptions_updated_at ON public.subscriptions;
CREATE TRIGGER trg_subscriptions_updated_at
    BEFORE UPDATE ON public.subscriptions
    FOR EACH ROW EXECUTE FUNCTION public.set_subscriptions_updated_at();
