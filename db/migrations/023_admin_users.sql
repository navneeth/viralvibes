-- Migration 023: Add admin_users table
-- Enables OAuth-based admin access control.
-- Run once in the Supabase SQL editor.

-- Create admin_users table to track which users have admin access
CREATE TABLE IF NOT EXISTS public.admin_users (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id           uuid        NOT NULL UNIQUE REFERENCES public.users(id) ON DELETE CASCADE,
    granted_at        timestamptz DEFAULT now(),
    granted_by        uuid        REFERENCES public.users(id) ON DELETE SET NULL,
    reason            text,
    created_at        timestamptz DEFAULT now(),
    updated_at        timestamptz DEFAULT now()
);

-- Index for fast user_id lookups
CREATE INDEX IF NOT EXISTS idx_admin_users_user_id
    ON public.admin_users(user_id);

-- Auto-update updated_at on every write
CREATE OR REPLACE FUNCTION public.set_admin_users_updated_at()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_admin_users_updated_at ON public.admin_users;
CREATE TRIGGER trg_admin_users_updated_at
    BEFORE UPDATE ON public.admin_users
    FOR EACH ROW EXECUTE FUNCTION public.set_admin_users_updated_at();

-- Enable row-level security
ALTER TABLE public.admin_users ENABLE ROW LEVEL SECURITY;

-- Policy: Only admins can view the admin_users table
CREATE POLICY "Admins can view all admin_users"
    ON public.admin_users
    FOR SELECT
    USING (
        auth.uid() IN (
            SELECT user_id FROM public.admin_users WHERE id IS NOT NULL
        )
    );
