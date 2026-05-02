-- Migration 029: Fix infinite recursion in admin_users RLS policy
--
-- Problem
-- -------
-- The SELECT policy on admin_users reads:
--
--   USING (auth.uid() IN (SELECT user_id FROM public.admin_users WHERE id IS NOT NULL))
--
-- PostgreSQL evaluates the USING expression *for every row being read*,
-- which triggers the same policy again → infinite recursion (PG error 42P17).
--
-- Fix
-- ---
-- 1. Drop the recursive policy.
-- 2. Create a SECURITY DEFINER helper function that reads admin_users as the
--    table owner (bypasses RLS entirely), so the policy never re-enters itself.
-- 3. Recreate the SELECT policy using the helper function.
-- 4. Also add a service-role bypass policy so the backend supabase-py client
--    (which uses the service-role key) can always read the table without
--    triggering RLS at all.
--
-- The backend db.py is_admin() call uses the service role key and will be
-- served by the service-role bypass policy — zero recursion risk.

-- ── Step 1: drop the broken policy ───────────────────────────────────────────
DROP POLICY IF EXISTS "Admins can view all admin_users" ON public.admin_users;

-- ── Step 2: security-definer helper ──────────────────────────────────────────
-- Runs as the defining role (table owner), so it never hits admin_users RLS.
CREATE OR REPLACE FUNCTION public.is_admin_user(check_uid uuid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    SELECT EXISTS (
        SELECT 1 FROM public.admin_users WHERE user_id = check_uid
    );
$$;

-- Only superuser / table owner should call this directly.
REVOKE ALL ON FUNCTION public.is_admin_user(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.is_admin_user(uuid) TO authenticated;
GRANT EXECUTE ON FUNCTION public.is_admin_user(uuid) TO service_role;

-- ── Step 3: non-recursive SELECT policy for authenticated users ───────────────
-- Uses the helper function — no direct reference to admin_users inside the
-- policy body, so there is no recursion.
CREATE POLICY "Admins can view all admin_users"
    ON public.admin_users
    FOR SELECT
    TO authenticated
    USING (public.is_admin_user(auth.uid()));

-- ── Step 4: full-access bypass for the service role ──────────────────────────
-- The backend uses the Supabase service-role key (bypasses RLS by default in
-- Supabase, but an explicit policy keeps things clear if that default changes).
CREATE POLICY "Service role full access"
    ON public.admin_users
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);
