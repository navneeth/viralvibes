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
-- 3. Add a service-role bypass policy so the backend supabase-py client
--    (which uses the service-role key) can always read the table.
--
-- The backend db.py is_admin() call uses the service-role key which bypasses
-- RLS by default in Supabase — the SECURITY DEFINER function is kept for
-- future use but is NOT granted to the authenticated role.  This prevents
-- authenticated users from directly calling public.is_admin_user(arbitrary_uuid)
-- to probe the admin roster.  No authenticated SELECT policy is created
-- because no current code path reads admin_users with a user-scoped key.

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

-- Restrict direct invocation: only the service_role (used by the backend)
-- and the function owner can call this.  authenticated users cannot call it
-- directly, which prevents probing whether an arbitrary UUID is an admin.
REVOKE ALL ON FUNCTION public.is_admin_user(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.is_admin_user(uuid) TO service_role;

-- ── Step 3: full-access bypass for the service role ──────────────────────────
-- The backend uses the Supabase service-role key (bypasses RLS by default in
-- Supabase, but an explicit policy keeps things clear if that default changes).
DROP POLICY IF EXISTS "Service role full access" ON public.admin_users;
CREATE POLICY "Service role full access"
    ON public.admin_users
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);
