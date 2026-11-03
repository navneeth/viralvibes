-- Restrict direct client access to app_stats to its existing read privileges.
-- Preserve service_role grants, RLS configuration, and RPC execution privileges.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
ON TABLE public.app_stats
FROM PUBLIC, anon, authenticated;

-- Expected: SELECT true for all three roles; non-read false for client roles
-- and true for service_role on the verified existing production schema.
SELECT role_name,
       has_table_privilege(
           role_name, 'public.app_stats', 'SELECT'
       ) AS can_select,
       has_table_privilege(
           role_name, 'public.app_stats',
           'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'
       ) AS has_any_non_read_privilege
FROM (
    VALUES ('anon'), ('authenticated'), ('service_role')
) AS roles(role_name);
