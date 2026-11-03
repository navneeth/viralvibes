-- Restrict direct client access to creator_peers to its existing read privileges.
-- Preserve service_role grants, sequence permissions, RLS, and RPC privileges.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
ON TABLE public.creator_peers
FROM PUBLIC, anon, authenticated;

-- Expected: SELECT true for all three roles; non-read false for client roles
-- and true for service_role on the verified existing production schema.
SELECT role_name,
       has_table_privilege(
           role_name, 'public.creator_peers', 'SELECT'
       ) AS can_select,
       has_table_privilege(
           role_name, 'public.creator_peers',
           'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'
       ) AS has_any_non_read_privilege
FROM (
    VALUES ('anon'), ('authenticated'), ('service_role')
) AS roles(role_name);
