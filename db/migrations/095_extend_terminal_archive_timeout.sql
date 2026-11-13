BEGIN;

ALTER FUNCTION public.archive_terminal_creator_syncs(integer, integer)
    SET statement_timeout = '15s';

COMMIT;