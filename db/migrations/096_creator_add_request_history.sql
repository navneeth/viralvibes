BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '60s';

-- Keep accepted user requests after terminal sync jobs are deleted.
CREATE TABLE IF NOT EXISTS public.creator_add_request_history (
    job_id bigint PRIMARY KEY,
    requested_by uuid REFERENCES public.users(id) ON DELETE SET NULL,
    created_at timestamptz NOT NULL
);

ALTER TABLE public.creator_add_request_history ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.creator_add_request_history FROM PUBLIC, anon, authenticated, service_role;
GRANT SELECT ON public.creator_add_request_history TO service_role;

-- Backfill every still-present user request, including rows converted to sync_stats.
-- Use migration time when an old job has no timestamp so it remains countable.
CREATE OR REPLACE FUNCTION public.record_creator_add_request()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF NEW.source = 'user_add' THEN
        INSERT INTO public.creator_add_request_history (job_id, requested_by, created_at)
        VALUES (
            NEW.id,
            NEW.requested_by,
            COALESCE(NEW.created_at, statement_timestamp())
        )
        ON CONFLICT (job_id) DO NOTHING;
    END IF;

    RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION public.record_creator_add_request()
    FROM PUBLIC, anon, authenticated;

DROP TRIGGER IF EXISTS trg_record_creator_add_request ON public.creator_sync_jobs;
CREATE TRIGGER trg_record_creator_add_request
    AFTER INSERT ON public.creator_sync_jobs
    FOR EACH ROW
    EXECUTE FUNCTION public.record_creator_add_request();

INSERT INTO public.creator_add_request_history (job_id, requested_by, created_at)
SELECT id, requested_by, COALESCE(created_at, statement_timestamp())
FROM public.creator_sync_jobs
WHERE source = 'user_add'
ON CONFLICT (job_id) DO NOTHING;

COMMENT ON TABLE public.creator_add_request_history IS
    'Durable record of accepted user creator-add requests; intentionally independent of creator_sync_jobs retention.';

COMMIT;
