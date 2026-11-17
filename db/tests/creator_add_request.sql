-- Disposable database only.
-- psql "$TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -f db/tests/creator_add_request.sql
\set ON_ERROR_STOP on

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN CREATE ROLE anon; END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
        CREATE ROLE authenticated;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
        CREATE ROLE service_role;
    END IF;
END $$;

CREATE TABLE public.users (id uuid PRIMARY KEY);
CREATE TABLE public.creators (
    id uuid PRIMARY KEY,
    channel_id text,
    custom_url text
);
CREATE TABLE public.creator_sync_jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    creator_id uuid,
    status text,
    job_type text,
    source text,
    created_at timestamptz DEFAULT statement_timestamp()
);

\ir ../migrations/018_creator_add_requests.sql
INSERT INTO public.users (id) VALUES
    ('00000000-0000-0000-0000-000000000001'),
    ('00000000-0000-0000-0000-000000000002');
INSERT INTO public.creator_sync_jobs (
    job_type, status, source, input_query, requested_by, created_at
)
VALUES (
    'sync_stats', 'failed', 'user_add', '@legacy',
    '00000000-0000-0000-0000-000000000001', statement_timestamp() - interval '2 hours'
);
\ir ../migrations/096_creator_add_request_history.sql

DO $$
DECLARE
    v_job_id bigint;
    v_count bigint;
BEGIN
    SELECT count(*) INTO v_count
    FROM public.creator_add_request_history AS history
    JOIN public.creator_sync_jobs AS job ON job.id = history.job_id
    WHERE job.input_query = '@legacy';
    IF v_count <> 1 THEN
        RAISE EXCEPTION 'Existing converted request was not backfilled';
    END IF;

    INSERT INTO public.creator_sync_jobs (
        job_type, status, source, input_query, requested_by, created_at
    )
    VALUES (
        'resolve_and_add', 'pending', 'user_add', '@new',
        '00000000-0000-0000-0000-000000000001', statement_timestamp()
    )
    RETURNING id INTO v_job_id;

    SELECT count(*) INTO v_count
    FROM public.creator_add_request_history
    WHERE job_id = v_job_id;
    IF v_count <> 1 THEN
        RAISE EXCEPTION 'New user request was not captured by the insert trigger';
    END IF;

    DELETE FROM public.creator_sync_jobs WHERE id = v_job_id;
    SELECT count(*) INTO v_count
    FROM public.creator_add_request_history
    WHERE job_id = v_job_id;
    IF v_count <> 1 THEN
        RAISE EXCEPTION 'Request history was lost when the sync job was deleted';
    END IF;

END;
$$;

SELECT 'creator_add_request SQL checks passed' AS result;
