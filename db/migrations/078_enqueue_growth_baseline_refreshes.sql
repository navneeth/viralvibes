-- Stage 3 of the active-job uniqueness rollout.
-- Requires 077_active_creator_sync_unique_index.sql to be present and valid.
-- This installs RPCs only; it does not enqueue or update creator/job rows.
--
-- Dry run (default):
--   SELECT * FROM public.enqueue_growth_baseline_refreshes();
-- Bounded insertion after reviewing the dry-run result:
--   SELECT * FROM public.enqueue_growth_baseline_refreshes(100, 10000, 250, false);
-- The RPC hard caps each call at 500 jobs and the pending queue at 1,000 jobs.

-- YouTube's published_at values are RFC 3339 timestamps. Validate the full
-- shape, then catch calendar/range errors that a regular expression cannot
-- detect (for example, month 99). Invalid values become NULL and are skipped.
CREATE OR REPLACE FUNCTION public.try_parse_creator_upload_timestamp(p_value text)
RETURNS timestamptz
LANGUAGE plpgsql
IMMUTABLE
STRICT
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF p_value !~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$' THEN
        RETURN NULL;
    END IF;
    RETURN p_value::timestamptz;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION public.try_parse_creator_upload_timestamp(text)
    FROM PUBLIC, anon, authenticated;

CREATE OR REPLACE FUNCTION public.enqueue_growth_baseline_refreshes(
    p_limit           integer DEFAULT 100,
    p_min_subscribers integer DEFAULT 10000,
    p_max_queue       integer DEFAULT 250,
    p_dry_run         boolean DEFAULT true
)
RETURNS TABLE (
    bucket       text,
    selected     bigint,
    lowest_subs  bigint,
    highest_subs bigint,
    dry_run      boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
#variable_conflict use_column
DECLARE
    v_pending bigint;
    v_budget  bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('public.enqueue_growth_baseline_refreshes', 0)
    );

    SELECT count(*) INTO v_pending
    FROM public.creator_sync_jobs
    WHERE status = 'pending';

    -- Clamp caller-supplied values so this maintenance lane stays bounded.
    v_budget := GREATEST(
        LEAST(
            COALESCE(p_limit, 0)::bigint,
            500::bigint,
            LEAST(COALESCE(p_max_queue, 0)::bigint, 1000::bigint) - v_pending
        ),
        0
    );

    DROP TABLE IF EXISTS pg_temp._growth_baseline_pick;
    CREATE TEMP TABLE _growth_baseline_pick ON COMMIT DROP AS
    SELECT
        c.id AS creator_id,
        c.current_subscribers,
        row_number() OVER (
            ORDER BY c.current_subscribers DESC NULLS LAST, c.id
        ) AS rn
    FROM public.creators AS c
    WHERE c.archived_at IS NULL
      AND c.sync_status = 'synced'
      AND c.last_synced_at IS NOT NULL
      AND COALESCE(c.current_subscribers, 0) >= COALESCE(p_min_subscribers, 0)
      AND c.prev_subscribers IS NOT NULL
      AND c.prev_subscribers > 0
      AND c.current_subscribers > c.prev_subscribers
      AND c.growth_window_days IS NULL
      AND c.prev_snapshot_at <= statement_timestamp() - interval '7 days'
      AND public.try_parse_creator_upload_timestamp(
              c.recent_upload->>'published_at'
          ) >= statement_timestamp() - interval '90 days'
      AND NOT EXISTS (
          SELECT 1
          FROM public.creator_sync_jobs AS j
          WHERE j.creator_id = c.id
            AND j.status IN ('pending', 'processing')
      )
    ORDER BY c.current_subscribers DESC NULLS LAST, c.id
    LIMIT v_budget;

    IF p_dry_run IS DISTINCT FROM false THEN
        RETURN QUERY
        SELECT 'positive_growth_missing_window'::text,
               count(*)::bigint,
               min(p.current_subscribers)::bigint,
               max(p.current_subscribers)::bigint,
               true
        FROM pg_temp._growth_baseline_pick AS p
        HAVING count(*) > 0;
    ELSE
        RETURN QUERY
        WITH ins AS (
            INSERT INTO public.creator_sync_jobs
                (creator_id, status, job_type, source, created_at)
            SELECT p.creator_id,
                   'pending',
                   'sync_stats',
                   'growth_baseline_refresh',
                   statement_timestamp() + p.rn * interval '1 millisecond'
            FROM pg_temp._growth_baseline_pick AS p
            ON CONFLICT DO NOTHING
            RETURNING creator_id
        )
        SELECT 'positive_growth_missing_window'::text,
               count(*)::bigint,
               min(p.current_subscribers)::bigint,
               max(p.current_subscribers)::bigint,
               false
        FROM pg_temp._growth_baseline_pick AS p
        JOIN ins ON ins.creator_id = p.creator_id
        HAVING count(*) > 0;
    END IF;
END;
$$;

COMMENT ON FUNCTION public.enqueue_growth_baseline_refreshes(integer, integer, integer, boolean) IS
    'Dry-run-first, queue-capped refresh lane for active synced creators with positive subscriber growth and a missing valid growth window.';

REVOKE ALL ON FUNCTION public.enqueue_growth_baseline_refreshes(integer, integer, integer, boolean)
    FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.enqueue_growth_baseline_refreshes(integer, integer, integer, boolean)
    TO service_role;
