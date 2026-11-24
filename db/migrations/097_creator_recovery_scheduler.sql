BEGIN;

ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS sync_incomplete_since timestamptz,
    ADD COLUMN IF NOT EXISTS sync_completion_attempts integer NOT NULL DEFAULT 0;

CREATE OR REPLACE FUNCTION public.track_creator_sync_completion()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF NEW.sync_status IN ('pending', 'synced_partial') THEN
        IF TG_OP = 'UPDATE' AND OLD.sync_status IN ('pending', 'synced_partial') THEN
            NEW.sync_incomplete_since := COALESCE(
                OLD.sync_incomplete_since, OLD.last_synced_at,
                OLD.first_seen_at AT TIME ZONE 'UTC', statement_timestamp()
            );
        ELSE
            NEW.sync_incomplete_since := COALESCE(
                NEW.last_synced_at, NEW.first_seen_at AT TIME ZONE 'UTC',
                statement_timestamp()
            );
        END IF;
        IF NEW.sync_status = 'synced_partial' AND TG_OP = 'UPDATE' THEN
            IF NEW.last_synced_at IS DISTINCT FROM OLD.last_synced_at THEN
                NEW.sync_completion_attempts := OLD.sync_completion_attempts + 1;
            END IF;
        ELSIF NEW.sync_status = 'synced_partial' THEN
            NEW.sync_completion_attempts := 1;
        END IF;
    ELSIF NEW.sync_status = 'synced' THEN
        NEW.sync_incomplete_since := NULL;
        NEW.sync_completion_attempts := 0;
    END IF;
    RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION public.track_creator_sync_completion()
FROM PUBLIC, anon, authenticated;

DROP TRIGGER IF EXISTS trg_track_creator_sync_completion ON public.creators;
CREATE TRIGGER trg_track_creator_sync_completion
BEFORE INSERT OR UPDATE OF sync_status, last_synced_at ON public.creators
FOR EACH ROW EXECUTE FUNCTION public.track_creator_sync_completion();

CREATE OR REPLACE FUNCTION public.creator_recovery_candidates(p_min_subscribers integer DEFAULT 10000)
RETURNS TABLE (
    creator_id uuid, current_subscribers bigint, bucket text, eligible_since timestamptz
)
LANGUAGE sql STABLE
SET search_path = pg_catalog, public, pg_temp
AS $$
    WITH classified AS (
        SELECT creator.id, creator.current_subscribers,
               CASE
                   WHEN creator.last_synced_at IS NULL THEN '2_never_synced'
                   WHEN creator.sync_status = 'pending'
                       THEN '1_incomplete'
                   WHEN creator.sync_status = 'synced_partial'
                       THEN '1_incomplete'
                   ELSE '3_stale'
               END AS bucket,
               CASE
                   WHEN creator.last_synced_at IS NULL
                       THEN creator.first_seen_at AT TIME ZONE 'UTC'
                   WHEN creator.sync_status = 'pending'
                       THEN creator.last_synced_at + interval '72 hours'
                   WHEN creator.sync_status IN ('synced_partial', 'failed')
                       THEN creator.last_synced_at + interval '24 hours'
                   ELSE creator.last_synced_at + interval '30 days'
               END AS eligible_since
        FROM public.creators AS creator
        WHERE creator.archived_at IS NULL
          AND creator.sync_status IS DISTINCT FROM 'invalid'
          AND creator.channel_id ~ '^UC[a-zA-Z0-9_-]{22}$'
          AND COALESCE(creator.current_subscribers, 0) >= COALESCE(p_min_subscribers, 0)
          AND (creator.sync_status IS DISTINCT FROM 'synced_partial'
               OR creator.sync_completion_attempts < 3)
          AND NOT EXISTS (
              SELECT 1 FROM public.creator_sync_jobs AS job
              WHERE job.creator_id = creator.id
                AND job.status IN ('pending', 'processing')
          )
          AND NOT EXISTS (
              SELECT 1 FROM public.creator_sync_jobs AS job
              WHERE job.creator_id = creator.id AND job.status = 'failed'
                AND job.job_type = 'sync_stats'
                -- Snapshot refreshes are optional and must not seal the creator
                -- out of the core recovery lane after exhausting their retries.
                AND job.source IS DISTINCT FROM 'video_snapshot_refresh'
                AND job.retry_count >= 3
                AND COALESCE(job.completed_at, job.started_at, job.created_at)
                    >= COALESCE(creator.last_synced_at, '-infinity'::timestamptz)
          )
    )
    SELECT classified.id, classified.current_subscribers::bigint,
           classified.bucket, classified.eligible_since
    FROM classified
    WHERE classified.eligible_since <= statement_timestamp();
$$;

CREATE OR REPLACE FUNCTION public.enqueue_creator_syncs(
    p_limit integer DEFAULT 700, p_min_subscribers integer DEFAULT 10000,
    p_max_queue integer DEFAULT 1000, p_dry_run boolean DEFAULT true
)
RETURNS TABLE (bucket text, selected bigint, lowest_subs bigint, highest_subs bigint, dry_run boolean)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
#variable_conflict use_column
DECLARE
    v_active bigint;
    v_budget bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('public.enqueue_creator_syncs', 0));
    SELECT count(*) INTO v_active FROM public.creator_sync_jobs
    WHERE status IN ('pending', 'processing');
    v_budget := GREATEST(LEAST(
        COALESCE(p_limit, 0)::bigint, 700::bigint,
        LEAST(COALESCE(p_max_queue, 0)::bigint, 1000::bigint) - v_active
    ), 0);

    DROP TABLE IF EXISTS pg_temp._enq_pick;
    CREATE TEMP TABLE _enq_pick ON COMMIT DROP AS
    WITH candidates AS MATERIALIZED (
        SELECT * FROM public.creator_recovery_candidates(p_min_subscribers)
    ), recovery AS MATERIALIZED (
        SELECT candidates.* FROM candidates
        WHERE candidates.bucket IN ('1_incomplete', '2_never_synced')
        ORDER BY candidates.current_subscribers DESC NULLS LAST,
                 candidates.eligible_since, candidates.creator_id
        LIMIT (v_budget + 1) / 2
    ), refresh AS MATERIALIZED (
        SELECT candidates.* FROM candidates WHERE candidates.bucket = '3_stale'
        ORDER BY candidates.current_subscribers DESC NULLS LAST,
                 candidates.eligible_since, candidates.creator_id
        LIMIT v_budget / 4
    ), reserved AS MATERIALIZED (
        SELECT * FROM recovery UNION ALL SELECT * FROM refresh
    ), oldest AS (
        SELECT candidates.* FROM candidates
        WHERE NOT EXISTS (
            SELECT 1 FROM reserved WHERE reserved.creator_id = candidates.creator_id
        )
        ORDER BY candidates.eligible_since, candidates.creator_id
        LIMIT v_budget - (SELECT count(*) FROM reserved)
    ), picked AS (
        SELECT * FROM reserved UNION ALL SELECT * FROM oldest
    )
    SELECT picked.*, row_number() OVER (
        ORDER BY picked.bucket, picked.current_subscribers DESC NULLS LAST, picked.creator_id
    ) AS rn FROM picked;

    IF p_dry_run IS DISTINCT FROM false THEN
        RETURN QUERY
        SELECT pick.bucket, count(*)::bigint, min(pick.current_subscribers)::bigint,
               max(pick.current_subscribers)::bigint, true
        FROM pg_temp._enq_pick AS pick GROUP BY pick.bucket ORDER BY pick.bucket;
    ELSE
        RETURN QUERY
        WITH inserted AS (
            INSERT INTO public.creator_sync_jobs (creator_id, status, job_type, source, created_at)
            SELECT pick.creator_id, 'pending', 'sync_stats', 'priority_enqueue',
                   statement_timestamp() + pick.rn * interval '1 millisecond'
            FROM pg_temp._enq_pick AS pick
            ON CONFLICT DO NOTHING RETURNING creator_id
        )
        SELECT pick.bucket, count(*)::bigint, min(pick.current_subscribers)::bigint,
               max(pick.current_subscribers)::bigint, false
        FROM pg_temp._enq_pick AS pick JOIN inserted USING (creator_id)
        GROUP BY pick.bucket ORDER BY pick.bucket;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.get_creator_recovery_health(p_min_subscribers integer DEFAULT 10000)
RETURNS TABLE (overdue_incomplete bigint, exhausted_partial bigint, oldest_incomplete_since timestamptz)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
    SELECT count(*) FILTER (WHERE incomplete_since < statement_timestamp() - interval '7 days'),
           count(*) FILTER (WHERE sync_status = 'synced_partial' AND sync_completion_attempts >= 3),
           min(incomplete_since)
    FROM (
        SELECT creator.sync_status, creator.sync_completion_attempts,
               COALESCE(creator.sync_incomplete_since, creator.last_synced_at,
                        creator.first_seen_at AT TIME ZONE 'UTC') AS incomplete_since
        FROM public.creators AS creator
        WHERE creator.archived_at IS NULL
          AND creator.sync_status IN ('pending', 'synced_partial')
          AND COALESCE(creator.current_subscribers, 0) >= COALESCE(p_min_subscribers, 0)
    ) incomplete;
$$;

CREATE OR REPLACE FUNCTION public.refresh_creator_sync_coverage(p_min_subscribers integer DEFAULT 10000)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    IF NOT pg_try_advisory_xact_lock(hashtextextended('public.refresh_creator_sync_coverage', 0)) THEN
        RETURN false;
    END IF;
    INSERT INTO public.creator_sync_coverage_snapshot
        (singleton, sampled_at, min_subscribers, eligible_creators_without_job, oldest_eligible_since)
    SELECT true, statement_timestamp(), COALESCE(p_min_subscribers, 0), count(*), min(eligible_since)
    FROM public.creator_recovery_candidates(p_min_subscribers)
    ON CONFLICT (singleton) DO UPDATE SET
        sampled_at = EXCLUDED.sampled_at,
        min_subscribers = EXCLUDED.min_subscribers,
        eligible_creators_without_job = EXCLUDED.eligible_creators_without_job,
        oldest_eligible_since = EXCLUDED.oldest_eligible_since;
    RETURN true;
END;
$$;

REVOKE ALL ON FUNCTION public.creator_recovery_candidates(integer)
FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.enqueue_creator_syncs(integer, integer, integer, boolean)
FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.get_creator_recovery_health(integer)
FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.refresh_creator_sync_coverage(integer)
FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.creator_recovery_candidates(integer),
    public.enqueue_creator_syncs(integer, integer, integer, boolean),
    public.get_creator_recovery_health(integer),
    public.refresh_creator_sync_coverage(integer) TO service_role;

COMMIT;
