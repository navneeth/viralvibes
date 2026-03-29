-- Applies only the parts that didn't land from the previous migration:
-- app_stats table + refresh_total_categories function.

-- ── 1. app_stats table ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.app_stats (
    key          text        PRIMARY KEY,
    value        bigint      NOT NULL DEFAULT 0,
    refreshed_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO public.app_stats (key, value)
VALUES ('total_categories', 0)
ON CONFLICT (key) DO NOTHING;


-- ── 2. refresh_total_categories RPC ──────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.refresh_total_categories()
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    v_count bigint;
BEGIN
    SELECT count(DISTINCT category.value)
    INTO v_count
    FROM public.creators c,
         LATERAL jsonb_array_elements_text(
             CASE
                 WHEN jsonb_typeof(c.topic_categories::jsonb) = 'array'
                     THEN c.topic_categories::jsonb
                 ELSE '[]'::jsonb
             END
         ) category(value)
    WHERE
        c.channel_name IS NOT NULL
        AND c.current_subscribers > 0
        AND c.topic_categories IS NOT NULL;

    INSERT INTO public.app_stats (key, value, refreshed_at)
    VALUES ('total_categories', coalesce(v_count, 0), now())
    ON CONFLICT (key) DO UPDATE
        SET value        = EXCLUDED.value,
            refreshed_at = EXCLUDED.refreshed_at;

    RETURN coalesce(v_count, 0);
END;
$function$;


-- ── 3. Update get_lists_meta to read total_categories from app_stats ──────────
-- (Re-applying in case the previous version still has the inline subquery)
CREATE OR REPLACE FUNCTION public.get_lists_meta()
RETURNS TABLE(
    total_creators   bigint,
    total_countries  bigint,
    total_languages  bigint,
    total_categories bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        m.total_creators,
        m.total_countries,
        m.total_languages,
        coalesce(s.value, 0) AS total_categories
    FROM public.mv_lists_meta m
    LEFT JOIN public.app_stats s ON s.key = 'total_categories';
END;
$function$;
