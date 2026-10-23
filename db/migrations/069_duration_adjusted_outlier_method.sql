-- Migration 069: versioned duration-adjusted creator breakout scores.
-- Legacy outlier_videos remain stored, but the profile hides them until the
-- worker writes the new method version and scored-sample count.

ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS outlier_scored_video_count integer,
    ADD COLUMN IF NOT EXISTS outlier_method_version text;

COMMENT ON COLUMN public.creators.outlier_method_version IS
    'Scoring method version used for outlier_videos; NULL means legacy scores are unversioned.';

COMMENT ON COLUMN public.creators.outlier_scored_video_count IS
    'Count of recent videos for which lifetime views/day had a valid prior-upload baseline.';

COMMENT ON COLUMN public.creators.outlier_count IS
    'Count of sampled videos whose duration-adjusted lifetime views/day exceeded their leave-one-out prior-upload baseline.';

COMMENT ON COLUMN public.creators.outlier_videos IS
    'Top five duration-adjusted lifetime views/day standouts, with expected/observed rates, prior-upload sample size, and method version.';

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'creators'
  AND column_name IN ('outlier_scored_video_count', 'outlier_method_version')
ORDER BY column_name;
