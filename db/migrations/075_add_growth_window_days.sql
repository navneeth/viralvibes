-- Record the actual elapsed time represented by creator growth metrics.
-- Nullable values mean there is not yet a valid baseline for the measurement.
ALTER TABLE public.creators
ADD COLUMN IF NOT EXISTS growth_window_days numeric;

COMMENT ON COLUMN public.creators.growth_window_days IS
    'Elapsed days between the previous creator snapshot and the current growth measurement';
