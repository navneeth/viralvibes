ALTER TABLE public.creators
    ADD COLUMN IF NOT EXISTS growth_window_days numeric(6,2);

COMMENT ON COLUMN public.creators.growth_window_days IS
    'Observed elapsed days between the baseline and current creator-stat snapshots used for *_change_30d values. NULL means the window is unknown or growth tracking is initializing.';