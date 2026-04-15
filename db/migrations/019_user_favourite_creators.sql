-- Migration 019: User favourite creators
--
-- Lets authenticated users bookmark creators for quick access from their
-- dashboard.  The table records a (user_id, creator_id) pair with a
-- timestamp so the UI can show recently-favourited creators first.
--
-- Design choices:
--   • ON DELETE CASCADE on both FKs: if a user is deleted their favourites
--     disappear automatically; if a creator is removed from the DB the row
--     vanishes quietly rather than leaving orphans.
--   • UNIQUE(user_id, creator_id): prevents duplicates and is the upsert
--     conflict target for idempotent toggle calls.
--   • No soft-delete: toggling removes the row outright (cheap and simple).
--
-- Run once in the Supabase SQL editor.

CREATE TABLE IF NOT EXISTS public.user_favourite_creators (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     uuid        NOT NULL REFERENCES public.users(id)    ON DELETE CASCADE,
    creator_id  uuid        NOT NULL REFERENCES public.creators(id) ON DELETE CASCADE,
    created_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_user_favourite_creator UNIQUE (user_id, creator_id)
);

-- Efficient lookup: "which creators has this user favourited?"
CREATE INDEX IF NOT EXISTS idx_user_fav_creators_user_id
    ON public.user_favourite_creators (user_id, created_at DESC);

-- Efficient lookup: "how many users have favourited this creator?" (future metric)
CREATE INDEX IF NOT EXISTS idx_user_fav_creators_creator_id
    ON public.user_favourite_creators (creator_id);
