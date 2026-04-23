-- Migration 021: User favourite lists
--
-- Lets authenticated users bookmark curated lists (tabs, country pages,
-- category pages, language pages) for one-click access from their dashboard.
--
-- Design choices:
--   • list_key  — compact identifier, e.g. "top-rated", "country:US",
--                 "category:Gaming", "language:en".  Unique per user.
--   • list_label — human-readable label stored at save time so the dashboard
--                  can render tiles without re-fetching list metadata.
--   • list_url  — relative path stored at save time so the tile can link
--                 directly without URL reconstruction logic on the dashboard.
--   • ON DELETE CASCADE on user FK: favourites vanish when the user is deleted.
--   • UNIQUE(user_id, list_key): prevents duplicates; used as upsert target.
--   • No soft-delete: toggling removes the row outright.
--
-- Run once in the Supabase SQL editor.

CREATE TABLE IF NOT EXISTS public.user_favourite_lists (
    id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    uuid        NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    list_key   text        NOT NULL,
    list_label text        NOT NULL DEFAULT '',
    list_url   text        NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_user_favourite_list UNIQUE (user_id, list_key)
);

-- Efficient lookup: "which lists has this user bookmarked?"
CREATE INDEX IF NOT EXISTS idx_user_fav_lists_user_id
    ON public.user_favourite_lists (user_id, created_at DESC);
