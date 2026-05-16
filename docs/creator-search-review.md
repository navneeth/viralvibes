# Creator Search Review

Date: 2026-05-16

## Goal

Review how users find creators from the `/creators` search bar, trace a popular-channel lookup, and identify friction that makes it harder for users to find their own channel in the database.

## Current Path

1. The search input is rendered in `views/creators.py::_render_filter_bar`.
   It submits a normal `GET /creators` request with `search`, plus hidden filter state for sort, grade, language, activity, age, country, and category.

2. `routes/creators.py::creators_route` reads `search`.
   If the raw value starts with `@`, it first calls `find_creator_by_handle(search)`.

3. `db.py::find_creator_by_handle` strips the leading `@`, lowercases the handle, and searches `creators.custom_url` with `ilike(normalized_handle)`.
   This is effectively an exact case-insensitive handle lookup, because no `%` wildcards are added.

4. Whether the handle lookup succeeds or fails, the route falls through to normal listing search.
   For `@MrBeast`, the route strips the leading `@`, so the database listing search receives `MrBeast`.

5. `db.py::get_creators` builds a Supabase/PostgREST query:
   - base filters: `channel_name IS NOT NULL`, `current_subscribers > 0`, `sync_status IN ('synced', 'pending')`
   - search OR filter: `channel_name ILIKE '%term%' OR custom_url ILIKE '%term%' OR primary_category ILIKE '%term%' OR keywords ILIKE '%term%'`
   - optional filters: grade, language, activity, age, country, category
   - sort and pagination

6. If no results are found, or if an `@handle` was not found by the preflight handle lookup, the UI can show a creator submission CTA.
   Authenticated users can submit a handle or UC channel ID through `POST /creators/request`.

7. `queue_creator_add_request` validates the input, deduplicates against `channel_id` or `custom_url`, rate-limits by user, and inserts a `creator_sync_jobs` row with `job_type='resolve_and_add'`.

8. The worker resolves the handle to a UC channel ID through YouTube, inserts a minimal creator stub if needed, and converts the job to `job_type='sync_stats'`.
   A later sync fills full channel stats.

## Popular Channel Trace: `@MrBeast`

Input: `/creators?search=@MrBeast`

Expected happy path if the creator exists and `custom_url = 'mrbeast'`:

1. `creators_route` detects handle mode.
2. `find_creator_by_handle('@MrBeast')` normalizes to `mrbeast`.
3. The DB lookup queries `custom_url ILIKE 'mrbeast'`.
4. If a row exists, the route logs the match but does not redirect directly to the profile.
5. The route strips the leading `@` and calls `get_creators(search='MrBeast', sort='subscribers', return_count=True)`.
6. `get_creators` searches four text columns with `%MrBeast%`, applies base filters, sorts by subscribers, and returns a paginated result grid.

Expected path if the exact handle is missing:

1. `find_creator_by_handle('@MrBeast')` returns no row.
2. `handle_not_found = True`.
3. The route still calls `get_creators(search='MrBeast')`, so similarly named channels or keyword matches can still appear.
4. If results exist, the user sees a "not found" style add CTA alongside possibly unrelated results.
5. If no results exist, the empty state becomes the add/request path.
6. On submit, the worker resolves `@mrbeast` to a UC channel ID, inserts or finds the creator, and schedules stats sync.

## Observations

The search bar is simple and predictable, but it mixes several user intents into one query: exact handle lookup, creator name search, category discovery, keyword discovery, and country-like natural language. This makes broad discovery easy, but it makes "find my exact channel" less deterministic.

The preflight handle lookup is useful, but the route does not short-circuit to a profile or prioritize the exact handle result. Even when `@MrBeast` is found, the user still lands on a general search grid sorted by subscribers.

`@` is stripped before normal search, which prevents noisy `keywords` matches on literal `@mentions`. That is a good precision improvement.

The code comments say only indexed `ILIKE` columns are safe in the multi-column search, but this repo only shows trigram indexes for `primary_category` and `channel_description`. I did not find matching migrations for `channel_name`, `custom_url`, or `keywords`. If those indexes do not exist in production, popular-name searches can still become expensive as the table grows.

`get_creators` includes `sync_status IN ('synced', 'pending')` while several migrations and comments describe partial indexes and aggregates restricted to `sync_status = 'synced'`. This can make planner behavior and result counts diverge from index assumptions, especially for search and listing pages.

The add-creator flow inserts a stub with `channel_name = 'Pending sync...'` and `current_subscribers = 0`. Because listing search requires `current_subscribers > 0`, newly submitted creators remain invisible in search until sync completes. The polling card helps only while the user stays on that submission response.

The fallback resolver uses YouTube `search.list(q=handle, type='channel', maxResults=1)` after `forUsername`. For exact `@handle` intent, a top search result can be the wrong channel when handles are similar or the target is small.

Search result ranking is inherited from the selected sort field. For "find my channel", exact `custom_url` and exact `channel_name` matches should outrank larger fuzzy or keyword matches, but the current query cannot express that ranking.

The route contains a disabled preview path (`youtube_api = None`). This suggests the intended direct "found on YouTube, add it" experience is currently absent from the frontend route.

## Supabase Verification

Live database checks on 2026-05-16 confirmed the highest-risk search assumptions:

- `pg_trgm` is installed.
- The `creators` table has roughly 810,976 rows.
- `custom_url` is present for roughly 728,256 rows.
- Roughly 728,252 `custom_url` values start with `@`.
- Roughly 184,010 `custom_url` values are mixed case.
- Roughly 728,252 rows are not normalized to the app's assumed format of lowercase handle without `@`.
- Exact lookup for `custom_url ILIKE 'mrbeast'` used a sequential scan, removed 810,976 rows by filter, and took roughly 1.0s.
- A broader check using `custom_url ILIKE '@mrbeast'` or normalized `LOWER(REGEXP_REPLACE(custom_url, '^@', '')) = 'mrbeast'` found the expected MrBeast channel row.
- A normalized lookup with `LOWER(LTRIM(custom_url, '@')) = 'mrbeast'` also used a sequential scan. It happened to return in roughly 14.5ms because the matching row appeared early, but the plan still has no index support and can scan the full table for misses or late matches.
- After creating a partial expression index on `LOWER(LTRIM(custom_url, '@'))`, the same lookup still used a sequential scan for `mrbeast`. The likely causes are that the query did not include the partial-index predicates and `LIMIT 1` made an early heap hit look cheap to the planner. Verification should test with the partial predicates and with a missing handle.
- With the partial predicates included (`custom_url IS NOT NULL` and `custom_url <> ''`), Postgres uses `idx_creators_custom_url_normalized_expr` for both existing and missing handles. The `mrbeast` lookup executed in roughly 0.216ms and a missing-handle lookup executed in roughly 0.118ms.
- A production log later showed `/creators?search=asianometry` timing out in `get_creators()` with Postgres `57014`. That is the broader OR-`ILIKE` listing query, not the exact-handle RPC path. It still needs matching trigram indexes for every searched text column or a ranked search RPC.
- A later trace for `@aiDotEngineer` showed that an exact-handle miss still fell through to two legacy `custom_url ILIKE` fallback requests and then the broad OR-`ILIKE` listing query. The app was tightened so an empty successful RPC response is treated as "not found" without legacy fallbacks, and exact `@handle` misses skip broad discovery and render the add-creator empty state directly.
- A plain handle-like search such as `aiengineer` can still hit the broad discovery path. The app now returns a clean empty exact-search result when that broad query times out after an exact-handle miss, and migration `038_ranked_creator_search_rpc.sql` adds a ranked search RPC intended to replace the PostgREST OR-`ILIKE` query for unfiltered text search.

This means the current exact-handle path is likely missing most existing creators because app code strips `@` before comparing against a column where most values include `@`. The broad `%term%` search can still find many creators, but it does so through a slower, lower-precision path.

## Friction Points For Users

1. A user who knows their exact handle may still get a generic results grid instead of their profile.
2. A smaller creator can be buried under larger creators that mention the search term in `keywords`.
3. A newly submitted creator is not discoverable in normal search until stats sync fills subscriber counts.
4. Missing-channel feedback depends on whether the query starts with `@`; plain handle input like `MrBeast` does not get the same exact-handle not-found path.
5. The submission path requires authentication, so unauthenticated users can discover a miss but cannot resolve it in-place.
6. The backend has no unified search intent model, so UI behavior, DB search, and add-request behavior each normalize inputs separately.

## Architectural Changes To Consider

### 1. Split Search Into Intent Stages

Create a `CreatorSearchService` that returns a typed result:

- `exact_match`: canonical channel ID, handle, or exact normalized handle match
- `ranked_matches`: fuzzy name/category/keyword results
- `missing_exact_handle`: safe to show add CTA
- `pending_request`: request already queued, with status

The route can then handle exact lookups decisively before rendering a broad discovery grid.

### 2. Add A Canonical Lookup Table Or Generated Columns

Normalize identifiers into a dedicated lookup surface:

- `channel_id`
- lowercased handle without `@`
- lowercased display name
- previous handles or aliases, if available later
- optional `creator_id`

Back it with unique/exact indexes for handles and channel IDs. This gives "find my channel" a fast path separate from fuzzy discovery.

Given the live data, the lowest-risk version is a stored generated column:

- `custom_url_normalized = LOWER(LTRIM(custom_url, '@'))`
- indexed with a btree partial index
- queried with `.eq("custom_url_normalized", normalized_handle)` from Supabase/PostgREST

This avoids rewriting the existing `custom_url` values immediately while giving the app a stable exact-match surface.

As an incremental step, production now has `find_creator_by_normalized_handle(p_handle text)`.
The app can call this RPC for exact handle checks while keeping the broader discovery search unchanged.

Migration `036_creator_search_exact_and_trgm.sql` captures this RPC, the normalized-handle expression index, and matching trigram indexes for the current broad search columns.

### 3. Move Fuzzy Search To A Ranked RPC

Replace the PostgREST OR `ILIKE` search with a Postgres RPC that can compute ranking:

1. exact `custom_url`
2. exact normalized `channel_name`
3. prefix handle/name
4. trigram similarity on handle/name
5. category match
6. keyword match

Return a `match_reason` and `match_score` so the UI can explain why a result appears.

### 4. Treat Pending Creators As Searchable

Add a lightweight pending result state for submitted creators. If a request has resolved to a `creator_id` but full stats are not synced yet, the search page should show a pending creator row/card instead of hiding it behind `current_subscribers > 0`.

### 5. Improve Handle Resolution Accuracy

Prefer exact URL/handle resolution over generic channel search where possible. If YouTube API limitations require `search.list`, validate the returned channel's `customUrl` or canonical handle during `get_channel_data` before accepting it as the requested handle.

### 6. Align Indexes With Query Semantics

Make the index set match the actual search columns and base filters. If the live query keeps `sync_status IN ('synced', 'pending')`, indexes should account for that, or the query should split synced listing results from pending add-request results.

### 7. Add Search Trace Tests

Add tests for:

- `@MrBeast` exact handle normalizes to `mrbeast`
- exact handle match is prioritized or redirects
- missing `@handle` shows add CTA without keyword noise
- plain `mrbeast` can still find `custom_url='mrbeast'`
- pending resolved creator appears in an expected pending state

## Suggested Next Step

Start by implementing the least invasive change: a `CreatorSearchService` or helper that classifies input into exact handle, UC channel ID, or broad text search, then make `@handle` exact matches redirect to `/creator/{id}` or render the exact result first. That reduces user friction immediately while preserving the current discovery grid.
