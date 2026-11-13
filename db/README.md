# Database Migrations

Sequential SQL migrations for the ViralVibes database schema.

## Migration Naming Convention

```
NNN_descriptive_name.sql
```

- `NNN`: Three-digit sequential number (001, 002, 003...)
- Use underscores for spaces
- Descriptive but concise

## Running Migrations

### Via psql (Supabase/Postgres)

```bash
# Set connection string
export DATABASE_URL="postgresql://postgres:[PASSWORD]@[HOST]:5432/postgres"

# Run a specific migration
psql $DATABASE_URL -v ON_ERROR_STOP=1 -f db/migrations/001_add_growth_tracking_columns.sql

# Run all migrations in order
for f in db/migrations/*.sql; do
  echo "Running $f..."
  psql $DATABASE_URL -v ON_ERROR_STOP=1 -f "$f" || break
done
```

### Via Supabase Dashboard

1. Go to SQL Editor in Supabase Dashboard
2. Copy migration contents
3. Run query
4. Verify with the diagnostic output

## Migration Checklist

- [ ] Add sequential number prefix
- [ ] Include descriptive comments
- [ ] Use `IF NOT EXISTS` for safety
- [ ] Add column comments for documentation
- [ ] Include verification/diagnostic queries
- [ ] Test on development database first
- [ ] Document breaking changes in commit message

## Current Migrations

### Scheduled Enrolled Snapshot Refresh (094)

Migration 094 adds `enqueue_video_snapshot_refreshes`, a service-role-only RPC
for recurring refresh of already-enrolled creators. It does not enroll creators
or spend admission budgets. A snapshot is due when missing or at least seven
days old; a fresh `[]` is a successful result and is not refreshed early.
Only unarchived, canonical-channel creators with `synced` or `synced_partial`
status qualify. Pending jobs (including delayed retries), processing jobs, and
snapshot-refresh failures within seven days exclude a creator.

The RPC uses the shared enqueue lock, accepts at most 40 jobs per call, and
inserts nothing when the pending queue has reached 250. It does not require
the general queue to drain. Jobs use the existing `sync_stats` handler with
source `video_snapshot_refresh`; oldest snapshot timestamps are selected first.
No additional worker, YouTube API path, or notebook setting is needed.

One-time rollout:

1. Test migration 094 on staging after the existing 077 and 087-093 migrations.
  Run the disposable SQL fixture below separately, never against production.
2. Apply 094 in Supabase SQL Editor. It is transactional and has no concurrent
  index build. Deploy this bootstrap/workflow change to the default branch.
  Keep the existing scheduled creator worker enabled and updated for enrollment
  support from 088. Allowlist-only creators are not automatically enrolled.
3. In GitHub repository Settings > Secrets and variables > Actions > Variables,
  set `VIDEO_SNAPSHOT_REFRESH_MODE` to `preview`. Run the existing **Creator
  Bootstrap** workflow and review its `Snapshot refresh preview` log, candidate
  UUIDs, and RPC latency. Preview does not insert snapshot jobs; normal bootstrap
  maintenance still runs. Confirm missing/stale eligible creators are returned
  and fresh, archived, active, and unenrolled creators are excluded.
4. After staging validation succeeds, change the variable to `apply` once.
  The existing bootstrap schedule sweeps at 23:50, 05:50, 11:50, and 17:50 UTC.
  At 40 jobs per sweep it provides 1,120 weekly slots, enough to cycle the full
  1,000-creator enrollment cap within seven days when the queue drains and the
  worker completes those jobs. A 250-pending-job ceiling and failures can delay
  individual snapshots, so this is a capacity target rather than a guarantee.
  The existing six-hour worker consumes the queued jobs without recurring manual
  intervention. This is a seven-day freshness target, not an exact execution
  guarantee: GitHub scheduling, queue capacity, failures, and quota can delay it.

The variable defaults to `off`. Set it back to `off` to stop future snapshot
enqueue passes; already-queued jobs and enrollment remain intact. An RPC failure
is logged and fails the bootstrap run without an immediate retry because a
timeout can have an unknown enqueue outcome. Archival failure remains fail-closed
for all enqueue passes.

Verify freshness and processing with these read-only queries:

```sql
SELECT count(*) AS enrolled,
     count(*) FILTER (
        WHERE recent_videos IS NULL OR recent_videos_fetched_at IS NULL
          OR recent_videos_fetched_at > now()
          OR recent_videos_fetched_at <= now() - interval '7 days'
     ) AS due
FROM public.creators
WHERE recent_videos_requested_at IS NOT NULL AND archived_at IS NULL;

SELECT creator_id, status, created_at, completed_at
FROM public.creator_sync_jobs
WHERE source = 'video_snapshot_refresh'
ORDER BY created_at DESC
LIMIT 25;

SELECT id, recent_videos_fetched_at, jsonb_array_length(recent_videos) AS cached_videos
FROM public.creators
WHERE recent_videos_requested_at IS NOT NULL
ORDER BY recent_videos_fetched_at DESC NULLS LAST
LIMIT 25;
```

The due count includes unhealthy/active creators excluded by the RPC, so it is
not a guaranteed enqueue count. Zero enrollment means this lane has no work;
enrollment growth remains governed by 088-090, not this scheduler.

Run against an **empty disposable Supabase database** with standard roles:

```bash
psql "$TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -f db/tests/video_snapshot_demand.sql
```

The fixture now also covers recurring freshness, NULL/repeated previews,
queue caps, batch limits, active-job deduplication, failed-refresh cooldown,
unchanged enrollment/admission budgets, and RPC permissions. It does not prove
production query latency or concurrent job-claim behavior; verify those in staging
before switching to `apply`.

### Recent Video Snapshot Foundation (087)

PR 1 adds nullable `creators.recent_videos` and
`recent_videos_fetched_at` columns plus a metadata-only normalization helper.
Snapshots contain at most 12 videos and 32 KiB of serialized JSON. No image
URLs, bytes, availability flags, scores, indexes, backfill, or new grants are
added. Existing creator-table access policies continue to apply; verify those
policies before enabling the pilot.

The constraint is installed `NOT VALID` to avoid scanning the creator table.
It still checks new inserts and updates. The migration does not populate data
or enable collection. Apply it separately from the worker rollout and verify
both columns using the included read-only query.

PR 2 opts selected creator UUIDs into snapshot writes using the existing
recent-video API response, with no additional YouTube requests. Collection is
disabled by default. Set `CREATOR_RECENT_VIDEO_ALLOWLIST` to a comma-separated
list of `creators.id` UUIDs (not channel IDs). Invalid UUIDs are ignored with a
warning; wildcards are not supported. The worker reads the environment per job.

For Kaggle, set the environment variable before launching the runner:

```python
import os
os.environ["CREATOR_RECENT_VIDEO_ALLOWLIST"] = "6488b4d0-96e9-4cb3-b7da-36cc086f8e75"
```

Use only reviewed pilot IDs. For other runtimes, configure the same variable in
the creator worker's environment; setting it on the web application alone does
not configure a separately deployed worker. This does not enqueue jobs: the
snapshot is collected the next time an enabled creator is normally synced.

Fetch failures preserve the snapshot; a successful empty response writes `[]`.
Malformed metadata that cannot produce valid rows does not clear cached data.
Other creators are not written; previously stored snapshots are retained if an
ID is later removed from the allowlist. Writes replace rather than append and
use `recent_videos_fetched_at` to reject older concurrent snapshots. Optional
writes run separately from the core creator update. Missing migration columns,
schema-cache lag, or snapshot-write errors must not fail an otherwise valid sync.

After a pilot run, verify only reviewed IDs using this read-only query:

```sql
SELECT id, recent_videos_fetched_at,
       jsonb_array_length(recent_videos) AS cached_video_count,
       octet_length(recent_videos::text) AS snapshot_bytes
FROM public.creators
WHERE id = '6488b4d0-96e9-4cb3-b7da-36cc086f8e75'::uuid;
```

To stop new collection, unset the variable or set it to an empty string. No
history, image data, or new queue lanes are introduced.

PR 3 renders the latest-12 snapshot after profile analytics and before similar
creators, using the existing creator-row read. Profiles with `NULL` or missing
snapshot data omit the section. An empty array renders a successful-empty
message; malformed rows do not become YouTube URLs. Missing metrics remain
unavailable rather than zero, and the metadata fetch date is shown when known.

Official thumbnails load directly from `i.ytimg.com`. Mid frames load when rows
approach the viewport; Early/Mid/Late controls select other frames without a
server request. The browser tries `maxresN.jpg` then `hqN.jpg`, rejects the
120-pixel placeholder, and offers retry on failure. An in-memory bounded cache
deduplicates probes; request guards ignore stale frame-selection responses.
Availability is a browser observation, not a stored flag or an honesty score.

History pagination, Shorts classification, scoring, image storage, and new
queue lanes remain deferred. Verify the public image URLs across representative
videos and browsers; frame timestamps and availability are not guaranteed.

### Video Snapshot Pilot Diagnostics (PR 4)

The Kaggle summary reports per-run video snapshots saved, snapshot-write errors,
and migration-column skips. These counters do not reset cumulative YouTube quota
accounting. A stale-write guard that returns no updated row is not counted as a
successful write or an error.

Optional coverage diagnostics are disabled by default. Enable them before a
normal worker run with:

```python
import os
os.environ["KAGGLE_REPORT_VIDEO_SNAPSHOT_HEALTH"] = "true"
```

The health reader inspects only the first 25 sorted, valid UUIDs configured in
`CREATOR_RECENT_VIDEO_ALLOWLIST`, using one primary-key-filtered read. It reports
configured/inspected/not-inspected counts, missing-or-inaccessible creators,
uncollected/empty/populated/invalid snapshots, cached video count, and snapshots
at least seven days old. Stale is a freshness diagnostic, not a content-quality
verdict or an automatic refresh trigger. Invalid includes malformed snapshot
shape, invalid video IDs, and missing, malformed, or future fetch timestamps.

No allowlist means no health read. Missing columns or read failures make health
unavailable, not healthy; warnings identify the reason. Report failures do not
interrupt the regular run summary. The read itself does not enqueue, update,
download images, or call YouTube. It transfers bounded cached metadata rather
than scanning the creator population. No migration or index is added.

After the worker has initialized, diagnostics can also be inspected separately:

```python
import worker.creator_worker as cw
print(cw.get_recent_video_snapshot_health())
```

This helper is read-only. A normal worker invocation is not: its existing job
processing and startup/end-of-run maintenance are unchanged. Disable automatic
health reads by unsetting `KAGGLE_REPORT_VIDEO_SNAPSHOT_HEALTH` or setting it to
`false`.

### Bounded Profile Demand Enrollment (088)

Set `VIDEO_SNAPSHOT_DEMAND_ENABLED=true` on the web application only after the
migration and staging tests pass. It is disabled by default. Authenticated
profiles with missing or seven-day-old snapshots issue a same-origin HTMX POST
after rendering. Anonymous profiles never request admission. GET rendering does
not enqueue or call YouTube; cached data is returned immediately.

The service-role-only `request_creator_video_snapshot` RPC persists
`creators.recent_videos_requested_at`, which all updated creator workers honor
without environment allowlists. Existing manual allowlists remain supported.
Restart workers after applying the migration so schema detection sees the new
column. If PostgREST's cache lags despite schema detection, workers use the
service-role-only `creator_video_snapshot_is_enrolled` RPC to preserve demand
state. Disabling the web flag stops new demand admissions; existing enrollment
and snapshots remain intact.

Admission is serialized with the existing creator-sync enqueue lock. Pending
jobs are reused under a row lock; processing jobs are deferred rather than
enrolled after their metadata read. Invalid channels and archived creators are
not admitted. Limits are fixed in SQL:

| Limit | Value |
| --- | --- |
| Admissions per UTC day | 50 |
| Admissions per user per UTC day | 10 |
| Per-creator request cooldown | 24 hours |
| Persistently enrolled creators | 1,000 |
| Pending queue ceiling for new jobs | 250 |

Enrollment has no automatic expiry; its cap bounds snapshot storage to at most
1,000 arrays of 12 videos and 32 KiB, excluding older manual-pilot snapshots.
The private daily budget table cleans up counters older than 30 days on every
admission RPC call, including calls rejected by admission limits. Actor UUIDs
are retained for that short rate-limit window even if a user is deleted,
preventing deletion from resetting the global budget. It is inaccessible to
client roles.

The existing worker scheduler must still be running. Admission does not promise
immediate processing, and this PR does not poll or automatically replace the
profile after a job completes. Revisit the profile after the worker processes it.
Busy, cooldown, queue/cohort limits, and unavailable admissions leave existing
data unchanged. No images, scores, history, or new job types are introduced.

With no manual allowlist and migration 088 detected, the opt-in health reader
inspects up to 25 most recently requested creators using the enrollment index
(26 rows fetched to identify more results). The report uses `scope=recent_demand`
and `has_more`; global and inaccessible counts are unknown rather than reported
as zero. An explicit manual allowlist continues to take precedence.

Migration 088 uses a regular `CREATE INDEX` so it can be run in the Supabase
web SQL editor. This index build takes a write-blocking lock on `creators`;
schedule it during a maintenance window if writes to that table cannot pause.
The migration depends on 087 and the active-job uniqueness/enqueue
serialization from 077 and 086. Verify the enrollment index is ready and valid;
do not enable the flag if its build failed. Apply the migration first, then
deploy and restart every creator worker. Verify all workers have loaded the
enrollment column and are healthy before enabling the web flag; do not enable
admissions while any old worker can claim sync jobs.

Run admission tests only on an empty disposable Supabase database with its
standard roles available. The fixture refuses existing application tables:

```bash
psql "$TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -f db/tests/video_snapshot_demand.sql
```

The SQL test covers enrollment, queue reuse, processing deferral, freshness,
cooldown, actor/creator rejection, caps, and RPC permissions. Concurrent
admission and job-claim behavior must also be verified in staging. Local
Python tests do not prove deployed SQL constraints or locks.

### Unknown-Activity Refresh Pilot (083)

Migration 083 installs `enqueue_unknown_activity_refreshes` without inserting
jobs. It accepts an explicit allowlist of up to five UUIDs, defaults to dry run,
and uses the existing `3_unknown_activity` eligibility rule. Apply mode requires
the valid active-job unique index from migration 077. Only `service_role` can
execute it. It uses the priority enqueue advisory lock and permits at most five
new jobs when the pending queue has room below 1,000; no general enqueue limits
are changed. Automatic candidate supply is a separate path provided by migrations
084-086 and remains disabled unless Kaggle is explicitly run with
`refresh_auto=True`.

Migration 089 connects that automatic batch to snapshot enrollment. Apply it
after migration 088 and before the next Kaggle run. Each nightly
`refresh_auto=True, refresh_dry_run=False` run can select up to five eligible
creators; only creators whose refresh jobs are actually inserted are enrolled.
Updated workers then collect snapshots for those jobs without a manual
`CREATOR_RECENT_VIDEO_ALLOWLIST`. Enrollment persists and snapshots are
collected again on later syncs for enrolled creators.

Automatic admissions and authenticated profile requests share the 50-per-UTC-
day limit and 1,000-creator lifetime cohort cap. Automatic runs count only new
enrollments against those limits; profile requests still also have their
existing per-user cap. The cohort can grow by at most five new creators per
nightly run, depending on eligibility. The Kaggle runner attempts this batch
only after the general FIFO queue has no currently eligible jobs; if it reaches
`max_jobs` first, the automatic batch does not run. Apply migrations 089-091, restart
every creator worker before running Kaggle. The script entry point
(`%run kaggle_worker.py`) and documented notebook invocation explicitly use
`refresh_auto=True` and `refresh_dry_run=False`, so the automatic batch is
attempted after the regular queue drains.

Apply the migration, then run this preview in Supabase SQL Editor:

```sql
SELECT * FROM public.enqueue_unknown_activity_refreshes(ARRAY[
  '6488b4d0-96e9-4cb3-b7da-36cc086f8e75'::uuid,
  '14076ba5-f130-4f86-9b53-ecf3b93ed077'::uuid,
  '1c540d99-99fc-447b-8ee0-5c2dee9006e7'::uuid,
  '4a9591a1-a84c-44d9-8b98-bbe0bc6ae963'::uuid,
  'd899ca2d-e87e-4e7a-ad74-cbc1ceca0dc8'::uuid
]);
```

Review the eligible rows before any apply call. Eligibility is rechecked each
time; creators already pending/processing or no longer stale will be excluded.
In Kaggle, supply this same reviewed allowlist as `refresh_creator_ids` to
`await kaggle_worker.run(...)`. The regular FIFO queue runs first. After it stays
empty for the configured number of polls, the pilot previews only the IDs that
fit within the remaining `max_jobs` budget and exits by default. After reviewing
that preview, explicitly pass `refresh_dry_run=False` to enqueue and process up
to five jobs, one at a time. The pilot fetches only returned allowlisted jobs
with source `unknown_activity_refresh`, so it never falls back to the general
FIFO backlog. Normal startup and end-of-run coverage maintenance are retained;
materialized-view refresh remains controlled by
`KAGGLE_REFRESH_MATERIALIZED_VIEWS`. No API key rotation or concurrent jobs are
added.

If the pilot RPC fails, the refresh phase stops without retrying the request;
the regular-job summary and end-of-run maintenance still run. An apply-mode
timeout has an unknown enqueue outcome: inspect allowlisted jobs before another
apply run. A dry-run timeout does not insert refresh jobs. Measure RPC duration
before increasing HTTP timeouts or enabling pilot writes.

The initial supplied audit showed 716 pending jobs and one processing job, with
all five pilot creators eligible. Local tests use mocked Supabase responses;
the SQL Editor dry-run returned five eligible rows, but the Kaggle dry-run RPC
timed out before returning a preview. Real pilot processing remains unverified.
After apply, verify
these creators' `last_synced_at` advances and inspect their job outcomes before
expanding the lane. Do not rerun enqueue on every poll or increase the pilot cap.

Automatic mode calls migration 090's `enqueue_unknown_activity_refresh_batch_after_drain`
after regular FIFO work drains. It serializes the queue check and refresh enqueue,
and the underlying batch RPC is restricted to this wrapper. Migration 084 requires the safe timestamp parser
from 078 and the active-job unique index from 077. Migration 085 adds the failed
refresh cooldown index using `CREATE INDEX CONCURRENTLY`; run it with psql
autocommit, outside a transaction. Migration 086 serializes direct
`creator_sync_jobs` inserts with the enqueue RPCs by using their shared advisory
lock. Apply 084-091 before enabling automatic mode. The guarded RPC defaults
to dry run, but dry-run mode still processes the regular worker queue; it is not
a preview-only invocation. Review candidate count and query latency before
setting `refresh_dry_run=False`.

### Concurrent Hero Stats Refresh (082)

Migration 082 records the tested plain-column unique index on
`mv_hero_stats(total_creators)` and concurrent `refresh_mv_hero_stats()`.
The populated view is a single-row aggregate, so this index is unique without
recreating the view or adding a column. The existing expression index is retained.

The function keeps its signature and grants but no longer disables the caller's
statement timeout. Installation is transactional with a 3-second lock timeout and
10-second statement timeout; these limits reset when the transaction ends. If
installation times out, investigate blockers before retrying. It does not execute
a refresh or change other refresh functions, worker code, or HTTP timeouts.

After installation, the new index must have all five diagnostic flags true.
Production function-definition verification and a service-role refresh with
3-second lock and 15-second statement limits passed. The measured refresh was
8,349 ms; the returned `rows_refreshed` value, 893,438, is the aggregate creator
count, not the number of rows in the single-row materialized view.

The equivalent SQL changes are already applied in production. This does not prove
refresh-time performance under every load or concurrent-reader behavior during
execution. Both bootstrap workflows now set `SUPABASE_TIMEOUT_S=30` and share the
`creator-bootstrap-maintenance` concurrency group with `cancel-in-progress: false`.
This gives maintenance requests a bounded HTTP budget and prevents those two
workflows from running together, including manual runs. Vercel and other worker
clients retain their existing timeout configuration. Other repositories, notebooks,
and direct SQL callers are not serialized by GitHub Actions concurrency.

The bootstrap workflow changes take effect after deployment; their runtime behavior
has not yet been verified in production. Database deadlines still apply and are not
disabled. A shorter live database deadline requires separate measurement and role
configuration review. Bootstrap cache invalidation is process-local; it does not
clear caches in running Vercel instances. Category-cache refresh and deep-pagination
timeouts remain separate issues.

### Creator Peers Client Read-Only Access (081)

Migration 081 records the production-verified removal of direct client write
privileges on `public.creator_peers`. It preserves existing SELECT and
service-role grants, sequence permissions, data, RLS, and RPC privileges.
It is safe to repeat after the manual SQL Editor change; it does not establish
missing grants for fresh/reset environments or change notebook code.

Its diagnostic query should show SELECT allowed for all three roles, non-read
privileges denied for `anon` and `authenticated`, and non-read privileges
retained for `service_role`. The similar-creators section was confirmed to
load after the manual change; the separate lookalike-page check remains
unverified.

The Kaggle producer initializes its client with `SUPABASE_SERVICE_KEY` and
reported 932,383 rows written before hardening. Service-role sequence USAGE was
verified and is unchanged. Post-change notebook writes remain unverified; do
not rerun the full embedding pipeline solely for a permission check. The
notebook's oversized existing-row lookup failures are a separate issue.

### Category Stats Cache Client Read-Only Access (080)

Migration 080 records the production-verified removal of direct client write
privileges on `public.category_stats_cache`. It preserves existing SELECT and
service-role privileges, data, RLS, and RPC permissions. It is safe to repeat
after the manual SQL Editor change and does not establish missing grants for
fresh/reset environments.

Its diagnostic query should show SELECT allowed for all three roles, non-read
privileges denied for `anon` and `authenticated`, and non-read privileges
retained for `service_role`. A creator profile's category chart was confirmed
to display after the manual change.

The bootstrap writer uses `SUPABASE_SERVICE_KEY`. Its aggregate RPC timed out
before the cache upsert, so successful refresh after hardening remains
unverified. This migration does not address the separate refresh timeout or
change worker code, workflows, or other tables.

### App Stats Client Read-Only Access (079)

Migration 079 records the production-verified removal of direct client write
privileges on `public.app_stats`. It preserves existing SELECT and service-role
privileges and does not change data, RLS, or RPC execution permissions. The
revocation is safe to repeat after the manual SQL Editor change.

Run `079_app_stats_client_read_only.sql` in the SQL Editor. Its diagnostic query
should show SELECT allowed for all three roles, non-read privileges denied for
`anon` and `authenticated`, and non-read privileges retained for `service_role`.
Then verify `/lists` still loads its counts and tabs. That smoke check passed
after the manual production change.

This migration does not establish missing grants for fresh/reset environments
and does not restrict SECURITY DEFINER RPCs. Those require separate, explicit
access decisions. It intentionally changes no other tables.

### Recent Upload Cadence Index (074)

Apply migration 074 with psql autocommit and verify the index is valid before
deploying code that queries the recent-cadence ranking. Do not run this migration
through a transaction-wrapping runner, `psql --single-transaction`, or a SQL
Editor transaction; `CREATE INDEX CONCURRENTLY` must run outside a transaction.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/074_recent_upload_cadence_index.sql
```

Verify the index before deploying the application change:

```sql
SELECT i.indisvalid, i.indislive, pg_get_indexdef(i.indexrelid) AS index_definition
FROM pg_index AS i
WHERE i.indexrelid = to_regclass('public.idx_creators_recent_cadence_synced');
```

The query must return one row with both flags true. If a canceled concurrent
build left an invalid index, drop only that invalid index outside a transaction,
then rerun migration 074 and verify again:

```sql
SELECT i.indisvalid
FROM pg_index AS i
WHERE i.indexrelid = to_regclass('public.idx_creators_recent_cadence_synced');
```

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c \
  "DROP INDEX CONCURRENTLY IF EXISTS public.idx_creators_recent_cadence_synced"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/074_recent_upload_cadence_index.sql
```

Do not drop a valid index as part of routine retries; `IF NOT EXISTS` does not
repair an invalid same-named index.

### Creator Keyword Provenance (068)

Apply [068_creator_keyword_provenance.sql](068_creator_keyword_provenance.sql)
before deploying source-aware profile/export code. It adds one nullable JSONB
column, with no backfill or additional indexes. Existing keyword arrays remain
valid; legacy rows display "Source unavailable". The migration's final query
verifies the column exists. This migration has not been applied automatically.

The external Kaggle producer can later write `transcript_keywords_metadata`
alongside its existing keyword array and upload timestamp:

```json
{
  "schema_version": 1,
  "source": "description",
  "computed_at": "2026-10-21T10:00:00Z",
  "extractor_version": "keyword-pipeline-v1",
  "model_id": "sentence-transformers/all-MiniLM-L6-v2",
  "model_revision": "actual-pinned-model-revision",
  "content_language": "en",
  "language_confidence": null,
  "sampled_video_count": 10,
  "transcript_video_count": 0,
  "description_video_count": 5,
  "accepted_word_count": 1500
}
```

Only record actual contributing sources and counts. Supported sources are
`transcript`, `description`, `mixed`, and `unknown`. Each sampled video contributes
at most one accepted source in this contract. Unknown metadata versions or
inconsistent counts display "Source unavailable". Word count alone never proves
that transcripts were fetched. `computed_at` is extraction time;
`transcript_keywords_updated_at` remains upload time. Timestamps must include a
timezone. Never include credentials, transcript bodies, or private campaign data.

CSV exports preserve their original columns and append Content Topics, Keyword
Source, Topics Computed At, Topics Uploaded At, and Extractor Version. Public
lookalike exports retain their existing access and email-only behavior. New topic
text cells neutralize spreadsheet-formula prefixes. No ranking, search, or
embedding changes are included. Roll back application code before removing the
column; retaining this nullable column is safe for old application versions.

The saved-creators download button uses `/me/favourites/export` to avoid the
existing static-file interception of `.csv` routes. Its filename remains
`saved-creators.csv`, and sign-in remains required.

### Duration-Adjusted Outlier Scores (069)

Apply [069_duration_adjusted_outlier_method.sql](069_duration_adjusted_outlier_method.sql)
before deploying the updated creator worker and profile. It adds nullable
`outlier_scored_video_count` and `outlier_method_version` columns; it does not
backfill or rewrite existing `outlier_videos`. Profiles keep showing legacy
raw-view-median results until the worker writes
`duration_adjusted_daily_views_percentile_v3` on the next creator sync.

The new score is a duration-weighted empirical percentile of lifetime views/day
against chronologically earlier uploads. There is no configurable multiplier
threshold. Older rows remain visible and are labeled as legacy until refreshed.

| # | File | Description | Status |
|---|------|-------------|--------|
| 001 | add_growth_tracking_columns.sql | Adds prev_* columns for 30-day growth tracking | ✅ Ready |

## Schema Documentation

Full schema documentation and table creation scripts are in `db/schema/`.

## Creator Sync Health Upgrade (070-072)

Migration 072 upgrades databases where 070/071 have already been applied. Do not
edit deployed function return types in place: the application now calls
`get_creator_sync_health_v2()`, while the original RPC remains available for older
deployments. Apply 072 before deploying the new web application or background code.
Without it, the card deliberately reports Unavailable instead of falling back to
the expensive original RPC.

### Production Prerequisites

The production inspection on 2026-10-26 confirmed a valid
`idx_creator_sync_jobs_pending_unique`, no duplicate pending creator jobs, and both
original functions restricted to service_role. The three queue-health indexes
from 070 were also valid. This does not establish index state in other databases.
072 tracks the unique-index prerequisite for those databases; existing valid
production indexes are preserved. On another database, resolve duplicate pending
jobs explicitly before creating the unique index. Do not silently delete jobs.

070 and 072 contain `CREATE INDEX CONCURRENTLY`: run them with psql autocommit,
not a transaction-wrapping runner or a single SQL Editor transaction. Do not use
psql's `--single-transaction` option. `IF NOT EXISTS` does not repair invalid indexes
or validate that an existing same-named index has the expected definition.

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/072_creator_sync_health_upgrade.sql
```

The invalid production `idx_creator_sync_jobs_pending_or_query` is intentionally
not dropped by this upgrade. Compare its definition with migration 032 (the
reported production definition includes `retry_at`, not `next_retry_at`). Inspect
the worker polling plan and rebuild the intended index separately, outside a
transaction. The new dashboard does not depend on that index. Check index validity
after any interrupted concurrent build before retrying a migration.

## Active Creator Sync Jobs and Growth Baseline (076-078)

Apply these stages in order after deploying the matching `db.py` enqueue changes:

1. `076_active_job_enqueue_compatibility.sql` replaces the broad enqueue RPC.
   It changes no table rows or indexes and can run in a normal transaction.
2. Immediately before `077_active_creator_sync_unique_index.sql`, rerun the
   active-job duplicate query in that file. Pause the creator sync worker, then
   run the concurrent index statement by itself in autocommit mode. Do not
   resume the worker until `indisready` and `indisvalid` both verify as true.
   If the build fails or leaves an invalid index, stop and inspect it before
   retrying; `IF NOT EXISTS` does not repair invalid indexes.
3. `078_enqueue_growth_baseline_refreshes.sql` installs the timestamp parser
   and dry-run-first baseline RPC. It does not enqueue jobs. Apply it only after
   stage 2 is valid, then inspect the dry-run result before considering any
   insert call with `p_dry_run = false`.

The existing pending-only unique index remains in place. Both SQL enqueue RPCs
use untargeted `ON CONFLICT DO NOTHING` so they tolerate that index as well as
the new active-job index during rollout.

### Monitoring And Scheduling

Queue health reads pending, processing, and completion aggregates only. It does
not scan creators. Ready waiting time starts at the later of creation and retry
eligibility, so intentional backoff is not counted as queue delay. Processing
jobs are also checked, preventing a stuck in-flight job from appearing idle.

For a daily worker, defaults allow a full scheduled gap and roughly 33 hours to
drain a 3,000-job queue at the observed ~2,200 jobs/day:

| Environment variable | Default hours | Meaning |
| --- | --- | --- |
| `CREATOR_SYNC_READY_WARNING_HOURS` | 48 | Work older than this is Behind |
| `CREATOR_SYNC_STALL_HOURS` | 72 | Minimum work age before Stalled |
| `CREATOR_SYNC_PROGRESS_GRACE_HOURS` | 26 | Stalled also requires no completions within this interval |
| `CREATOR_SYNC_COVERAGE_MAX_AGE_HOURS` | 48 | Coverage older than this is marked stale |

Idle means no runnable or processing work; delayed retries do not make it unhealthy.
Queue depth and unqueued inventory alone do not set the health badge. These are
waiting/progress indicators, not a worker heartbeat or proof that a scheduled run
executed. No workflow schedules or YouTube quota policies are changed.

Coverage is a separate stored snapshot using the exact candidate rules used by
enqueueing: subscriber threshold (10,000 by default), invalid/archive exclusions,
never-synced priority, active 30-day refresh, and unknown-activity 90-day refresh.
Its oldest wait measures time since eligibility, not total time since last sync.
The minimum subscriber threshold and collection age appear on the card. A missing,
invalid, or stale snapshot does not hide live queue health or become a zero count.

Bootstrap maintenance refreshes coverage unless `--no-stats` is supplied. Kaggle
also refreshes it at the end of a run that processed jobs. Neither refresh enqueues
anything. Failures preserve the last stored snapshot and are logged with timing.
The existing web-client timeout still bounds background HTTP calls: if refreshes
time out, inspect the candidate query directly and run the refresh through a
database-side maintenance schedule rather than increasing dashboard timeouts.

After applying 072, initialize and time coverage outside a page request:

```sql
SELECT public.refresh_creator_sync_coverage(10000);
SELECT * FROM public.get_creator_sync_health_v2();
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM public.get_creator_sync_health_v2();
```

Run these as service_role or an administrator. Keep the web service-role key
server-only. Do not grant the RPCs or snapshot table to anon/authenticated. Measure
the new queue-only RPC in production before adding a 60-second request cache; the
previous timeout came from a different query that scanned creator inventory.

### Enqueue Safeguards And Verification

Enqueueing remains dry-run by default, including an explicit NULL dry-run argument.
Temporary-table cleanup supports repeated calls within one transaction. An advisory
transaction lock serializes calls to this enqueue function when allocating its
queue budget; independent legacy queue producers are not covered by that lock.
The pending unique index remains the final duplicate guard. Automatic enqueue
cron scheduling is still disabled; review a dry run before enabling any schedule.

Migration 090 adds `enqueue_unknown_activity_refresh_batch_after_drain`. It takes
the shared creator-enqueue advisory lock, rejects the batch if any pending or
processing jobs exist, then delegates to the existing automatic refresh RPC.
Migrations 092-093 optimize terminal archival after the observed bootstrap timeout.
Run 092's `CREATE INDEX CONCURRENTLY` separately in autocommit (no transaction
wrapper), verify `indisvalid` and `indisready`, then apply 093. The replacement
uses index-ordered failed-job selection instead of a global history aggregate;
it keeps the existing locking, recovery checks, and 100-creator update cap.
Measure RPC latency in the target database before resuming scheduled bootstrap.
Do not bypass archival failures or clear retry delays to restore queue supply.

The unguarded batch RPC is internal to the wrapper, and failed-to-pending job
rescues take the same lock. Apply migration 091 before the next worker/bootstrap
run; it atomically archives terminal failures, skips recovered or active
creators, and blocks jobs for archived creators. Verify it on a
disposable database with an application schema and four unused creators:

```bash
psql "$TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -f db/tests/terminal_creator_sync_archive.sql
```

The drain wrapper is verified separately on a disposable database:

```bash
psql "$TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -f db/tests/automatic_refresh_drain.sql
```

Run the SQL integration script only on an empty, disposable database:

```bash
psql "$TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -f db/tests/creator_sync_health.sql
```

It applies 070-072 and checks dry-run safety, repeated calls, priority order, queue
limits, eligibility parity, retry backfill/time zones, runnable wait age, and RPC
permissions. Fixtures deliberately refuse to overwrite existing application tables.
