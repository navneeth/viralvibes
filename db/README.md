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

Automatic mode calls migration 084's `enqueue_unknown_activity_refresh_batch`
after regular FIFO work drains. Migration 084 requires the safe timestamp parser
from 078 and the active-job unique index from 077. Migration 085 adds the failed
refresh cooldown index using `CREATE INDEX CONCURRENTLY`; run it with psql
autocommit, outside a transaction. Migration 086 serializes direct
`creator_sync_jobs` inserts with the enqueue RPCs by using their shared advisory
lock. Apply 084-086 before enabling automatic mode. The automatic RPC defaults
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

Run the SQL integration script only on an empty, disposable database:

```bash
psql "$TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -f db/tests/creator_sync_health.sql
```

It applies 070-072 and checks dry-run safety, repeated calls, priority order, queue
limits, eligibility parity, retry backfill/time zones, runnable wait age, and RPC
permissions. Fixtures deliberately refuse to overwrite existing application tables.
