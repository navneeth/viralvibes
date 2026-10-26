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
