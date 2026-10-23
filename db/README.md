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
psql $DATABASE_URL -f db/migrations/001_add_growth_tracking_columns.sql

# Run all migrations in order
for f in db/migrations/*.sql; do
  echo "Running $f..."
  psql $DATABASE_URL -f "$f"
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
