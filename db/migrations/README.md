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
psql $DATABASE_URL -f db/migrations/003_add_growth_tracking_columns.sql

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

| # | File | Description | Status |
|---|------|-------------|--------|
| 003 | add_growth_tracking_columns.sql | Adds prev_* columns for 30-day growth tracking | ✅ Ready |

## Schema Documentation

Full schema documentation and table creation scripts are in `db/schema/`.
