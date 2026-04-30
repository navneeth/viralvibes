# Admin Dashboard Setup Guide

## Overview

The admin dashboard is now protected by **OAuth user ID** (recommended) with a fallback to static token. Only users in the `admin_users` database table can access `/admin`, or users with the static `ADMIN_TOKEN`.

## Step 1: Apply the Migration

Run the migration to create the `admin_users` table:

```bash
# In Supabase SQL Editor, copy and paste the contents of:
db/migrations/023_admin_users.sql
```

Or run it programmatically:

```python
python3 -c "
from db import init_supabase
client = init_supabase()
migration = open('db/migrations/023_admin_users.sql').read()
client.rpc('execute_sql', {'sql': migration}).execute()
"
```

## Step 2: (Optional) Generate a Static Token

If you want a fallback static token (for emergency access):

```bash
python3 -c "import secrets; token = secrets.token_urlsafe(32); print(f'ADMIN_TOKEN={token}')" >> .env
```

Add it to your `.env`:
```bash
ADMIN_TOKEN=your_generated_token_here
```

## Step 3: Grant Admin Access to Users

Find a user's UUID from their session or Google OAuth ID, then grant admin access:

### Method A: Using the helper script (Recommended)

```bash
# Grant admin access
python3 scripts/manage_admins.py grant <user_id> "Main developer"

# List all admins
python3 scripts/manage_admins.py list

# Check if a user is admin
python3 scripts/manage_admins.py check <user_id>

# Revoke admin access
python3 scripts/manage_admins.py revoke <user_id>
```

### Method B: Direct SQL

```sql
-- Grant admin access
INSERT INTO admin_users (user_id, reason, granted_by)
VALUES ('550e8400-e29b-41d4-a716-446655440000', 'Main developer', NULL);

-- List admins
SELECT user_id, reason, granted_at FROM admin_users ORDER BY granted_at DESC;

-- Revoke admin access
DELETE FROM admin_users WHERE user_id = '550e8400-e29b-41d4-a716-446655440000';
```

## Step 4: Access the Dashboard

### Via OAuth (Recommended)

1. Log in with Google OAuth at `/login`
2. Your user_id is automatically checked against `admin_users` table
3. If authorized, you can access `/admin`

```bash
http://localhost:5001/admin
```

### Via Static Token (Fallback)

```bash
http://localhost:5001/admin?token=<ADMIN_TOKEN>
```

Or use the cookie (set automatically after first access with token):

```bash
# Token is saved as httponly cookie, valid for subsequent requests
http://localhost:5001/admin
```

## Finding Your User ID

After logging in with Google OAuth, your `user_id` is stored in the session. You can find it by:

1. **In Python after login**:
```python
from dotenv import load_dotenv
load_dotenv()

# After OAuth callback in your session
print(f"My user_id: {session.get('user_id')}")
```

2. **In browser DevTools** (if session is logged):
```javascript
// Check localStorage or cookies for user_id
console.log(document.cookie)
```

3. **In Supabase dashboard**:
   - Go to Authentication → Users
   - Copy the UUID of your user

## Access Control Priority

The admin check uses this priority:

1. **OAuth user ID** (if logged in) — Check `admin_users` table
2. **Static token** — Query param `?token=<ADMIN_TOKEN>` or cookie `admin_token=<ADMIN_TOKEN>`
3. **Deny** — If neither matches

This means:
- ✅ A logged-in OAuth user in `admin_users` can access `/admin` without a token
- ✅ A user with the static token can access `/admin` without being in the database
- ❌ Others are denied

## Revoking Access

```bash
# Remove from admin_users table
python3 scripts/manage_admins.py revoke <user_id>

# Or SQL:
DELETE FROM admin_users WHERE user_id = '...';
```

Revocation is immediate — no app restart needed.

## Troubleshooting

**"401 Unauthorised" when accessing `/admin`**

- ✅ Check: Are you logged in with Google OAuth?
- ✅ Check: Is your `user_id` in the `admin_users` table?
- ✅ Check: Do you have the static `ADMIN_TOKEN` in .env?
- ✅ Check: Are you passing `?token=<ADMIN_TOKEN>` in the URL?

```bash
# Verify admin status
python3 scripts/manage_admins.py check <your_user_id>

# Grant if needed
python3 scripts/manage_admins.py grant <your_user_id> "Developer"
```

**Supabase client is None**

Make sure environment variables are set:
```bash
echo $SUPABASE_URL
echo $SUPABASE_KEY
```

## Architecture

```
Request to /admin
        ↓
    _is_authorised(req, sess, supabase_client)
        ↓
    ┌─────────────────────────────────────┐
    │ Priority 1: OAuth User ID           │ (if sess.user_id exists)
    │   → Query admin_users table         │
    │   → Return True if found            │
    └─────────────────────────────────────┘
        ↓ (if Priority 1 fails)
    ┌─────────────────────────────────────┐
    │ Priority 2: Static Token            │
    │   → Check ?token=X or admin_token   │
    │   → Compare with ADMIN_TOKEN env    │
    └─────────────────────────────────────┘
        ↓ (if Priority 2 fails)
    ✅ ALLOW or ❌ DENY (401)
```

## Database Schema

```sql
admin_users (
  id          UUID PRIMARY KEY
  user_id     UUID NOT NULL UNIQUE (references users.id)
  reason      TEXT
  granted_at  TIMESTAMPTZ (auto)
  granted_by  UUID (references users.id, nullable)
  created_at  TIMESTAMPTZ (auto)
  updated_at  TIMESTAMPTZ (auto)
)
```

- **user_id**: User UUID to grant admin access
- **reason**: Why this user was granted access (optional, for audit trail)
- **granted_by**: Admin who granted the access (optional)
- **granted_at**: When access was granted (auto)

## Next Steps

Real data queries in `_fetch_admin_data()`:

```python
def _fetch_admin_data() -> tuple[dict, dict, dict, list[dict]]:
    """Replace stubs with real Supabase queries."""
    # Query total creators, synced_today, pending, failed, processing...
    # Query sync status breakdown
    # Query worker health metrics
    # Query recent jobs
```

See `routes/admin.py` for details.
