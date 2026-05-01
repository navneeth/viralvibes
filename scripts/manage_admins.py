#!/usr/bin/env python3
"""
scripts/manage_admins.py — Grant/revoke admin access for ViralVibes users.

IMPORTANT: user_id refers to public.users.id, NOT auth.users.id.
           Use the email-based commands to avoid ambiguity.

Commands:
    grant  <email|uuid> [reason]  — grant admin access
    revoke <email|uuid>           — revoke admin access
    check  <email|uuid>           — check if a user is an admin
    list                          — list all admins with email lookup

Environment:
    Loads .env by default. Override with --env /path/to/.env

Examples:
    python3 scripts/manage_admins.py grant user@example.com "Main developer"
    python3 scripts/manage_admins.py check user@example.com
    python3 scripts/manage_admins.py revoke user@example.com
    python3 scripts/manage_admins.py list
    python3 scripts/manage_admins.py --env .env.production list
"""

import sys
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Bootstrap: allow --env <file> as first two args before anything else loads
# ---------------------------------------------------------------------------
_args = sys.argv[1:]
_env_file = ".env"
if len(_args) >= 2 and _args[0] == "--env":
    _env_file = _args[1]
    _args = _args[2:]  # strip --env <file> so the rest of the script sees clean args

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv(_env_file, override=True)

from db import init_supabase

sb = init_supabase()
if not sb:
    print(f"❌  Supabase not initialised (env: {_env_file}).")
    print("    Check NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_KEY are set.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve(email_or_uuid: str) -> tuple[str, str] | None:
    """
    Return (user_id, email) from public.users for the given email or UUID.
    Prints a clear error and returns None when not found.

    This is the critical guard: admin_users.user_id is a FK to public.users.id.
    Inserting an auth.users.id (or any random UUID) that has no matching
    public.users row will raise a FK violation.
    """
    field = "email" if "@" in email_or_uuid else "id"
    try:
        resp = sb.table("users").select("id, email").eq(field, email_or_uuid).limit(1).execute()
    except Exception as e:
        print(f"❌  DB error resolving {email_or_uuid!r}: {e}")
        return None

    if not resp.data:
        if field == "id":
            print(f"❌  No row found in public.users for id={email_or_uuid!r}")
            print("    Note: admin_users references public.users, not auth.users.")
            print("    Try using the email address instead to avoid UUID confusion.")
        else:
            print(f"❌  No user found with email {email_or_uuid!r}")
            print("    The user must have logged in at least once before being granted admin.")
        return None

    row = resp.data[0]
    return row["id"], row["email"]


def _already_admin(user_id: str) -> bool:
    resp = sb.table("admin_users").select("id").eq("user_id", user_id).limit(1).execute()
    return bool(resp.data)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def grant_admin(target: str, reason: str = "") -> bool:
    resolved = _resolve(target)
    if not resolved:
        return False
    user_id, email = resolved

    if _already_admin(user_id):
        print(f"ℹ️   {email} ({user_id}) is already an admin. No change.")
        return True

    try:
        sb.table("admin_users").insert(
            {
                "user_id": user_id,
                "reason": reason or None,
            }
        ).execute()
        print(f"✅  Granted admin access to {email} ({user_id})")
        print(f"    Reason : {reason or '(none)'}")
        print()
        print("    ⚠️  The user must log out and back in for the change to take effect.")
        print("        (is_admin is cached in session at login time)")
        return True
    except Exception as e:
        print(f"❌  Error inserting into admin_users: {e}")
        return False


def revoke_admin(target: str) -> bool:
    resolved = _resolve(target)
    if not resolved:
        return False
    user_id, email = resolved

    if not _already_admin(user_id):
        print(f"ℹ️   {email} ({user_id}) is not an admin. No change.")
        return True

    try:
        sb.table("admin_users").delete().eq("user_id", user_id).execute()
        print(f"✅  Revoked admin access from {email} ({user_id})")
        print()
        print("    ⚠️  The user must log out and back in for the change to take effect.")
        return True
    except Exception as e:
        print(f"❌  Error revoking admin access: {e}")
        return False


def check_admin(target: str) -> None:
    resolved = _resolve(target)
    if not resolved:
        return
    user_id, email = resolved

    try:
        resp = (
            sb.table("admin_users")
            .select("reason, granted_at")
            .eq("user_id", user_id)
            .limit(1)
            .execute()
        )
    except Exception as e:
        print(f"❌  DB error: {e}")
        return

    if resp.data:
        row = resp.data[0]
        print(f"✅  {email} IS an admin")
        print(f"    user_id : {user_id}")
        print(f"    reason  : {row.get('reason') or '(none)'}")
        print(f"    granted : {(row.get('granted_at') or '')[:19]}")
    else:
        print(f"❌  {email} ({user_id}) is NOT an admin")
        print(f'\n    To grant: python3 scripts/manage_admins.py grant {email} "<reason>"')


def list_admins() -> None:
    try:
        resp = (
            sb.table("admin_users")
            .select("user_id, reason, granted_at")
            .order("granted_at", desc=True)
            .execute()
        )
    except Exception as e:
        print(f"❌  DB error: {e}")
        return

    if not resp.data:
        print("ℹ️   No admin users yet.")
        return

    # Bulk-resolve emails from public.users
    ids = [r["user_id"] for r in resp.data]
    try:
        users_resp = sb.table("users").select("id, email").in_("id", ids).execute()
        id_to_email = {u["id"]: u["email"] for u in (users_resp.data or [])}
    except Exception:
        id_to_email = {}

    print(f"\n📋  {len(resp.data)} admin user(s):\n")
    print(f"{'Email':<35} {'User ID':<36} {'Reason':<25} {'Granted':<19}")
    print("-" * 118)
    for row in resp.data:
        uid = row.get("user_id", "")
        email = id_to_email.get(uid, "(unknown)")
        reason = row.get("reason") or "(none)"
        granted_at = (row.get("granted_at") or "")[:19]
        print(f"{email:<35} {uid:<36} {reason:<25} {granted_at:<19}")
    print()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    if not _args:
        print(__doc__)
        sys.exit(1)

    cmd = _args[0].lower()

    if cmd == "grant" and len(_args) >= 2:
        reason = " ".join(_args[2:]) if len(_args) > 2 else ""
        grant_admin(_args[1], reason)

    elif cmd == "revoke" and len(_args) >= 2:
        revoke_admin(_args[1])

    elif cmd == "check" and len(_args) >= 2:
        check_admin(_args[1])

    elif cmd == "list":
        list_admins()

    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
