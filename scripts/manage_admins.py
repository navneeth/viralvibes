#!/usr/bin/env python3
"""
scripts/manage_admins.py — Grant/revoke admin access for users.

Usage:
    python3 scripts/manage_admins.py grant <user_id> <reason>
    python3 scripts/manage_admins.py revoke <user_id>
    python3 scripts/manage_admins.py list
    python3 scripts/manage_admins.py check <user_id>

Example:
    python3 scripts/manage_admins.py grant 550e8400-e29b-41d4-a716-446655440000 "Main developer"
    python3 scripts/manage_admins.py list
    python3 scripts/manage_admins.py revoke 550e8400-e29b-41d4-a716-446655440000
"""

import sys
import os
from pathlib import Path

# Add parent directory to path so we can import db, etc.
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

# Load env vars
load_dotenv()

# Initialize Supabase
from db import init_supabase

supabase_client = init_supabase()

if not supabase_client:
    print("❌ Error: Supabase client not initialized. Check SUPABASE_URL and SUPABASE_KEY.")
    sys.exit(1)


def grant_admin(user_id: str, reason: str = "", granted_by_id: str | None = None):
    """Grant admin access to a user."""
    try:
        supabase_client.table("admin_users").insert(
            {
                "user_id": user_id,
                "reason": reason or None,
                "granted_by": granted_by_id,
            }
        ).execute()
        print(f"✅ Granted admin access to {user_id}")
        print(f"   Reason: {reason or '(none)'}")
        return True
    except Exception as e:
        print(f"❌ Error granting admin access: {e}")
        return False


def revoke_admin(user_id: str):
    """Revoke admin access from a user."""
    try:
        supabase_client.table("admin_users").delete().eq("user_id", user_id).execute()
        print(f"✅ Revoked admin access from {user_id}")
        return True
    except Exception as e:
        print(f"❌ Error revoking admin access: {e}")
        return False


def list_admins():
    """List all admin users."""
    try:
        resp = (
            supabase_client.table("admin_users")
            .select("id, user_id, reason, granted_at")
            .order("granted_at", desc=True)
            .execute()
        )

        if not resp.data:
            print("ℹ️  No admin users yet.")
            return

        print(f"\n📋 {len(resp.data)} admin user(s):\n")
        print(f"{'User ID':<36} {'Reason':<30} {'Granted':<20}")
        print("-" * 86)

        for admin in resp.data:
            user_id = admin.get("user_id", "")[:36]
            reason = admin.get("reason") or "(none)"
            granted_at = admin.get("granted_at", "")[:19] if admin.get("granted_at") else "?"

            print(f"{user_id:<36} {reason:<30} {granted_at:<20}")

        print()
    except Exception as e:
        print(f"❌ Error listing admins: {e}")


def check_admin(user_id: str):
    """Check if a user is an admin."""
    try:
        resp = (
            supabase_client.table("admin_users")
            .select("id, reason, granted_at")
            .eq("user_id", user_id)
            .limit(1)
            .execute()
        )

        if resp.data:
            admin = resp.data[0]
            print(f"✅ {user_id} IS an admin")
            print(f"   Reason: {admin.get('reason') or '(none)'}")
            print(f"   Granted: {admin.get('granted_at', '?')[:19]}")
        else:
            print(f"❌ {user_id} is NOT an admin")
            print("\n   To grant admin access:")
            print(f"   python3 scripts/manage_admins.py grant {user_id} <reason>")
    except Exception as e:
        print(f"❌ Error checking admin: {e}")


def main():
    """CLI entrypoint."""
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "grant" and len(sys.argv) >= 3:
        user_id = sys.argv[2]
        reason = " ".join(sys.argv[3:]) if len(sys.argv) > 3 else ""
        grant_admin(user_id, reason)

    elif command == "revoke" and len(sys.argv) >= 3:
        user_id = sys.argv[2]
        revoke_admin(user_id)

    elif command == "list":
        list_admins()

    elif command == "check" and len(sys.argv) >= 3:
        user_id = sys.argv[2]
        check_admin(user_id)

    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
