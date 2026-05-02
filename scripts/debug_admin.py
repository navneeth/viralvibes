#!/usr/bin/env python3
"""
Debug script to check admin setup and ID mapping.

Usage:
    python3 scripts/debug_admin.py <email>    - Debug a specific user
    python3 scripts/debug_admin.py --list     - List all admin users
    python3 scripts/debug_admin.py --session  - Show all active sessions
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

from db import init_supabase

sb = init_supabase()
if not sb:
    print("❌ Supabase not initialized")
    sys.exit(1)


def debug_user(email: str):
    """Check user, auth_providers, and admin_users entries"""
    print(f"\n🔍 Debugging user: {email}")
    print("=" * 60)

    # 1. Find user in users table
    try:
        resp = sb.table("users").select("*").eq("email", email).execute()
        if not resp.data:
            print(f"❌ User not found in public.users")
            return
        user = resp.data[0]
        user_id = user["id"]
        print(f"✅ Found user in public.users")
        print(f"   ID: {user_id}")
        print(f"   Email: {user['email']}")
        print(f"   Name: {user.get('name', 'N/A')}")
    except Exception as e:
        print(f"❌ Error querying users: {e}")
        return

    # 2. Check auth_providers
    try:
        resp = sb.table("auth_providers").select("*").eq("user_id", user_id).execute()
        if resp.data:
            print(f"✅ Found {len(resp.data)} auth provider(s)")
            for row in resp.data:
                print(f"   Provider: {row.get('provider')}")
                print(f"   Provider User ID: {row.get('provider_user_id')}")
        else:
            print(f"⚠️  No auth providers found")
    except Exception as e:
        print(f"❌ Error querying auth_providers: {e}")

    # 3. Check admin_users
    try:
        resp = sb.table("admin_users").select("*").eq("user_id", user_id).execute()
        if resp.data:
            print(f"✅ User IS ADMIN")
            for row in resp.data:
                print(f"   Granted at: {row.get('granted_at')}")
                print(f"   Reason: {row.get('reason')}")
        else:
            print(f"❌ User is NOT admin (not in admin_users table)")
    except Exception as e:
        print(f"❌ Error querying admin_users: {e}")

    # 4. Test the is_admin function
    from db import is_admin

    try:
        result = is_admin(user_id)
        print(f"\n📋 is_admin({user_id}) = {result}")
    except Exception as e:
        print(f"❌ Error calling is_admin: {e}")


def list_admins():
    """List all admin users"""
    print("\n🔍 All Admin Users")
    print("=" * 60)

    try:
        # Get all admins with their email
        resp = sb.table("admin_users").select("*").execute()
        if not resp.data:
            print("No admin users found")
            return

        print(f"Found {len(resp.data)} admin(s):\n")
        for admin_row in resp.data:
            user_id = admin_row.get("user_id")
            # Look up user email
            try:
                user_resp = sb.table("users").select("email, name").eq("id", user_id).execute()
                if user_resp.data:
                    email = user_resp.data[0]["email"]
                    name = user_resp.data[0].get("name", "N/A")
                    print(f"✅ {email}")
                    print(f"   Name: {name}")
                    print(f"   User ID: {user_id}")
                    print(f"   Granted: {admin_row.get('granted_at')}")
                    print(f"   Reason: {admin_row.get('reason')}\n")
                else:
                    print(f"❌ Admin entry has unknown user_id: {user_id}")
            except Exception as e:
                print(f"❌ Error looking up email for {user_id}: {e}")
    except Exception as e:
        print(f"❌ Error querying admin_users: {e}")


def list_sessions():
    """Show all active sessions from the sessions table (if it exists)."""
    print("\n🔍 Active Sessions")
    print("=" * 60)

    try:
        resp = sb.table("sessions").select("user_id, created_at, expires_at").execute()
        if not resp.data:
            print("No active sessions found (or sessions table does not exist).")
            return

        print(f"Found {len(resp.data)} session(s):\n")
        for row in resp.data:
            user_id = row.get("user_id", "unknown")
            created_at = row.get("created_at", "—")
            expires_at = row.get("expires_at", "—")
            # Best-effort email lookup
            email = "—"
            try:
                u = sb.table("users").select("email").eq("id", user_id).execute()
                if u.data:
                    email = u.data[0]["email"]
            except Exception:
                pass
            print(f"  user_id={user_id}  email={email}")
            print(f"    created={created_at}  expires={expires_at}\n")

    except Exception as e:
        print(f"❌ Error querying sessions: {e}")
        print("   (The sessions table may not exist in your schema.)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 scripts/debug_admin.py <email>    - Debug specific user")
        print("  python3 scripts/debug_admin.py --list     - List all admins")
        print("  python3 scripts/debug_admin.py --session  - Show active sessions")
        sys.exit(1)

    if sys.argv[1] == "--list":
        list_admins()
    elif sys.argv[1] == "--session":
        list_sessions()
    else:
        debug_user(sys.argv[1])
