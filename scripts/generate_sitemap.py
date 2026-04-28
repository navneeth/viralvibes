#!/usr/bin/env python3
"""
Generate sitemap.xml for ViralVibes application.

Queries Supabase for all synced creator profiles and writes public/sitemap.xml.
Run at deploy time (or manually) to keep the static sitemap up to date.

Usage:
    python scripts/generate_sitemap.py
"""

import os
import sys
from pathlib import Path

# Add project root to path so we can import from db, secrets_loader, etc.
_PROJECT_ROOT = str(Path(__file__).parent.parent)
sys.path.insert(0, _PROJECT_ROOT)

from secrets_loader import load_secrets  # noqa: E402

load_secrets()

from db import init_supabase  # noqa: E402
from services.sitemap import STATIC_ROUTES, build_sitemap_xml  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fetch_synced_creators(client) -> list:
    """Return list of {id, last_updated_at} dicts for all synced creators."""
    try:
        resp = (
            client.table("creators")
            .select("id, last_updated_at")
            .eq("sync_status", "synced")
            .execute()
        )
        return resp.data or []
    except Exception as e:
        print(f"Warning: could not fetch creators from Supabase: {e}")
        return []


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def generate_sitemap() -> bool:
    """Generate public/sitemap.xml with static routes + all synced creator pages."""
    client = init_supabase()
    if client is None:
        print(
            "Warning: could not connect to Supabase — writing static-routes-only sitemap. "
            "Ensure NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_KEY are set for full output."
        )
        creators = []
    else:
        creators = _fetch_synced_creators(client)
        print(f"Found {len(creators)} synced creators.")

    xml_content = build_sitemap_xml(creators)

    public_dir = os.path.join(_PROJECT_ROOT, "public")
    sitemap_path = os.path.join(public_dir, "sitemap.xml")

    try:
        os.makedirs(public_dir, exist_ok=True)
        with open(sitemap_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        print(
            f"Sitemap written to {sitemap_path} "
            f"({len(STATIC_ROUTES)} static + {len(creators)} creator URLs)"
        )
    except IOError as e:
        print(f"Error writing sitemap: {e}")
        return False

    return True


if __name__ == "__main__":
    generate_sitemap()
