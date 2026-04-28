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
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
from xml.dom import minidom

# Add project root to path so we can import from db, secrets_loader, etc.
_PROJECT_ROOT = str(Path(__file__).parent.parent)
sys.path.insert(0, _PROJECT_ROOT)

from secrets_loader import load_secrets  # noqa: E402

load_secrets()

from db import init_supabase  # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://viralvibes.fyi"

# (path, changefreq, priority)
STATIC_ROUTES = [
    ("/", "daily", "1.0"),
    ("/analyze", "weekly", "0.8"),
    ("/creators", "daily", "0.8"),
    ("/lists", "weekly", "0.7"),
    ("/lists/categories", "weekly", "0.6"),
    ("/lists/countries", "weekly", "0.6"),
    ("/lists/languages", "weekly", "0.6"),
    ("/pricing", "monthly", "0.5"),
    ("/terms", "monthly", "0.3"),
    ("/privacy", "monthly", "0.3"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _prettify(elem: ET.Element) -> str:
    """Return a pretty-printed XML string for the Element."""
    rough_string = ET.tostring(elem, "utf-8")
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


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


def build_sitemap_xml(creators: list) -> str:
    """Build and return the complete sitemap XML string."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    urlset = ET.Element("urlset", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")

    # Static routes
    for path, changefreq, priority in STATIC_ROUTES:
        url_el = ET.SubElement(urlset, "url")
        ET.SubElement(url_el, "loc").text = urljoin(BASE_URL, path)
        ET.SubElement(url_el, "lastmod").text = today
        ET.SubElement(url_el, "changefreq").text = changefreq
        ET.SubElement(url_el, "priority").text = priority

    # Dynamic creator pages
    for creator in creators:
        creator_id = creator.get("id")
        if not creator_id:
            continue
        raw_lastmod = creator.get("last_updated_at") or ""
        lastmod = raw_lastmod[:10] if len(raw_lastmod) >= 10 else today

        url_el = ET.SubElement(urlset, "url")
        ET.SubElement(url_el, "loc").text = urljoin(BASE_URL, f"/creator/{creator_id}")
        ET.SubElement(url_el, "lastmod").text = lastmod
        ET.SubElement(url_el, "changefreq").text = "weekly"
        ET.SubElement(url_el, "priority").text = "0.7"

    return _prettify(urlset)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def generate_sitemap() -> bool:
    """Generate public/sitemap.xml with static routes + all synced creator pages."""
    client = init_supabase()
    if client is None:
        print(
            "Error: could not connect to Supabase. "
            "Ensure NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_KEY are set."
        )
        return False

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
