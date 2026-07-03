"""Shared sitemap-building utilities.

Single source of truth for which routes appear in the sitemap and how
they are serialised to XML. Consumed by:

  - scripts/generate_sitemap.py  — deploy-time static file generation
  - main.py @rt("/sitemap.xml")  — live route with 24 h in-process cache
"""

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import urljoin
from xml.dom import minidom

from constants import SITE_BASE_URL

BASE_URL = SITE_BASE_URL

# (path, changefreq, priority)
STATIC_ROUTES: list[tuple[str, str, str]] = [
    ("/", "daily", "1.0"),
    ("/analysis", "weekly", "0.8"),
    ("/creators", "daily", "0.9"),
    ("/creators/top", "daily", "0.8"),
    ("/creators/top/gaming", "daily", "0.8"),
    ("/creators/top/entertainment", "daily", "0.8"),
    ("/creators/top/music", "daily", "0.8"),
    ("/creators/top/education", "daily", "0.8"),
    ("/creators/top/howto-style", "daily", "0.8"),
    ("/lists", "weekly", "0.7"),
    ("/lists/categories", "weekly", "0.6"),
    ("/lists/countries", "weekly", "0.6"),
    ("/lists/languages", "weekly", "0.6"),
    ("/pricing", "monthly", "0.5"),
    ("/terms", "monthly", "0.3"),
    ("/privacy", "monthly", "0.3"),
]


# ---------------------------------------------------------------------------
# Supabase fetch helpers — shared by the deploy-time script and live route
# so the query definition has a single location.  Both callers supply their
# own Supabase client (script uses init_supabase(); app uses supabase_client).
# ---------------------------------------------------------------------------


def fetch_synced_creators(client) -> list:
    """Return ``{id, last_updated_at}`` rows for all synced creators."""
    try:
        resp = (
            client.table("creators")
            .select("id, last_updated_at")
            .eq("sync_status", "synced")
            .execute()
        )
        return resp.data or []
    except Exception:
        return []


def fetch_aplus_creators(client) -> list:
    """Return ``{custom_url, last_updated_at}`` for A+ synced creators with a handle.

    Scoped to A+ so the sitemap stays well under Google's 50k-URL ceiling and
    crawl budget focuses on the highest-quality cohort.
    """
    try:
        resp = (
            client.table("creators")
            .select("custom_url, last_updated_at")
            .eq("sync_status", "synced")
            .eq("quality_grade", "A+")
            .not_.is_("custom_url", "null")
            .execute()
        )
        return resp.data or []
    except Exception:
        return []


def _prettify(elem: ET.Element) -> str:
    rough = ET.tostring(elem, "utf-8")
    return minidom.parseString(rough).toprettyxml(indent="  ")


def _lastmod_or_today(row: dict, today: str) -> str:
    """Pull a 10-char (YYYY-MM-DD) lastmod off a row, falling back to today."""
    raw = row.get("last_updated_at") or ""
    return raw[:10] if len(raw) >= 10 else today


def build_sitemap_xml(
    creators: list,
    *,
    aplus_creators: list | None = None,
) -> str:
    """Build and return the complete sitemap XML string.

    Args:
        creators: List of ``{id, last_updated_at}`` dicts for synced creators.
                  Each becomes a ``/creator/{id}`` entry. Pass an empty list
                  to emit static routes only (e.g. in CI when DB is offline).
        aplus_creators: Optional list of ``{custom_url, last_updated_at}``
                  dicts for A+ creators with a usable handle. Each becomes a
                  ``/creators/like/{handle}`` lookalike-page entry. Scoped to
                  A+ by the caller so the sitemap stays well under Google's
                  50k-URL ceiling and crawl budget focuses on the most
                  rankable creators.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    urlset = ET.Element("urlset", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")

    for path, changefreq, priority in STATIC_ROUTES:
        url_el = ET.SubElement(urlset, "url")
        ET.SubElement(url_el, "loc").text = urljoin(BASE_URL, path)
        ET.SubElement(url_el, "lastmod").text = today
        ET.SubElement(url_el, "changefreq").text = changefreq
        ET.SubElement(url_el, "priority").text = priority

    for creator in creators:
        creator_id = creator.get("id")
        if not creator_id:
            continue
        url_el = ET.SubElement(urlset, "url")
        ET.SubElement(url_el, "loc").text = urljoin(BASE_URL, f"/creator/{creator_id}")
        ET.SubElement(url_el, "lastmod").text = _lastmod_or_today(creator, today)
        ET.SubElement(url_el, "changefreq").text = "weekly"
        ET.SubElement(url_el, "priority").text = "0.7"

    for creator in aplus_creators or []:
        handle = (creator.get("custom_url") or "").lstrip("@").lower()
        if not handle:
            continue
        url_el = ET.SubElement(urlset, "url")
        ET.SubElement(url_el, "loc").text = urljoin(BASE_URL, f"/creators/like/{handle}")
        ET.SubElement(url_el, "lastmod").text = _lastmod_or_today(creator, today)
        ET.SubElement(url_el, "changefreq").text = "weekly"
        ET.SubElement(url_el, "priority").text = "0.6"

    return _prettify(urlset)
