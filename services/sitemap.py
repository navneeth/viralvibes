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

BASE_URL = "https://viralvibes.fyi"

# (path, changefreq, priority)
STATIC_ROUTES: list[tuple[str, str, str]] = [
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


def _prettify(elem: ET.Element) -> str:
    rough = ET.tostring(elem, "utf-8")
    return minidom.parseString(rough).toprettyxml(indent="  ")


def build_sitemap_xml(creators: list) -> str:
    """Build and return the complete sitemap XML string.

    Args:
        creators: List of ``{id, last_updated_at}`` dicts for synced creators.
                  Pass an empty list to emit static routes only (e.g. in CI).
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
        raw_lastmod = creator.get("last_updated_at") or ""
        lastmod = raw_lastmod[:10] if len(raw_lastmod) >= 10 else today

        url_el = ET.SubElement(urlset, "url")
        ET.SubElement(url_el, "loc").text = urljoin(BASE_URL, f"/creator/{creator_id}")
        ET.SubElement(url_el, "lastmod").text = lastmod
        ET.SubElement(url_el, "changefreq").text = "weekly"
        ET.SubElement(url_el, "priority").text = "0.7"

    return _prettify(urlset)
