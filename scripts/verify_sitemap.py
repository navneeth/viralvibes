#!/usr/bin/env python3
"""Verify that public/sitemap.xml is well-formed and contains the expected static routes."""

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

SITEMAP_PATH = Path(__file__).parent.parent / "public" / "sitemap.xml"
NAMESPACE = "http://www.sitemaps.org/schemas/sitemap/0.9"
MIN_URLS = 10  # number of static routes in generate_sitemap.py


def main() -> None:
    if not SITEMAP_PATH.exists():
        print(f"ERROR: {SITEMAP_PATH} not found", file=sys.stderr)
        sys.exit(1)

    try:
        tree = ET.parse(SITEMAP_PATH)
    except ET.ParseError as exc:
        print(f"ERROR: sitemap is not valid XML: {exc}", file=sys.stderr)
        sys.exit(1)

    root = tree.getroot()
    urls = root.findall(f"{{{NAMESPACE}}}url")

    if len(urls) < MIN_URLS:
        print(
            f"ERROR: expected at least {MIN_URLS} URLs, got {len(urls)}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"✅ Sitemap valid: {len(urls)} URL(s)")


if __name__ == "__main__":
    main()
