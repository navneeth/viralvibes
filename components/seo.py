"""SEO primitives for landing pages.

Reusable building blocks for canonical URLs, Open Graph / Twitter cards,
and JSON-LD structured data. These are pure FastHTML FT components — no
MonsterUI dependency — so they can be dropped into any ``<head>`` tree.

Used by ``/creators/top`` and ``/creators/top/{category}`` initially;
future landing pages (brands, lists, programmatic SEO) should reuse the
same helpers to keep markup consistent.
"""

from __future__ import annotations

import json
from typing import Any

from fasthtml.common import Link, Meta, Script

from constants import SITE_BASE_URL

__all__ = [
    "SITE_BASE_URL",
    "canonical_url",
    "Canonical",
    "OgTags",
    "JsonLd",
    "BreadcrumbList",
    "ItemListJsonLd",
    "MetaDescription",
]


def canonical_url(path: str) -> str:
    """Return a fully-qualified canonical URL for ``path``.

    Ensures a single leading slash and no double-base if ``path`` is
    already absolute.
    """
    if path.startswith(("http://", "https://")):
        return path
    if not path.startswith("/"):
        path = "/" + path
    return f"{SITE_BASE_URL}{path}"


def Canonical(path: str) -> Link:
    """``<link rel="canonical" href="...">`` for the given path."""
    return Link(rel="canonical", href=canonical_url(path))


def OgTags(
    *,
    title: str,
    description: str,
    path: str,
    image: str | None = None,
    og_type: str = "website",
) -> tuple:
    """Open Graph + Twitter card meta tags.

    Returns a tuple of FT nodes (FastHTML splats tuples into the parent),
    so callers can write::

        Head(
            Canonical(path),
            *OgTags(title=..., description=..., path=...),
        )
    """
    url = canonical_url(path)
    img = image or f"{SITE_BASE_URL}/static/favicon.jpeg"
    tags = [
        Meta(property="og:title", content=title),
        Meta(property="og:description", content=description),
        Meta(property="og:url", content=url),
        Meta(property="og:type", content=og_type),
        Meta(property="og:image", content=img),
        Meta(property="og:site_name", content="ViralVibes"),
        Meta(name="twitter:card", content="summary_large_image"),
        Meta(name="twitter:title", content=title),
        Meta(name="twitter:description", content=description),
        Meta(name="twitter:image", content=img),
    ]
    return tuple(tags)


def JsonLd(data: dict[str, Any]) -> Script:
    """Emit a ``<script type="application/ld+json">`` block.

    Uses ``ensure_ascii=False`` so non-ASCII creator names render verbatim,
    and ``separators`` to keep the payload compact.
    """
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return Script(payload, type="application/ld+json")


def BreadcrumbList(items: list[tuple[str, str | None]]) -> Script:
    """Emit a ``BreadcrumbList`` JSON-LD block.

    ``items`` is an ordered list of ``(name, url)`` pairs representing the
    navigation trail from the site root to the current page.  The last item
    is conventionally the current page and may omit the URL (pass ``None``).

    Example::

        BreadcrumbList([
            ("Home",          "/"),
            ("Top Creators",  "/creators/top"),
            ("MrBeast",       None),   # current page — no URL required
        ])
    """
    elements = []
    for position, (name, url) in enumerate(items, start=1):
        entry: dict[str, Any] = {
            "@type": "ListItem",
            "position": position,
            "name": name,
        }
        if url is not None:
            entry["item"] = canonical_url(url)
        elements.append(entry)
    return JsonLd(
        {
            "@context": "https://schema.org",
            "@type": "BreadcrumbList",
            "itemListElement": elements,
        }
    )


def ItemListJsonLd(
    *,
    name: str,
    description: str,
    items: list[tuple[str, str]],
) -> Script:
    """Build an ``ItemList`` JSON-LD block from ``(name, url)`` pairs.

    Google can use this to render rich snippets for ranked lists.
    Position is 1-based.
    """
    list_elements = [
        {
            "@type": "ListItem",
            "position": idx,
            "name": item_name,
            "url": canonical_url(item_url),
        }
        for idx, (item_name, item_url) in enumerate(items, start=1)
    ]
    return JsonLd(
        {
            "@context": "https://schema.org",
            "@type": "ItemList",
            "name": name,
            "description": description,
            "numberOfItems": len(items),
            "itemListElement": list_elements,
        }
    )


def MetaDescription(description: str) -> Meta:
    """Standard ``<meta name="description">``."""
    return Meta(name="description", content=description)
