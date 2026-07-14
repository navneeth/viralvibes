"""Helpers for public programmatic ranking landing pages."""

from __future__ import annotations

from functools import lru_cache

import pycountry

from utils import slugify

_CATEGORY_SLUG_ALIASES: dict[str, str] = {
    "gaming": "Video game culture",
    "games": "Video game culture",
    "video-games": "Video game culture",
    "lifestyle": "Lifestyle (sociology)",
    "fitness": "Physical fitness",
    "tech": "Technology",
    "sports": "Sports game",
}

_CATEGORY_PUBLIC_SLUGS: dict[str, str] = {
    "Video game culture": "gaming",
    "Lifestyle (sociology)": "lifestyle",
    "Physical fitness": "fitness",
    "Technology": "technology",
    "Sports game": "sports",
}

# Start with high-intent, broad niches that map to the existing YouTube topic
# taxonomy. The route supports more combinations; the sitemap should stay
# selective until live DB counts prove long-tail pages are non-empty.
RANKING_SITEMAP_CATEGORIES: tuple[str, ...] = (
    "Video game culture",
    "Music",
    "Entertainment",
    "Technology",
    "Fashion",
    "Food",
    "Physical fitness",
    "Business",
    "Sports game",
    "Lifestyle (sociology)",
)

RANKING_SITEMAP_COUNTRY_CODES: tuple[str, ...] = (
    "US",
    "GB",
    "CA",
    "AU",
    "IN",
    "JP",
    "KR",
    "BR",
    "MX",
    "DE",
    "FR",
    "ES",
)

_COUNTRY_SLUG_ALIASES: dict[str, str] = {
    "usa": "US",
    "u-s": "US",
    "united-states": "US",
    "united-states-of-america": "US",
    "uk": "GB",
    "u-k": "GB",
    "great-britain": "GB",
    "united-kingdom": "GB",
    "south-korea": "KR",
    "korea-republic-of": "KR",
    "russia": "RU",
    "vietnam": "VN",
}


def resolve_ranking_category_slug(slug: str) -> str | None:
    """Resolve an SEO-friendly ranking category slug to a canonical topic label."""
    return _CATEGORY_SLUG_ALIASES.get(slugify(str(slug or "")))


def ranking_category_slug(category_name: str) -> str:
    """Return the public ranking slug for a canonical topic category label."""
    return _CATEGORY_PUBLIC_SLUGS.get(str(category_name or ""), slugify(category_name))


def country_slug(country_code: str) -> str:
    """Return the canonical public URL slug for an ISO alpha-2 country code."""
    country = pycountry.countries.get(alpha_2=str(country_code or "").upper())
    if not country:
        return slugify(str(country_code or ""))
    display = getattr(country, "common_name", country.name)
    return slugify(display)


@lru_cache(maxsize=1)
def _country_slug_map() -> dict[str, str]:
    """Return a slug-to-alpha2 lookup for pycountry names plus common aliases."""
    mapping = dict(_COUNTRY_SLUG_ALIASES)
    for country in pycountry.countries:
        code = country.alpha_2.upper()
        for attr in ("name", "common_name", "official_name"):
            value = getattr(country, attr, None)
            if value:
                mapping.setdefault(slugify(value), code)
        mapping.setdefault(code.lower(), code)
    return mapping


def resolve_country_slug(slug: str) -> str | None:
    """Resolve a public country slug or alpha-2 code to uppercase alpha-2."""
    raw = str(slug or "").strip()
    if not raw:
        return None
    if len(raw) == 2:
        country = pycountry.countries.get(alpha_2=raw.upper())
        if country:
            return country.alpha_2.upper()
    return _country_slug_map().get(slugify(raw))


def ranking_path(category_name: str, country_code: str) -> str:
    """Return the canonical ranking path for a category/country pair."""
    return f"/rankings/{ranking_category_slug(category_name)}/{country_slug(country_code)}"


def iter_ranking_sitemap_paths() -> list[str]:
    """Return the bounded set of programmatic ranking URLs for the sitemap."""
    return [
        ranking_path(category, country_code)
        for category in RANKING_SITEMAP_CATEGORIES
        for country_code in RANKING_SITEMAP_COUNTRY_CODES
    ]
