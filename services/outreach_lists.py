"""
Bridge saved creator lists into the outreach harness.

No new campaign schema yet: importing a list simply bulk-saves its top creators
into ``user_favourite_creators`` so the existing outreach page/export can use
them.
"""

from __future__ import annotations

from typing import Any

from db import get_creators
from db_lists import (
    get_most_active_creators,
    get_new_channels,
    get_rising_creators,
    get_top_rated_creators,
    get_veteran_creators,
)


IMPORT_LIMIT_DEFAULT = 25
IMPORT_LIMIT_MAX = 100

_FLAT_LIST_LOADERS = {
    "top-rated": get_top_rated_creators,
    "most-active": get_most_active_creators,
    "rising": get_rising_creators,
    "veterans": get_veteran_creators,
    "new-channels": get_new_channels,
}


def clamp_import_limit(raw_limit: int | str | None) -> int:
    try:
        value = int(raw_limit or IMPORT_LIMIT_DEFAULT)
    except (TypeError, ValueError):
        value = IMPORT_LIMIT_DEFAULT
    return max(1, min(IMPORT_LIMIT_MAX, value))


def is_importable_list_key(list_key: str) -> bool:
    if list_key in _FLAT_LIST_LOADERS:
        return True
    return list_key.startswith(("country:", "category:", "language:"))


def get_creators_for_outreach_list(
    list_key: str,
    limit: int = IMPORT_LIMIT_DEFAULT,
) -> list[dict[str, Any]]:
    """
    Resolve a saved list key to creator rows.

    Aggregate explorer tabs such as ``by-country`` are not importable because
    they are collections of lists rather than creator lists.
    """
    limit = clamp_import_limit(limit)

    if list_key in _FLAT_LIST_LOADERS:
        return _FLAT_LIST_LOADERS[list_key](limit=limit)

    if list_key.startswith("country:"):
        country_code = list_key.split(":", 1)[1].upper()
        result = get_creators(country_filter=country_code, sort="subscribers", limit=limit)
        return list(result or [])

    if list_key.startswith("category:"):
        category = list_key.split(":", 1)[1]
        result = get_creators(category_filter=category, sort="subscribers", limit=limit)
        return list(result or [])

    if list_key.startswith("language:"):
        language_code = list_key.split(":", 1)[1].lower()
        result = get_creators(language_filter=language_code, sort="subscribers", limit=limit)
        return list(result or [])

    return []
