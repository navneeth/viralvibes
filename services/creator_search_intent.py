from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum, auto
from urllib.parse import urlparse


class SearchIntentKind(Enum):
    EXACT_HANDLE = auto()
    CHANNEL_ID = auto()
    TEXT_SEARCH = auto()
    INVALID_HANDLE = auto()


@dataclass(frozen=True)
class SearchIntent:
    kind: SearchIntentKind
    raw: str
    normalized: str | None
    display: str
    error: str | None = None


HANDLE_BODY_RE = re.compile(r"^[a-zA-Z0-9._-]{3,30}$")
CHANNEL_ID_RE = re.compile(r"^UC[a-zA-Z0-9_-]{22}$")
URL_HANDLE_RE = re.compile(r"(?:youtube\.com/(?:@|channel/)|youtu\.be/)([A-Za-z0-9._-]+)", re.I)


def normalize_handle(handle_or_slug: str) -> str:
    """Normalize a YouTube handle to the same canonical form used by the DB index.

    The DB index uses PostgreSQL's `ltrim(custom_url, '@')`, which strips only
    leading `@` characters. This Python helper must match the same behavior.
    """
    if handle_or_slug is None:
        return ""
    value = str(handle_or_slug).strip()
    if not value:
        return ""
    return value.lstrip("@").lower()


def _extract_youtube_target(raw: str) -> str | None:
    """Extract a candidate handle or channel id from a YouTube URL string."""
    if not raw:
        return None

    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        host = (parsed.hostname or "").lower()
        is_youtube_host = (
            host == "youtu.be"
            or host == "youtube.com"
            or host.endswith(".youtube.com")
        )
        if is_youtube_host:
            if parsed.path.startswith("/@"):
                return parsed.path[2:]
            if parsed.path.startswith("/channel/"):
                return parsed.path[len("/channel/") :]
            if parsed.path.startswith("/@"):
                return parsed.path[2:]
            if parsed.path.startswith("/"):
                return parsed.path.lstrip("/")

    match = URL_HANDLE_RE.search(raw)
    if match:
        return match.group(1)
    return None


def classify_creator_search(raw: str) -> SearchIntent:
    """Classify a raw creator-search input into a single, typed intent.

    The result is designed to be consumed by the route, the view, and the add-queue
    without each layer re-deriving intent from a raw string.
    """
    if raw is None:
        return SearchIntent(
            kind=SearchIntentKind.TEXT_SEARCH,
            raw="",
            normalized=None,
            display="",
        )

    text = str(raw).strip()
    if not text:
        return SearchIntent(
            kind=SearchIntentKind.TEXT_SEARCH,
            raw="",
            normalized=None,
            display="",
        )

    if "youtube.com" in text.lower() or "youtu.be" in text.lower() or "/@" in text:
        extracted = _extract_youtube_target(text)
        if extracted:
            if CHANNEL_ID_RE.fullmatch(extracted):
                return SearchIntent(
                    kind=SearchIntentKind.CHANNEL_ID,
                    raw=text,
                    normalized=extracted,
                    display=extracted,
                )
            normalized = normalize_handle(extracted)
            if HANDLE_BODY_RE.fullmatch(normalized):
                return SearchIntent(
                    kind=SearchIntentKind.EXACT_HANDLE,
                    raw=text,
                    normalized=normalized,
                    display=f"@{normalized}",
                )
            return SearchIntent(
                kind=SearchIntentKind.INVALID_HANDLE,
                raw=text,
                normalized=None,
                display="",
                error="Handles must be 3–30 characters and use letters, numbers, dots, underscores, or dashes.",
            )

    if any(ch.isspace() for ch in text):
        return SearchIntent(
            kind=SearchIntentKind.TEXT_SEARCH,
            raw=text,
            normalized=None,
            display=text,
        )

    if CHANNEL_ID_RE.fullmatch(text):
        return SearchIntent(
            kind=SearchIntentKind.CHANNEL_ID,
            raw=text,
            normalized=text,
            display=text,
        )

    if text.startswith("@"):
        normalized = normalize_handle(text)
        if HANDLE_BODY_RE.fullmatch(normalized):
            return SearchIntent(
                kind=SearchIntentKind.EXACT_HANDLE,
                raw=text,
                normalized=normalized,
                display=f"@{normalized}",
            )
        return SearchIntent(
            kind=SearchIntentKind.INVALID_HANDLE,
            raw=text,
            normalized=None,
            display="",
            error="Handles must be 3–30 characters and use letters, numbers, dots, underscores, or dashes.",
        )

    if HANDLE_BODY_RE.fullmatch(text):
        normalized = normalize_handle(text)
        return SearchIntent(
            kind=SearchIntentKind.EXACT_HANDLE,
            raw=text,
            normalized=normalized,
            display=f"@{normalized}",
        )

    return SearchIntent(
        kind=SearchIntentKind.TEXT_SEARCH,
        raw=text,
        normalized=None,
        display=text,
    )
