"""
services/mentions.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Zero-cost mentions for a creator profile using only what the worker already
stores: channel_id and channel_name.

Two sources:
  YouTube RSS   — last 15 videos from the channel itself (uses channel_id)
  Google News   — external press/mentions (uses channel_name)

No API keys. No DB writes. Called lazily from the profile route.
Cached in-process for 30 minutes so repeated profile visits don't hammer
the upstream feeds.
"""

from __future__ import annotations

import logging
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote_plus

import httpx

logger = logging.getLogger(__name__)

# ── Models ─────────────────────────────────────────────────────────────────


@dataclass
class VideoItem:
    title: str
    url: str
    published: str  # human-readable date string
    view_count: Optional[int]
    thumbnail_url: str


@dataclass
class MentionItem:
    title: str
    url: str
    source: str  # e.g. "BBC News"
    published: str
    snippet: str


@dataclass
class MentionBundle:
    channel_id: str
    channel_name: str
    recent_videos: list[VideoItem] = field(default_factory=list)
    news_mentions: list[MentionItem] = field(default_factory=list)
    fetched_at: float = field(default_factory=time.monotonic)

    @property
    def has_any(self) -> bool:
        return bool(self.recent_videos or self.news_mentions)


# ── In-process cache (TTL: 30 minutes) ────────────────────────────────────
#
# TTL keeps entries fresh, and a max size cap prevents unbounded growth
# across many channel_id/channel_name pairs.

_CACHE_TTL = 30 * 60  # seconds
_MAX_CACHE_ENTRIES = 128  # hard cap to avoid unbounded growth
_cache: dict[str, MentionBundle] = {}


def _cache_key(channel_id: str, channel_name: str) -> str:
    return f"{channel_id}:{channel_name.lower()}"


def _cached(channel_id: str, channel_name: str) -> Optional[MentionBundle]:
    key = _cache_key(channel_id, channel_name)
    bundle = _cache.get(key)
    if bundle and (time.monotonic() - bundle.fetched_at) < _CACHE_TTL:
        return bundle
    return None


def _prune_cache_now() -> None:
    """Evict expired entries and trim cache down to _MAX_CACHE_ENTRIES.

    Called opportunistically on cache writes so the cache can't grow
    without bound in higher-tenant scenarios.
    """
    if not _cache:
        return

    now = time.monotonic()

    # Drop anything older than TTL.
    expired_keys = [key for key, bundle in _cache.items() if now - bundle.fetched_at > _CACHE_TTL]
    for key in expired_keys:
        _cache.pop(key, None)

    # Enforce max size, evicting oldest first by fetched_at.
    excess = len(_cache) - _MAX_CACHE_ENTRIES
    if excess > 0:
        # Sort by fetched_at so we remove the stalest entries first.
        for key, _bundle in sorted(_cache.items(), key=lambda item: item[1].fetched_at)[:excess]:
            _cache.pop(key, None)


def _store(bundle: MentionBundle) -> None:
    _prune_cache_now()
    _cache[_cache_key(bundle.channel_id, bundle.channel_name)] = bundle


# ── YouTube RSS ────────────────────────────────────────────────────────────
# https://www.youtube.com/feeds/videos.xml?channel_id=UCxxxxxx
# No API key. Returns last 15 published videos with view counts.

_YT_RSS = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
_YT_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "yt": "http://www.youtube.com/xml/schemas/2015",
    "media": "http://search.yahoo.com/mrss/",
}


def _parse_yt_date(raw: str) -> str:
    """Convert ISO-8601 to a readable 'Jan 5 2025' string (platform-agnostic)."""
    try:
        dt = datetime.fromisoformat(raw)
        # Use platform-agnostic formatting: compose from day + strftime
        return f"{dt.day} {dt.strftime('%b %Y')}"
    except Exception:
        return raw[:10]


def fetch_recent_videos(channel_id: str, limit: int = 6) -> list[VideoItem]:
    url = _YT_RSS.format(channel_id=channel_id)
    try:
        with httpx.Client(timeout=8.0) as client:
            resp = client.get(url, headers={"User-Agent": "ViralVibesBot/1.0"})
            resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status == 404:
            # Channel deleted, private, or terminated — expected for churned channels.
            # Not an error: log at DEBUG so error monitors stay clean.
            logger.debug(
                "YouTube RSS 404 for channel %s — channel may be deleted or private",
                channel_id,
            )
        elif status == 429:
            logger.warning(
                "YouTube RSS rate-limited (429) for channel %s — backing off",
                channel_id,
            )
        else:
            logger.warning(
                "YouTube RSS HTTP %d for channel %s",
                status,
                channel_id,
            )
        return []
    except httpx.TimeoutException:
        logger.warning("YouTube RSS request timed out for channel %s", channel_id)
        return []
    except Exception as exc:
        logger.error(
            "Failed to fetch YouTube RSS feed for channel %s: %r",
            channel_id,
            exc,
        )
        return []

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as exc:
        logger.error(
            "Failed to parse YouTube RSS XML",
            extra={"channel_id": channel_id, "exception": repr(exc)},
        )
        return []

    videos: list[VideoItem] = []
    for entry in root.findall("atom:entry", _YT_NS)[:limit]:
        title_el = entry.find("atom:title", _YT_NS)
        link_el = entry.find("atom:link", _YT_NS)
        pub_el = entry.find("atom:published", _YT_NS)
        stats_el = entry.find("yt:statistics", _YT_NS)
        thumb_el = entry.find("media:group/media:thumbnail", _YT_NS)

        title = title_el.text if title_el is not None else ""
        url = (link_el.get("href") if link_el is not None else "") or ""
        pub = _parse_yt_date(pub_el.text or "") if pub_el is not None else ""

        view_count: Optional[int] = None
        if stats_el is not None:
            try:
                view_count = int(stats_el.get("viewCount", 0))
            except (ValueError, TypeError):
                pass

        thumb = ""
        if thumb_el is not None:
            thumb = thumb_el.get("url", "")

        if title and url:
            videos.append(
                VideoItem(
                    title=title,
                    url=url,
                    published=pub,
                    view_count=view_count,
                    thumbnail_url=thumb,
                )
            )

    return videos


# ── Google News RSS ────────────────────────────────────────────────────────
# https://news.google.com/rss/search?q={query}&hl=en
# No API key. Returns Google News headlines for the query.
# Google exposes this as their official RSS product; it's not scraping.

_GNEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=en&gl=US&ceid=US:en"


def _strip_source(title: str) -> tuple[str, str]:
    """
    Google News titles are formatted 'Headline - Source Name'.
    Split on ' - ' from the right to separate headline from source.
    """
    if " - " in title:
        parts = title.rsplit(" - ", 1)
        return parts[0].strip(), parts[1].strip()
    return title.strip(), ""


def _parse_gnews_date(raw: str) -> str:
    """Convert RFC-2822 pubDate to a readable string (platform-agnostic)."""
    try:
        # e.g. 'Mon, 05 Jan 2026 10:00:00 GMT'
        from email.utils import parsedate_to_datetime

        dt = parsedate_to_datetime(raw)
        # Use platform-agnostic formatting: compose from day + strftime
        return f"{dt.day} {dt.strftime('%b %Y')}"
    except Exception:
        return raw[:16] if raw else ""


def fetch_news_mentions(
    channel_name: str,
    limit: int = 6,
) -> list[MentionItem]:
    # Narrow to YouTube mentions to reduce noise
    query = f'"{channel_name}" youtube'
    url = _GNEWS_RSS.format(query=quote_plus(query))

    try:
        with httpx.Client(timeout=8.0, follow_redirects=True) as client:
            resp = client.get(url, headers={"User-Agent": "ViralVibesBot/1.0"})
            resp.raise_for_status()
    except Exception as exc:
        logger.error(
            "Failed to fetch Google News RSS feed",
            extra={"channel_name": channel_name, "exception": repr(exc)},
        )
        return []

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as exc:
        logger.error(
            "Failed to parse Google News RSS XML",
            extra={"exception": repr(exc)},
        )
        return []

    channel_el = root.find("channel")
    if channel_el is None:
        return []

    mentions: list[MentionItem] = []
    for item in channel_el.findall("item")[:limit]:
        raw_title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub_raw = (item.findtext("pubDate") or "").strip()
        desc = (item.findtext("description") or "").strip()

        headline, source = _strip_source(raw_title)
        if not headline or not link:
            continue

        mentions.append(
            MentionItem(
                title=headline,
                url=link,
                source=source,
                published=_parse_gnews_date(pub_raw),
                snippet=desc[:200],
            )
        )

    return mentions


# ── Public API ─────────────────────────────────────────────────────────────


def get_mentions(
    channel_id: str,
    channel_name: str,
    video_limit: int = 6,
    news_limit: int = 6,
) -> MentionBundle:
    """
    Return a MentionBundle for the given creator, served from cache when fresh.

    Args:
        channel_id:   YouTube channel ID (UC...)  — used for RSS feed
        channel_name: Display name — used for Google News search
        video_limit:  Max recent videos to return
        news_limit:   Max news mentions to return
    """
    cached = _cached(channel_id, channel_name)
    if cached:
        return cached

    recent_videos = fetch_recent_videos(channel_id, limit=video_limit)
    news_mentions = fetch_news_mentions(channel_name, limit=news_limit)

    bundle = MentionBundle(
        channel_id=channel_id,
        channel_name=channel_name,
        recent_videos=recent_videos,
        news_mentions=news_mentions,
    )
    _store(bundle)
    return bundle
