"""
Seed creator discovery from multiple sources.

Supports:
- CSV: Local list of channels (youtubers.csv or specified path)
- Wikipedia (most-subscribed): Top 100 by subscriber count
- Wikipedia (most-viewed): Top 50 by total view count
- Wikidata SPARQL: ~500+ notable channels with Wikipedia articles,
                   sourced via the P2397 YouTube channel ID property
- Wikidata Extended: Targeted per-entity-type SPARQL queries (YouTubers,
                     musicians, bands, comedians, athletes, journalists,
                     news orgs, businesses, filmmakers, YouTube-channel
                     entities). Each query runs with its own high LIMIT
                     and is rate-limited between calls (per WIKIDATA_RATE_LIMIT_SLEEP).
                     Yields ~5,000–10,000 additional channels with zero YouTube
                     API cost.

Quota strategy
--------------
YouTube Data API v3 costs vary significantly by operation:
  - channels.list by UC ID    →   1 unit  (free lookup)
  - channels.list by @handle  →   1 unit  (cheap)
  - search.list by name       → 100 units (expensive — avoid where possible)

Wikipedia and Wikidata sources require NO YouTube API calls at all.
They return UC IDs directly, so seeding from them is completely free.

This script minimises quota usage by resolving in priority order:
  1. UC IDs detected directly in CSV   → 0 API calls
  2. @handles detected in CSV          → 1 unit each (channels.list)
  3. Plain names                        → 100 units each (search.list)

For plain-name rows the script stops resolving once the remaining
quota budget (--quota-budget, default 2000 units) would be exceeded,
and logs which channels were skipped so you can add their @handles
to the CSV instead.

Usage
-----
  python scripts/seed_creators.py scripts/youtubers.csv
  python scripts/seed_creators.py scripts/youtubers.csv --quota-budget 5000
  python scripts/seed_creators.py scripts/youtubers.csv --dry-run
  python scripts/seed_creators.py --no-wikipedia --no-wikidata   # CSV only
  python scripts/seed_creators.py --no-csv                       # scraped sources only
"""

import asyncio
import csv
import logging
import os
import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent))

import db
from constants import CREATOR_TABLE
from db import init_supabase, queue_creator_sync, setup_logging
from services.channel_utils import ChannelIDValidator, YouTubeResolver

logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================
# Wikidata recommends ≤ 1 request / 5 s from automated clients.
# Using 6 s provides safe margin and accounts for processing time.
WIKIDATA_RATE_LIMIT_SLEEP = 6  # seconds

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class DiscoveredCreator:
    channel_id: str
    channel_name: Optional[str]
    channel_url: Optional[str]
    source: str  # "csv", "wikipedia"
    source_rank: int


@dataclass
class SeedStats:
    upserted: int = 0  # genuinely new rows inserted
    skipped_existing: int = 0  # rows already in DB — skipped entirely
    failed: int = 0
    quota_skipped: int = 0  # name-only rows skipped to preserve quota
    unresolvable: int = 0  # genuinely could not resolve
    sync_jobs_queued: int = 0
    quota_units_used: int = 0
    skipped_names: List[str] = field(default_factory=list)

    def total_attempted(self) -> int:
        return self.upserted + self.failed

    def summary(self) -> str:
        lines = [
            "─" * 60,
            "SEEDING COMPLETE",
            "─" * 60,
            f"  Inserted (new):          {self.upserted}",
            f"  Skipped (already in DB): {self.skipped_existing}",
            f"  Sync jobs queued:        {self.sync_jobs_queued}",
            f"  Failed (DB errors):      {self.failed}",
            f"  Unresolvable:            {self.unresolvable}",
            f"  Quota-skipped:           {self.quota_skipped}",
            f"  API units used:          {self.quota_units_used}",
        ]
        if self.skipped_names:
            lines += [
                "",
                f"  ⚠️  {len(self.skipped_names)} channels skipped to preserve quota.",
                "  Add their @handles or UC IDs to the CSV to resolve for free:",
            ]
            for name in self.skipped_names[:20]:
                lines.append(f"    - {name}")
            if len(self.skipped_names) > 20:
                lines.append(f"    ... and {len(self.skipped_names) - 20} more")
        lines.append("─" * 60)
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# CSV resolution — quota-aware
# ---------------------------------------------------------------------------

# Column names to try in order when looking for the channel name/identifier
_NAME_COLUMNS = ("Channel Name", "channel_name", "Name", "name", "Creator", "creator")
_ID_COLUMNS = ("Channel ID", "channel_id", "ID", "id")
_URL_COLUMNS = ("Channel URL", "channel_url", "URL", "url", "Link", "link")
_SEARCH_COST = 100  # YouTube search.list quota units per call
_HANDLE_COST = 1  # YouTube channels.list quota units per call


def _detect_column(fieldnames: List[str], candidates: Tuple[str, ...]) -> Optional[str]:
    """Return the first candidate column name present in fieldnames."""
    for col in candidates:
        if col in fieldnames:
            return col
    return None


def _classify_identifier(value: str, validator: ChannelIDValidator) -> str:
    """
    Classify a raw CSV cell value as one of:
      "channel_id" — already a UCxxx ID
      "handle"     — starts with @ or is a youtube.com/@ URL
      "name"       — plain channel name (requires expensive search)
    """
    stripped = value.strip()
    if validator.is_valid(stripped):
        return "channel_id"
    if validator.extract_from_url(stripped):
        return "channel_id"
    if stripped.startswith("@") or "/@" in stripped or "/c/" in stripped:
        return "handle"
    return "name"


async def resolve_csv(
    csv_path: str,
    resolver: YouTubeResolver,
    validator: ChannelIDValidator,
    quota_budget: int,
    dry_run: bool = False,
) -> Tuple[List[DiscoveredCreator], SeedStats]:
    """
    Read CSV and resolve each row to a channel_id.

    Resolution priority (to minimise quota):
      1. Direct UC channel IDs in ID or URL columns → 0 units
      2. @handles → 1 unit each (channels.list, not search.list)
      3. Plain names → 100 units each (search.list) — stop if budget exceeded

    Returns (creators, partial_stats) where partial_stats tracks quota usage
    and skipped rows for inclusion in the final summary.
    """
    if not Path(csv_path).exists():
        logger.error(f"CSV file not found: {csv_path}")
        return [], SeedStats()

    logger.info(f"📄 Reading CSV: {csv_path}")
    stats = SeedStats()
    creators: List[DiscoveredCreator] = []
    seen_ids: Set[str] = set()

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            logger.error("CSV is empty or has no header row")
            return [], stats

        # Detect columns once, before the row loop
        id_col = _detect_column(reader.fieldnames, _ID_COLUMNS)
        url_col = _detect_column(reader.fieldnames, _URL_COLUMNS)
        name_col = _detect_column(reader.fieldnames, _NAME_COLUMNS)

        logger.debug(f"CSV columns → id:{id_col!r}, url:{url_col!r}, name:{name_col!r}")

        rows = [r for r in reader if any(r.values())]
        random.shuffle(rows)

    logger.info(f"  {len(rows)} non-empty rows to process (order randomised)")
    remaining_budget = quota_budget

    for rank, row in enumerate(rows, start=1):
        channel_id: Optional[str] = None
        channel_name: Optional[str] = (
            row.get(name_col, "").strip() if name_col else None
        )
        channel_url: Optional[str] = row.get(url_col, "").strip() if url_col else None

        # ── Step 1: try to get the channel ID directly (zero API cost) ────────
        raw_id = row.get(id_col, "").strip() if id_col else ""
        raw_url = channel_url or ""

        for candidate in (raw_id, raw_url):
            if not candidate:
                continue
            extracted = validator.extract_from_url(candidate) or (
                candidate if validator.is_valid(candidate) else None
            )
            if extracted:
                channel_id = extracted
                logger.debug(
                    f"  [{rank}] {channel_name or candidate} → {channel_id} (direct, 0 units)"
                )
                break

        # ── Step 2: @handle resolution (1 unit) ───────────────────────────────
        if not channel_id and channel_name:
            kind = _classify_identifier(channel_name, validator)

            if kind == "channel_id":
                channel_id = validator.extract_from_url(channel_name) or channel_name
                logger.debug(
                    f"  [{rank}] {channel_name} → {channel_id} (direct, 0 units)"
                )

            elif kind == "handle":
                if remaining_budget >= _HANDLE_COST:
                    if not dry_run:
                        channel_id = await resolver.resolve_handle_to_channel_id(
                            channel_name
                        )
                    else:
                        channel_id = f"DRY_RUN_{rank}"
                    if channel_id:
                        remaining_budget -= _HANDLE_COST
                        stats.quota_units_used += _HANDLE_COST
                        logger.debug(
                            f"  [{rank}] {channel_name} → {channel_id} (handle, 1 unit)"
                        )
                    else:
                        logger.warning(
                            f"  [{rank}] Could not resolve handle: {channel_name}"
                        )
                        stats.unresolvable += 1
                        continue
                else:
                    logger.warning(
                        f"  [{rank}] {channel_name}: budget exhausted, skipping handle resolution"
                    )
                    stats.quota_skipped += 1
                    stats.skipped_names.append(channel_name)
                    continue

            else:  # plain name — expensive search
                if remaining_budget >= _SEARCH_COST:
                    if not dry_run:
                        channel_id = await resolver.resolve_handle_to_channel_id(
                            channel_name
                        )
                    else:
                        channel_id = f"DRY_RUN_{rank}"
                    if channel_id:
                        remaining_budget -= _SEARCH_COST
                        stats.quota_units_used += _SEARCH_COST
                        logger.debug(
                            f"  [{rank}] {channel_name!r} → {channel_id} (search, 100 units)"
                            f" | budget remaining: {remaining_budget}"
                        )
                    else:
                        logger.warning(
                            f"  [{rank}] Could not resolve: {channel_name!r}"
                        )
                        stats.unresolvable += 1
                        continue
                else:
                    # Budget exhausted — log and skip, don't silently lose
                    stats.quota_skipped += 1
                    stats.skipped_names.append(channel_name or f"row {rank}")
                    continue

        if not channel_id:
            logger.warning(f"  [{rank}] No identifier found in row: {dict(row)}")
            stats.unresolvable += 1
            continue

        if channel_id in seen_ids:
            logger.debug(f"  [{rank}] Duplicate in CSV: {channel_id}, skipping")
            continue
        seen_ids.add(channel_id)

        creators.append(
            DiscoveredCreator(
                channel_id=channel_id,
                channel_name=channel_name,
                channel_url=channel_url,
                source="csv",
                source_rank=rank,
            )
        )

    logger.info(
        f"✅ CSV: {len(creators)} creators resolved | "
        f"{stats.quota_units_used} API units used | "
        f"{stats.quota_skipped} rows skipped (budget) | "
        f"{stats.unresolvable} unresolvable"
    )
    if stats.quota_skipped > 0:
        logger.warning(
            f"  ⚠️  {stats.quota_skipped} channels skipped to preserve quota. "
            "Add @handles or UC IDs to the CSV to resolve them without search quota."
        )
    return creators, stats


# ---------------------------------------------------------------------------
# Wikipedia source
# ---------------------------------------------------------------------------


async def fetch_wikipedia(validator: ChannelIDValidator) -> List[DiscoveredCreator]:
    """
    Fetch top YouTube channels from Wikipedia's most-subscribed list.

    Extracts UC channel IDs directly from href attributes — no API calls needed.
    Note: Wikipedia has been migrating channel links to @handle format; the
    UC-ID regex will only match rows that still use /channel/UCxxx links.
    Rows with handle-only links are not resolved here to avoid quota spend.
    """
    url = "https://en.wikipedia.org/wiki/List_of_most-subscribed_YouTube_channels"
    logger.info("📖 Fetching from Wikipedia...")
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ViralVibesSeed/1.0)"}

    try:
        resp = requests.get(url, timeout=30, headers=headers)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Wikipedia fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table", class_="wikitable")
    if not tables:
        logger.warning("No wikitables found — Wikipedia structure may have changed")
        return []

    creators: List[DiscoveredCreator] = []
    seen: Set[str] = set()
    rank = 1

    for table in tables[:2]:
        for link in table.find_all("a", href=True):
            channel_id = validator.extract_from_url(link["href"])
            if channel_id and channel_id not in seen:
                seen.add(channel_id)
                creators.append(
                    DiscoveredCreator(
                        channel_id=channel_id,
                        channel_name=link.get_text(strip=True) or None,
                        channel_url=f"https://www.youtube.com/channel/{channel_id}",
                        source="wikipedia",
                        source_rank=rank,
                    )
                )
                rank += 1
            if rank > 200:
                break
        if rank > 200:
            break

    logger.info(
        f"✅ Wikipedia (most-subscribed): {len(creators)} creators found via UC ID extraction"
    )
    if len(creators) < 10:
        logger.warning(
            "  Very few channels extracted from Wikipedia. "
            "Wikipedia may have switched to @handle links — "
            "handle resolution from Wikipedia is not currently implemented "
            "to avoid quota spend."
        )
    return creators


# ---------------------------------------------------------------------------
# Wikipedia source — most-viewed channels
# ---------------------------------------------------------------------------


async def fetch_wikipedia_most_viewed(
    validator: ChannelIDValidator,
) -> List[DiscoveredCreator]:
    """
    Fetch top 50 YouTube channels from Wikipedia's most-viewed list.

    URL: https://en.wikipedia.org/wiki/List_of_most-viewed_YouTube_channels
    Structure: single wikitable with UC IDs in href attributes — identical
    parsing approach to fetch_wikipedia (most-subscribed).

    No YouTube API calls required — UC IDs extracted directly from links.
    Complements the subscriber-ranked list with view-count-ranked channels
    (e.g. children's content and music labels rank higher here).
    """
    url = "https://en.wikipedia.org/wiki/List_of_most-viewed_YouTube_channels"
    logger.info("📖 Fetching from Wikipedia (most-viewed)...")
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ViralVibesSeed/1.0)"}

    try:
        resp = requests.get(url, timeout=30, headers=headers)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Wikipedia (most-viewed) fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table", class_="wikitable")
    if not tables:
        logger.warning(
            "No wikitables found on most-viewed page — Wikipedia structure may have changed"
        )
        return []

    creators: List[DiscoveredCreator] = []
    seen: Set[str] = set()
    rank = 1

    for table in tables[:1]:  # Only the first table on this page
        for link in table.find_all("a", href=True):
            channel_id = validator.extract_from_url(link["href"])
            if channel_id and channel_id not in seen:
                seen.add(channel_id)
                creators.append(
                    DiscoveredCreator(
                        channel_id=channel_id,
                        channel_name=link.get_text(strip=True) or None,
                        channel_url=f"https://www.youtube.com/channel/{channel_id}",
                        source="wikipedia_most_viewed",
                        source_rank=rank,
                    )
                )
                rank += 1
        if rank > 100:
            break

    logger.info(
        f"✅ Wikipedia (most-viewed): {len(creators)} creators found via UC ID extraction"
    )
    if len(creators) < 5:
        logger.warning(
            "  Very few channels extracted from Wikipedia most-viewed. "
            "Page structure may have changed or links may now use @handles."
        )
    return creators


# ---------------------------------------------------------------------------
# Wikidata SPARQL source
# ---------------------------------------------------------------------------

# SPARQL query: all items with a YouTube channel ID (P2397), optionally
# filtered to those that are instances of "YouTube channel" (Q2178147) or
# simply any notable entity with a channel. We request the raw channel ID,
# the English label, and the subscriber count (P3744) for sorting.
_WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

_WIKIDATA_QUERY = """
SELECT DISTINCT ?channelId ?label ?subscribers WHERE {
  ?item wdt:P2397 ?channelId .
  OPTIONAL { ?item wdt:P3744 ?subscribers . }
  OPTIONAL {
    ?item rdfs:label ?label .
    FILTER(LANG(?label) = "en")
  }
}
ORDER BY DESC(?subscribers)
LIMIT 600
"""


async def fetch_wikidata(validator: ChannelIDValidator) -> List[DiscoveredCreator]:
    """
    Fetch notable YouTube channels from Wikidata's public SPARQL endpoint.

    Wikidata property P2397 is the YouTube channel ID — it stores the raw
    UC... identifier directly, so no YouTube API calls are needed at all.

    Coverage: ~500–600 notable channels that have Wikipedia articles and a
    verified YouTube channel ID entry (musicians, media companies, politicians,
    sports teams, educational channels, major creators). Ordered by subscriber
    count (P3744) where available so the most prominent channels get the lowest
    source_rank values.

    Endpoint: https://query.wikidata.org/sparql
    Rate limit: 1 request / 5 s recommended. We make exactly one call.
    ToS: CC0 licensed data — fully open for reuse.
    """
    logger.info("🌐 Fetching from Wikidata SPARQL...")
    headers = {
        "User-Agent": "ViralVibesSeed/1.0 (seed_creators.py; contact via project repo)",
        "Accept": "application/sparql-results+json",
    }
    params = {"query": _WIKIDATA_QUERY, "format": "json"}

    try:
        resp = requests.get(
            _WIKIDATA_SPARQL_ENDPOINT,
            params=params,
            headers=headers,
            timeout=60,  # SPARQL can be slow
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.error(f"Wikidata SPARQL fetch failed: {e}")
        return []
    except ValueError as e:
        logger.error(f"Wikidata SPARQL response was not valid JSON: {e}")
        return []

    bindings = data.get("results", {}).get("bindings", [])
    if not bindings:
        logger.warning(
            "Wikidata SPARQL returned no results — query or endpoint may have changed."
        )
        return []

    creators: List[DiscoveredCreator] = []
    seen: Set[str] = set()
    rank = 1
    skipped_invalid = 0

    for row in bindings:
        raw_id = row.get("channelId", {}).get("value", "").strip()

        # Wikidata P2397 stores the raw UC... ID (without the URL prefix).
        # Validate it before trusting it — the property is sometimes used
        # incorrectly for handle strings or legacy /user/ names.
        if not raw_id:
            continue

        # Normalise: strip any accidental URL prefix that editors may have added.
        # Treat the value as a YouTube URL only if it actually parses to a
        # youtube.com (or subdomain) hostname; otherwise, fall back to raw ID.
        channel_id = None
        if raw_id.startswith(("http://", "https://")):
            parsed = urlparse(raw_id)
            host = parsed.hostname or ""
            if host == "youtube.com" or host.endswith(".youtube.com"):
                channel_id = validator.extract_from_url(raw_id)
        if channel_id is None and validator.is_valid(raw_id):
            channel_id = raw_id
        if channel_id is None:
            # Not a UC ID — could be a handle or legacy username.
            # Skip to avoid API cost; user can add to CSV with @handle prefix.
            skipped_invalid += 1
            logger.debug(f"  Wikidata: skipping non-UC value: {raw_id!r}")
            continue

        if not channel_id or channel_id in seen:
            continue

        seen.add(channel_id)
        label = row.get("label", {}).get("value") or None

        creators.append(
            DiscoveredCreator(
                channel_id=channel_id,
                channel_name=label,
                channel_url=f"https://www.youtube.com/channel/{channel_id}",
                source="wikidata",
                source_rank=rank,
            )
        )
        rank += 1

    logger.info(
        f"✅ Wikidata: {len(creators)} creators found "
        f"({skipped_invalid} non-UC entries skipped)"
    )
    if len(creators) < 50:
        logger.warning(
            "  Fewer results than expected from Wikidata. "
            "The SPARQL endpoint may be rate-limiting or the query timed out."
        )
    return creators


# ---------------------------------------------------------------------------
# Wikidata Extended — targeted per-entity-type queries
# ---------------------------------------------------------------------------
#
# The broad _WIKIDATA_QUERY above fetches any item with P2397, ordered by
# subscriber count, but its LIMIT 600 means many real creators are missed.
#
# The queries below each target a specific Wikidata entity class.  Running
# them separately lets us use a high per-class LIMIT and still ORDER BY
# subscribers so the best signal rises to the top.  All results are UC IDs
# sourced directly from P2397 — zero YouTube API cost.
#
# Entity classes used (Wikidata QIDs):
#   Q17125263  — YouTuber (content creator whose primary medium is YouTube)
#   Q177220    — singer / solo musician
#   Q215380    — musical group / band
#   Q245068    — comedian
#   Q2066131   — athlete
#   Q1930187   — journalist
#   Q1331793   — news media organisation
#   Q4830453   — business / brand / company
#   Q2526255   — film director / filmmaker
#   Q2178147   — YouTube channel (the channel itself as a Wikidata entity)
#
# The Q2178147 query is qualitatively different from the others: instead of
# finding people/organisations that *have* a YouTube channel, it finds items
# that *are* YouTube channels — a separate, largely non-overlapping population
# in Wikidata where the channel entity is modelled directly (e.g. many gaming
# channels, commentary channels, and brand channels live here rather than
# under a person entry).
#
# Rate limit: Wikidata recommends ≤ 1 request / 5 s from automated clients.
# We sleep WIKIDATA_RATE_LIMIT_SLEEP between calls to stay safely within that bound.
# ---------------------------------------------------------------------------

_WIKIDATA_ENTITY_QUERIES: List[Tuple[str, str]] = [
    (
        "YouTubers",
        """
        SELECT DISTINCT ?channelId ?label ?subscribers WHERE {
          ?item wdt:P31 wd:Q17125263 ;
                wdt:P2397 ?channelId .
          OPTIONAL { ?item wdt:P3744 ?subscribers . }
          OPTIONAL { ?item rdfs:label ?label . FILTER(LANG(?label) = "en") }
        }
        ORDER BY DESC(?subscribers)
        LIMIT 2000
        """,
    ),
    (
        "Musicians",
        """
        SELECT DISTINCT ?channelId ?label ?subscribers WHERE {
          ?item wdt:P31 wd:Q177220 ;
                wdt:P2397 ?channelId .
          OPTIONAL { ?item wdt:P3744 ?subscribers . }
          OPTIONAL { ?item rdfs:label ?label . FILTER(LANG(?label) = "en") }
        }
        ORDER BY DESC(?subscribers)
        LIMIT 2000
        """,
    ),
    (
        "Bands",
        """
        SELECT DISTINCT ?channelId ?label ?subscribers WHERE {
          ?item wdt:P31 wd:Q215380 ;
                wdt:P2397 ?channelId .
          OPTIONAL { ?item wdt:P3744 ?subscribers . }
          OPTIONAL { ?item rdfs:label ?label . FILTER(LANG(?label) = "en") }
        }
        ORDER BY DESC(?subscribers)
        LIMIT 1000
        """,
    ),
    (
        "Comedians",
        """
        SELECT DISTINCT ?channelId ?label ?subscribers WHERE {
          ?item wdt:P31 wd:Q245068 ;
                wdt:P2397 ?channelId .
          OPTIONAL { ?item wdt:P3744 ?subscribers . }
          OPTIONAL { ?item rdfs:label ?label . FILTER(LANG(?label) = "en") }
        }
        ORDER BY DESC(?subscribers)
        LIMIT 1000
        """,
    ),
    (
        "Athletes",
        """
        SELECT DISTINCT ?channelId ?label ?subscribers WHERE {
          ?item wdt:P31 wd:Q2066131 ;
                wdt:P2397 ?channelId .
          OPTIONAL { ?item wdt:P3744 ?subscribers . }
          OPTIONAL { ?item rdfs:label ?label . FILTER(LANG(?label) = "en") }
        }
        ORDER BY DESC(?subscribers)
        LIMIT 1000
        """,
    ),
    (
        "Journalists",
        """
        SELECT DISTINCT ?channelId ?label ?subscribers WHERE {
          ?item wdt:P31 wd:Q1930187 ;
                wdt:P2397 ?channelId .
          OPTIONAL { ?item wdt:P3744 ?subscribers . }
          OPTIONAL { ?item rdfs:label ?label . FILTER(LANG(?label) = "en") }
        }
        ORDER BY DESC(?subscribers)
        LIMIT 500
        """,
    ),
    (
        "NewsOrgs",
        """
        SELECT DISTINCT ?channelId ?label ?subscribers WHERE {
          ?item wdt:P31 wd:Q1331793 ;
                wdt:P2397 ?channelId .
          OPTIONAL { ?item wdt:P3744 ?subscribers . }
          OPTIONAL { ?item rdfs:label ?label . FILTER(LANG(?label) = "en") }
        }
        ORDER BY DESC(?subscribers)
        LIMIT 500
        """,
    ),
    (
        "Businesses",
        """
        SELECT DISTINCT ?channelId ?label ?subscribers WHERE {
          ?item wdt:P31 wd:Q4830453 ;
                wdt:P2397 ?channelId .
          OPTIONAL { ?item wdt:P3744 ?subscribers . }
          OPTIONAL { ?item rdfs:label ?label . FILTER(LANG(?label) = "en") }
        }
        ORDER BY DESC(?subscribers)
        LIMIT 500
        """,
    ),
    (
        "Filmmakers",
        """
        SELECT DISTINCT ?channelId ?label ?subscribers WHERE {
          ?item wdt:P31 wd:Q2526255 ;
                wdt:P2397 ?channelId .
          OPTIONAL { ?item wdt:P3744 ?subscribers . }
          OPTIONAL { ?item rdfs:label ?label . FILTER(LANG(?label) = "en") }
        }
        ORDER BY DESC(?subscribers)
        LIMIT 500
        """,
    ),
    # ── NEW ──────────────────────────────────────────────────────────────────
    # Q2178147 = "YouTube channel" as a direct Wikidata entity.
    #
    # The queries above find people/orgs that *have* a YouTube channel via
    # P2397.  This query finds items that *are* YouTube channels — a separate
    # population where the channel itself is the Wikidata subject (rather than
    # the person behind it).  Gaming channels, commentary channels, brand
    # channels, and many non-English creators tend to be modelled this way.
    #
    # No subscriber data is stored on most of these items (P3744 is sparse
    # for channel-entities), so we ORDER BY label as a stable fallback rather
    # than getting an arbitrary ordering.  LIMIT 5000 captures the bulk of
    # this population without risking a Wikidata query timeout.
    (
        "YouTubeChannelEntities",
        """
        SELECT DISTINCT ?channelId ?label WHERE {
          ?item wdt:P31 wd:Q2178147 ;
                wdt:P2397 ?channelId .
          OPTIONAL { ?item rdfs:label ?label . FILTER(LANG(?label) = "en") }
        }
        ORDER BY ?label
        LIMIT 5000
        """,
    ),
]


def _parse_wikidata_bindings(
    bindings: List[dict],
    validator: ChannelIDValidator,
    source: str,
    seen: Set[str],
    start_rank: int,
) -> Tuple[List[DiscoveredCreator], int]:
    """
    Shared parser for Wikidata SPARQL result bindings.

    Applies the same UC-ID validation and URL-prefix normalisation used in
    fetch_wikidata().  Mutates `seen` in-place so callers share dedup state
    across multiple query results.

    Returns (creators, skipped_invalid_count).
    """
    creators: List[DiscoveredCreator] = []
    skipped_invalid = 0
    rank = start_rank

    for row in bindings:
        raw_id = row.get("channelId", {}).get("value", "").strip()
        if not raw_id:
            continue

        channel_id = None
        if raw_id.startswith(("http://", "https://")):
            parsed = urlparse(raw_id)
            host = parsed.hostname or ""
            if host == "youtube.com" or host.endswith(".youtube.com"):
                channel_id = validator.extract_from_url(raw_id)
        if channel_id is None and validator.is_valid(raw_id):
            channel_id = raw_id
        if channel_id is None:
            skipped_invalid += 1
            logger.debug(f"  Wikidata ({source}): skipping non-UC value: {raw_id!r}")
            continue

        if channel_id in seen:
            continue

        seen.add(channel_id)
        label = row.get("label", {}).get("value") or None

        creators.append(
            DiscoveredCreator(
                channel_id=channel_id,
                channel_name=label,
                channel_url=f"https://www.youtube.com/channel/{channel_id}",
                source=source,
                source_rank=rank,
            )
        )
        rank += 1

    return creators, skipped_invalid


async def fetch_wikidata_extended(
    validator: ChannelIDValidator,
    already_seen: Optional[Set[str]] = None,
) -> List[DiscoveredCreator]:
    """
    Run targeted Wikidata SPARQL queries, one per creator entity type.

    Unlike the broad fetch_wikidata() which fires a single generic query
    with LIMIT 600, this function issues one query per Wikidata class
    (YouTuber, Musician, Band, Comedian, Athlete, …, YouTubeChannelEntities)
    each with its own high LIMIT.  Because each class is fetched independently
    the results are additive — a channel that is both a musician and a YouTuber
    will appear in whichever class query returns it first, then be deduped out
    of subsequent ones.

    already_seen: optional set of channel_ids to skip from the start
                  (pass the set collected from earlier sources to avoid
                  re-adding channels already queued for seeding).

    Rate limiting: WIKIDATA_RATE_LIMIT_SLEEP seconds between requests (Wikidata recommends ≤ 1/5 s).
    Cost: zero YouTube API units — all data comes from Wikidata P2397.
    Expected yield: 3,000–8,000 additional unique creators beyond the broad query.
    """
    logger.info("🌐 Fetching from Wikidata Extended (entity-type queries)...")

    seen: Set[str] = set(already_seen) if already_seen else set()
    all_creators: List[DiscoveredCreator] = []
    rank = 1

    wikidata_headers = {
        "User-Agent": "ViralVibesSeed/1.0 (seed_creators.py; contact via project repo)",
        "Accept": "application/sparql-results+json",
    }

    for entity_label, query in _WIKIDATA_ENTITY_QUERIES:
        logger.info(f"  ↳ Querying: {entity_label}...")
        try:
            resp = requests.get(
                _WIKIDATA_SPARQL_ENDPOINT,
                params={"query": query, "format": "json"},
                headers=wikidata_headers,
                timeout=90,  # entity queries can be slower than the broad one
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.warning(
                f"    Wikidata Extended ({entity_label}) request failed: {e}"
            )
            time.sleep(WIKIDATA_RATE_LIMIT_SLEEP)
            continue
        except ValueError as e:
            logger.warning(f"    Wikidata Extended ({entity_label}) invalid JSON: {e}")
            time.sleep(WIKIDATA_RATE_LIMIT_SLEEP)
            continue

        bindings = data.get("results", {}).get("bindings", [])
        batch, skipped = _parse_wikidata_bindings(
            bindings,
            validator,
            source=f"wikidata_{entity_label.lower()}",
            seen=seen,
            start_rank=rank,
        )
        logger.info(
            f"    → {len(batch)} new creators " f"({skipped} non-UC entries skipped)"
        )
        all_creators.extend(batch)
        rank += len(batch)

        time.sleep(
            WIKIDATA_RATE_LIMIT_SLEEP
        )  # respect Wikidata's rate limit between queries

    logger.info(f"✅ Wikidata Extended: {len(all_creators)} additional creators found")
    return all_creators


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------


async def upsert_creator(
    creator: DiscoveredCreator, dry_run: bool = False
) -> Tuple[Optional[str], bool]:
    """
    Insert creator only if not already present.

    Returns (creator_uuid, is_new):
      - is_new=True  → row was just inserted
      - is_new=False → row already existed in DB (nothing written)
      - (None, False) → DB error
    """
    if dry_run:
        logger.info(
            f"  [dry-run] Would insert: {creator.channel_id} ({creator.channel_name})"
        )
        return f"dry-run-{creator.channel_id}", True

    if not db.supabase_client:
        logger.error("Supabase client not available")
        return None, False

    try:
        # Check existence first
        existing = (
            db.supabase_client.table(CREATOR_TABLE)
            .select("id")
            .eq("channel_id", creator.channel_id)
            .limit(1)
            .execute()
        )
        if existing.data:
            return existing.data[0]["id"], False  # Already in DB — skip

        payload = {
            "channel_id": creator.channel_id,
            "source": creator.source,
            "source_rank": creator.source_rank,
        }
        # Populate name/url if we have them — reduces work for the sync worker
        if creator.channel_name:
            payload["channel_name"] = creator.channel_name
        if creator.channel_url:
            payload["channel_url"] = creator.channel_url

        resp = db.supabase_client.table(CREATOR_TABLE).insert(payload).execute()

        if not resp.data:
            logger.error(f"DB insert returned no data for {creator.channel_id}")
            return None, False

        return resp.data[0]["id"], True

    except Exception as e:
        logger.error(f"DB error for {creator.channel_id}: {e}")
        return None, False


async def seed_creators(
    creators: List[DiscoveredCreator],
    stats: SeedStats,
    dry_run: bool = False,
) -> SeedStats:
    """
    Upsert each creator and queue a sync job. Updates stats in-place.

    Uses db.queue_creator_sync() — which deduplicates pending jobs —
    instead of raw inserts into creator_sync_jobs.
    """
    for creator in creators:
        creator_id, is_new = await upsert_creator(creator, dry_run=dry_run)

        if creator_id is None:
            stats.failed += 1
            continue

        if not is_new:
            # Already in DB — skip silently (no sync job, no log noise)
            stats.skipped_existing += 1
            logger.debug(
                f"  ⏭  {creator.channel_id} | {creator.channel_name or '?'} already in DB — skipped"
            )
            continue

        if creator_id.startswith("dry-run-"):
            stats.upserted += 1
            continue

        # Queue sync job via the canonical db function (includes dedup)
        queued = queue_creator_sync(creator_id, source=creator.source)
        if queued:
            stats.sync_jobs_queued += 1

        stats.upserted += 1
        logger.info(
            f"  ✅ {creator.channel_id} | {creator.channel_name or '?'} "
            f"| source={creator.source} rank={creator.source_rank}"
        )

    return stats


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args():
    import argparse

    parser = argparse.ArgumentParser(
        description="Seed YouTube creators into the ViralVibes database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "csv_path",
        nargs="?",
        help="Path to CSV file with creator names/handles/IDs",
    )
    parser.add_argument(
        "--quota-budget",
        type=int,
        default=2000,
        metavar="UNITS",
        help=(
            "Max YouTube API quota units to spend on name→ID resolution. "
            "Searches cost 100 units each; handle lookups cost 1. "
            "Default: 2000 (= 20 searches or 2000 handle lookups)."
        ),
    )
    parser.add_argument(
        "--no-wikipedia",
        action="store_true",
        help="Skip the Wikipedia most-subscribed source",
    )
    parser.add_argument(
        "--no-wikipedia-views",
        action="store_true",
        help="Skip the Wikipedia most-viewed source",
    )
    parser.add_argument(
        "--no-wikidata",
        action="store_true",
        help="Skip the Wikidata SPARQL source (~500 notable channels, no API cost)",
    )
    parser.add_argument(
        "--no-wikidata-extended",
        action="store_true",
        help=(
            "Skip the Wikidata Extended entity-type queries "
            "(YouTubers, musicians, athletes, YouTube channel entities, etc.). "
            "Adds ~3,000–8,000 channels at zero API cost but takes ~90 s "
            "due to Wikidata rate-limit sleeps."
        ),
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Skip the CSV source (run scraped sources only)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve channels and show what would be inserted, without writing to DB",
    )
    return parser.parse_args()


async def main():
    load_dotenv()
    setup_logging()
    args = _parse_args()

    # ── Supabase ────────────────────────────────────────────────────────────
    if not args.dry_run:
        logger.info("Initializing Supabase...")
        client = init_supabase()
        if not client:
            logger.error("❌ Failed to initialize Supabase — check env vars")
            sys.exit(1)
    else:
        logger.info("[dry-run] Skipping Supabase init")

    # ── YouTube resolver ────────────────────────────────────────────────────
    api_key = os.getenv("YOUTUBE_API_KEY_CREATORS") or os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        logger.error(
            "❌ No YouTube API key found (YOUTUBE_API_KEY_CREATORS or YOUTUBE_API_KEY)"
        )
        sys.exit(1)

    resolver = YouTubeResolver(api_key=api_key)
    validator = ChannelIDValidator()

    # ── Resolve CSV path ────────────────────────────────────────────────────
    csv_path = args.csv_path
    if csv_path is None:
        for candidate in ("youtubers.csv", "./youtubers.csv", "data/youtubers.csv"):
            if Path(candidate).exists():
                csv_path = candidate
                break

    # ── Collect creators from all sources ───────────────────────────────────
    all_creators: List[DiscoveredCreator] = []
    combined_stats = SeedStats()

    if csv_path and not args.no_csv:
        csv_creators, csv_stats = await resolve_csv(
            csv_path,
            resolver=resolver,
            validator=validator,
            quota_budget=args.quota_budget,
            dry_run=args.dry_run,
        )
        all_creators.extend(csv_creators)
        # Carry quota stats forward for final summary
        combined_stats.quota_units_used += csv_stats.quota_units_used
        combined_stats.quota_skipped += csv_stats.quota_skipped
        combined_stats.unresolvable += csv_stats.unresolvable
        combined_stats.skipped_names.extend(csv_stats.skipped_names)
    elif args.no_csv:
        logger.info("CSV source disabled via --no-csv")
    else:
        logger.info("No CSV path provided — skipping CSV source")

    if not args.no_wikipedia:
        # No API calls — always safe to run
        wiki_creators = await fetch_wikipedia(validator)
        all_creators.extend(wiki_creators)
    else:
        logger.info("Wikipedia (most-subscribed) disabled via --no-wikipedia")

    if not args.no_wikipedia_views:
        wiki_views_creators = await fetch_wikipedia_most_viewed(validator)
        all_creators.extend(wiki_views_creators)
    else:
        logger.info("Wikipedia (most-viewed) disabled via --no-wikipedia-views")

    if not args.no_wikidata:
        # Single SPARQL call — no YouTube quota spend, ~500 notable channels
        wikidata_creators = await fetch_wikidata(validator)
        all_creators.extend(wikidata_creators)
    else:
        logger.info("Wikidata SPARQL disabled via --no-wikidata")

    if not args.no_wikidata_extended:
        # One SPARQL call per entity type — no YouTube quota spend, ~3–8k channels.
        # Pass already-seen IDs so the extended queries skip duplicates up front.
        already_seen = {c.channel_id for c in all_creators}
        wikidata_ext_creators = await fetch_wikidata_extended(
            validator, already_seen=already_seen
        )
        all_creators.extend(wikidata_ext_creators)
    else:
        logger.info("Wikidata Extended disabled via --no-wikidata-extended")

    # Deduplicate across sources (CSV wins over Wikipedia for same channel_id)
    seen: Set[str] = set()
    unique_creators: List[DiscoveredCreator] = []
    for c in all_creators:
        if c.channel_id not in seen:
            seen.add(c.channel_id)
            unique_creators.append(c)

    logger.info(
        f"\nTotal unique creators to seed: {len(unique_creators)} "
        f"({len(all_creators) - len(unique_creators)} duplicates across sources removed)"
    )

    if not unique_creators:
        logger.error("❌ No creators resolved from any source")
        sys.exit(1)

    # ── Write to DB ─────────────────────────────────────────────────────────
    logger.info(
        f"{'[dry-run] ' if args.dry_run else ''}Seeding {len(unique_creators)} creators..."
    )
    combined_stats = await seed_creators(
        unique_creators, combined_stats, dry_run=args.dry_run
    )

    # ── Summary ─────────────────────────────────────────────────────────────
    logger.info("\n" + combined_stats.summary())


if __name__ == "__main__":
    asyncio.run(main())
