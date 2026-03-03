"""
Seed creator discovery from multiple sources.

Supports:
- CSV: Local list of channels (youtubers.csv or specified path)
- Wikipedia (most-subscribed): Top 200 by subscriber count (random sampling available)
- Wikipedia (most-viewed): Top 100 by total view count (random sampling available)
- Wikidata SPARQL: ~500+ notable channels with Wikipedia articles,
                   sourced via the P2397 YouTube channel ID property (random sampling available)

Random Sampling Strategy
-------------------------
Wikipedia and Wikidata sources support random sampling (--sample-size) to enable
diverse creator discovery across multiple runs:

  - Without sampling: Same top channels returned every run (useful for first run)
  - With sampling (default 50): Random 50 channels per run, different each time
  - Multiple runs gradually cover the entire dataset without quota waste

This allows running the script daily to discover new creators without hitting
the same channels repeatedly.

Quota strategy
--------------
YouTube Data API v3 costs vary significantly by operation:
  - channels.list by UC ID    →   1 unit  (free lookup)
  - channels.list by @handle  →   1 unit  (cheap)
  - search.list by name       → 100 units (expensive — avoid where possible)

Wikipedia and Wikidata sources require NO YouTube API calls at all.
They return UC IDs directly, so seeding from them is completely free.

This script minimises quota usage by:
  1. Processing free sources FIRST (Wikipedia, Wikidata)
  2. Then resolving CSV in priority order:
     a. UC IDs detected directly in CSV   → 0 API calls
     b. @handles detected in CSV          → 1 unit each (channels.list)
     c. Plain names                        → 100 units each (search.list)

For plain-name rows the script stops resolving once the remaining
quota budget (--quota-budget, default 5000 units) would be exceeded,
and logs which channels were skipped so you can add their @handles
to the CSV instead.

Usage
-----
  # Default: random 50 from each source, 5000 quota budget
  python scripts/seed_creators.py scripts/youtubers.csv

  # Custom sample size for diverse discovery
  python scripts/seed_creators.py --sample-size 30

  # No sampling (get all top channels - useful for first run)
  python scripts/seed_creators.py --sample-size 0

  # Higher quota for more CSV names
  python scripts/seed_creators.py scripts/youtubers.csv --quota-budget 10000

  # Dry run to see what would be discovered
  python scripts/seed_creators.py scripts/youtubers.csv --dry-run

  # CSV only (skip scraped sources)
  python scripts/seed_creators.py scripts/youtubers.csv --no-wikipedia --no-wikidata

  # Scraped sources only (skip CSV)
  python scripts/seed_creators.py --no-csv

  # Daily discovery run (recommended)
  python scripts/seed_creators.py --sample-size 50 --quota-budget 5000
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


async def fetch_wikipedia(
    validator: ChannelIDValidator, sample_size: Optional[int] = None
) -> List[DiscoveredCreator]:
    """
    Fetch top YouTube channels from Wikipedia's most-subscribed list.

    Extracts UC channel IDs directly from href attributes — no API calls needed.
    Note: Wikipedia has been migrating channel links to @handle format; the
    UC-ID regex will only match rows that still use /channel/UCxxx links.
    Rows with handle-only links are not resolved here to avoid quota spend.

    Args:
        sample_size: Number of random channels to return. If None, returns all.
                     Enables diverse discovery across multiple runs.
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

    # Apply random sampling if requested
    if sample_size and sample_size < len(creators):
        original_count = len(creators)
        creators = random.sample(creators, sample_size)
        logger.info(
            f"✅ Wikipedia (most-subscribed): {len(creators)} random channels "
            f"selected from {original_count} total (enables diverse discovery)"
        )
    else:
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
    validator: ChannelIDValidator, sample_size: Optional[int] = None
) -> List[DiscoveredCreator]:
    """
    Fetch top 50 YouTube channels from Wikipedia's most-viewed list.

    URL: https://en.wikipedia.org/wiki/List_of_most-viewed_YouTube_channels
    Structure: single wikitable with UC IDs in href attributes — identical
    parsing approach to fetch_wikipedia (most-subscribed).

    No YouTube API calls required — UC IDs extracted directly from links.
    Complements the subscriber-ranked list with view-count-ranked channels
    (e.g. children's content and music labels rank higher here).

    Args:
        sample_size: Number of random channels to return. If None, returns all.
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

    # Apply random sampling if requested
    if sample_size and sample_size < len(creators):
        original_count = len(creators)
        creators = random.sample(creators, sample_size)
        logger.info(
            f"✅ Wikipedia (most-viewed): {len(creators)} random channels "
            f"selected from {original_count} total"
        )
    else:
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


async def fetch_wikidata(
    validator: ChannelIDValidator, sample_size: Optional[int] = None
) -> List[DiscoveredCreator]:
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

    Args:
        sample_size: Number of random channels to return. If None, returns all.
                     Random sampling enables discovering different channels each run.
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

    # Apply random sampling if requested (after full fetch to preserve subscriber rank)
    if sample_size and sample_size < len(creators):
        original_count = len(creators)
        # Random sample but preserve some high-ranked channels
        # Take top 20% guaranteed + random sample from rest
        guaranteed = min(sample_size // 5, len(creators))
        remaining_sample = sample_size - guaranteed

        if remaining_sample > 0 and len(creators) > guaranteed:
            guaranteed_creators = creators[:guaranteed]
            remaining_creators = creators[guaranteed:]
            sampled_remaining = random.sample(
                remaining_creators, min(remaining_sample, len(remaining_creators))
            )
            creators = guaranteed_creators + sampled_remaining
        else:
            creators = creators[:sample_size]

        logger.info(
            f"✅ Wikidata: {len(creators)} channels selected from {original_count} total "
            f"({guaranteed} top-ranked + {len(creators) - guaranteed} random)"
        )
    else:
        logger.info(
            f"✅ Wikidata: {len(creators)} creators found "
            f"({skipped_invalid} non-UC entries skipped)"
        )

    if len(creators) < 50 and not sample_size:
        logger.warning(
            "  Fewer results than expected from Wikidata. "
            "The SPARQL endpoint may be rate-limiting or the query timed out."
        )
    return creators


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
        default=5000,
        metavar="UNITS",
        help=(
            "Max YouTube API quota units to spend on name→ID resolution. "
            "Searches cost 100 units each; handle lookups cost 1. "
            "Default: 5000 (= 50 searches or 5000 handle lookups). "
            "Daily quota limit is typically 10,000 units."
        ),
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=50,
        metavar="N",
        help=(
            "Random sample size from Wikipedia/Wikidata sources. "
            "Enables diverse discovery across multiple runs. "
            "Set to 0 for all channels (original behavior). "
            "Default: 50"
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

    # Process FREE sources first (no quota cost) to maximize discovery
    # Random sampling enables different channels each run
    sample_size = args.sample_size if args.sample_size > 0 else None

    if not args.no_wikipedia:
        # No API calls — always safe to run
        wiki_creators = await fetch_wikipedia(validator, sample_size=sample_size)
        all_creators.extend(wiki_creators)
    else:
        logger.info("Wikipedia (most-subscribed) disabled via --no-wikipedia")

    if not args.no_wikipedia_views:
        wiki_views_creators = await fetch_wikipedia_most_viewed(
            validator, sample_size=sample_size
        )
        all_creators.extend(wiki_views_creators)
    else:
        logger.info("Wikipedia (most-viewed) disabled via --no-wikipedia-views")

    if not args.no_wikidata:
        # Single SPARQL call — no YouTube quota spend, ~500 notable channels
        wikidata_creators = await fetch_wikidata(validator, sample_size=sample_size)
        all_creators.extend(wikidata_creators)
    else:
        logger.info("Wikidata SPARQL disabled via --no-wikidata")

    # Process CSV LAST so free sources don't waste quota on duplicates
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
