"""
Seed creator discovery from Wikipedia's "Most-subscribed YouTube channels" list.

Updated version with new schema support:
- source_rank: Position in source ranking (e.g., Wikipedia rank)
- country_code: Creator's country (auto-extracted from YouTube API)
- discovered_at: When creator was discovered (automatic)

Integrates improved code patterns:
- Regex-based channel ID extraction (fast, reliable)
- YouTubeResolver for unified API operations
- ChannelIDValidator for format validation
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import db
from constants import CREATOR_SYNC_JOBS_TABLE, CREATOR_TABLE
from db import init_supabase, setup_logging
from services.channel_utils import ChannelIDValidator, YouTubeResolver

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_most-subscribed_YouTube_channels"
logger = logging.getLogger(__name__)


def extract_youtube_urls(html: str) -> List[str]:
    """Extract YouTube channel URLs from Wikipedia HTML."""
    soup = BeautifulSoup(html, "html.parser")
    urls = set()

    # Target the main content tables (wikitable class contains the subscription data)
    tables = soup.find_all("table", class_="wikitable")
    if not tables:
        logger.warning("No wikitable found - Wikipedia structure may have changed")
        search_area = soup
    else:
        # Create a new soup containing only the tables
        search_area = BeautifulSoup(str(tables), "html.parser")
        logger.debug(f"Found {len(tables)} wikitables to search")

    # Find YouTube links within the targeted area
    for link in search_area.find_all("a", href=True):
        href = link["href"]

        # Match various YouTube URL formats by checking the hostname
        try:
            from urllib.parse import urlparse

            parsed = urlparse(
                href
                if href.startswith("http") or href.startswith("//")
                else f"https://{href.lstrip('/')}"
            )
            host = parsed.hostname.lower() if parsed.hostname else None
            if host and (host == "youtube.com" or host.endswith(".youtube.com")):
                # Handle relative Wikipedia links
                if href.startswith("/wiki/"):
                    continue

                # Extract full YouTube URLs
                if href.startswith("http"):
                    urls.add(href)
                elif href.startswith("//"):
                    urls.add("https:" + href)
        except Exception as e:
            logger.debug(f"Error parsing URL {href}: {e}")
            continue

    return list(urls)


async def add_creator_with_source(
    channel_id: str,
    source: str = "wikipedia",
    source_rank: Optional[int] = None,
) -> Optional[str]:
    """
    Add creator to database with source information.

    Args:
        channel_id: YouTube channel ID (UCxxxxxx)
        source: Where creator came from (wikipedia, playlist, api, manual)
        source_rank: Position in source ranking (e.g., rank on Wikipedia)

    Returns:
        Creator ID if successful, None otherwise
    """
    if not db.supabase_client:
        logger.error("Supabase client not available")
        return None

    try:
        # Check if already exists
        existing = (
            db.supabase_client.table(CREATOR_TABLE)
            .select("id")
            .eq("channel_id", channel_id)
            .limit(1)
            .execute()
        )

        if existing.data:
            logger.debug(f"Creator already exists: {channel_id}")
            return existing.data[0]["id"]

        # Insert with source info + discovered_at (auto-set to now)
        insert_data = {
            "channel_id": channel_id,
            "source": source,
        }

        if source_rank is not None:
            insert_data["source_rank"] = source_rank

        # discovered_at gets set to NOW() by database default

        response = db.supabase_client.table(CREATOR_TABLE).insert(insert_data).execute()

        if response.data:
            creator_id = response.data[0]["id"]
            logger.info(
                f"Created creator: {channel_id} (source={source}, rank={source_rank})"
            )
            return creator_id

        logger.error(f"Insert returned no data for {channel_id}")
        return None

    except Exception as e:
        logger.error(f"Error creating creator {channel_id}: {e}")
        return None


async def enqueue_creator_sync(creator_id: str) -> bool:
    """Enqueue a creator sync job."""
    if not db.supabase_client:
        logger.error("Supabase client not available")
        return False

    try:
        result = (
            db.supabase_client.table(CREATOR_SYNC_JOBS_TABLE)
            .insert(
                {
                    "creator_id": creator_id,
                    "job_type": "refresh_stats",
                    "status": "pending",
                }
            )
            .execute()
        )
        return bool(result.data)
    except Exception as e:
        logger.error(f"Error enqueueing sync job for creator {creator_id}: {e}")
        return False


async def process_creators(
    urls: List[str],
    validator: ChannelIDValidator,
    resolver: YouTubeResolver,
) -> dict:
    """
    Process discovered URLs and add creators to database.

    Args:
        urls: List of YouTube URLs from Wikipedia
        validator: ChannelIDValidator instance
        resolver: YouTubeResolver instance

    Returns:
        Stats dict with counts (enqueued, skipped, failed)
    """
    stats = {"enqueued": 0, "skipped": 0, "failed": 0, "duplicate": 0}
    seen = set()

    for position, url in enumerate(sorted(urls), 1):
        # Strategy 1: Extract directly using regex (fast, no API)
        channel_id = validator.extract_from_url(url)

        # Strategy 2: Resolve handle if regex didn't find it
        if not channel_id and ("/@" in url or "/user/" in url or "/c/" in url):
            logger.debug(f"Attempting handle resolution for: {url}")
            channel_id = await resolver.resolve_handle_to_channel_id(url)

        if not channel_id:
            stats["skipped"] += 1
            logger.debug(f"Could not extract channel ID from: {url}")
            continue

        # Check for duplicates within this batch
        if channel_id in seen:
            stats["duplicate"] += 1
            logger.debug(f"Duplicate: {channel_id}")
            continue

        seen.add(channel_id)

        # Add creator with source ranking
        creator_id = await add_creator_with_source(
            channel_id=channel_id,
            source="wikipedia",
            source_rank=position,
        )

        if not creator_id:
            stats["failed"] += 1
            continue

        # Enqueue sync job
        if await enqueue_creator_sync(creator_id):
            stats["enqueued"] += 1
            logger.info(f"✅ Enqueued: {channel_id} (rank #{position})")
        else:
            stats["failed"] += 1

    return stats


async def main():
    """Main seeding workflow."""
    load_dotenv()
    setup_logging()

    logger.info("Initializing Supabase...")
    client = init_supabase()
    if not client:
        logger.error("❌ Failed to initialize Supabase client")
        logger.error(
            "Make sure NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY are set"
        )
        sys.exit(1)

    # Initialize YouTube resolver
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        logger.warning("⚠️  YOUTUBE_API_KEY not set - handle resolution will be skipped")

    resolver = YouTubeResolver(api_key=api_key)
    validator = ChannelIDValidator()

    logger.info("Fetching Wikipedia page...")

    # Add User-Agent header to avoid being blocked
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        response = requests.get(WIKI_URL, timeout=15, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch page: {e}")
        sys.exit(1)

    logger.info(f"Downloaded page: {len(response.text)} characters")

    urls = extract_youtube_urls(response.text)
    logger.info(f"Found {len(urls)} YouTube URLs")

    if not urls:
        logger.error("No URLs found - Wikipedia structure may have changed")
        sys.exit(1)

    # Process creators
    logger.info("Processing creators...")
    stats = await process_creators(urls, validator, resolver)

    # Report results
    total = sum(stats.values())
    logger.info("\n" + "=" * 70)
    logger.info("SEEDING COMPLETE")
    logger.info("=" * 70)
    logger.info(f"✅ Enqueued:   {stats['enqueued']}")
    logger.info(f"⭐️  Skipped:   {stats['skipped']}")
    logger.info(f"🔄 Duplicate: {stats['duplicate']}")
    logger.info(f"❌ Failed:    {stats['failed']}")
    logger.info(f"📊 Total:     {total}")
    if total > 0:
        success_rate = (stats["enqueued"] / total) * 100
        logger.info(f"📈 Success:   {success_rate:.1f}%")
    logger.info("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
