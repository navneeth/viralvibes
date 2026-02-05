import logging
import re
import sys
from pathlib import Path
from typing import List
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import db  # Import module to access the global supabase_client after init
from constants import CREATOR_SYNC_JOBS_TABLE, CREATOR_TABLE
from db import init_supabase, setup_logging

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_most-subscribed_YouTube_channels"

# Get logger instance
logger = logging.getLogger(__name__)


def extract_youtube_urls(html: str) -> List[str]:
    """
    Extract YouTube channel URLs from Wikipedia HTML.
    Targets the specific "Most-subscribed channels" table to avoid noise.

    The Wikipedia page structure typically has tables with class 'wikitable'
    containing the channel data. We target those specifically rather than
    scanning the entire page.
    """
    soup = BeautifulSoup(html, "html.parser")

    urls = set()

    # Target the main content tables (wikitable class contains the subscription data)
    # This avoids footer links, navigation, citations, etc.
    tables = soup.find_all("table", class_="wikitable")

    if not tables:
        logger.warning("No wikitable found - Wikipedia structure may have changed")
        # Fallback to scanning entire page if table structure changed
        search_area = soup
    else:
        # Create a new soup containing only the tables
        search_area = BeautifulSoup(str(tables), "html.parser")
        logger.debug(f"Found {len(tables)} wikitables to search")

    # Find YouTube links within the targeted area
    for link in search_area.find_all("a", href=True):
        href = link["href"]

        # Match various YouTube URL formats by checking the hostname
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

    return list(urls)


def normalize_channel_id(url: str) -> str | None:
    """
    Extract channel_id or handle from YouTube URL.
    Handles multiple formats: @handle, /channel/, /user/, /c/
    """
    try:
        parsed = urlparse(url)
        path = parsed.path

        # Remove query parameters
        if "?" in url:
            url = url.split("?")[0]

        # Modern @handle format
        if "/@" in url:
            match = re.search(r"/@([^/?]+)", url)
            if match:
                return "@" + match.group(1)

        # Channel ID format
        if "/channel/" in path:
            parts = path.split("/channel/")
            if len(parts) > 1:
                channel_id = parts[1].strip("/")
                if channel_id:
                    return "channel:" + channel_id

        # User format
        if "/user/" in path:
            parts = path.split("/user/")
            if len(parts) > 1:
                user = parts[1].strip("/")
                if user:
                    return "user:" + user

        # /c/ format
        if "/c/" in path:
            parts = path.split("/c/")
            if len(parts) > 1:
                channel = parts[1].strip("/")
                if channel:
                    return "c:" + channel

        return None
    except Exception as e:
        logger.warning(f"Error parsing URL {url}: {e}")
        return None


def extract_raw_channel_id(url: str) -> str | None:
    """
    Extract ONLY the raw YouTube channel ID (UCxxxxxx format).

    Handles:
    - /channel/UCxxxxxx → UCxxxxxx
    - /@handle → Resolve to UCxxxxxx via API (optional)
    - /user/name → Skip (needs API resolution)
    - /c/name → Skip (needs API resolution)

    Returns raw channel ID or None if not directly extractable.
    """
    try:
        parsed = urlparse(url)
        path = parsed.path

        # Only handle direct channel ID format
        if "/channel/" in path:
            parts = path.split("/channel/")
            if len(parts) > 1:
                channel_id = parts[1].strip("/").split("?")[0]
                # Validate format (UC followed by 22 chars)
                if channel_id.startswith("UC") and len(channel_id) == 24:
                    return channel_id

        # Log other formats for manual review
        if "/@" in path or "/user/" in path or "/c/" in path:
            logger.info(f"Skipping non-direct channel URL (needs resolution): {url}")

        return None
    except Exception as e:
        logger.warning(f"Error parsing URL {url}: {e}")
        return None


def get_or_create_creator(channel_id: str) -> str | None:
    """
    Create minimal creator record with just channel_id.
    Worker will populate full metadata via YouTube API.
    """
    if not db.supabase_client:
        logger.error("Supabase client not available")
        return None

    try:
        # First, try to find existing creator by channel_id
        response = (
            db.supabase_client.table(CREATOR_TABLE)
            .select("id")
            .eq("channel_id", channel_id)
            .limit(1)
            .execute()
        )

        if response.data:
            creator_id = response.data[0]["id"]
            logger.debug(f"Creator already exists: {channel_id} → {creator_id}")
            return creator_id

        # Create minimal record (worker will enrich)
        insert_response = (
            db.supabase_client.table(CREATOR_TABLE)
            .insert(
                {
                    "channel_id": channel_id,
                    # Don't set channel_name - let worker fetch real name
                    # Don't set channel_url - let worker construct it
                }
            )
            .execute()
        )

        if insert_response.data:
            creator_id = insert_response.data[0]["id"]
            logger.info(f"Created minimal creator record: {channel_id} → {creator_id}")
            return creator_id

    except Exception as e:
        logger.error(f"Error creating creator {channel_id}: {e}")
        return None


def enqueue_creator_sync(creator_id: str) -> bool:
    """Enqueue a creator sync job in Supabase"""
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


def main():
    # Load environment variables
    load_dotenv()

    # Setup logging
    setup_logging()

    logger.info("Initializing Supabase...")
    client = init_supabase()

    if not client:
        logger.error("❌ Failed to initialize Supabase client")
        logger.error(
            "Make sure NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY are set"
        )
        sys.exit(1)

    logger.info("Fetching Wikipedia page...")

    # Add User-Agent header to avoid being blocked
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        response = requests.get(WIKI_URL, timeout=15, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Error fetching page: {e}")
        sys.exit(1)

    html = response.text
    logger.info(f"Page size: {len(html)} characters")

    urls = extract_youtube_urls(html)
    logger.info(f"Found {len(urls)} YouTube URLs")

    if len(urls) == 0:
        logger.warning("No URLs found. This may indicate:")
        logger.warning("  - Wikipedia page structure changed")
        logger.warning("  - No YouTube links in the article")
        logger.warning("  - Network/access issue")
        sys.exit(1)

    seen = set()
    enqueued = 0
    failed = 0

    for url in sorted(urls):
        channel_id = normalize_channel_id(url)
        if not channel_id or channel_id in seen:
            continue

        seen.add(channel_id)

        # Step 1: Get or create creator
        creator_id = get_or_create_creator(channel_id)
        if not creator_id:
            logger.warning(f"Failed to get/create creator for: {channel_id}")
            failed += 1
            continue

        # Step 2: Enqueue sync job
        if enqueue_creator_sync(creator_id):
            logger.info(f"✓ Enqueued: {channel_id} (creator_id: {creator_id})")
            enqueued += 1
        else:
            logger.warning(f"Failed to enqueue sync job for: {channel_id}")
            failed += 1

    logger.info(f"\n✅ Successfully enqueued {enqueued} creators")
    if failed > 0:
        logger.warning(f"⚠️  Failed to enqueue {failed} creators")


if __name__ == "__main__":
    main()
