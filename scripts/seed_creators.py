import logging
import os
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


def build_youtube_client():
    """Build YouTube API client for channel validation."""
    try:
        from googleapiclient.discovery import build

        api_key = os.getenv("YOUTUBE_API_KEY")
        if not api_key:
            logger.warning("YOUTUBE_API_KEY not set - skipping validation")
            return None

        return build("youtube", "v3", developerKey=api_key)
    except Exception as e:
        logger.warning(f"Failed to build YouTube client: {e}")
        return None


def validate_channel_id(channel_id: str, youtube=None) -> bool:
    """
    Validate that a channel ID exists on YouTube.

    Args:
        channel_id: Raw YouTube channel ID (UCxxxxxx)
        youtube: Optional YouTube API client

    Returns:
        True if valid, False if invalid or cannot be verified
    """
    if not youtube:
        # Without API client, do basic format validation
        return channel_id.startswith("UC") and len(channel_id) == 24

    try:
        request = youtube.channels().list(part="snippet", id=channel_id)
        response = request.execute()
        return bool(response.get("items"))
    except Exception as e:
        logger.debug(f"Failed to validate {channel_id}: {e}")
        return False


def extract_youtube_urls(html: str) -> List[str]:
    """
    Extract YouTube channel URLs from Wikipedia HTML.
    Targets the specific "Most-subscribed channels" table to avoid noise.
    """
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


def resolve_channel_id_from_handle(handle: str, youtube=None) -> str | None:
    """
    Resolve YouTube handle (@username, /user/, /c/) to raw channel ID.

    Uses YouTube API to convert handles to proper channel IDs.

    Args:
        handle: YouTube handle or username (with or without @)
        youtube: YouTube API client (required for resolution)

    Returns:
        Raw channel ID (UCxxxxxx) or None if resolution fails
    """
    if not youtube:
        logger.debug(f"YouTube API client required for handle resolution: {handle}")
        return None

    try:
        # Clean up handle format
        username = handle.lstrip("/@").split("?")[0].split("#")[0].strip()

        if not username:
            return None

        logger.debug(f"Resolving handle: {handle} -> username: {username}")

        # Try forUsername first (most common)
        try:
            request = youtube.channels().list(part="id,snippet", forUsername=username)
            response = request.execute()

            if response.get("items"):
                channel_id = response["items"][0]["id"]
                channel_name = response["items"][0]["snippet"]["title"]
                logger.info(
                    f"✅ Resolved handle '{handle}' to {channel_id} ({channel_name})"
                )
                return channel_id
        except Exception as e:
            logger.debug(f"forUsername failed for {username}: {e}")

        # Try forCustomUrl as fallback (for @handle format)
        try:
            request = youtube.channels().list(
                part="id,snippet", forCustomUrl="@" + username
            )
            response = request.execute()

            if response.get("items"):
                channel_id = response["items"][0]["id"]
                channel_name = response["items"][0]["snippet"]["title"]
                logger.info(
                    f"✅ Resolved custom URL '@{username}' to {channel_id} ({channel_name})"
                )
                return channel_id
        except Exception as e:
            logger.debug(f"forCustomUrl failed for @{username}: {e}")

        logger.warning(f"Could not resolve handle: {handle}")
        return None

    except Exception as e:
        logger.error(f"Error resolving handle {handle}: {e}")
        return None


def extract_raw_channel_id(url: str, youtube=None) -> str | None:
    """
    Extract raw YouTube channel IDs from various URL formats.

    Supports:
    - Direct IDs: /channel/UCxxxxxx (no API needed)
    - Handles: /@username (requires YouTube API)
    - User URLs: /user/username (requires YouTube API)
    - Custom URLs: /c/customname (requires YouTube API)

    Args:
        url: YouTube channel URL
        youtube: Optional YouTube API client for handle resolution

    Returns:
        Raw channel ID (UCxxxxxx) or None if not extractable
    """
    try:
        parsed = urlparse(url)
        path = parsed.path

        # ========== Format 1: Direct channel ID ==========
        if "/channel/" in path:
            parts = path.split("/channel/")
            if len(parts) > 1:
                channel_id = parts[1].strip("/").split("?")[0].split("#")[0]

                # Validate format: UC + 22 characters
                if channel_id.startswith("UC") and len(channel_id) == 24:
                    logger.debug(f"Extracted direct channel ID: {channel_id}")
                    return channel_id
                else:
                    logger.debug(f"Invalid channel ID format: {channel_id}")
                    return None

        # ========== Format 2: @handle (/@username) ==========
        if "/@" in path:
            if youtube:
                handle = path.split("/@")[1].split("?")[0].split("#")[0].strip("/")
                return resolve_channel_id_from_handle("@" + handle, youtube)
            else:
                logger.debug(f"Skipping @handle URL (YouTube API not available): {url}")
                return None

        # ========== Format 3: /user/ URL ==========
        if "/user/" in path:
            if youtube:
                username = (
                    path.split("/user/")[1].split("?")[0].split("#")[0].strip("/")
                )
                return resolve_channel_id_from_handle(username, youtube)
            else:
                logger.debug(f"Skipping /user/ URL (YouTube API not available): {url}")
                return None

        # ========== Format 4: /c/ URL ==========
        if "/c/" in path:
            if youtube:
                customname = path.split("/c/")[1].split("?")[0].split("#")[0].strip("/")
                return resolve_channel_id_from_handle(customname, youtube)
            else:
                logger.debug(f"Skipping /c/ URL (YouTube API not available): {url}")
                return None

        logger.warning(f"Unrecognized URL format: {url}")
        return None

    except Exception as e:
        logger.warning(f"Error parsing URL {url}: {e}")
        return None


def get_or_create_creator(channel_id: str) -> str | None:
    """
    Get existing creator or create minimal creator record.

    ⚠️ CRITICAL: Store ONLY raw channel IDs (UCxxxxxx format)
    Worker and collation logic depend on this consistency.

    Args:
        channel_id: Raw YouTube channel ID (UCxxxxxx)

    Returns:
        Creator UUID if successful, None on error
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

        # Create minimal record with ONLY channel_id
        # Worker will fetch real metadata (name, URL, stats) via YouTube API
        insert_response = (
            db.supabase_client.table(CREATOR_TABLE)
            .insert(
                {
                    "channel_id": channel_id,
                    # Leave NULL: channel_name, channel_url, etc.
                    # Worker fills these in via YouTube API
                }
            )
            .execute()
        )

        if insert_response.data:
            creator_id = insert_response.data[0]["id"]
            logger.info(f"Created minimal creator: {channel_id} → {creator_id}")
            return creator_id

        logger.error(f"Failed to create creator {channel_id}: insert returned no data")
        return None

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

    # Build YouTube client for validation AND handle resolution
    youtube = build_youtube_client()

    if not youtube:
        logger.warning("⚠️  YouTube API not available - will skip handle resolution")
        logger.warning("   Only direct /channel/UCxxxxx URLs will be processed")

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

    # Extract raw channel IDs (with handle resolution if API available)
    seen = set()
    enqueued = 0
    skipped = 0
    failed = 0

    for url in sorted(urls):
        # Pass youtube client to enable handle resolution
        channel_id = extract_raw_channel_id(url, youtube)

        if not channel_id:
            skipped += 1
            continue

        if channel_id in seen:
            logger.debug(f"Skipping duplicate: {channel_id}")
            continue

        seen.add(channel_id)

        # Optional: Validate against YouTube API
        if youtube and not validate_channel_id(channel_id, youtube):
            logger.warning(f"Channel validation failed: {channel_id}")
            # Still try to add it - may have different API quota issues

        # Step 1: Get or create creator
        creator_id = get_or_create_creator(channel_id)
        if not creator_id:
            logger.warning(f"Failed to get/create creator: {channel_id}")
            failed += 1
            continue

        # Step 2: Enqueue sync job
        if enqueue_creator_sync(creator_id):
            logger.info(f"✅ Enqueued: {channel_id}")
            enqueued += 1
        else:
            logger.warning(f"Failed to enqueue: {channel_id}")
            failed += 1

    logger.info(f"\n✅ Successfully enqueued {enqueued} creators")
    logger.info(f"⭐️  Skipped {skipped} non-resolvable URLs (YouTube API unavailable)")
    if failed > 0:
        logger.warning(f"❌ Failed to process {failed} creators")

    # Summary
    total_processed = enqueued + skipped + failed
    success_rate = (enqueued / total_processed * 100) if total_processed > 0 else 0
    logger.info(
        f"\n📊 Success rate: {success_rate:.1f}% ({enqueued}/{total_processed})"
    )


if __name__ == "__main__":
    main()
