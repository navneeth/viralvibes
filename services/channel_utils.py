"""
YouTube API utilities and channel management service.
Integrates:
- Regex-based channel ID extraction/validation
- Handle → channel ID resolution
- Channel data normalization (YouTube API → internal schema)
- Verified status detection

This module is the single source of truth for YouTube API interactions
and channel ID validation across the codebase.

This version extracts all available data from YouTube API that's useful
for creator analytics:

From YouTube API channels.list response:
├─ snippet: title, description, customUrl, publishedAt, defaultLanguage, thumbnails
├─ statistics: subscribers, views, videos, commentCount, hiddenSubscriberCount
└─ brandingSettings: keywords, featuredChannels, country, bannerImage

Available data NOT currently stored (for future features):
├─ bannerImageUrl (for profile header)
├─ keywords (creator-defined topics)
├─ featuredChannelsUrls (related channels)
├─ defaultLanguage (content language)
├─ commentCount (on channel)
└─ hiddenSubscriberCount (for approximate math)
"""

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class ChannelIDValidator:
    """
    Regex-based YouTube channel ID extraction and validation.

    Single source of truth for channel ID pattern.
    Works offline - no API calls required.
    """

    # YouTube channel IDs are: UC followed by 22 alphanumeric/underscore/hyphen chars
    CHANNEL_ID_RE = re.compile(r"(UC[a-zA-Z0-9_-]{22})")

    @staticmethod
    def extract_from_url(url: str) -> Optional[str]:
        """
        Extract UC channel ID from a YouTube URL if present.

        Args:
            url: Any string potentially containing a YouTube URL or channel ID

        Returns:
            Channel ID if found (UCxxxxxx), None otherwise
        """
        if not url:
            return None

        match = ChannelIDValidator.CHANNEL_ID_RE.search(url)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def extract_from_text(text: str) -> set[str]:
        """
        Extract all UC channel IDs from unstructured text.

        Args:
            text: Any text containing channel IDs

        Returns:
            Set of unique channel IDs found
        """
        if not text:
            return set()
        return set(ChannelIDValidator.CHANNEL_ID_RE.findall(text))

    @staticmethod
    def is_valid(channel_id: str) -> bool:
        """
        Validate that a string is a properly formatted channel ID.

        Args:
            channel_id: String to validate

        Returns:
            True if valid channel ID format, False otherwise
        """
        if not channel_id:
            return False
        match = ChannelIDValidator.CHANNEL_ID_RE.fullmatch(channel_id)
        return match is not None


class YouTubeResolver:
    """
    YouTube API client for comprehensive channel data resolution.

    Provides unified interface for:
    - Resolving handles (@username, /user/, /c/) to channel IDs
    - Fetching comprehensive channel data from YouTube API
    - Normalizing API responses to internal schema
    - Extracting country, language, keywords, and other metadata

    Handles both sync and async operations gracefully.
    """

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize resolver with optional API key.

        Args:
            api_key: YouTube API key. If None, methods requiring API will raise.
        """
        self.api_key = api_key
        self._youtube_client = None
        self._validator = ChannelIDValidator()

    def _get_youtube_client(self):
        """Lazy load YouTube API client."""
        if not self._youtube_client:
            if not self.api_key:
                raise ValueError("YOUTUBE_API_KEY not configured")
            self._youtube_client = build("youtube", "v3", developerKey=self.api_key)
        return self._youtube_client

    def validate_channel_id(self, channel_id: str) -> bool:
        """
        Fast format validation using regex (no API call).

        Args:
            channel_id: Channel ID to validate

        Returns:
            True if properly formatted, False otherwise
        """
        return self._validator.is_valid(channel_id)

    async def resolve_handle_to_channel_id(self, handle: str) -> Optional[str]:
        """
        Resolve @handle, /user/, /c/ to actual channel ID.

        Tries multiple YouTube API methods:
        1. forUsername (for old-style usernames, fastest)
        2. Search API (for @-handles and custom URLs, more flexible)

        Args:
            handle: YouTube handle (@username, username, or custom URL)

        Returns:
            Channel ID (UCxxxxxx) if found, None otherwise
        """
        if not handle:
            return None

        # Normalize handle format
        handle = handle.lstrip("/@").split("?")[0].split("#")[0].strip()
        if not handle:
            return None

        youtube = self._get_youtube_client()

        logger.debug(f"[YouTubeResolver] Resolving handle: {handle}")

        # Strategy 1: Try as old-style username with forUsername
        # This works for legacy usernames but NOT for @-handles
        try:
            request = youtube.channels().list(
                part="id,snippet",
                forUsername=handle,
            )
            response = await self._execute_async(request)

            if response.get("items"):
                channel_id = response["items"][0]["id"]
                channel_name = response["items"][0]["snippet"]["title"]
                logger.info(
                    f"[YouTubeResolver] ✅ Resolved {handle} → {channel_id} ({channel_name})"
                )
                return channel_id
        except HttpError as e:
            logger.debug(f"[YouTubeResolver] forUsername failed: {e}")

        # Strategy 2: Use search API for @-handles and custom URLs
        # More flexible than forUsername, works with @-handles
        try:
            request = youtube.search().list(
                part="snippet",
                q=handle,
                type="channel",
                maxResults=1,
            )
            response = await self._execute_async(request)

            if response.get("items"):
                channel_id = response["items"][0]["snippet"]["channelId"]
                channel_name = response["items"][0]["snippet"]["title"]
                logger.info(
                    f"[YouTubeResolver] ✅ Resolved @{handle} → {channel_id} ({channel_name})"
                )
                return channel_id
        except HttpError as e:
            logger.debug(f"[YouTubeResolver] Search API failed: {e}")

        logger.warning(f"[YouTubeResolver] ❌ Could not resolve: {handle}")
        return None

    async def get_channel_data(self, channel_id: str) -> Optional[dict]:
        """
        Fetch and normalize comprehensive channel data from YouTube API.

        Args:
            channel_id: YouTube channel ID (UCxxxxxx)

        Returns:
            Normalized channel data dict, or None if not found
        """
        if not self._validator.is_valid(channel_id):
            logger.warning(f"[YouTubeResolver] Invalid channel ID format: {channel_id}")
            return None

        youtube = self._get_youtube_client()

        try:
            request = youtube.channels().list(
                part="id,snippet,statistics,brandingSettings,topicDetails",
                id=channel_id,
            )
            response = await self._execute_async(request)

            if not response.get("items"):
                logger.error(f"[YouTubeResolver] Channel not found: {channel_id}")
                return None

            channel_item = response["items"][0]
            return self.normalize_channel(channel_item)

        except HttpError as e:
            logger.error(f"[YouTubeResolver] Failed to fetch {channel_id}: {e}")
            return None

    @staticmethod
    def normalize_channel(item: dict) -> dict:
        """
        Normalize YouTube API channel response to internal schema.

        Maps YouTube API fields to our database columns and extracts
        comprehensive metadata (country, language, keywords, joined date, etc).

        Args:
            item: Channel item from YouTube API response

        Returns:
            Normalized dict with internal schema keys
        """
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        branding = item.get("brandingSettings", {})
        channel_branding = branding.get("channel", {})
        topic_details = item.get("topicDetails", {})

        # Extract and normalize keywords from branding
        # YouTube API may return keywords as list or string
        raw_keywords = channel_branding.get("keywords") or []
        if isinstance(raw_keywords, list):
            keywords = ", ".join(raw_keywords)
        else:
            keywords = raw_keywords or ""

        # Fall back to extraction from description if no keywords
        if not keywords:
            description = snippet.get("description", "")
            keywords = extract_keywords_from_description(description)

        # Extract featured channels
        featured_urls = channel_branding.get("featuredChannelsUrls", [])
        featured_count = len(featured_urls) if featured_urls else 0

        return {
            # ═══════════════════════════════════════════════════════════
            # IDENTITY (Stored in creators table)
            # ═══════════════════════════════════════════════════════════
            "channel_id": item.get("id"),
            "channel_name": snippet.get("title"),
            "channel_description": snippet.get("description"),
            "channel_url": f"https://www.youtube.com/channel/{item.get('id')}",
            # ═══════════════════════════════════════════════════════════
            # CUSTOM PRESENCE (Stored in creators table)
            # ═══════════════════════════════════════════════════════════
            "custom_url": snippet.get("customUrl"),  # NEW: @username or custom URL
            "custom_url_available": snippet.get("customUrl")
            is not None,  # Has verified custom URL
            # ═══════════════════════════════════════════════════════════
            # TIMESTAMPS (Stored in creators table)
            # ═══════════════════════════════════════════════════════════
            "published_at": snippet.get(
                "publishedAt"
            ),  # NEW: Channel creation date (ISO 8601)
            "joined_at": snippet.get("publishedAt"),  # Alias for convenience
            # ═══════════════════════════════════════════════════════════
            # BRANDING / VISUAL (Stored in creators table)
            # ═══════════════════════════════════════════════════════════
            "channel_thumbnail_url": snippet.get("thumbnails", {})
            .get("high", {})
            .get("url"),
            "channel_thumbnail_default": snippet.get("thumbnails", {})
            .get("default", {})
            .get("url"),
            "banner_image_url": branding.get("image", {}).get(
                "bannerImageUrl"
            ),  # NEW: Profile header image
            # ═══════════════════════════════════════════════════════════
            # STATISTICS (Stored in creators table)
            # ═══════════════════════════════════════════════════════════
            "current_subscribers": int(statistics.get("subscriberCount", 0) or 0),
            "current_view_count": int(statistics.get("viewCount", 0) or 0),
            "current_video_count": int(statistics.get("videoCount", 0) or 0),
            "hidden_subscriber_count": statistics.get(
                "hiddenSubscriberCount", False
            ),  # NEW
            # ═══════════════════════════════════════════════════════════
            # METADATA (Stored in creators table)
            # ═══════════════════════════════════════════════════════════
            "country_code": channel_branding.get(
                "country"
            ),  # Already existed, enhanced
            "country": channel_branding.get("country"),  # Alias
            "default_language": snippet.get(
                "defaultLanguage"
            ),  # NEW: Content language (e.g., "en", "ja")
            "keywords": keywords,  # NEW: Creator-defined topics
            "keywords_raw": channel_branding.get(
                "keywords", ""
            ),  # NEW: Raw from branding
            # ═══════════════════════════════════════════════════════════
            # RELATIONSHIPS (Stored in creators table)
            # ═══════════════════════════════════════════════════════════
            "featured_channels_count": featured_count,  # NEW: Number of featured channels
            "featured_channels_urls": featured_urls,  # NEW: List of featured channel URLs
            "topic_categories": topic_details.get(
                "topicCategories", []
            ),  # NEW: Topic IDs
            # ═══════════════════════════════════════════════════════════
            # VERIFICATION (Stored in creators table)
            # ═══════════════════════════════════════════════════════════
            "verified": snippet.get("customUrl") is not None,
            "official": snippet.get("customUrl") is not None,  # Alias
            # ═══════════════════════════════════════════════════════════
            # COMPUTED / DERIVED
            # ═══════════════════════════════════════════════════════════
            "channel_age_days": calculate_channel_age(
                snippet.get("publishedAt")
            ),  # NEW: Days since creation
            "monthly_uploads": estimate_monthly_uploads(
                int(statistics.get("videoCount", 0) or 0),
                snippet.get("publishedAt"),
            ),  # NEW: Estimated videos/month
        }


def extract_keywords_from_description(description: str) -> str:
    """
    Extract keywords from channel description.

    Looks for hashtags (#keyword) and extracts first line as fallback.

    Args:
        description: Channel description text

    Returns:
        Keywords string (comma-separated)
    """
    if not description:
        return ""

    # Extract hashtags

    hashtags = re.findall(r"#(\w+)", description)
    if hashtags:
        return ", ".join(hashtags[:5])  # First 5 hashtags

    # Fallback: first line
    first_line = description.split("\n")[0].strip()
    if first_line and len(first_line) < 200:
        return first_line

    return ""


def calculate_channel_age(published_at: Optional[str]) -> Optional[int]:
    """
    Calculate days since channel creation.

    Args:
        published_at: ISO 8601 datetime string

    Returns:
        Number of days since creation, or None if can't calculate
    """
    if not published_at:
        return None

    try:
        # Parse ISO 8601 datetime
        published = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = now - published
        return delta.days
    except Exception as e:
        logger.warning(f"Could not calculate channel age: {e}")
        return None


def estimate_monthly_uploads(
    video_count: int, published_at: Optional[str]
) -> Optional[float]:
    """
    Estimate monthly video uploads.

    Args:
        video_count: Total number of videos
        published_at: Channel creation date (ISO 8601)

    Returns:
        Estimated videos per month, or None if can't calculate
    """
    if not video_count or not published_at:
        return None

    channel_age_days = calculate_channel_age(published_at)
    if not channel_age_days or channel_age_days < 1:
        return None

    try:
        months = max(1, channel_age_days / 30.44)  # Average days per month
        return round(video_count / months, 2)
    except Exception as e:
        logger.warning(f"Could not estimate monthly uploads: {e}")
        return None


async def ingest_creator(
    channel_id: Optional[str] = None,
    handle: Optional[str] = None,
    resolver: Optional[YouTubeResolver] = None,
    api_key: Optional[str] = None,
) -> Optional[dict]:
    """
    Unified creator ingestion from channel_id or handle.

    Resolves handle if needed, fetches comprehensive data from YouTube,
    and returns normalized creator data ready for database insertion.

    Args:
        channel_id: YouTube channel ID (UCxxxxxx) - optional
        handle: YouTube handle (@username) - optional
        resolver: YouTubeResolver instance (created if not provided)
        api_key: YouTube API key (used if resolver not provided)

    Returns:
        Normalized creator data dict, or None if unsuccessful

    Example:
        # From channel ID
        creator = await ingest_creator(channel_id="UCxxx")

        # From handle
        creator = await ingest_creator(handle="@username")
    """
    if not resolver:
        resolver = YouTubeResolver(api_key=api_key)

    # Resolve handle to channel_id if needed
    if not channel_id and handle:
        logger.info(f"[ingest_creator] Resolving handle: {handle}")
        channel_id = await resolver.resolve_handle_to_channel_id(handle)

    if not channel_id:
        logger.warning(f"[ingest_creator] Could not resolve: handle={handle}")
        return None

    # Fetch comprehensive data from YouTube
    logger.info(f"[ingest_creator] Fetching data for: {channel_id}")
    creator_data = await resolver.get_channel_data(channel_id)

    if not creator_data:
        logger.warning(f"[ingest_creator] Failed to fetch: {channel_id}")
        return None

    logger.info(
        f"[ingest_creator] ✅ Ingested: {creator_data['channel_id']} "
        f"({creator_data.get('country_code', 'Unknown')}, "
        f"{creator_data.get('default_language', 'Unknown')})"
    )
    return creator_data
