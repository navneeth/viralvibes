"""
Validation module for YouTube-related functionality.

This module contains validation functions for YouTube URLs, playlists, and other
YouTube-related data structures.
"""

import logging
import re
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import parse_qs, urlparse

# Get logger instance
logger = logging.getLogger(__name__)


class ChannelIDValidator:
    """Extract and validate canonical YouTube channel IDs without API dependencies."""

    CHANNEL_ID_RE = re.compile(r"(UC[a-zA-Z0-9_-]{22})")

    @staticmethod
    def extract_from_url(url: str) -> Optional[str]:
        if not url:
            return None
        match = ChannelIDValidator.CHANNEL_ID_RE.search(url)
        return match.group(1) if match else None

    @staticmethod
    def extract_from_text(text: str) -> set[str]:
        if not text:
            return set()
        return set(ChannelIDValidator.CHANNEL_ID_RE.findall(text))

    @staticmethod
    def is_valid(channel_id: str) -> bool:
        if not channel_id:
            return False
        return ChannelIDValidator.CHANNEL_ID_RE.fullmatch(channel_id) is not None


@dataclass
class YoutubePlaylist:
    """
    Represents a YouTube playlist URL for validation.

    Attributes:
        playlist_url (str): The URL of the YouTube playlist to validate.
    """

    playlist_url: str

    def __post_init__(self):
        """Normalize the playlist URL by stripping whitespace."""
        self.playlist_url = self.playlist_url.strip()


class YoutubePlaylistValidator:
    """
    Validator for YouTube playlist URLs.

    This class provides methods to validate YouTube playlist URLs and extract
    relevant information from them.
    """

    ALLOWED_DOMAINS = {
        "www.youtube.com",
        "youtube.com",
        "m.youtube.com",
        "music.youtube.com",
        "youtu.be",  # shortened share URLs from the YouTube mobile app
    }

    @classmethod
    def validate(cls, playlist: YoutubePlaylist) -> List[str]:
        """
        Validate a YouTube playlist URL.

        Args:
            playlist (YoutubePlaylist): The playlist object containing the URL to validate.

        Returns:
            List[str]: List of validation error messages. Empty list if validation passes.
        """
        errors = []
        try:
            parsed_url = urlparse(playlist.playlist_url)

            # Validate domain
            if parsed_url.netloc not in cls.ALLOWED_DOMAINS:
                errors.append("Invalid YouTube URL: Domain is not a recognized youtube.com domain")
                return errors

            query_params = parse_qs(parsed_url.query)

            # Accept three URL shapes that all carry a valid playlist ID:
            #   1. /playlist?list=...          — standard desktop/web URL
            #   2. /watch?v=...&list=...       — YouTube app share on mobile
            #   3. youtu.be/<id>?list=...      — shortened share link
            is_standard = parsed_url.path == "/playlist"
            is_watch = parsed_url.path == "/watch" and "list" in query_params
            is_short = parsed_url.netloc == "youtu.be"

            if not (is_standard or is_watch or is_short):
                errors.append("Invalid YouTube URL: Not a playlist URL")
                return errors

            # Validate playlist ID
            if "list" not in query_params:
                errors.append("Invalid YouTube URL: Missing playlist ID")
                return errors

            playlist_id = query_params["list"][0]
            if not playlist_id:
                errors.append("Invalid YouTube URL: Empty playlist ID")
                return errors

            logger.info(f"Successfully validated playlist URL: {playlist.playlist_url}")
            return errors

        except Exception as e:
            logger.error(f"Error validating playlist URL: {str(e)}")
            errors.append("Invalid URL format")
            return errors

    @classmethod
    def extract_playlist_id(cls, playlist_url: str) -> Optional[str]:
        """
        Extract the playlist ID from a YouTube playlist URL.

        Args:
            playlist_url (str): The YouTube playlist URL.

        Returns:
            Optional[str]: The playlist ID if found, None otherwise.
        """
        try:
            parsed_url = urlparse(playlist_url)
            query_params = parse_qs(parsed_url.query)
            return query_params.get("list", [None])[0]
        except Exception as e:
            logger.error(f"Error extracting playlist ID: {str(e)}")
            return None
