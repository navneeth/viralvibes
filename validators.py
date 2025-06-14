"""
Validation module for YouTube-related functionality.

This module contains validation functions for YouTube URLs, playlists, and other
YouTube-related data structures.
"""

import logging
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import parse_qs, urlparse

# Get logger instance
logger = logging.getLogger(__name__)


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
                errors.append(
                    "Invalid YouTube URL: Domain is not a recognized youtube.com domain"
                )
                return errors

            # Validate path
            if parsed_url.path != "/playlist":
                errors.append("Invalid YouTube URL: Not a playlist URL")
                return errors

            # Validate playlist ID
            query_params = parse_qs(parsed_url.query)
            if "list" not in query_params:
                errors.append("Invalid YouTube URL: Missing playlist ID")
                return errors

            playlist_id = query_params["list"][0]
            if not playlist_id:
                errors.append("Invalid YouTube URL: Empty playlist ID")
                return errors

            logger.info(
                f"Successfully validated playlist URL: {playlist.playlist_url}"
            )
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
