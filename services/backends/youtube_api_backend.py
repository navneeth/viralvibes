# services/backends/youtube_api_backend.py
"""
Complete YouTube Data API v3 backend implementation.

Features:
- Paginated fetching of all playlist videos
- Batch video statistics fetching (up to 50 per request)
- Quota error detection and handling
- Progress tracking
- Efficient API usage
"""

import logging
import re
from typing import List, Optional, Tuple

import isodate
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .base import (
    BackendError,
    PlaylistMetadata,
    ProcessingEstimate,
    ProcessingStats,
    QuotaExceededError,
    VideoData,
    YouTubeBackend,
)

logger = logging.getLogger(__name__)


class YouTubeApiBackend(YouTubeBackend):
    """
    YouTube Data API v3 backend.

    This backend uses the official YouTube Data API, which is fast and reliable
    but has daily quota limits. When quota is exhausted, it raises QuotaExceededError
    to trigger fallback to yt-dlp if configured.

    API Costs (per request):
    - playlists.list: 1 unit
    - playlistItems.list: 1 unit
    - videos.list: 1 unit

    Default daily quota: 10,000 units
    """

    MAX_RESULTS_PER_REQUEST = 50  # YouTube API maximum

    def __init__(self, api_key: str):
        """
        Initialize YouTube Data API backend.

        Args:
            api_key: YouTube Data API v3 key

        Raises:
            ValueError: If API key is not provided
        """
        if not api_key:
            raise ValueError("YouTube API key is required")

        self.api_key = api_key
        self.youtube = build("youtube", "v3", developerKey=api_key)
        self._stats = ProcessingStats()

    def get_processing_stats(self) -> ProcessingStats:
        """Get current processing statistics (minimal for API backend)."""
        return self._stats

    def estimate_processing_time(
        self, playlist_count: int, expand_all: bool = True
    ) -> ProcessingEstimate:
        """
        Estimate processing time for YouTube API (much faster than yt-dlp).

        The API is very fast - approximately 50 videos per request.
        Total time is dominated by network latency.

        Args:
            playlist_count: Total number of videos
            expand_all: Whether to process all videos

        Returns:
            ProcessingEstimate object
        """
        videos_to_expand = playlist_count if expand_all else min(50, playlist_count)

        # API is fast - approximately 50 videos per request
        # Need 2 passes: one for video IDs, one for statistics
        video_id_batches = (
            videos_to_expand + self.MAX_RESULTS_PER_REQUEST - 1
        ) // self.MAX_RESULTS_PER_REQUEST
        stats_batches = video_id_batches  # Same number for stats

        total_batches = video_id_batches + stats_batches

        # Rough estimate: ~0.5 seconds per API call + 1s for initial metadata
        total_seconds = total_batches * 0.5 + 1.0

        return ProcessingEstimate(
            total_videos=playlist_count,
            videos_to_expand=videos_to_expand,
            estimated_seconds=total_seconds,
            estimated_minutes=total_seconds / 60,
            batch_count=total_batches,
        )

    async def close(self):
        """No cleanup needed for API backend."""
        pass

    def _extract_playlist_id(self, url: str) -> str:
        """
        Extract playlist ID from YouTube URL.

        Args:
            url: YouTube playlist URL

        Returns:
            Playlist ID

        Raises:
            ValueError: If no playlist ID found in URL
        """
        match = re.search(r"list=([a-zA-Z0-9_-]+)", url)
        if not match:
            raise ValueError(
                f"Invalid playlist URL - no playlist ID found. "
                f"Expected format: https://www.youtube.com/playlist?list=..."
            )
        return match.group(1)

    def _parse_duration(self, duration_str: str) -> int:
        """
        Parse ISO 8601 duration string to seconds.

        YouTube API returns durations in ISO 8601 format (e.g., "PT1H23M45S")

        Args:
            duration_str: ISO 8601 duration string

        Returns:
            Duration in seconds
        """
        try:
            return int(isodate.parse_duration(duration_str).total_seconds())
        except (isodate.ISO8601Error, TypeError, ValueError) as e:
            logger.error(f"Failed to parse duration '{duration_str}': {e}")
            return 0

    async def fetch_playlist_preview(self, url: str) -> PlaylistMetadata:
        """
        Fetch lightweight playlist metadata.

        Makes a single API call to playlists.list endpoint.
        Cost: 1 API unit

        Args:
            url: YouTube playlist URL

        Returns:
            PlaylistMetadata object

        Raises:
            QuotaExceededError: If API quota is exhausted
            BackendError: If API request fails
        """
        try:
            playlist_id = self._extract_playlist_id(url)

            response = (
                self.youtube.playlists()
                .list(part="snippet,contentDetails", id=playlist_id, maxResults=1)
                .execute()
            )

            if not response.get("items"):
                raise BackendError(
                    f"Playlist not found: {playlist_id}. "
                    "It may be private, deleted, or the ID is incorrect."
                )

            item = response["items"][0]
            snippet = item["snippet"]
            content_details = item.get("contentDetails", {})

            return PlaylistMetadata(
                title=snippet.get("title", "Untitled Playlist"),
                channel_name=snippet.get("channelTitle", "Unknown Channel"),
                channel_thumbnail=(
                    snippet.get("thumbnails", {}).get("high", {}).get("url", "")
                ),
                video_count=content_details.get("itemCount", 0),
            )

        except HttpError as e:
            if e.resp.status == 403:
                error_details = e.error_details if hasattr(e, "error_details") else []
                for detail in error_details:
                    if detail.get("reason") == "quotaExceeded":
                        raise QuotaExceededError(
                            "YouTube API quota exceeded. "
                            "Try again tomorrow or enable yt-dlp fallback."
                        ) from e
                # Other 403 errors (e.g., invalid API key)
                raise BackendError(
                    f"API access forbidden: {e}. Check your API key."
                ) from e
            raise BackendError(f"API request failed: {e}") from e
        except ValueError as e:
            raise BackendError(str(e)) from e
        except Exception as e:
            logger.error(f"Failed to fetch preview: {e}")
            raise BackendError(f"Preview fetch failed: {e}") from e

    async def fetch_playlist_videos(
        self,
        url: str,
        max_videos: Optional[int] = None,
        progress_callback: Optional[callable] = None,
    ) -> Tuple[List[VideoData], PlaylistMetadata]:
        """
        Fetch complete playlist data with video statistics.

        Process:
        1. Fetch playlist metadata
        2. Fetch all video IDs (paginated)
        3. Fetch video statistics in batches of 50

        API Costs:
        - 1 unit for playlist metadata
        - 1 unit per 50 video IDs
        - 1 unit per 50 video statistics

        For a 200 video playlist:
        - Metadata: 1 unit
        - Video IDs: 4 units (4 pages)
        - Statistics: 4 units (4 batches)
        - Total: 9 units

        Args:
            url: YouTube playlist URL
            max_videos: Maximum videos to fetch (None = all)
            progress_callback: Optional callback(current, total, metadata)

        Returns:
            Tuple of (list of VideoData, PlaylistMetadata)

        Raises:
            QuotaExceededError: If API quota is exhausted
            BackendError: If API request fails
        """
        try:
            playlist_id = self._extract_playlist_id(url)

            # Step 1: Fetch metadata
            logger.info(f"Fetching playlist metadata for: {playlist_id}")
            metadata = await self.fetch_playlist_preview(url)

            # Step 2: Fetch all video IDs
            logger.info(
                f"Fetching video IDs from playlist ({metadata.video_count} total)"
            )
            video_ids = await self._fetch_all_video_ids(
                playlist_id, max_videos, progress_callback, metadata.video_count
            )

            if not video_ids:
                logger.warning("No videos found in playlist")
                return [], metadata

            # Step 3: Fetch video statistics in batches
            logger.info(f"Fetching statistics for {len(video_ids)} videos")
            videos = await self._fetch_video_statistics(
                video_ids, progress_callback, metadata.video_count
            )

            logger.info(f"Successfully fetched {len(videos)} videos from YouTube API")
            return videos, metadata

        except QuotaExceededError:
            raise
        except HttpError as e:
            if e.resp.status == 403:
                raise QuotaExceededError("YouTube API quota exceeded") from e
            raise BackendError(f"API request failed: {e}") from e
        except Exception as e:
            logger.exception(f"Failed to fetch playlist: {e}")
            raise BackendError(f"Playlist fetch failed: {e}") from e

    async def _fetch_all_video_ids(
        self,
        playlist_id: str,
        max_videos: Optional[int],
        progress_callback: Optional[callable],
        total_count: int,
    ) -> List[str]:
        """
        Fetch all video IDs from playlist using pagination.

        The API returns up to 50 items per request. We paginate through
        all results using nextPageToken.

        Args:
            playlist_id: YouTube playlist ID
            max_videos: Maximum videos to fetch (None = all)
            progress_callback: Progress callback
            total_count: Total playlist size (for progress reporting)

        Returns:
            List of video IDs

        Raises:
            QuotaExceededError: If quota is exhausted
            HttpError: If API request fails
        """
        video_ids = []
        next_page_token = None
        page_num = 0

        while max_videos is None or len(video_ids) < max_videos:
            try:
                page_num += 1
                max_results = min(
                    self.MAX_RESULTS_PER_REQUEST,
                    (
                        max_videos - len(video_ids)
                        if max_videos
                        else self.MAX_RESULTS_PER_REQUEST
                    ),
                )

                logger.debug(
                    f"Fetching video IDs page {page_num} "
                    f"(max {max_results} items, token: {next_page_token})"
                )

                response = (
                    self.youtube.playlistItems()
                    .list(
                        part="contentDetails",
                        playlistId=playlist_id,
                        maxResults=max_results,
                        pageToken=next_page_token,
                    )
                    .execute()
                )

                for item in response.get("items", []):
                    video_id = item["contentDetails"]["videoId"]
                    video_ids.append(video_id)

                # Progress update
                if progress_callback:
                    await progress_callback(
                        len(video_ids), total_count, {"phase": "fetching_ids"}
                    )

                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

            except HttpError as e:
                if e.resp.status == 403:
                    raise QuotaExceededError("YouTube API quota exceeded") from e
                raise

        logger.info(f"Fetched {len(video_ids)} video IDs in {page_num} pages")
        return video_ids

    async def _fetch_video_statistics(
        self,
        video_ids: List[str],
        progress_callback: Optional[callable],
        total_count: int,
    ) -> List[VideoData]:
        """
        Fetch detailed statistics for videos in batches.

        The API allows fetching up to 50 videos per request by passing
        a comma-separated list of IDs.

        Args:
            video_ids: List of video IDs
            progress_callback: Progress callback
            total_count: Total playlist size (for progress reporting)

        Returns:
            List of VideoData objects

        Raises:
            QuotaExceededError: If quota is exhausted
            HttpError: If API request fails
        """
        videos = []
        batch_num = 0

        for i in range(0, len(video_ids), self.MAX_RESULTS_PER_REQUEST):
            batch_ids = video_ids[i : i + self.MAX_RESULTS_PER_REQUEST]
            batch_num += 1
            total_batches = (
                len(video_ids) + self.MAX_RESULTS_PER_REQUEST - 1
            ) // self.MAX_RESULTS_PER_REQUEST

            logger.debug(
                f"Fetching statistics batch {batch_num}/{total_batches} "
                f"({len(batch_ids)} videos)"
            )

            try:
                response = (
                    self.youtube.videos()
                    .list(
                        part="snippet,statistics,contentDetails",
                        id=",".join(batch_ids),
                    )
                    .execute()
                )

                for idx, item in enumerate(response.get("items", []), start=i + 1):
                    snippet = item.get("snippet", {})
                    statistics = item.get("statistics", {})
                    content_details = item.get("contentDetails", {})

                    videos.append(
                        VideoData(
                            rank=idx,
                            id=item["id"],
                            title=snippet.get("title", "N/A"),
                            views=int(statistics.get("viewCount", 0)),
                            likes=int(statistics.get("likeCount", 0)),
                            dislikes=0,  # YouTube API no longer exposes dislikes
                            comments=int(statistics.get("commentCount", 0)),
                            duration=self._parse_duration(
                                content_details.get("duration", "PT0S")
                            ),
                            uploader=snippet.get("channelTitle", "N/A"),
                            thumbnail=(
                                snippet.get("thumbnails", {})
                                .get("high", {})
                                .get("url", "")
                            ),
                            rating=None,  # No rating data from API
                        )
                    )

                # Progress update
                if progress_callback:
                    await progress_callback(
                        len(videos), total_count, {"phase": "fetching_stats"}
                    )

            except HttpError as e:
                if e.resp.status == 403:
                    raise QuotaExceededError("YouTube API quota exceeded") from e
                logger.error(
                    f"Failed to fetch batch starting at index {i}: {e}. "
                    "Some videos may be missing from results."
                )
                self._stats.failed_videos += len(batch_ids)

        logger.info(
            f"Fetched statistics for {len(videos)}/{len(video_ids)} videos "
            f"in {batch_num} batches"
        )
        return videos  # services/backends/youtube_api_backend.py


"""
Complete YouTube Data API v3 backend implementation.

Features:
- Paginated fetching of all playlist videos
- Batch video statistics fetching (up to 50 per request)
- Quota error detection and handling
- Progress tracking
- Efficient API usage
"""

import logging
import re
from typing import List, Optional, Tuple

import isodate
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .base import (
    BackendError,
    PlaylistMetadata,
    ProcessingEstimate,
    ProcessingStats,
    QuotaExceededError,
    VideoData,
    YouTubeBackend,
)

logger = logging.getLogger(__name__)


class YouTubeApiBackend(YouTubeBackend):
    """
    YouTube Data API v3 backend.

    This backend uses the official YouTube Data API, which is fast and reliable
    but has daily quota limits. When quota is exhausted, it raises QuotaExceededError
    to trigger fallback to yt-dlp if configured.

    API Costs (per request):
    - playlists.list: 1 unit
    - playlistItems.list: 1 unit
    - videos.list: 1 unit

    Default daily quota: 10,000 units
    """

    MAX_RESULTS_PER_REQUEST = 50  # YouTube API maximum

    def __init__(self, api_key: str):
        """
        Initialize YouTube Data API backend.

        Args:
            api_key: YouTube Data API v3 key

        Raises:
            ValueError: If API key is not provided
        """
        if not api_key:
            raise ValueError("YouTube API key is required")

        self.api_key = api_key
        self.youtube = build("youtube", "v3", developerKey=api_key)
        self._stats = ProcessingStats()

    def get_processing_stats(self) -> ProcessingStats:
        """Get current processing statistics (minimal for API backend)."""
        return self._stats

    def estimate_processing_time(
        self, playlist_count: int, expand_all: bool = True
    ) -> ProcessingEstimate:
        """
        Estimate processing time for YouTube API (much faster than yt-dlp).

        The API is very fast - approximately 50 videos per request.
        Total time is dominated by network latency.

        Args:
            playlist_count: Total number of videos
            expand_all: Whether to process all videos

        Returns:
            ProcessingEstimate object
        """
        videos_to_expand = playlist_count if expand_all else min(50, playlist_count)

        # API is fast - approximately 50 videos per request
        # Need 2 passes: one for video IDs, one for statistics
        video_id_batches = (
            videos_to_expand + self.MAX_RESULTS_PER_REQUEST - 1
        ) // self.MAX_RESULTS_PER_REQUEST
        stats_batches = video_id_batches  # Same number for stats

        total_batches = video_id_batches + stats_batches

        # Rough estimate: ~0.5 seconds per API call + 1s for initial metadata
        total_seconds = total_batches * 0.5 + 1.0

        return ProcessingEstimate(
            total_videos=playlist_count,
            videos_to_expand=videos_to_expand,
            estimated_seconds=total_seconds,
            estimated_minutes=total_seconds / 60,
            batch_count=total_batches,
        )

    async def close(self):
        """No cleanup needed for API backend."""
        pass

    def _extract_playlist_id(self, url: str) -> str:
        """
        Extract playlist ID from YouTube URL.

        Args:
            url: YouTube playlist URL

        Returns:
            Playlist ID

        Raises:
            ValueError: If no playlist ID found in URL
        """
        match = re.search(r"list=([a-zA-Z0-9_-]+)", url)
        if not match:
            raise ValueError(
                f"Invalid playlist URL - no playlist ID found. "
                f"Expected format: https://www.youtube.com/playlist?list=..."
            )
        return match.group(1)

    def _parse_duration(self, duration_str: str) -> int:
        """
        Parse ISO 8601 duration string to seconds.

        YouTube API returns durations in ISO 8601 format (e.g., "PT1H23M45S")

        Args:
            duration_str: ISO 8601 duration string

        Returns:
            Duration in seconds
        """
        try:
            return int(isodate.parse_duration(duration_str).total_seconds())
        except (isodate.ISO8601Error, TypeError, ValueError) as e:
            logger.error(f"Failed to parse duration '{duration_str}': {e}")
            return 0

    async def fetch_playlist_preview(self, url: str) -> PlaylistMetadata:
        """
        Fetch lightweight playlist metadata.

        Makes a single API call to playlists.list endpoint.
        Cost: 1 API unit

        Args:
            url: YouTube playlist URL

        Returns:
            PlaylistMetadata object

        Raises:
            QuotaExceededError: If API quota is exhausted
            BackendError: If API request fails
        """
        try:
            playlist_id = self._extract_playlist_id(url)

            response = (
                self.youtube.playlists()
                .list(part="snippet,contentDetails", id=playlist_id, maxResults=1)
                .execute()
            )

            if not response.get("items"):
                raise BackendError(
                    f"Playlist not found: {playlist_id}. "
                    "It may be private, deleted, or the ID is incorrect."
                )

            item = response["items"][0]
            snippet = item["snippet"]
            content_details = item.get("contentDetails", {})

            return PlaylistMetadata(
                title=snippet.get("title", "Untitled Playlist"),
                channel_name=snippet.get("channelTitle", "Unknown Channel"),
                channel_thumbnail=(
                    snippet.get("thumbnails", {}).get("high", {}).get("url", "")
                ),
                video_count=content_details.get("itemCount", 0),
            )

        except HttpError as e:
            if e.resp.status == 403:
                error_details = e.error_details if hasattr(e, "error_details") else []
                for detail in error_details:
                    if detail.get("reason") == "quotaExceeded":
                        raise QuotaExceededError(
                            "YouTube API quota exceeded. "
                            "Try again tomorrow or enable yt-dlp fallback."
                        ) from e
                # Other 403 errors (e.g., invalid API key)
                raise BackendError(
                    f"API access forbidden: {e}. Check your API key."
                ) from e
            raise BackendError(f"API request failed: {e}") from e
        except ValueError as e:
            raise BackendError(str(e)) from e
        except Exception as e:
            logger.error(f"Failed to fetch preview: {e}")
            raise BackendError(f"Preview fetch failed: {e}") from e

    async def fetch_playlist_videos(
        self,
        url: str,
        max_videos: Optional[int] = None,
        progress_callback: Optional[callable] = None,
    ) -> Tuple[List[VideoData], PlaylistMetadata]:
        """
        Fetch complete playlist data with video statistics.

        Process:
        1. Fetch playlist metadata
        2. Fetch all video IDs (paginated)
        3. Fetch video statistics in batches of 50

        API Costs:
        - 1 unit for playlist metadata
        - 1 unit per 50 video IDs
        - 1 unit per 50 video statistics

        For a 200 video playlist:
        - Metadata: 1 unit
        - Video IDs: 4 units (4 pages)
        - Statistics: 4 units (4 batches)
        - Total: 9 units

        Args:
            url: YouTube playlist URL
            max_videos: Maximum videos to fetch (None = all)
            progress_callback: Optional callback(current, total, metadata)

        Returns:
            Tuple of (list of VideoData, PlaylistMetadata)

        Raises:
            QuotaExceededError: If API quota is exhausted
            BackendError: If API request fails
        """
        try:
            playlist_id = self._extract_playlist_id(url)

            # Step 1: Fetch metadata
            logger.info(f"Fetching playlist metadata for: {playlist_id}")
            metadata = await self.fetch_playlist_preview(url)

            # Step 2: Fetch all video IDs
            logger.info(
                f"Fetching video IDs from playlist ({metadata.video_count} total)"
            )
            video_ids = await self._fetch_all_video_ids(
                playlist_id, max_videos, progress_callback, metadata.video_count
            )

            if not video_ids:
                logger.warning("No videos found in playlist")
                return [], metadata

            # Step 3: Fetch video statistics in batches
            logger.info(f"Fetching statistics for {len(video_ids)} videos")
            videos = await self._fetch_video_statistics(
                video_ids, progress_callback, metadata.video_count
            )

            logger.info(f"Successfully fetched {len(videos)} videos from YouTube API")
            return videos, metadata

        except QuotaExceededError:
            raise
        except HttpError as e:
            if e.resp.status == 403:
                raise QuotaExceededError("YouTube API quota exceeded") from e
            raise BackendError(f"API request failed: {e}") from e
        except Exception as e:
            logger.exception(f"Failed to fetch playlist: {e}")
            raise BackendError(f"Playlist fetch failed: {e}") from e

    async def _fetch_all_video_ids(
        self,
        playlist_id: str,
        max_videos: Optional[int],
        progress_callback: Optional[callable],
        total_count: int,
    ) -> List[str]:
        """
        Fetch all video IDs from playlist using pagination.

        The API returns up to 50 items per request. We paginate through
        all results using nextPageToken.

        Args:
            playlist_id: YouTube playlist ID
            max_videos: Maximum videos to fetch (None = all)
            progress_callback: Progress callback
            total_count: Total playlist size (for progress reporting)

        Returns:
            List of video IDs

        Raises:
            QuotaExceededError: If quota is exhausted
            HttpError: If API request fails
        """
        video_ids = []
        next_page_token = None
        page_num = 0

        while max_videos is None or len(video_ids) < max_videos:
            try:
                page_num += 1
                max_results = min(
                    self.MAX_RESULTS_PER_REQUEST,
                    (
                        max_videos - len(video_ids)
                        if max_videos
                        else self.MAX_RESULTS_PER_REQUEST
                    ),
                )

                logger.debug(
                    f"Fetching video IDs page {page_num} "
                    f"(max {max_results} items, token: {next_page_token})"
                )

                response = (
                    self.youtube.playlistItems()
                    .list(
                        part="contentDetails",
                        playlistId=playlist_id,
                        maxResults=max_results,
                        pageToken=next_page_token,
                    )
                    .execute()
                )

                for item in response.get("items", []):
                    video_id = item["contentDetails"]["videoId"]
                    video_ids.append(video_id)

                # Progress update
                if progress_callback:
                    await progress_callback(
                        len(video_ids), total_count, {"phase": "fetching_ids"}
                    )

                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

            except HttpError as e:
                if e.resp.status == 403:
                    raise QuotaExceededError("YouTube API quota exceeded") from e
                raise

        logger.info(f"Fetched {len(video_ids)} video IDs in {page_num} pages")
        return video_ids

    async def _fetch_video_statistics(
        self,
        video_ids: List[str],
        progress_callback: Optional[callable],
        total_count: int,
    ) -> List[VideoData]:
        """
        Fetch detailed statistics for videos in batches.

        The API allows fetching up to 50 videos per request by passing
        a comma-separated list of IDs.

        Args:
            video_ids: List of video IDs
            progress_callback: Progress callback
            total_count: Total playlist size (for progress reporting)

        Returns:
            List of VideoData objects

        Raises:
            QuotaExceededError: If quota is exhausted
            HttpError: If API request fails
        """
        videos = []
        batch_num = 0

        for i in range(0, len(video_ids), self.MAX_RESULTS_PER_REQUEST):
            batch_ids = video_ids[i : i + self.MAX_RESULTS_PER_REQUEST]
            batch_num += 1
            total_batches = (
                len(video_ids) + self.MAX_RESULTS_PER_REQUEST - 1
            ) // self.MAX_RESULTS_PER_REQUEST

            logger.debug(
                f"Fetching statistics batch {batch_num}/{total_batches} "
                f"({len(batch_ids)} videos)"
            )

            try:
                response = (
                    self.youtube.videos()
                    .list(
                        part="snippet,statistics,contentDetails",
                        id=",".join(batch_ids),
                    )
                    .execute()
                )

                for idx, item in enumerate(response.get("items", []), start=i + 1):
                    snippet = item.get("snippet", {})
                    statistics = item.get("statistics", {})
                    content_details = item.get("contentDetails", {})

                    videos.append(
                        VideoData(
                            rank=idx,
                            id=item["id"],
                            title=snippet.get("title", "N/A"),
                            views=int(statistics.get("viewCount", 0)),
                            likes=int(statistics.get("likeCount", 0)),
                            dislikes=0,  # YouTube API no longer exposes dislikes
                            comments=int(statistics.get("commentCount", 0)),
                            duration=self._parse_duration(
                                content_details.get("duration", "PT0S")
                            ),
                            uploader=snippet.get("channelTitle", "N/A"),
                            thumbnail=(
                                snippet.get("thumbnails", {})
                                .get("high", {})
                                .get("url", "")
                            ),
                            rating=None,  # No rating data from API
                        )
                    )

                # Progress update
                if progress_callback:
                    await progress_callback(
                        len(videos), total_count, {"phase": "fetching_stats"}
                    )

            except HttpError as e:
                if e.resp.status == 403:
                    raise QuotaExceededError("YouTube API quota exceeded") from e
                logger.error(
                    f"Failed to fetch batch starting at index {i}: {e}. "
                    "Some videos may be missing from results."
                )
                self._stats.failed_videos += len(batch_ids)

        logger.info(
            f"Fetched statistics for {len(videos)}/{len(video_ids)} videos "
            f"in {batch_num} batches"
        )
        return videos
