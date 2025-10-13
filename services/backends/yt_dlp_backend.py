# services/backends/yt_dlp_backend.py
"""
Complete yt-dlp backend implementation with resilience features.

Features:
- Bot detection handling with exponential backoff
- Concurrent video and dislike data fetching
- Rate limiting with random delays
- Batch processing with progress tracking
- User agent rotation
- Cookie support
- Comprehensive error handling and retry logic
"""

import asyncio
import logging
import os
import random
import time
from typing import List, Optional, Tuple

import httpx
import yt_dlp

from .base import (
    BackendError,
    BotChallengeError,
    PlaylistMetadata,
    ProcessingEstimate,
    ProcessingStats,
    RateLimitError,
    VideoData,
    YouTubeBackend,
)

logger = logging.getLogger(__name__)

DISLIKE_API_URL = "https://returnyoutubedislikeapi.com/votes?videoId={}"


class YtDlpBackend(YouTubeBackend):
    """
    yt-dlp backend with bot detection handling and retry logic.

    This backend fetches data directly from YouTube without using the API,
    making it useful when API quotas are exhausted. However, it's slower
    and more prone to bot detection.
    """

    def __init__(
        self,
        cookies_file: Optional[str] = None,
        batch_size: int = 10,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        min_video_delay: float = 0.5,
        max_video_delay: float = 1.5,
        min_batch_delay: float = 2.0,
        max_batch_delay: float = 4.0,
    ):
        """
        Initialize yt-dlp backend.

        Args:
            cookies_file: Path to browser cookies file (helps avoid bot detection)
            batch_size: Number of videos to process in each batch
            max_retries: Maximum retry attempts for failed requests
            retry_delay: Base delay between retries (exponential backoff)
            min_video_delay: Minimum delay between video fetches (seconds)
            max_video_delay: Maximum delay between video fetches (seconds)
            min_batch_delay: Minimum delay between batches (seconds)
            max_batch_delay: Maximum delay between batches (seconds)
        """
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.min_video_delay = min_video_delay
        self.max_video_delay = max_video_delay
        self.min_batch_delay = min_batch_delay
        self.max_batch_delay = max_batch_delay

        # Setup cookies
        self.cookies_file = cookies_file or os.getenv(
            "COOKIES_FILE", "/tmp/cookies.txt"
        )
        if not os.path.exists(self.cookies_file):
            logger.warning(
                f"Cookies file not found at {self.cookies_file}. "
                "YouTube may block requests. Export cookies from your browser."
            )
        else:
            logger.info(f"Using cookies from {self.cookies_file}")

        # Configure yt-dlp options
        self.ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "nocheckcertificate": True,
            "extract_flat": "in_playlist",  # Fast playlist fetch
            "cachedir": False,  # Don't write to cache
            "skip_download": True,
            "user-agent": self._get_random_user_agent(),
            "retries": max_retries,
            "fragment_retries": max_retries,
        }

        # Add cookies if file exists
        if os.path.exists(self.cookies_file):
            self.ydl_opts["cookiefile"] = self.cookies_file

        self.ydl = yt_dlp.YoutubeDL(self.ydl_opts)

        # Persistent HTTP client for dislike API
        self._dislike_client: Optional[httpx.AsyncClient] = None

        # Track failed videos and statistics
        self._failed_videos: List[str] = []
        self._stats = ProcessingStats()

    def _get_random_user_agent(self) -> str:
        """Return a random realistic user agent string."""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]
        return random.choice(user_agents)

    def get_processing_stats(self) -> ProcessingStats:
        """Get current processing statistics."""
        return self._stats

    def estimate_processing_time(
        self, playlist_count: int, expand_all: bool = True
    ) -> ProcessingEstimate:
        """
        Estimate the time required to process a playlist.

        The estimate accounts for:
        - Initial playlist fetch
        - Video info fetching (with delays to avoid bot detection)
        - Dislike API calls (concurrent with video fetching)
        - Batch delays
        - Retry overhead (20% buffer)

        Args:
            playlist_count: Total number of videos in playlist
            expand_all: Whether to expand all videos (True) or use default limit

        Returns:
            ProcessingEstimate object with timing information
        """
        videos_to_expand = playlist_count if expand_all else min(20, playlist_count)

        # Time estimates (in seconds)
        flat_fetch_time = 2.0  # Initial playlist fetch
        avg_video_fetch_time = (self.min_video_delay + self.max_video_delay) / 2
        avg_dislike_fetch_time = 0.2  # Dislike API is fast
        avg_batch_delay = (self.min_batch_delay + self.max_batch_delay) / 2

        # Calculate batch processing time
        batch_count = (videos_to_expand + self.batch_size - 1) // self.batch_size

        # Time per batch: max(video_fetch, dislike_fetch) since they're concurrent
        time_per_batch = (
            max(
                avg_video_fetch_time * self.batch_size,
                avg_dislike_fetch_time * self.batch_size,
            )
            + avg_batch_delay
        )

        total_seconds = flat_fetch_time + (time_per_batch * batch_count)

        # Add buffer for retries and overhead (20%)
        total_seconds *= 1.2

        return ProcessingEstimate(
            total_videos=playlist_count,
            videos_to_expand=videos_to_expand,
            estimated_seconds=total_seconds,
            estimated_minutes=total_seconds / 60,
            batch_count=batch_count,
        )

    async def _get_dislike_client(self) -> httpx.AsyncClient:
        """Get or create persistent HTTP client for dislike API."""
        if self._dislike_client is None or self._dislike_client.is_closed:
            self._dislike_client = httpx.AsyncClient(
                timeout=httpx.Timeout(10.0, connect=5.0),
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
                transport=httpx.AsyncHTTPTransport(retries=self.max_retries),
            )
        return self._dislike_client

    async def close(self):
        """Close HTTP client and cleanup resources."""
        if self._dislike_client and not self._dislike_client.is_closed:
            await self._dislike_client.aclose()
            self._dislike_client = None
        logger.debug("yt-dlp backend closed")

    async def fetch_playlist_preview(self, url: str) -> PlaylistMetadata:
        """
        Fetch lightweight playlist metadata without video details.

        Uses yt-dlp's flat extraction for fast results.

        Args:
            url: YouTube playlist URL

        Returns:
            PlaylistMetadata object

        Raises:
            BackendError: If fetching fails
        """
        try:
            info = await asyncio.to_thread(self.ydl.extract_info, url, download=False)

            return PlaylistMetadata(
                title=info.get("title", "Untitled Playlist"),
                channel_name=info.get("uploader", "Unknown Channel"),
                channel_thumbnail=self._extract_thumbnail(info),
                video_count=info.get("playlist_count", len(info.get("entries", []))),
            )
        except Exception as e:
            logger.error(f"Failed to fetch playlist preview: {e}")
            raise BackendError(f"Preview fetch failed: {e}") from e

    async def fetch_playlist_videos(
        self,
        url: str,
        max_videos: Optional[int] = None,
        progress_callback: Optional[callable] = None,
    ) -> Tuple[List[VideoData], PlaylistMetadata]:
        """
        Fetch complete playlist data with video details.

        Process:
        1. Fetch flat playlist structure (fast)
        2. Fetch detailed info for each video in batches
        3. Fetch dislike data concurrently
        4. Combine results

        Args:
            url: YouTube playlist URL
            max_videos: Maximum videos to process (None = all)
            progress_callback: Optional callback(current, total, metadata)

        Returns:
            Tuple of (list of VideoData, PlaylistMetadata)

        Raises:
            BotChallengeError: If bot detection persists
            BackendError: If fetching fails
        """
        try:
            # Phase 1: Get flat playlist structure (fast)
            logger.info(f"Fetching playlist structure: {url}")
            playlist_info = await asyncio.to_thread(
                self.ydl.extract_info, url, download=False
            )

            metadata = PlaylistMetadata(
                title=playlist_info.get("title", "Untitled Playlist"),
                channel_name=playlist_info.get("uploader", "Unknown Channel"),
                channel_thumbnail=self._extract_thumbnail(playlist_info),
                video_count=playlist_info.get(
                    "playlist_count", len(playlist_info.get("entries", []))
                ),
            )

            entries = playlist_info.get("entries", [])
            if not entries:
                logger.warning("No entries found in playlist")
                return [], metadata

            # Determine videos to process
            videos_to_process = entries if max_videos is None else entries[:max_videos]

            # Log processing estimate
            estimate = self.estimate_processing_time(
                metadata.video_count, expand_all=(max_videos is None)
            )
            logger.info(
                f"Processing {len(videos_to_process)} of {metadata.video_count} videos. "
                f"ETA: {estimate} ({estimate.batch_count} batches)"
            )

            # Phase 2: Fetch detailed video data in batches
            videos = await self._fetch_video_batch(
                videos_to_process,
                metadata.channel_name,
                metadata.video_count,
                progress_callback,
            )

            logger.info(
                f"Successfully processed {len(videos)}/{len(videos_to_process)} videos. "
                f"Stats: {self._stats.to_dict()}"
            )

            return videos, metadata

        except BotChallengeError:
            raise
        except Exception as e:
            logger.exception(f"Failed to fetch playlist: {e}")
            raise BackendError(f"Playlist fetch failed: {e}") from e
        finally:
            # Don't close here - let the service manage lifecycle
            pass

    async def _fetch_video_batch(
        self,
        entries: List[dict],
        default_uploader: str,
        total_count: int,
        progress_callback: Optional[callable],
    ) -> List[VideoData]:
        """
        Fetch video details in batches with concurrent dislike fetching.

        Args:
            entries: List of video entries from playlist
            default_uploader: Default channel name
            total_count: Total playlist size (for progress)
            progress_callback: Progress callback function

        Returns:
            List of VideoData objects
        """
        client = await self._get_dislike_client()
        all_videos = []
        start_time = time.time()

        for i in range(0, len(entries), self.batch_size):
            batch = entries[i : i + self.batch_size]
            batch_num = i // self.batch_size + 1
            total_batches = (len(entries) - 1) // self.batch_size + 1

            logger.info(
                f"Processing batch {batch_num}/{total_batches} "
                f"(videos {i + 1}-{min(i + self.batch_size, len(entries))})"
            )

            # Fetch video info and dislikes concurrently
            video_tasks = [
                self._fetch_video_info(entry.get("url", ""), entry.get("id", ""))
                for entry in batch
            ]
            dislike_tasks = [
                self._fetch_dislike_data(client, entry.get("id", ""))
                for entry in batch
                if entry.get("id")
            ]

            try:
                video_infos, dislike_results = await asyncio.gather(
                    asyncio.gather(*video_tasks, return_exceptions=True),
                    asyncio.gather(*dislike_tasks, return_exceptions=True),
                )

                # Build dislike map
                dislike_map = {
                    vid: data
                    for vid, data in dislike_results
                    if not isinstance((vid, data), Exception)
                }

                # Combine results
                for rank, (entry, info) in enumerate(
                    zip(batch, video_infos), start=i + 1
                ):
                    if isinstance(info, Exception):
                        logger.warning(f"Video {rank} raised exception: {info}")
                        self._stats.failed_videos += 1
                        self._failed_videos.append(entry.get("id", "unknown"))
                        continue

                    if not info:
                        self._stats.failed_videos += 1
                        self._failed_videos.append(entry.get("id", "unknown"))
                        continue

                    vid = info.get("id", entry.get("id", ""))
                    dislike_data = dislike_map.get(vid, {})

                    all_videos.append(
                        VideoData(
                            rank=rank,
                            id=vid,
                            title=info.get("title", "N/A"),
                            views=info.get("view_count", 0) or 0,
                            likes=dislike_data.get("likes", info.get("like_count", 0))
                            or 0,
                            dislikes=dislike_data.get("dislikes", 0),
                            comments=info.get("comment_count", 0) or 0,
                            duration=info.get("duration", 0) or 0,
                            uploader=info.get("uploader", default_uploader),
                            thumbnail=info.get("thumbnail", ""),
                            rating=dislike_data.get("rating"),
                        )
                    )

                # Progress callback with time estimate
                processed = len(all_videos)
                if progress_callback:
                    elapsed = time.time() - start_time
                    if processed > 0:
                        estimated_total = (elapsed / processed) * len(entries)
                        remaining = estimated_total - elapsed
                        await progress_callback(
                            processed,
                            total_count,
                            {
                                "elapsed": elapsed,
                                "remaining": remaining,
                                "batch": batch_num,
                                "total_batches": total_batches,
                            },
                        )

            except Exception as e:
                logger.error(f"Error processing batch {batch_num}: {e}")
                # Continue with next batch instead of failing completely

            # Delay between batches (except for the last one)
            if i + self.batch_size < len(entries):
                batch_delay = random.uniform(self.min_batch_delay, self.max_batch_delay)
                logger.debug(f"Waiting {batch_delay:.2f}s before next batch")
                await asyncio.sleep(batch_delay)

        return all_videos

    async def _fetch_video_info(
        self, url: str, video_id: str, retry_count: int = 0
    ) -> dict:
        """
        Fetch video metadata with retry logic and bot detection handling.

        Args:
            url: Video URL
            video_id: Video ID (for logging)
            retry_count: Current retry attempt

        Returns:
            Video info dictionary

        Raises:
            BotChallengeError: If bot detection persists after retries
        """
        try:
            # Random delay to avoid bot detection
            await asyncio.sleep(
                random.uniform(self.min_video_delay, self.max_video_delay)
            )

            return await asyncio.to_thread(self.ydl.extract_info, url, download=False)

        except Exception as e:
            error_str = str(e)

            # Check for bot challenge
            if "Sign in to confirm you're not a bot" in error_str or any(
                kw in error_str.lower()
                for kw in ["captcha", "verify", "unusual traffic", "automated requests"]
            ):
                self._stats.bot_challenges += 1

                if retry_count < self.max_retries:
                    wait_time = self.retry_delay * (
                        2**retry_count
                    )  # Exponential backoff
                    logger.warning(
                        f"Bot challenge for {video_id}. "
                        f"Waiting {wait_time}s before retry "
                        f"(attempt {retry_count + 1}/{self.max_retries})"
                    )
                    await asyncio.sleep(wait_time)

                    # Rotate user agent on retry
                    self.ydl.params["user-agent"] = self._get_random_user_agent()

                    return await self._fetch_video_info(url, video_id, retry_count + 1)
                else:
                    logger.error(
                        f"Bot challenge persists after {self.max_retries} retries for {video_id}"
                    )
                    raise BotChallengeError(
                        f"YouTube bot challenge after {self.max_retries} retries"
                    ) from e

            # Retry on other errors
            if retry_count < self.max_retries:
                self._stats.total_retries += 1
                logger.warning(
                    f"Failed to fetch {video_id}: {e}. "
                    f"Retrying (attempt {retry_count + 1}/{self.max_retries})"
                )
                await asyncio.sleep(self.retry_delay)
                return await self._fetch_video_info(url, video_id, retry_count + 1)

            logger.warning(
                f"Failed to fetch video {video_id} after {self.max_retries} retries: {e}"
            )
            return {}

    async def _fetch_dislike_data(
        self, client: httpx.AsyncClient, video_id: str, retry_count: int = 0
    ) -> Tuple[str, dict]:
        """
        Fetch dislike data from Return YouTube Dislike API.

        Args:
            client: HTTP client
            video_id: Video ID
            retry_count: Current retry attempt

        Returns:
            Tuple of (video_id, dislike_data)
        """
        try:
            # Small random delay
            await asyncio.sleep(random.uniform(0.1, 0.3))

            response = await client.get(DISLIKE_API_URL.format(video_id))

            if response.status_code == 200:
                data = response.json()
                return video_id, {
                    "dislikes": data.get("dislikes", 0),
                    "likes": data.get("likes", 0),
                    "rating": data.get("rating"),
                }
            elif response.status_code == 429:  # Rate limited
                if retry_count < self.max_retries:
                    self._stats.rate_limits += 1
                    wait_time = self.retry_delay * (2**retry_count)
                    logger.warning(
                        f"Rate limited on dislike API for {video_id}. "
                        f"Retrying in {wait_time}s "
                        f"(attempt {retry_count + 1}/{self.max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    return await self._fetch_dislike_data(
                        client, video_id, retry_count + 1
                    )

            logger.warning(
                f"Failed to fetch dislike data for {video_id}: HTTP {response.status_code}"
            )

        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.PoolTimeout) as e:
            if retry_count < self.max_retries:
                self._stats.total_retries += 1
                logger.warning(
                    f"Timeout for dislike API {video_id}. "
                    f"Retrying (attempt {retry_count + 1}/{self.max_retries})"
                )
                await asyncio.sleep(self.retry_delay)
                return await self._fetch_dislike_data(client, video_id, retry_count + 1)
            logger.warning(
                f"Dislike fetch timeout for {video_id} after {self.max_retries} retries"
            )
        except Exception as e:
            logger.warning(f"Dislike fetch failed for {video_id}: {e}")

        # Return empty data on failure
        return video_id, {
            "dislikes": 0,
            "likes": 0,
            "rating": None,
        }

    def _extract_thumbnail(self, info: dict) -> str:
        """
        Extract the highest quality channel thumbnail from playlist info.

        Args:
            info: Playlist info dictionary

        Returns:
            Thumbnail URL
        """
        if "thumbnails" in info:
            thumbs = sorted(
                info["thumbnails"],
                key=lambda x: x.get("width", 0),
                reverse=True,
            )
            return thumbs[0].get("url", "") if thumbs else ""
        return ""
