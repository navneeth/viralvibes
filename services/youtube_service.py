# youtube_service.py
"""
YouTube Service for fetching and processing YouTube playlist data.
This module provides functionality to fetch and process YouTube playlist data using yt-dlp.
Enhanced with full playlist processing, resilience features, and time estimation.
"""

import asyncio
import logging
import os
import random
import re
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import httpx
import isodate
import polars as pl

# Optional imports
try:
    import yt_dlp
except ImportError:
    yt_dlp = None

try:
    from googleapiclient.discovery import build
except ImportError:
    build = None


from services.youtube_errors import (
    YouTubeBotChallengeError,
    YouTubeServiceError,
)
from services.youtube_transforms import _enrich_dataframe, normalize_columns
from services.youtube_utils import (
    ProcessingEstimate,
    extract_all_tags,
    extract_categories,
    get_category_name,
)

from .config import YouTubeConfig

# Get logger instance
logger = logging.getLogger(__name__)


DISLIKE_API_URL = "https://returnyoutubedislikeapi.com/votes?videoId={}"


def transform_api_df(api_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform YouTube API list of videos to chart-friendly column names
    compatible with the existing enrichment and chart functions.
    """
    transformed = []
    for idx, v in enumerate(api_list, start=1):
        transformed.append(
            {
                "Rank": v.get("Rank", idx),
                "id": v.get("id"),
                "Title": v.get("Title", "N/A"),
                "Views": v.get("Views", 0),
                "Likes": v.get("Likes", 0),
                "Dislikes": v.get("Dislikes", 0),
                "Comments": v.get("Comments", 0),
                "Duration": v.get("Duration", 0),
                "Uploader": v.get("Uploader", "N/A"),
                "Thumbnail": v.get("Thumbnail", ""),
                "Rating": v.get("Rating", 0.0),
                "Controversy": v.get("Controversy", 0.0),
                "Engagement Rate Raw": v.get("Engagement Rate Raw", 0.0),
            }
        )
    return transformed


# ============================================================================
# Abstract Base Backend
# ============================================================================


class YouTubeBackendBase(ABC):
    """Abstract base class for YouTube data backends."""

    def __init__(self, cfg=None):
        self.cfg = cfg or YouTubeConfig()

    @abstractmethod
    async def get_playlist_data(
        self,
        playlist_url: str,
        max_expanded: Optional[int],
        progress_callback: callable,
    ):
        """Fetch playlist data. Returns (df, name, channel, thumb, stats)."""
        pass

    @abstractmethod
    async def get_playlist_preview(self, playlist_url: str):
        """Get preview. Returns (title, channel, thumb, length, desc, privacy, published)."""
        pass

    async def close(self):
        """Optional cleanup. Override if needed."""
        pass

    def estimate_processing_time(
        self, playlist_count: int, expand_all: bool = True
    ) -> ProcessingEstimate:
        """
        Estimate the time required to process a playlist.

        Args:
            playlist_count: Total number of videos in playlist
            expand_all: Whether to expand all videos (True) or use default limit

        Returns:
            ProcessingEstimate object with timing information
        """
        videos_to_expand = playlist_count if expand_all else min(20, playlist_count)

        # Time estimates (in seconds)
        flat_fetch_time = 2.0  # Initial playlist fetch
        avg_video_fetch_time = (self.cfg.min_video_delay + self.cfg.max_video_delay) / 2
        avg_dislike_fetch_time = 0.2
        avg_batch_delay = (self.cfg.min_batch_delay + self.cfg.max_batch_delay) / 2

        # Calculate batch processing time
        batch_count = (
            videos_to_expand + self.cfg.batch_size - 1
        ) // self.cfg.batch_size

        # Time per batch: max(video_fetch, dislike_fetch) since they're concurrent
        time_per_batch = (
            max(
                avg_video_fetch_time * self.cfg.batch_size,
                avg_dislike_fetch_time * self.cfg.batch_size,
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


class YouTubeBackendYTDLP(YouTubeBackendBase):
    def __init__(self, cfg=None, ydl_opts=None):
        super().__init__(cfg)

        if yt_dlp is None:
            raise ImportError("yt-dlp is not installed.")

        self._dislike_client = None
        self._processing_stats = {
            "total_retries": 0,
            "failed_videos": 0,
            "bot_challenges": 0,
            "rate_limits": 0,
        }

        cookies_file = os.getenv("COOKIES_FILE", "/tmp/cookies.txt")
        if not os.path.exists(cookies_file):
            logger.warning(
                f"[YouTubeService] Cookies file not found at {cookies_file}. "
                f"YouTube may block requests."
            )
        else:
            logger.info(f"[YouTubeService] Using cookies from {cookies_file}")

        # 1. Define base default options
        base_opts = {
            "quiet": True,
            "nocheckcertificate": True,
            # 🚀 Allows lightweight fetch with URLs
            "extract_flat": "in_playlist",
            # 🚀 prevent yt-dlp from writing to ~/.cache
            "cachedir": False,
            "skip_download": True,
            "cookiefile": cookies_file,
            # Add user agent rotation
            "user-agent": self._get_random_user_agent(),
            # Add retries at yt-dlp level
            "retries": self.cfg.max_retries,
            "fragment_retries": self.cfg.max_retries,
        }

        self.ydl_opts = base_opts.copy()
        if ydl_opts:
            self.ydl_opts.update(ydl_opts)

        cookies_file = os.getenv("COOKIES_FILE")
        if cookies_file and os.path.exists(cookies_file):
            logger.info(f"Using cookies from file: {cookies_file}")
            self.ydl_opts["cookiefile"] = cookies_file
        elif cookies_file:
            logger.warning(f"COOKIES_FILE set, but file not found at: {cookies_file}")

        self.ydl = yt_dlp.YoutubeDL(self.ydl_opts)

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

    async def _get_dislike_client(self) -> httpx.AsyncClient:
        """Get or create persistent HTTP client for dislike API."""
        if self._dislike_client is None or self._dislike_client.is_closed:
            self._dislike_client = httpx.AsyncClient(
                timeout=httpx.Timeout(10.0, connect=5.0),
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
                transport=httpx.AsyncHTTPTransport(retries=self.cfg.max_retries),
            )
        return self._dislike_client

    async def close(self):
        """Close the persistent HTTP client."""
        if self._dislike_client and not self._dislike_client.is_closed:
            await self._dislike_client.aclose()
            self._dislike_client = None

    def _extract_channel_thumbnail(self, playlist_info: dict) -> str:
        """Extract the channel thumbnail URL from playlist info.

        Args:
            playlist_info (dict): The playlist information dictionary.

        Returns:
            str: The channel thumbnail URL.
        """
        if "thumbnails" in playlist_info:
            thumbs = sorted(
                playlist_info["thumbnails"],
                key=lambda x: x.get("width", 0),
                reverse=True,
            )
            return thumbs[0].get("url", "") if thumbs else ""
        return ""

    async def get_playlist_preview(
        self, playlist_url: str
    ) -> Tuple[str, str, str, int, str, str, str]:
        """Get lightweight playlist preview."""
        try:
            info = await asyncio.to_thread(
                self.ydl.extract_info, playlist_url, download=False
            )
            title = info.get("title", "Untitled Playlist")
            channel = info.get("uploader", "Unknown Channel")
            thumb = self._extract_channel_thumbnail(info)
            length = info.get("playlist_count", len(info.get("entries", [])))
            description = info.get("description", "")
            # yt-dlp doesn't expose privacy status easily, set to unknown
            privacy = "Unknown"
            published = info.get("upload_date", "")

            return title, channel, thumb, length, description, privacy, published
        except Exception as e:
            logger.warning(f"Failed to fetch yt-dlp preview: {e}")
            return "Preview unavailable", "", "", 0, "", "Unknown", ""

    async def get_playlist_data(
        self,
        playlist_url: str,
        max_expanded: Optional[int],
        progress_callback: callable = None,
    ) -> Tuple[pl.DataFrame, str, str, str, Dict[str, float]]:
        """Fetch and process video information from a YouTube playlist URL."""
        logger.info(f"Starting analysis for playlist: {playlist_url}")

        try:
            # Phase 1: flat playlist skeleton
            playlist_info = await asyncio.to_thread(
                self.ydl.extract_info, playlist_url, download=False
            )
            playlist_name = playlist_info.get("title", "Untitled Playlist")
            channel_name = playlist_info.get("uploader", "Unknown Channel")
            channel_thumb = self._extract_channel_thumbnail(playlist_info)
            entries = playlist_info.get("entries", [])
            playlist_count = playlist_info.get("playlist_count", len(entries))

            # NEW: Extract metadata available from yt-dlp
            description = playlist_info.get("description", "")
            published = playlist_info.get("upload_date", "")

            logger.info(f"Playlist: {playlist_name} | Published: {published}")

            if not entries:
                logger.warning("No entries in playlist.")
                return pl.DataFrame(), playlist_name, channel_name, channel_thumb, {}

            estimate = self.estimate_processing_time(
                playlist_count, expand_all=(max_expanded is None)
            )
            logger.info(
                f"Processing estimate: {estimate.videos_to_expand} videos, "
                f"~{estimate.batch_count} batches, ETA: {estimate}"
            )

            # Build skeleton DataFrame (lightweight info)
            skeleton_rows = [
                {
                    "Rank": idx + 1,
                    "id": e.get("id"),
                    "Title": e.get("title", "N/A"),
                    "Views": e.get("view_count", 0) or 0,
                    "Likes": 0,
                    "Dislikes": 0,
                    "Comments": 0,
                    "Duration": e.get("duration", 0) or 0,
                    "Uploader": e.get("uploader", channel_name),
                    "Thumbnail": e.get("thumbnail", ""),
                    "Rating": None,
                }
                for idx, e in enumerate(entries)
                if e
            ]
            skeleton_df = pl.DataFrame(skeleton_rows)

            # Phase 2: expand videos (all or limited)
            try:
                expanded = await self._fetch_all_video_data(
                    entries, max_expanded, playlist_count, progress_callback
                )
                expanded_df = pl.DataFrame(expanded) if expanded else pl.DataFrame()

                # Merge expanded stats into skeleton (by id)
                if not expanded_df.is_empty():
                    merged = skeleton_df.join(
                        expanded_df, on="id", how="left", suffix="_exp"
                    )

                    # Prefer expanded values where available
                    for col in [
                        "Views",
                        "Likes",
                        "Dislikes",
                        "Comments",
                        "Duration",
                        "Rating",
                    ]:
                        merged = merged.with_columns(
                            pl.coalesce([pl.col(f"{col}_exp"), pl.col(col)]).alias(col)
                        )
                    df = merged.drop([c for c in merged.columns if c.endswith("_exp")])
                else:
                    df = skeleton_df

            except YouTubeBotChallengeError:
                logger.warning(
                    "Persistent bot challenge. Returning skeleton data only."
                )
                df = skeleton_df
            except Exception as e:
                logger.error(f"Expansion failed: {e}. Falling back to skeleton only.")
                df = skeleton_df

            # Finalize with enrichment (stats, formatted columns, etc.)
            df, stats = _enrich_dataframe(df, playlist_count)
            stats["processing_stats"] = self._processing_stats

            # NEW: Add metadata to stats (yt-dlp doesn't have privacy/podcast info)
            stats.update(
                {
                    "description": description,
                    "privacy_status": "Unknown",  # yt-dlp doesn't expose this
                    "published_at": published,
                    "default_language": "",
                    "podcast_status": "unknown",
                }
            )

            df = normalize_columns(df)
            return df, playlist_name, channel_name, channel_thumb, stats

        except YouTubeBotChallengeError:
            logger.error(f"Flat fetch blocked by bot challenge: {playlist_url}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error for {playlist_url}: {e}")
            raise
        finally:
            # Close the dislike client when done
            await self.close()

    async def _fetch_all_video_data(
        self,
        videos: List[Dict[str, Any]],
        max_expanded: Optional[int],
        playlist_count: int,
        progress_callback: callable = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch full metadata and dislike data for videos concurrently.

        Args:
            videos: List of video entries from playlist
            max_expanded: Max videos to process (None = all)
            playlist_count: Total playlist size
            progress_callback: Progress update callback
        """
        # Process all videos if max_expanded is None
        videos_to_process = videos if max_expanded is None else videos[:max_expanded]
        video_urls = [v.get("url") for v in videos_to_process]

        logger.info(
            f"Processing {len(video_urls)} videos in batches of {self.cfg.batch_size}"
        )

        # Use persistent client for dislike API
        client = await self._get_dislike_client()

        all_video_infos = []
        all_dislike_results = []
        start_time = time.time()

        for i in range(0, len(video_urls), self.cfg.batch_size):
            batch_urls = video_urls[i : i + self.cfg.batch_size]
            batch_videos = videos_to_process[i : i + self.cfg.batch_size]
            batch_num = i // self.cfg.batch_size + 1
            total_batches = (len(video_urls) - 1) // self.cfg.batch_size + 1

            logger.info(
                f"Processing batch {batch_num}/{total_batches} "
                f"(videos {i + 1}-{min(i + self.cfg.batch_size, len(video_urls))})"
            )

            # Fetch video info and dislike data for this batch
            video_info_tasks = [self._fetch_video_info_async(url) for url in batch_urls]
            dislike_tasks = [
                self._fetch_dislike_data_async(client, v.get("id", ""))
                for v in batch_videos
                if v.get("id")
            ]

            try:
                batch_video_infos, batch_dislike_results = await asyncio.gather(
                    asyncio.gather(*video_info_tasks, return_exceptions=True),
                    asyncio.gather(*dislike_tasks, return_exceptions=True),
                )

                # Progress update with time estimate
                processed = min(i + len(batch_urls), len(video_urls))
                if progress_callback:
                    elapsed = time.time() - start_time
                    if processed > 0:
                        estimated_total = (elapsed / processed) * len(video_urls)
                        remaining = estimated_total - elapsed
                        await progress_callback(
                            processed,
                            playlist_count,
                            {
                                "elapsed": elapsed,
                                "remaining": remaining,
                                "batch": batch_num,
                                "total_batches": total_batches,
                            },
                        )

                # Filter out exceptions and add to results
                all_video_infos.extend(
                    [
                        info
                        for info in batch_video_infos
                        if not isinstance(info, Exception) and info
                    ]
                )
                all_dislike_results.extend(
                    [
                        result
                        for result in batch_dislike_results
                        if not isinstance(result, Exception)
                    ]
                )

            except Exception as e:
                logger.error(f"Error processing batch {batch_num}: {e}")
                # Continue with next batch instead of failing completely

            # Add delay between batches (except for the last one)
            if i + self.cfg.batch_size < len(video_urls):
                batch_delay = random.uniform(
                    self.cfg.min_batch_delay, self.cfg.max_batch_delay
                )
                logger.debug(f"Waiting {batch_delay:.2f}s before next batch")
                await asyncio.sleep(batch_delay)

        # Build dislike map
        dislike_map = {vid: data for vid, data in all_dislike_results}

        # Combine results
        combined = []
        for rank, vi in enumerate(all_video_infos, start=1):
            if not vi or isinstance(vi, Exception):
                continue
            vid = vi.get("id", "")
            dd = dislike_map.get(vid, {})
            combined.append(
                {
                    "Rank": rank,
                    "id": vid,
                    "Title": vi.get("title", "N/A"),
                    "Views": vi.get("view_count", 0),
                    "Likes": dd.get("likes", vi.get("like_count", 0)),
                    "Dislikes": dd.get("dislikes", 0),
                    "Comments": vi.get("comment_count", 0),
                    "Duration": vi.get("duration", 0),
                    "Uploader": vi.get("uploader", "N/A"),
                    "Thumbnail": vi.get("thumbnail", ""),
                    "Rating": dd.get("rating"),
                }
            )

        logger.info(
            f"Processing complete. Successfully processed {len(combined)}/{len(video_urls)} videos. "
            f"Stats: {self._processing_stats}"
        )

        return combined

    async def _fetch_video_info_async(
        self, video_url: str, retry_count: int = 0
    ) -> Dict[str, Any]:
        """Fetch full metadata for a single video asynchronously with retry logic."""
        try:
            # Add random delay between video fetches to appear more human-like
            delay = random.uniform(self.cfg.min_video_delay, self.cfg.max_video_delay)
            await asyncio.sleep(delay)

            return await asyncio.to_thread(
                self.ydl.extract_info, video_url, download=False
            )
        except Exception as e:
            error_str = str(e)

            # Check for bot challenge
            if "Sign in to confirm you're not a bot" in error_str or any(
                keyword in error_str.lower()
                for keyword in [
                    "captcha",
                    "verify",
                    "unusual traffic",
                    "automated requests",
                ]
            ):
                self._processing_stats["bot_challenges"] += 1
                if retry_count < self.cfg.max_retries:
                    wait_time = self.cfg.retry_delay * (2**retry_count)
                    logger.warning(
                        f"Bot challenge for {video_url}. "
                        f"Waiting {wait_time}s before retry (attempt {retry_count + 1}/{self.cfg.max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    # Rotate user agent on retry
                    self.ydl.params["user-agent"] = self._get_random_user_agent()
                    return await self._fetch_video_info_async(
                        video_url, retry_count + 1
                    )
                else:
                    logger.error(
                        f"Bot challenge persists after {self.cfg.max_retries} retries for {video_url}"
                    )
                    raise YouTubeBotChallengeError(
                        f"YouTube bot challenge after {self.cfg.max_retries} retries"
                    ) from e

            # Retry on other errors
            if retry_count < self.cfg.max_retries:
                self._processing_stats["total_retries"] += 1
                logger.warning(
                    f"Failed to fetch {video_url}: {e}. "
                    f"Retrying (attempt {retry_count + 1}/{self.cfg.max_retries})"
                )
                await asyncio.sleep(self.cfg.retry_delay)
                return await self._fetch_video_info_async(video_url, retry_count + 1)

            logger.warning(
                f"Failed to expand video {video_url} after {self.cfg.max_retries} retries: {e}"
            )
            self._processing_stats["failed_videos"] += 1
            return {}

    async def _fetch_dislike_data_async(
        self, client: httpx.AsyncClient, video_id: str, retry_count: int = 0
    ) -> Tuple[str, Dict[str, Any]]:
        """Fetch dislike data for a video asynchronously with retry logic."""
        try:
            # Add small random delay between dislike API requests
            await asyncio.sleep(random.uniform(0.1, 0.3))

            response = await client.get(DISLIKE_API_URL.format(video_id))
            if response.status_code == 200:
                data = response.json()
                return video_id, {
                    "dislikes": data.get("dislikes", 0),
                    "likes": data.get("likes", 0),
                    "rating": data.get("rating"),
                    "viewCount_api": data.get("viewCount"),
                    "deleted": data.get("deleted", False),
                }
            elif response.status_code == 429:  # Rate limited
                if retry_count < self.cfg.max_retries:
                    self._processing_stats["rate_limits"] += 1
                    wait_time = self.cfg.retry_delay * (
                        2**retry_count
                    )  # Exponential backoff
                    logger.warning(
                        f"Rate limited on dislike API for {video_id}. "
                        f"Retrying in {wait_time}s (attempt {retry_count + 1}/{self.cfg.max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    return await self._fetch_dislike_data_async(
                        client, video_id, retry_count + 1
                    )

            logger.warning(
                f"Failed to fetch dislike data for {video_id}: HTTP {response.status_code}"
            )
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.PoolTimeout) as e:
            if retry_count < self.cfg.max_retries:
                self._processing_stats["total_retries"] += 1
                logger.warning(
                    f"Timeout for video {video_id}. "
                    f"Retrying (attempt {retry_count + 1}/{self.cfg.max_retries})"
                )
                await asyncio.sleep(self.cfg.retry_delay)
                return await self._fetch_dislike_data_async(
                    client, video_id, retry_count + 1
                )
            logger.warning(
                f"Dislike fetch timeout for video {video_id} after {self.cfg.max_retries} retries"
            )
        except Exception as e:
            logger.warning(f"Dislike fetch failed for video {video_id}: {e}")

        return video_id, {
            "dislikes": 0,
            "likes": 0,
            "rating": None,
            "viewCount_api": None,
            "deleted": False,
        }


# ============================================================================
# YouTube Data API Backend Implementation
# ============================================================================


class YouTubeBackendAPI(YouTubeBackendBase):
    """Self-contained YouTube Data API v3 backend implementation.
    - Validates data at every step
    - Returns valid (but empty) structures on failure
    - Logs detailed diagnostic info
    - Never returns None for DataFrames
    """

    YOUTUBE_API_MAX_RESULTS = 50

    def __init__(self, cfg: YouTubeConfig = None):
        super().__init__(cfg)

        if build is None:
            raise ImportError("google-api-python-client is not installed.")

        YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")  # for YouTube Data API
        if not YOUTUBE_API_KEY:
            raise ValueError("YOUTUBE_API_KEY environment variable not set.")

        self.youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    def _create_empty_dataframe(self) -> pl.DataFrame:
        """
        Create a valid empty DataFrame with all required columns.
        This ensures consistency when playlists have no videos.
        """
        return pl.DataFrame(
            {
                "Rank": [],
                "id": [],
                "Title": [],
                "Views": [],
                "Likes": [],
                "Dislikes": [],
                "Comments": [],
                "Duration": [],
                "Uploader": [],
                "Thumbnail": [],
                "Rating": [],
            }
        )

    def _create_empty_stats(self, actual_count: int = 0) -> Dict[str, Any]:
        """Create default stats for empty playlists."""
        return {
            "total_views": 0,
            "total_likes": 0,
            "total_dislikes": 0,
            "total_comments": 0,
            "avg_engagement": 0.0,
            "actual_playlist_count": actual_count,
            "processed_video_count": 0,
            "backend": "youtubeapi",
        }

    def _extract_playlist_id(self, playlist_url: str) -> str:
        """Extract playlist ID from URL with validation."""
        m = re.search(r"list=([a-zA-Z0-9_-]+)", playlist_url)
        if not m:
            raise ValueError(f"Invalid playlist URL: {playlist_url}")
        return m.group(1)

    async def get_playlist_preview(
        self, playlist_url: str
    ) -> Tuple[str, str, str, int, str, str, str]:
        """Get lightweight playlist preview."""
        try:
            playlist_id = self._extract_playlist_id(playlist_url)

            # Fetch with status and localized fields
            resp = (
                self.youtube.playlists()
                .list(
                    part="snippet,contentDetails,status", id=playlist_id, maxResults=1
                )
                .execute()
            )
            if not resp["items"]:
                logger.warning(f"Playlist not found: {playlist_id}")
                return "Preview unavailable", "", "", 0, "", "Unknown", ""

            item = resp["items"][0]
            sn = item["snippet"]
            cd = item.get("contentDetails", {})
            status = item.get("status", {})

            title = sn.get("title", "Untitled Playlist")
            channel = sn.get("channelTitle", "Unknown Channel")
            thumb = sn.get("thumbnails", {}).get("high", {}).get("url", "")
            length = cd.get("itemCount", 0)
            description = sn.get("description", "")
            privacy_status = status.get("privacyStatus", "Unknown")
            published = sn.get("publishedAt", "")

            return title, channel, thumb, length, description, privacy_status, published

        except Exception as e:
            logger.error(f"Failed to fetch playlist preview for {playlist_url}: {e}")
            return "Preview unavailable", "", "", 0, "", "Unknown", ""

    async def get_playlist_data(
        self,
        playlist_url: str,
        max_expanded: Optional[int],
        progress_callback: Optional[callable],
    ) -> Tuple[pl.DataFrame, str, str, str, Dict[str, Any]]:
        """
        YouTube Data API implementation with full playlist support.
        Fetch complete playlist data with extended metadata.

        Returns valid (possibly empty) structures even on failure.
        Never returns None for DataFrames.
        """
        playlist_id = None
        playlist_name = "Unknown Playlist"
        channel_name = "Unknown Channel"
        channel_thumb = ""

        try:
            # ==================== Step 1: Extract Playlist ID ====================
            playlist_id = self._extract_playlist_id(playlist_url)
            logger.info(f"[YouTubeAPI] Fetching playlist: {playlist_id}")

            # ==================== Step 2: Fetch Playlist Metadata ====================
            playlist_metadata = await self._fetch_playlist_metadata(playlist_id)
            if not playlist_metadata:
                logger.error(f"Failed to fetch metadata for playlist {playlist_id}")
                return (
                    self._create_empty_dataframe(),
                    playlist_name,
                    channel_name,
                    channel_thumb,
                    self._create_empty_stats(),
                )

            # Unpack metadata
            playlist_name = playlist_metadata["playlist_name"]
            channel_name = playlist_metadata["channel_name"]
            channel_thumb = playlist_metadata["channel_thumb"]
            total_count = playlist_metadata["total_count"]

            logger.info(
                f"[YouTubeAPI] Playlist: {playlist_name} | "
                f"Channel: {channel_name} | Videos: {total_count}"
            )

            # ==================== Step 3: Fetch Video IDs ====================
            video_ids = await self._fetch_video_ids(
                playlist_id, max_expanded, progress_callback
            )

            if not video_ids:
                logger.warning(
                    f"[YouTubeAPI] Playlist {playlist_id} has no accessible videos. "
                    f"This could be due to: age restrictions, privacy settings, "
                    f"deleted videos, or geographic restrictions."
                )

                # Return empty but valid structure with metadata
                empty_stats = self._create_empty_stats(total_count)
                empty_stats.update(playlist_metadata["extra_metadata"])

                return (
                    self._create_empty_dataframe(),
                    playlist_name,
                    channel_name,
                    channel_thumb,
                    empty_stats,
                )

            logger.info(f"[YouTubeAPI] Found {len(video_ids)} accessible videos")

            # ==================== Step 4: Fetch Video Details ====================
            videos = await self._fetch_video_details(video_ids, progress_callback)

            if not videos:
                logger.warning(
                    f"[YouTubeAPI] Failed to fetch details for any videos in {playlist_id}"
                )
                empty_stats = self._create_empty_stats(total_count)
                empty_stats.update(playlist_metadata["extra_metadata"])

                return (
                    self._create_empty_dataframe(),
                    playlist_name,
                    channel_name,
                    channel_thumb,
                    empty_stats,
                )

            logger.info(
                f"[YouTubeAPI] Successfully fetched {len(videos)} video details"
            )

            # ==================== Step 5: Create and Enrich DataFrame ====================
            df = self._create_dataframe_from_videos(videos)

            # Validate DataFrame creation
            if df is None or not isinstance(df, pl.DataFrame):
                logger.error(
                    f"[YouTubeAPI] DataFrame creation failed for {playlist_id}"
                )
                df = self._create_empty_dataframe()

            # Normalize and enrich
            df = normalize_columns(df)
            df, stats = _enrich_dataframe(df, total_count)

            # Add playlist-level metadata
            stats.update(playlist_metadata["extra_metadata"])
            stats["backend"] = "youtubeapi"
            stats["analyzed_videos"] = len(videos)

            # Add enhanced metadata if videos were processed
            if len(videos) > 0:
                stats["videos_with_captions"] = len(
                    [v for v in videos if v.get("Caption")]
                )
                stats["hd_videos_count"] = len(
                    [v for v in videos if v.get("Definition") == "hd"]
                )
                stats["tags_list"] = extract_all_tags(videos)
                stats["categories"] = extract_categories(videos)

            logger.info(f"[YouTubeAPI] Processing complete for {playlist_id}")

            return df, playlist_name, channel_name, channel_thumb, stats

        except ValueError as e:
            # Invalid URL or playlist ID
            logger.error(f"[YouTubeAPI] Invalid input: {e}")
            return (
                self._create_empty_dataframe(),
                playlist_name,
                channel_name,
                channel_thumb,
                self._create_empty_stats(),
            )

        except Exception as e:
            # Catch-all for unexpected errors
            logger.exception(
                f"[YouTubeAPI] Unexpected error processing {playlist_id}: {e}"
            )
            return (
                self._create_empty_dataframe(),
                playlist_name,
                channel_name,
                channel_thumb,
                self._create_empty_stats(),
            )

    async def _fetch_video_details(
        self,
        video_ids: List[str],
        progress_callback: Optional[callable],
    ) -> List[Dict[str, Any]]:
        """Fetch detailed video information in batches."""
        videos = []

        try:
            for i in range(0, len(video_ids), self.YOUTUBE_API_MAX_RESULTS):
                batch = video_ids[i : i + self.YOUTUBE_API_MAX_RESULTS]
                batch_num = i // self.YOUTUBE_API_MAX_RESULTS + 1

                logger.debug(
                    f"[YouTubeAPI] Fetching batch {batch_num} ({len(batch)} videos)"
                )

                resp = (
                    self.youtube.videos()
                    .list(
                        part="snippet,statistics,contentDetails",
                        id=",".join(batch),
                    )
                    .execute()
                )

                items = resp.get("items", [])
                if not items:
                    logger.warning(f"[YouTubeAPI] Batch {batch_num} returned no items")
                    continue

                for idx, it in enumerate(items, start=i + 1):
                    video_data = self._parse_video_item(it, idx)
                    if video_data:
                        videos.append(video_data)

                # Progress callback
                if progress_callback:
                    await progress_callback(len(videos), len(video_ids), {})

            return videos

        except Exception as e:
            logger.error(f"Failed to fetch video details: {e}")
            return videos  # Return partial results

    async def _fetch_playlist_metadata(
        self, playlist_id: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch and parse playlist metadata."""
        try:
            resp = (
                self.youtube.playlists()
                .list(
                    part="snippet,contentDetails,status",
                    id=playlist_id,
                    maxResults=1,
                )
                .execute()
            )

            if not resp.get("items"):
                logger.error(f"Playlist not found: {playlist_id}")
                return None

            item = resp["items"][0]
            sn = item.get("snippet", {})
            cd = item.get("contentDetails", {})
            status = item.get("status", {})

            return {
                "playlist_name": sn.get("title", "Untitled Playlist"),
                "channel_name": sn.get("channelTitle", "Unknown Channel"),
                "channel_id": sn.get("channelId", ""),
                "channel_thumb": sn.get("thumbnails", {})
                .get("high", {})
                .get("url", ""),
                "total_count": cd.get("itemCount", 0),
                "extra_metadata": {
                    "description": sn.get("description", ""),
                    "privacy_status": status.get("privacyStatus", "Unknown"),
                    "published_at": sn.get("publishedAt", ""),
                    "default_language": sn.get("defaultLanguage", ""),
                    "podcast_status": status.get("podcastStatus", "disabled"),
                    "playlist_id": playlist_id,
                },
            }

        except Exception as e:
            logger.error(f"Failed to fetch metadata for {playlist_id}: {e}")
            return None

    async def _fetch_video_ids(
        self,
        playlist_id: str,
        max_expanded: Optional[int],
        progress_callback: Optional[callable],
    ) -> List[str]:
        """Fetch all video IDs from playlist with pagination."""
        video_ids = []
        nextPageToken = None
        page_count = 0

        try:
            while max_expanded is None or len(video_ids) < max_expanded:
                page_count += 1

                items_resp = (
                    self.youtube.playlistItems()
                    .list(
                        part="contentDetails",
                        playlistId=playlist_id,
                        maxResults=min(
                            self.YOUTUBE_API_MAX_RESULTS,
                            (
                                max_expanded - len(video_ids)
                                if max_expanded is not None
                                else self.YOUTUBE_API_MAX_RESULTS
                            ),
                        ),
                        pageToken=nextPageToken,
                    )
                    .execute()
                )

                items = items_resp.get("items", [])
                if not items:
                    logger.warning(
                        f"[YouTubeAPI] No items returned on page {page_count} for {playlist_id}"
                    )
                    break

                for it in items:
                    video_id = it.get("contentDetails", {}).get("videoId")
                    if video_id:
                        video_ids.append(video_id)

                logger.debug(
                    f"[YouTubeAPI] Page {page_count}: fetched {len(items)} video IDs "
                    f"(total: {len(video_ids)})"
                )

                nextPageToken = items_resp.get("nextPageToken")
                if not nextPageToken:
                    break

            return video_ids

        except Exception as e:
            logger.error(f"Failed to fetch video IDs for {playlist_id}: {e}")
            return video_ids  # Return partial results

    def _parse_video_item(
        self, item: Dict[str, Any], rank: int
    ) -> Optional[Dict[str, Any]]:
        """Parse a single video item from API response."""
        try:
            video_id = item.get("id", "")
            sn = item.get("snippet", {})
            stats = item.get("statistics", {})
            cd = item.get("contentDetails", {})

            return {
                "Rank": rank,
                "id": video_id,
                "Title": sn.get("title", "N/A"),
                "Description": sn.get("description", ""),
                "Views": int(stats.get("viewCount", 0)),
                "Likes": int(stats.get("likeCount", 0)),
                "Dislikes": 0,  # YouTube API doesn't expose dislikes
                "Comments": int(stats.get("commentCount", 0)),
                "Duration": self._parse_iso8601_duration(cd.get("duration", "PT0S")),
                "PublishedAt": sn.get("publishedAt", ""),
                "Uploader": sn.get("channelTitle", "N/A"),
                "Thumbnail": sn.get("thumbnails", {}).get("high", {}).get("url", ""),
                "Tags": sn.get("tags", []),
                "CategoryId": sn.get("categoryId", ""),
                "CategoryName": get_category_name(sn.get("categoryId", "")),
                "Caption": cd.get("caption", "false").lower() == "true",
                "Licensed": cd.get("licensedContent", False),
                "Definition": cd.get("definition", "sd"),
                "Dimension": cd.get("dimension", "2d"),
                "Rating": None,
            }
        except Exception as e:
            logger.warning(
                f"Failed to parse video item {item.get('id', 'unknown')}: {e}"
            )
            return None

    def _create_dataframe_from_videos(
        self, videos: List[Dict[str, Any]]
    ) -> pl.DataFrame:
        """
        Create DataFrame from video list with validation.
        Always returns a valid DataFrame (possibly empty).
        """
        try:
            if not videos:
                logger.warning("[YouTubeAPI] No videos to create DataFrame from")
                return self._create_empty_dataframe()

            df = pl.DataFrame(videos)

            # Validate DataFrame
            if df is None or not isinstance(df, pl.DataFrame):
                logger.error("[YouTubeAPI] DataFrame creation returned invalid type")
                return self._create_empty_dataframe()

            if df.is_empty():
                logger.warning("[YouTubeAPI] Created empty DataFrame from videos")
                return self._create_empty_dataframe()

            logger.info(
                f"[YouTubeAPI] Created DataFrame with {df.height} rows, {df.width} columns"
            )
            return df

        except Exception as e:
            logger.error(f"[YouTubeAPI] Failed to create DataFrame: {e}")
            return self._create_empty_dataframe()

    def _parse_iso8601_duration(self, duration: str) -> int:
        """Parse ISO 8601 duration string to seconds."""
        try:
            return int(isodate.parse_duration(duration).total_seconds())
        except (isodate.ISO8601Error, TypeError, ValueError) as e:
            logger.error(f"Failed to parse ISO8601 duration '{duration}': {e}")
            return 0


# ============================================================================
# Main Service Class (Facade)
# ============================================================================


class YoutubePlaylistService:
    """
    Service facade for fetching and processing YouTube playlist data.
    Delegates to backend implementations.
    """

    DISPLAY_HEADERS = [
        "Rank",
        "Title",
        "Views",
        "Likes",
        "Dislikes",
        "Comments",
        "Duration",
        "Engagement Rate",
        "Controversy",
        "Rating",
    ]

    def __init__(self, backend: str = "youtubeapi", ydl_opts: dict = None, config=None):
        """
        Initialize the service with optional yt-dlp options.
        Args:
            backend: "yt-dlp" or "youtubeapi"
            ydl_opts: Custom yt-dlp options (only used for yt-dlp backend)
            config: YouTubeConfig instance
        """
        self.backend = backend
        self.cfg = config or YouTubeConfig()

        if backend == "yt-dlp" and yt_dlp is None:
            raise ImportError("yt-dlp is required for the yt-dlp backend")
        if backend == "youtubeapi" and build is None:
            raise ImportError(
                "google-api-python-client is required for the youtubeapi backend"
            )

        # Persistent HTTP client for dislike API
        self._dislike_client = None
        self._failed_videos = []  # Track failed videos for retry
        self._processing_stats = {
            "total_retries": 0,
            "failed_videos": 0,
            "bot_challenges": 0,
            "rate_limits": 0,
        }

        if backend == "yt-dlp":
            self.handler = YouTubeBackendYTDLP(self.cfg, ydl_opts)
            self.ydl = self.handler.ydl  # backward compatible
        elif backend == "youtubeapi":
            self.handler = YouTubeBackendAPI()
            self.youtube = self.handler.youtube  # backward compatible
        else:
            raise ValueError("backend must be 'yt-dlp' or 'youtubeapi'")

    # --------------------------
    # Public entrypoints
    # --------------------------

    async def get_playlist_data(
        self,
        playlist_url: str,
        max_expanded: Optional[int] = None,
        progress_callback: Optional[callable] = None,
    ) -> Tuple[pl.DataFrame, str, str, str, Dict[str, float]]:
        """
        Fetch playlist data with optional limit on expanded videos.

        Args:
            playlist_url: YouTube playlist URL
            max_expanded: Maximum videos to fully process. None = process all videos
            progress_callback: Optional callback for progress updates
        Returns:
            Tuple of (DataFrame, playlist_name, channel_name, channel_thumb, stats)
        """
        return await self.handler.get_playlist_data(
            playlist_url, max_expanded, progress_callback
        )

    async def get_playlist_preview(
        self, playlist_url: str
    ) -> Tuple[str, str, str, int, str, str, str]:
        """
        Extract lightweight playlist preview.

        Args:
            playlist_url: YouTube playlist URL

        Returns:
            Tuple of (title, channel, thumbnail, length, description, privacy_status, published_date)
        """
        return await self.handler.get_playlist_preview(playlist_url)

    async def close(self):
        """Close any persistent connections."""
        await self.handler.close()

    @classmethod
    def get_display_headers(cls) -> List[str]:
        """Get the standard display headers for playlist data."""
        return cls.DISPLAY_HEADERS
