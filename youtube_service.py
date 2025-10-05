# youtube_service.py

"""
YouTube Service for fetching and processing YouTube playlist data.
This module provides functionality to fetch and process YouTube playlist data using yt-dlp.
Enhanced with full playlist processing, resilience features, and time estimation.
"""

import asyncio
import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx
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

import isodate

from utils import calculate_engagement_rate, format_duration, format_number

# Get logger instance
logger = logging.getLogger(__name__)

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")  # for YouTube Data API
DISLIKE_API_URL = "https://returnyoutubedislikeapi.com/votes?videoId={}"

# Configuration for rate limiting
MIN_VIDEO_DELAY = float(os.getenv("MIN_VIDEO_DELAY", "0.5"))
MAX_VIDEO_DELAY = float(os.getenv("MAX_VIDEO_DELAY", "2.0"))
MIN_BATCH_DELAY = float(os.getenv("MIN_BATCH_DELAY", "1.0"))
MAX_BATCH_DELAY = float(os.getenv("MAX_BATCH_DELAY", "3.0"))

# Resilience configuration
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = float(os.getenv("RETRY_DELAY", "5.0"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))


@dataclass
class ProcessingEstimate:
    """Estimate for processing time and resources."""

    total_videos: int
    videos_to_expand: int
    estimated_seconds: float
    estimated_minutes: float
    batch_count: int

    def __str__(self):
        if self.estimated_minutes < 1:
            return f"~{int(self.estimated_seconds)} seconds"
        elif self.estimated_minutes < 60:
            return f"~{int(self.estimated_minutes)} minutes"
        else:
            hours = self.estimated_minutes / 60
            return f"~{hours:.1f} hours"


class YouTubeBotChallengeError(Exception):
    """Raised when YouTube serves a validation or CAPTCHA page."""


class YouTubeServiceError(Exception):
    """Base exception for YouTube service errors."""


class RateLimitError(YouTubeServiceError):
    """Raised when rate limits are exceeded."""


def parse_yt_dlp_output(raw_output: str):
    try:
        return json.loads(raw_output)
    except json.JSONDecodeError:
        # Likely HTML instead of JSON
        if "<html" in raw_output.lower():
            if "captcha" in raw_output.lower() or "consent.youtube.com" in raw_output:
                raise YouTubeBotChallengeError("YouTube served a validation page.")
        raise  # propagate other JSON errors


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


class YoutubePlaylistService:
    """Service for fetching and processing YouTube playlist data."""

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
    YOUTUBE_API_MAX_RESULTS = 50

    def __init__(self, backend: str = "youtubeapi", ydl_opts: dict = None):
        """
        Initialize the service with optional yt-dlp options.
        Args:
            ydl_opts (dict): Custom yt-dlp options. If None, uses default options.
            backend: "yt-dlp" or "youtubeapi"
        """
        self.backend = backend
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
            if yt_dlp is None:
                raise ImportError("yt-dlp is not installed.")
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
                "retries": MAX_RETRIES,
                "fragment_retries": MAX_RETRIES,
            }

            self.ydl_opts = base_opts.copy()
            if ydl_opts:
                self.ydl_opts.update(ydl_opts)

            cookies_file = os.getenv("COOKIES_FILE")
            if cookies_file and os.path.exists(cookies_file):
                logger.info(f"Using cookies from file: {cookies_file}")
                self.ydl_opts["cookiefile"] = cookies_file
            elif cookies_file:
                logger.warning(
                    f"COOKIES_FILE set, but file not found at: {cookies_file}"
                )
            self.ydl = yt_dlp.YoutubeDL(self.ydl_opts)

        elif backend == "youtubeapi":
            if build is None:
                raise ImportError("google-api-python-client is not installed.")
            if not YOUTUBE_API_KEY:
                raise ValueError("YOUTUBE_API_KEY environment variable not set.")
            self.youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

        else:
            raise ValueError("backend must be 'yt-dlp' or 'youtubeapi'")

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
                # Add retry transport
                transport=httpx.AsyncHTTPTransport(retries=MAX_RETRIES),
            )
        return self._dislike_client

    async def close(self):
        """Close the persistent HTTP client."""
        if self._dislike_client and not self._dislike_client.is_closed:
            await self._dislike_client.aclose()
            self._dislike_client = None

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
        avg_video_fetch_time = (MIN_VIDEO_DELAY + MAX_VIDEO_DELAY) / 2
        avg_dislike_fetch_time = 0.2
        avg_batch_delay = (MIN_BATCH_DELAY + MAX_BATCH_DELAY) / 2

        # Calculate batch processing time
        batch_count = (videos_to_expand + BATCH_SIZE - 1) // BATCH_SIZE

        # Time per batch: max(video_fetch, dislike_fetch) since they're concurrent
        time_per_batch = (
            max(avg_video_fetch_time * BATCH_SIZE, avg_dislike_fetch_time * BATCH_SIZE)
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

    # --------------------------
    # Public entrypoints
    # --------------------------

    async def get_playlist_data(
        self,
        playlist_url: str,
        max_expanded: Optional[int] = None,
        progress_callback: callable = None,
    ) -> Tuple[pl.DataFrame, str, str, str, Dict[str, float]]:
        """
        Fetch playlist data with optional limit on expanded videos.

        Args:
            playlist_url: YouTube playlist URL
            max_expanded: Maximum videos to fully process. None = process all videos
            progress_callback: Optional callback for progress updates
        """
        if self.backend == "yt-dlp":
            return await self._get_playlist_data_ytdlp(
                playlist_url, max_expanded, progress_callback
            )
        elif self.backend == "youtubeapi":
            return await self._get_playlist_data_youtubeapi(
                playlist_url, max_expanded, progress_callback
            )
        else:
            raise ValueError(f"Unsupported backend: {self.backend}")

    async def get_playlist_preview(
        self, playlist_url: str
    ) -> Tuple[str, str, str, int]:
        """
        Extract lightweight playlist name, uploader, and thumbnail.
        Lightweight preview: (playlist_title, channel_name, channel_thumbnail, playlist_length)
        """
        if self.backend == "yt-dlp":
            try:
                info = await asyncio.to_thread(
                    self.ydl.extract_info, playlist_url, download=False
                )
                # Fallbacks in case metadata is sparse
                title = info.get("title", "Untitled Playlist")
                channel = info.get("uploader", "Unknown Channel")
                thumb = self._extract_channel_thumbnail(info)
                length = info.get("playlist_count", len(info.get("entries", [])))
                return title, channel, thumb, length
            except Exception as e:
                logger.warning(f"Failed to fetch yt-dlp preview: {e}")
                return "Preview unavailable", "", "", 0

        elif self.backend == "youtubeapi":
            try:
                m = re.search(r"list=([a-zA-Z0-9_-]+)", playlist_url)
                if not m:
                    raise ValueError("Invalid playlist URL")
                playlist_id = m.group(1)
                resp = (
                    self.youtube.playlists()
                    .list(part="snippet,contentDetails", id=playlist_id, maxResults=1)
                    .execute()
                )
                if not resp["items"]:
                    return "Preview unavailable", "", "", 0
                sn = resp["items"][0]["snippet"]
                cd = resp["items"][0].get("contentDetails", {})
                title = sn.get("title", "Untitled Playlist")
                channel = sn.get("channelTitle", "Unknown Channel")
                thumb = sn.get("thumbnails", {}).get("high", {}).get("url", "")
                length = cd.get("itemCount", 0)
                return title, channel, thumb, length
            except Exception as e:
                logger.warning(f"Failed to fetch API preview: {e}")
                return "Preview unavailable", "", "", 0

    # --------------------------
    # yt-dlp implementation with resilience
    # --------------------------

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
                if retry_count < MAX_RETRIES:
                    self._processing_stats["rate_limits"] += 1
                    wait_time = RETRY_DELAY * (2**retry_count)  # Exponential backoff
                    logger.warning(
                        f"Rate limited on dislike API for {video_id}. "
                        f"Retrying in {wait_time}s (attempt {retry_count + 1}/{MAX_RETRIES})"
                    )
                    await asyncio.sleep(wait_time)
                    return await self._fetch_dislike_data_async(
                        client, video_id, retry_count + 1
                    )

            logger.warning(
                f"Failed to fetch dislike data for {video_id}: HTTP {response.status_code}"
            )
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.PoolTimeout) as e:
            if retry_count < MAX_RETRIES:
                self._processing_stats["total_retries"] += 1
                logger.warning(
                    f"Timeout for video {video_id}. "
                    f"Retrying (attempt {retry_count + 1}/{MAX_RETRIES})"
                )
                await asyncio.sleep(RETRY_DELAY)
                return await self._fetch_dislike_data_async(
                    client, video_id, retry_count + 1
                )
            logger.warning(
                f"Dislike fetch timeout for video {video_id} after {MAX_RETRIES} retries"
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

    async def _fetch_video_info_async(
        self, video_url: str, retry_count: int = 0
    ) -> Dict[str, Any]:
        """Fetch full metadata for a single video asynchronously with retry logic."""
        try:
            # Add random delay between video fetches to appear more human-like
            delay = random.uniform(MIN_VIDEO_DELAY, MAX_VIDEO_DELAY)
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
                if retry_count < MAX_RETRIES:
                    wait_time = RETRY_DELAY * (2**retry_count)
                    logger.warning(
                        f"Bot challenge for {video_url}. "
                        f"Waiting {wait_time}s before retry (attempt {retry_count + 1}/{MAX_RETRIES})"
                    )
                    await asyncio.sleep(wait_time)
                    # Rotate user agent on retry
                    self.ydl.params["user-agent"] = self._get_random_user_agent()
                    return await self._fetch_video_info_async(
                        video_url, retry_count + 1
                    )
                else:
                    logger.error(
                        f"Bot challenge persists after {MAX_RETRIES} retries for {video_url}"
                    )
                    raise YouTubeBotChallengeError(
                        f"YouTube bot challenge after {MAX_RETRIES} retries"
                    ) from e

            # Retry on other errors
            if retry_count < MAX_RETRIES:
                self._processing_stats["total_retries"] += 1
                logger.warning(
                    f"Failed to fetch {video_url}: {e}. "
                    f"Retrying (attempt {retry_count + 1}/{MAX_RETRIES})"
                )
                await asyncio.sleep(RETRY_DELAY)
                return await self._fetch_video_info_async(video_url, retry_count + 1)

            logger.warning(
                f"Failed to expand video {video_url} after {MAX_RETRIES} retries: {e}"
            )
            self._processing_stats["failed_videos"] += 1
            return {}

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

        logger.info(f"Processing {len(video_urls)} videos in batches of {BATCH_SIZE}")

        # Use persistent client for dislike API
        client = await self._get_dislike_client()

        all_video_infos = []
        all_dislike_results = []
        start_time = time.time()

        for i in range(0, len(video_urls), BATCH_SIZE):
            batch_urls = video_urls[i : i + BATCH_SIZE]
            batch_videos = videos_to_process[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(video_urls) - 1) // BATCH_SIZE + 1

            logger.info(
                f"Processing batch {batch_num}/{total_batches} "
                f"(videos {i + 1}-{min(i + BATCH_SIZE, len(video_urls))})"
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
            if i + BATCH_SIZE < len(video_urls):
                batch_delay = random.uniform(MIN_BATCH_DELAY, MAX_BATCH_DELAY)
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

    async def _get_playlist_data_ytdlp(
        self,
        playlist_url: str,
        max_expanded: Optional[int],
        progress_callback: callable = None,
    ) -> Tuple[pl.DataFrame, str, str, str, Dict[str, float]]:
        """
        Fetch and process video information from a YouTube playlist URL.

        Args:
            playlist_url: The URL of the YouTube playlist
            max_expanded: Maximum videos to process (None = all videos)
            progress_callback: Optional callback for progress updates
        """
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

            if not entries:
                logger.warning("No entries in playlist.")
                return pl.DataFrame(), playlist_name, channel_name, channel_thumb, {}

            # Log processing estimate
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
            df, stats = self._enrich_dataframe(df, playlist_count)
            stats["processing_stats"] = self._processing_stats
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

    # --------------------------
    # YouTube Data API implementation (full processing)
    # --------------------------

    async def _get_playlist_data_youtubeapi(
        self,
        playlist_url: str,
        max_expanded: Optional[int],
        progress_callback: callable = None,
    ) -> Tuple[pl.DataFrame, str, str, str, Dict[str, float]]:
        """YouTube Data API implementation with full playlist support."""
        m = re.search(r"list=([a-zA-Z0-9_-]+)", playlist_url)
        if not m:
            raise ValueError("Invalid playlist URL")
        playlist_id = m.group(1)

        # Fetch playlist metadata
        resp = (
            self.youtube.playlists()
            .list(part="snippet,contentDetails", id=playlist_id, maxResults=1)
            .execute()
        )
        if not resp["items"]:
            raise ValueError("Playlist not found")
        sn = resp["items"][0]["snippet"]
        cd = resp["items"][0].get("contentDetails", {})
        playlist_name = sn.get("title", "Untitled Playlist")
        channel_name = sn.get("channelTitle", "Unknown Channel")
        channel_thumb = sn.get("thumbnails", {}).get("high", {}).get("url", "")
        total_count = cd.get("itemCount", 0)

        # Fetch ALL video IDs (or up to max_expanded)
        video_ids, nextPageToken = [], None
        while max_expanded is None or len(video_ids) < max_expanded:
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
            for it in items_resp["items"]:
                video_ids.append(it["contentDetails"]["videoId"])
            nextPageToken = items_resp.get("nextPageToken")
            if not nextPageToken:
                break

        # Fetch video statistics in batches
        videos = []
        for i in range(0, len(video_ids), self.YOUTUBE_API_MAX_RESULTS):
            batch = video_ids[i : i + self.YOUTUBE_API_MAX_RESULTS]
            resp = (
                self.youtube.videos()
                .list(part="snippet,statistics,contentDetails", id=",".join(batch))
                .execute()
            )
            for idx, it in enumerate(resp["items"], start=i + 1):
                stats = it.get("statistics", {})
                sn = it.get("snippet", {})
                cd = it.get("contentDetails", {})
                videos.append(
                    {
                        "Rank": idx,
                        "id": it["id"],
                        "Title": sn.get("title", "N/A"),
                        "Views": int(stats.get("viewCount", 0)),
                        "Likes": int(stats.get("likeCount", 0)),
                        "Dislikes": 0,  # API does not expose
                        "Comments": int(stats.get("commentCount", 0)),
                        "Duration": self._parse_iso8601_duration(
                            cd.get("duration", "PT0S")
                        ),
                        "Uploader": sn.get("channelTitle", "N/A"),
                        "Thumbnail": sn.get("thumbnails", {})
                        .get("high", {})
                        .get("url", ""),
                        "Rating": None,
                    }
                )

        # --- Transform for compatibility ---
        transformed_videos = transform_api_df(videos)

        df = pl.DataFrame(transformed_videos)
        df, stats = self._enrich_dataframe(df, len(video_ids))
        return df, playlist_name, channel_name, channel_thumb, stats

    # --------------------------
    # Common helpers
    # --------------------------

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

    def _parse_iso8601_duration(self, duration: str) -> int:
        try:
            return int(isodate.parse_duration(duration).total_seconds())
        except (isodate.ISO8601Error, TypeError, ValueError) as e:
            logger.error(f"Failed to parse ISO8601 duration '{duration}': {e}")
            return 0

    def _enrich_dataframe(
        self, df: pl.DataFrame, actual_playlist_count: int = None
    ) -> Tuple[pl.DataFrame, Dict[str, Any]]:
        """Calculate summary statistics for the playlist."""
        if df.is_empty():
            return df, {
                "total_views": 0,
                "total_likes": 0,
                "total_dislikes": 0,
                "total_comments": 0,
                "avg_engagement": 0.0,
                "actual_playlist_count": actual_playlist_count or 0,
                "processed_video_count": 0,
            }
        df = df.with_columns(
            [
                (
                    1
                    - (pl.col("Likes") - pl.col("Dislikes")).abs()
                    / (pl.col("Likes") + pl.col("Dislikes"))
                )
                .fill_nan(0.0)
                .alias("Controversy"),
                (
                    (pl.col("Likes") + pl.col("Dislikes") + pl.col("Comments"))
                    / pl.col("Views")
                )
                .fill_nan(0.0)
                .alias("Engagement Rate Raw"),
            ]
        )
        stats = {
            "total_views": df["Views"].sum(),
            "total_likes": df["Likes"].sum(),
            "total_dislikes": df["Dislikes"].sum(),
            "total_comments": df["Comments"].sum(),
            "avg_engagement": df["Engagement Rate Raw"].mean(),
            "actual_playlist_count": actual_playlist_count or df.height,
            "processed_video_count": df.height,
        }
        df = df.with_columns(
            [
                pl.col("Views")
                .map_elements(format_number, return_dtype=pl.String)
                .alias("Views Formatted"),
                pl.col("Likes")
                .map_elements(format_number, return_dtype=pl.String)
                .alias("Likes Formatted"),
                pl.col("Dislikes")
                .map_elements(format_number, return_dtype=pl.String)
                .alias("Dislikes Formatted"),
                pl.col("Comments")
                .map_elements(format_number, return_dtype=pl.String)
                .alias("Comments Formatted"),
                pl.col("Duration")
                .map_elements(format_duration, return_dtype=pl.String)
                .alias("Duration Formatted"),
                pl.col("Controversy")
                .map_elements(lambda x: f"{x:.2%}", return_dtype=pl.String)
                .alias("Controversy Formatted"),
                pl.col("Engagement Rate Raw")
                .map_elements(lambda x: f"{x:.2%}", return_dtype=pl.String)
                .alias("Engagement Rate Formatted"),
            ]
        )
        return df, stats

    @classmethod
    def get_display_headers(cls) -> List[str]:
        return cls.DISPLAY_HEADERS
