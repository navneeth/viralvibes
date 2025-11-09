import asyncio
import logging
import os
import random
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
import polars as pl
import yt_dlp

from services.youtube_backend_base import DISLIKE_API_URL, YouTubeBackendBase
from services.youtube_errors import YouTubeBotChallengeError
from services.youtube_transforms import _enrich_dataframe, normalize_columns


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
