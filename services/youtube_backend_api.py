import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import isodate
import polars as pl
from googleapiclient.discovery import build

from services.config import YouTubeConfig
from services.youtube_backend_base import YouTubeBackendBase
from services.youtube_transforms import _enrich_dataframe, normalize_columns
from services.youtube_utils import (
    extract_all_tags,
    extract_categories,
    get_category_name,
)

# Get logger instance
logger = logging.getLogger(__name__)

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
                "Rank": pl.Series([], dtype=pl.Int64),
                "id": pl.Series([], dtype=pl.Utf8),
                "Title": pl.Series([], dtype=pl.Utf8),
                "Description": pl.Series([], dtype=pl.Utf8),
                "Views": pl.Series([], dtype=pl.Int64),
                "Likes": pl.Series([], dtype=pl.Int64),
                "Dislikes": pl.Series([], dtype=pl.Int64),
                "Comments": pl.Series([], dtype=pl.Int64),
                "Duration": pl.Series([], dtype=pl.Int64),
                "PublishedAt": pl.Series([], dtype=pl.Utf8),
                "Uploader": pl.Series([], dtype=pl.Utf8),
                "Thumbnail": pl.Series([], dtype=pl.Utf8),
                "Tags": pl.Series([], dtype=pl.List(pl.Utf8)),
                "CategoryId": pl.Series([], dtype=pl.Utf8),
                "CategoryName": pl.Series([], dtype=pl.Utf8),
                "Caption": pl.Series([], dtype=pl.Boolean),
                "Licensed": pl.Series([], dtype=pl.Boolean),
                "Definition": pl.Series([], dtype=pl.Utf8),
                "Dimension": pl.Series([], dtype=pl.Utf8),
                "Rating": pl.Series([], dtype=pl.Float64),
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

            df = YouTubeBackendAPI._canonicalize_api_df(df)

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
        """Fetch detailed video information in batches (robust; returns partial results)."""
        videos = []

        try:
            # iterate in batches (max 50 per YouTube API)
            for i in range(0, len(video_ids), self.YOUTUBE_API_MAX_RESULTS):
                batch = video_ids[i : i + self.YOUTUBE_API_MAX_RESULTS]
                batch_num = i // self.YOUTUBE_API_MAX_RESULTS + 1

                logger.debug(
                    f"[YouTubeAPI] Fetching batch {batch_num} ({len(batch)} videos)"
                )

                try:
                    resp = (
                        self.youtube.videos()
                        .list(
                            part="snippet,statistics,contentDetails",
                            id=",".join(batch),
                        )
                        .execute()
                    )
                except Exception as e:
                    # Log and continue with next batch (quota/temporary errors may happen)
                    logger.error(
                        f"[YouTubeAPI] Exception while fetching videos for batch {batch_num}: {e}"
                    )
                    continue

                items = resp.get("items", [])
                if not items:
                    logger.warning(
                        f"[YouTubeAPI] Batch {batch_num} returned no items for ids: {batch}"
                    )
                    # continue to next batch instead of failing — allow partial results
                    if progress_callback:
                        await progress_callback(
                            len(videos), len(video_ids), {"batch": batch_num}
                        )
                    continue

                # parse returned items
                for idx, it in enumerate(items, start=i + 1):
                    video_data = self._parse_video_item(it, idx)
                    if video_data:
                        videos.append(video_data)

                # progress callback
                if progress_callback:
                    await progress_callback(
                        len(videos), len(video_ids), {"batch": batch_num}
                    )

            return videos

        except Exception as e:
            logger.exception(f"[YouTubeAPI] Failed to fetch video details: {e}")
            return videos  # return whatever partial results we have

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
        """Parse a single video item from API response (defensive)."""
        try:
            # id is usually present at top-level for videos.list results
            video_id = item["id"] or item["snippet"]["resourceId"]["videoId"]
            video_url = f"https://youtu.be/{video_id}"
            thumbs = item.get("snippet", {}).get("thumbnails", {})
            thumbnail = (
                thumbs.get("high")
                or thumbs.get("medium")
                or thumbs.get("default")
                or {}
            ).get("url", "")
            sn = item.get("snippet", {}) or {}
            stats = item.get("statistics", {}) or {}
            cd = item.get("contentDetails", {}) or {}

            # safe numeric parsing with fallbacks
            def safe_int(x, default=0):
                try:
                    return int(x)
                except Exception:
                    return default

            title = sn.get("title", "N/A")
            description = sn.get("description", "")
            view_count = safe_int(stats.get("viewCount", 0))
            like_count = safe_int(stats.get("likeCount", 0))
            comment_count = safe_int(stats.get("commentCount", 0))
            duration_iso = cd.get("duration", "PT0S")
            duration_seconds = self._parse_iso8601_duration(duration_iso)

            tags = sn.get("tags", []) or []
            category_id = sn.get("categoryId", "")
            category_name = get_category_name(category_id)

            # caption/captions field handling: contentDetails 'caption' may be boolean-string
            caption_field = cd.get("caption", "false")
            has_caption = False
            if isinstance(caption_field, bool):
                has_caption = caption_field
            elif isinstance(caption_field, str):
                has_caption = caption_field.lower() == "true"

            return {
                "Rank": rank,
                "id": video_id,
                "Title": title,
                "Description": description,
                "Views": view_count,
                "Likes": like_count,
                "Dislikes": 0,  # YouTube API doesn't expose dislikes
                "Comments": comment_count,
                "Duration": duration_seconds,
                "PublishedAt": sn.get("publishedAt", ""),
                "Uploader": sn.get("channelTitle", "N/A"),
                "Thumbnail": sn.get("thumbnails", {}).get("high", {}).get("url", ""),
                "Tags": tags,
                "CategoryId": category_id,
                "CategoryName": category_name,
                "Caption": has_caption,
                "Licensed": cd.get("licensedContent", False),
                "Definition": cd.get("definition", "sd"),
                "Dimension": cd.get("dimension", "2d"),
                "Rating": None,
            }
        except Exception as e:
            logger.warning(
                f"[YouTubeAPI] Failed to parse video item {item.get('id', 'unknown')}: {e}"
            )
            return None

    def _create_dataframe_from_videos(
        self, videos: List[Dict[str, Any]]
    ) -> pl.DataFrame:
        """
        Create DataFrame from video list with validation and consistent columns.
        Always returns a valid DataFrame (possibly empty).
        """
        if not videos:
            return self._create_empty_dataframe()

        try:
            # Build DataFrame; Polars will infer dtypes
            df = pl.DataFrame(videos)

            # Get expected schema from empty template
            template = self._create_empty_dataframe()
            expected_cols = template.schema

            # Add missing columns with appropriate defaults
            for col_name, col_dtype in expected_cols.items():
                if col_name not in df.columns:
                    df = df.with_columns(
                        pl.Series(col_name, [None] * df.height, dtype=col_dtype)
                    )

            # Ensure columns match expected schema and ordering
            df = df.select(list(expected_cols.keys()))

            # Type coercion for numeric columns
            for col_name, col_dtype in expected_cols.items():
                if col_name in df.columns:
                    if col_dtype in (pl.Int64, pl.Int32):
                        df = df.with_columns(
                            pl.col(col_name).fill_null(0).cast(col_dtype)
                        )
                    elif col_dtype == pl.Float64:
                        df = df.with_columns(
                            pl.col(col_name).fill_null(0.0).cast(col_dtype)
                        )

            logger.info(f"[YouTubeAPI] Created DataFrame with {df.height} rows")
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

    @staticmethod
    def _canonicalize_api_df(df: Optional[pl.DataFrame]) -> pl.DataFrame:
        """Canonicalise column names coming directly from YouTube Data API."""
        if df is None or df.is_empty():
            return df

        # Helper function to find column by possible names
        def find_col(possible_names):
            for name in possible_names:
                if name in df.columns:
                    return name
            return None

        # Rename columns to consistent format
        rename_map = {
            # snippet fields
            "snippet.title": "Title",
            "snippet.description": "Description",
            "snippet.thumbnails.default.url": "Thumbnail",
            "snippet.thumbnails.medium.url": "Thumbnail",
            "snippet.thumbnails.high.url": "Thumbnail",
            "snippet.channelTitle": "Uploader",
            "snippet.publishedAt": "PublishedAt",
            "snippet.categoryId": "CategoryId",
            "snippet.tags": "Tags",
            "snippet.resourceId.videoId": "Video ID",
            # statistics fields
            "statistics.viewCount": "Views",
            "statistics.likeCount": "Likes",
            "statistics.dislikeCount": "Dislikes",
            "statistics.commentCount": "Comments",
            # contentDetails fields
            "contentDetails.duration": "Duration",
            "contentDetails.definition": "Definition",
            "contentDetails.dimension": "Dimension",
            "contentDetails.caption": "Caption",
            "contentDetails.licensedContent": "Licensed",
            # other
            "id": "id",
        }
        # 2. Safe Rename: Only rename columns that actually exist in the DF
        existing_cols = set(df.columns)
        valid_renames = {k: v for k, v in rename_map.items() if k in existing_cols}

        if valid_renames:
            df = df.rename(valid_renames)

        # 3. Backfill Missing Columns: Ensure strict schema even if API dropped data
        # This prevents downstream errors when accessing 'Title' or 'Views' later.
        expected_columns = {
            "Title": pl.Utf8,
            "Description": pl.Utf8,
            "PublishedAt": pl.Utf8,
            "Video ID": pl.Utf8,
            "Thumbnail": pl.Utf8,
            "Channel Title": pl.Utf8,
            "Views": pl.Int64,
            "Likes": pl.Int64,
            "Dislikes": pl.Int64,
            "Comments": pl.Int64,
            "Duration": pl.Utf8,
        }

        missing_cols_exprs = []
        for col_name, dtype in expected_columns.items():
            if col_name not in df.columns:
                # Fill missing text cols with "Unknown" or "", numbers with 0
                if dtype == pl.Utf8:
                    fill_val = "Unknown" if col_name == "Title" else None
                    missing_cols_exprs.append(
                        pl.lit(fill_val).cast(dtype).alias(col_name)
                    )
                else:
                    missing_cols_exprs.append(pl.lit(0).cast(dtype).alias(col_name))

        if missing_cols_exprs:
            df = df.with_columns(missing_cols_exprs)

        # 4. Type Casting (Ensure numeric columns are actually numbers)
        # API JSON often returns numbers as strings
        df = df.with_columns(
            [
                pl.col("Views").cast(pl.Int64, strict=False).fill_null(0),
                pl.col("Likes").cast(pl.Int64, strict=False).fill_null(0),
                pl.col("Dislikes").cast(pl.Int64, strict=False).fill_null(0),
                pl.col("Comments").cast(pl.Int64, strict=False).fill_null(0),
            ]
        )

        # after mapping title/rank/counts/duration/etc, add:
        if "id" in df.columns:
            # build short youtube url column reliably
            df = df.with_columns(
                (
                    pl.concat_str(
                        [pl.lit("https://youtu.be/"), pl.col("id").cast(pl.Utf8)]
                    )
                ).alias("video_url")
            )
        # normalize thumbnail column name
        thumb_src = find_col(["Thumbnail", "thumbnail", "thumbnailUrl"])
        if thumb_src:
            df = df.with_columns(pl.col(thumb_src).cast(pl.Utf8).alias("thumbnail"))

        return df
