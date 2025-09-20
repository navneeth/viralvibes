import logging
import random

import yt_dlp

from constants import KNOWN_PLAYLISTS, SEARCH_QUERIES

# Get logger instance
logger = logging.getLogger(__name__)


def get_random_remix_playlist(max_results: int = 50) -> dict:
    """
    Get a random remix playlist from YouTube search results.

    Args:
        max_results (int): Maximum number of search results to fetch

    Returns:
        dict: Playlist information including title, URL, video count, etc.

    Raises:
        ValueError: If no playlists can be found after trying all methods
    """
    # First, try to find playlists through search (though this rarely works)
    try:
        playlist = _try_search_for_playlists(max_results)
        if playlist:
            return playlist
    except (
        yt_dlp.utils.DownloadError,
        yt_dlp.utils.ExtractorError,
        ConnectionError,
        TimeoutError,
        OSError,
    ) as e:
        logger.warning(f"Search method failed: {e}")

    # Fallback to known playlists
    if not KNOWN_PLAYLISTS:
        raise ValueError("No fallback playlists available")

    logger.info("Search failed, using fallback playlist.")
    return random.choice(KNOWN_PLAYLISTS)


def _try_search_for_playlists(max_results: int) -> dict:
    """
    Try to find playlists through YouTube search (rarely successful).

    Args:
        max_results (int): Maximum number of search results to fetch

    Returns:
        dict: Playlist information if found, None otherwise
    """
    queries = SEARCH_QUERIES[:]
    random.shuffle(queries)

    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "skip_download": True,
        "noplaylist": False,
        "default_search": "ytsearch",
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for query in queries:
            try:
                search_url = f"ytsearch{max_results}:{query}"
                info = ydl.extract_info(search_url, download=False)
                entries = info.get("entries", [])

                # Log entry structures for debugging
                for entry in entries:
                    logger.debug(f"yt_dlp entry structure: {entry}")

                # Robust playlist detection
                playlists = []
                for entry in entries:
                    # Multiple ways to detect playlists
                    is_playlist_type = entry.get("_type") == "playlist"
                    url_is_playlist = "playlist" in entry.get("url", "")
                    id_is_playlist = str(entry.get("id", "")).startswith("PL")

                    if is_playlist_type or url_is_playlist or id_is_playlist:
                        playlists.append(entry)
                    else:
                        logger.debug(f"Entry not detected as playlist: {entry}")

                if playlists:
                    selected = random.choice(playlists)
                    logger.info(
                        f"Found playlist via search: '{selected.get('title', 'Unknown')}'"
                    )

                    # Safe access to 'id' field with fallback
                    playlist_id = selected.get("id")
                    if not playlist_id:
                        logger.warning(f"Playlist entry missing 'id' field: {selected}")
                        continue

                    return {
                        "title": selected.get("title"),
                        "url": f"https://www.youtube.com/playlist?list={playlist_id}",
                        "video_count": selected.get("video_count"),
                        "channel": selected.get("uploader"),
                        "query_used": query,
                    }

            except (
                yt_dlp.utils.DownloadError,
                yt_dlp.utils.ExtractorError,
                ConnectionError,
                TimeoutError,
                OSError,
            ) as e:
                logger.warning(f"Search attempt failed for query '{query}': {e}")
                continue

    logger.info("YouTube search did not return any playlists.")
    return None


if __name__ == "__main__":
    # To make the script less verbose, we'll only show warnings and errors.
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

    try:
        playlist = get_random_remix_playlist(max_results=5)
        print("🎧 Random Remix Playlist:")
        print(f"   Title: {playlist['title']}")
        print(f"   URL: {playlist['url']}")
        print(f"   Videos: {playlist['video_count']}")
        print(f"   Uploader: {playlist['channel']}")
        print(f"   Source: {playlist['query_used']}")
    except ValueError as e:
        print(f"❌ Error: {e}")
    except (
        yt_dlp.utils.DownloadError,
        yt_dlp.utils.ExtractorError,
        ConnectionError,
        TimeoutError,
        OSError,
    ) as e:
        print(f"❌ YouTube/Network error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        logger.exception("Unexpected error in main")
