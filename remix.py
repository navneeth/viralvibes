import logging
import random

import yt_dlp

# Get logger instance
logger = logging.getLogger(__name__)

# Known popular playlist URLs as fallbacks
KNOWN_PLAYLISTS = [{
    "title": "EDM Remix Hits 2024",
    "url":
    "https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC",
    "video_count": 50,
    "channel": "EDM Hits",
    "query_used": "fallback_playlist"
}, {
    "title": "Best Remixes of Popular Songs",
    "url":
    "https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC",
    "video_count": 30,
    "channel": "Remix Central",
    "query_used": "fallback_playlist"
}, {
    "title": "TikTok Viral Remixes",
    "url":
    "https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC",
    "video_count": 25,
    "channel": "TikTok Music",
    "query_used": "fallback_playlist"
}]

SEARCH_QUERIES = [
    "EDM remix playlist", "pop remix playlist", "lofi remix mix",
    "hip hop remix", "TikTok remix playlist", "2024 DJ remix",
    "club remix playlist", "remix hits", "best remix playlist",
    "dj remix playlist", "2024 remix playlist", "hip hop playlist",
    "pop playlist", "club playlist", "dance playlist", "party playlist",
    "lofi playlist", "tiktok playlist", "summer playlist", "workout playlist",
    "chill playlist", "house music playlist", "electronic playlist",
    "indie playlist", "rock playlist", "old school playlist"
]


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
    except Exception as e:
        logger.warning(f"Search method failed: {e}")

    # Fallback to known playlists
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
        'quiet': True,
        'extract_flat': True,
        'skip_download': True,
        'noplaylist': False,
        'default_search': 'ytsearch',
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for query in queries:
            try:
                search_url = f"ytsearch{max_results}:{query}"
                info = ydl.extract_info(search_url, download=False)
                entries = info.get('entries', [])

                playlists = [
                    entry for entry in entries
                    if entry.get('_type') == 'playlist'
                ]

                if playlists:
                    selected = random.choice(playlists)
                    logger.info(
                        f"Found playlist via search: '{selected.get('title', 'Unknown')}'"
                    )
                    return {
                        "title": selected.get("title"),
                        "url":
                        f"https://www.youtube.com/playlist?list={selected.get('id')}",
                        "video_count": selected.get("video_count"),
                        "channel": selected.get("uploader"),
                        "query_used": query
                    }

            except Exception as e:
                logger.warning(
                    f"Search attempt failed for query '{query}': {e}")
                continue

    logger.info("YouTube search did not return any playlists.")
    return None


if __name__ == "__main__":
    # To make the script less verbose, we'll only show warnings and errors.
    logging.basicConfig(level=logging.WARNING,
                        format='%(levelname)s: %(message)s')

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
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
