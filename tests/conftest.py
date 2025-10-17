"""
Pytest configuration for ViralVibes tests.
"""

import importlib
import os
import sys
from typing import Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest
from dotenv import load_dotenv

# Ensure project root is importable
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Load .env ONCE (safe no-op)
load_dotenv()

# ✅ ONE DEFAULT SYSTEM - NO CONFLICTS
os.environ.setdefault("YOUTUBE_API_KEY", "mock-api-key-for-testing")
os.environ.setdefault("NEXT_PUBLIC_SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("NEXT_PUBLIC_SUPABASE_ANON_KEY", "anon-key-for-tests")

# Minimal contract mapping: modules -> expected attributes (tests can assert against this)
DEFAULT_EXPECTED_EXPORTS: Dict[str, List[str]] = {
    "worker.worker": ["Worker", "handle_job"],
    "db": [
        "upsert_playlist_stats",
        "get_cached_playlist_stats",
        "PLAYLIST_STATS_TABLE",
    ],
    "services.youtube_service": ["YoutubePlaylistService", "YouTubeBotChallengeError"],
    "constants": ["PLAYLIST_STATS_TABLE"],
}

_validated = False


def _check_module_exports(module_name: str, keys: List[str]):
    """
    Import a module and assert it exports the given keys.
    If module can't be imported, skip the assertion (some tests may not need all modules).
    """
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        pytest.skip(f"Module {module_name} not importable: {e}")
    missing = [k for k in keys if not hasattr(mod, k)]
    if missing:
        raise AssertionError(f"Module {module_name} missing exports: {missing}")


# ✅ ONE AUTOUSE FIXTURE - RULES ALL
@pytest.fixture(autouse=True, scope="session")
def setup_test_environment():
    """✅ SINGLE SOURCE OF TRUTH: Loads env + validates + contracts"""
    print("🚀 conftest.py: LOADING ENVIRONMENT...")  # DEBUG

    # Validate env (with defaults)
    assert os.getenv("YOUTUBE_API_KEY"), "YOUTUBE_API_KEY missing"
    print(f"✅ YOUTUBE_API_KEY: {os.getenv('YOUTUBE_API_KEY')[:10]}...")

    # Validate contracts ONCE
    global _validated
    if not _validated:
        for mod, keys in DEFAULT_EXPECTED_EXPORTS.items():
            _check_module_exports(mod, keys)
        _validated = True
        print("✅ Module contracts VALIDATED")

    yield  # Run tests
    print("🏁 conftest.py: TESTS COMPLETE")


@pytest.fixture
def mock_youtube_api():
    """Fully synchronous YouTube Data API mock compatible with YoutubePlaylistService."""
    mock = MagicMock()

    # Chained API interface
    mock.playlists.return_value = mock
    mock.playlistItems.return_value = mock
    mock.videos.return_value = mock

    def list(**kwargs):
        mock._last_kwargs = kwargs
        return mock

    def execute():
        kwargs = getattr(mock, "_last_kwargs", {})

        # --- 1️⃣ Playlist metadata ---
        if (
            "id" in kwargs
            and "playlistId" not in kwargs
            and "videos" not in mock._last_kwargs.get("part", "")
            and "statistics" not in mock._last_kwargs.get("part", "")
        ):
            return {
                "items": [
                    {
                        "snippet": {
                            "title": "Mock Playlist",
                            "channelTitle": "Mock Channel",
                            "thumbnails": {
                                "high": {"url": "https://example.com/mock_thumb.jpg"}
                            },
                        },
                        "contentDetails": {"itemCount": 3},
                    }
                ]
            }

        # --- 2️⃣ Playlist items ---
        if "playlistId" in kwargs:
            return {
                "items": [
                    {"contentDetails": {"videoId": f"video_{i}"}} for i in range(3)
                ]
            }

        # --- 3️⃣ Video details (this is where the 'id' KeyError happened) ---
        if (
            "id" in kwargs
            and "snippet" in kwargs.get("part", "")
            and "statistics" in kwargs.get("part", "")
        ):
            ids = kwargs["id"]
            video_ids = ids.split(",") if isinstance(ids, str) else ids
            return {
                "items": [
                    {
                        "id": vid,
                        "snippet": {
                            "title": f"Mock Video {i}",
                            "channelTitle": "Mock Channel",
                            "thumbnails": {
                                "high": {"url": f"https://example.com/{vid}.jpg"}
                            },
                        },
                        "statistics": {
                            "viewCount": "1000",
                            "likeCount": "100",
                            "commentCount": "10",
                        },
                        "contentDetails": {"duration": "PT3M45S"},
                    }
                    for i, vid in enumerate(video_ids)
                ]
            }

        return {"items": []}

    mock.list.side_effect = list
    mock.execute.side_effect = execute

    return mock
