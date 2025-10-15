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
    """Mock YouTube API."""
    mock_youtube = MagicMock()
    mock_youtube.playlistItems().list().execute = AsyncMock(
        return_value={"items": [], "nextPageToken": None}
    )
    mock_youtube.videos().list().execute = AsyncMock(return_value={"items": []})
    return mock_youtube
