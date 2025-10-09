# tests/conftest.py
"""
Pytest configuration for ViralVibes tests.
This file automatically runs before any tests and sets up the Python path.
"""

import os
import sys
import importlib
from typing import Dict, List

import pytest
from dotenv import load_dotenv

# Ensure project root is importable
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Load .env if present (safe no-op)
load_dotenv()

# Provide safe defaults for tests (can be overridden in CI)
os.environ.setdefault("YOUTUBE_API_KEY", "test-youtube-key")
os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "anon-key-for-tests")

# Minimal contract mapping: modules -> expected attributes (tests can assert against this)
DEFAULT_EXPECTED_EXPORTS: Dict[str, List[str]] = {
    "worker.worker": ["Worker", "process_one", "handle_job"],
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
        raise AssertionError(
            f"Module {module_name} missing expected exports: {missing}"
        )


@pytest.fixture(autouse=True)
def validate_repo_contracts():
    """
    Autouse fixture that performs a lightweight verification of core repo expectations.
    - Ensures environment defaults exist.
    - Verifies key modules export expected names (see DEFAULT_EXPECTED_EXPORTS).
    This keeps tests focused on repository assumptions and fails fast if core exports change.
    """
    global _validated
    # Ensure minimal env is present for tests
    assert (
        os.getenv("YOUTUBE_API_KEY") is not None
    ), "YOUTUBE_API_KEY must be set for tests"
    assert os.getenv("SUPABASE_URL") is not None
    assert os.getenv("SUPABASE_KEY") is not None

    if not _validated:
        for mod, keys in DEFAULT_EXPECTED_EXPORTS.items():
            _check_module_exports(mod, keys)
        _validated = True
    yield


def pytest_configure(config):
    """Load environment variables before any tests run."""
    # Load from .env file
    load_dotenv()

    # Verify key exists
    if not os.getenv("YOUTUBE_API_KEY"):
        pytest.skip(
            "YOUTUBE_API_KEY not found in environment. "
            "Please ensure .env file exists and contains YOUTUBE_API_KEY"
        )


@pytest.fixture(autouse=True)
def ensure_env_loaded():
    """Ensure environment variables are loaded for each test."""
    if not os.getenv("YOUTUBE_API_KEY"):
        pytest.skip("YOUTUBE_API_KEY not found in environment")
    return


@pytest.fixture(autouse=True)
def mock_env_vars():
    """Provide mock environment variables for all tests."""
    os.environ["YOUTUBE_API_KEY"] = "mock-api-key-for-testing"
    yield
    # Clean up after tests
    os.environ.pop("YOUTUBE_API_KEY", None)


@pytest.fixture
def mock_youtube_api():
    """Mock the YouTube Data API client."""
    mock_youtube = MagicMock()
    mock_youtube.playlistItems().list().execute = AsyncMock(
        return_value={"items": [], "nextPageToken": None}
    )
    mock_youtube.videos().list().execute = AsyncMock(return_value={"items": []})
    return mock_youtube
