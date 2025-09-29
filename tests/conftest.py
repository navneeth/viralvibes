# tests/conftest.py
"""
Pytest configuration for ViralVibes tests.
This file automatically runs before any tests and sets up the Python path.
"""

import os
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest
from dotenv import load_dotenv

# Add the project root directory to Python path so tests can import modules
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


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
