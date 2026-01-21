"""
Pytest configuration for ViralVibes tests.

Updated for db.py changes:
- Added mock Supabase client with new schema (dashboard_id, view_count, share_count)
- Updated test data to match playlist_stats table structure
- Added fixtures for mocking database functions
"""

import importlib
import io
import json
import os
import sys
from datetime import datetime
from typing import Dict, List
from unittest.mock import AsyncMock, MagicMock

import polars as pl
import pytest
from dotenv import load_dotenv
from itsdangerous import URLSafeTimedSerializer

# ✅ Set TESTING=1 BEFORE any imports that might load main.py
os.environ["TESTING"] = "1"

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
        "get_dashboard_event_counts",
        "record_dashboard_event",
        # "get_supabase",  # ← TODO: Add after db.py is updated
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
    print("🚀 conftest.py: LOADING ENVIRONMENT...")

    # ✅ Ensure TESTING is set (redundant but safe)
    os.environ["TESTING"] = "1"

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


# ============================================================================
# 🆕 Mock Supabase Client (Updated for new db.py schema)
# ============================================================================


class MockSupabaseTable:
    """Mock Supabase table with chainable query methods."""

    def __init__(self, table_name, data):
        self.table_name = table_name
        self.data = data
        self._filters = {}
        self._single = False
        self._insert_data = None
        self._update_data = None
        self._delete_filters = {}

    def select(self, cols: str):
        """Mock select() - store column names."""
        self._select_cols = cols
        return self

    def eq(self, col: str, value) -> "MockSupabaseTable":
        """Mock eq() filter."""
        self._filters[col] = value
        return self

    def limit(self, n: int) -> "MockSupabaseTable":
        """Mock limit()."""
        self._limit_count = n
        return self

    def order(self, col: str, desc: bool = False) -> "MockSupabaseTable":
        """Mock order()."""
        self._order_col = col
        self._order_desc = desc
        return self

    def single(self) -> "MockSupabaseTable":
        """Mock single() - returns one row instead of array."""
        self._single = True
        return self

    def insert(self, payload):
        """Mock insert operation."""
        self._insert_data = payload
        return self

    def update(self, payload):
        """Mock update operation."""
        self._update_data = payload
        return self

    def execute(self):
        """Execute the query and return mock data."""
        # Handle INSERT operations
        if self._insert_data:
            # ✅ Handle dashboard_events inserts
            if self.table_name == "dashboard_events":
                # Add to in-memory storage
                event_id = len(self.data.get("dashboard_events", {})) + 1
                event_data = {**self._insert_data, "id": event_id}

                # Store by dashboard_id for easy lookup
                dashboard_id = self._insert_data.get("dashboard_id")
                if dashboard_id not in self.data["dashboard_events"]:
                    self.data["dashboard_events"][dashboard_id] = []

                self.data["dashboard_events"][dashboard_id].append(event_data)
                return Response([event_data])

            # Handle other inserts...
            return Response([{**self._insert_data, "id": 1}])

        # Handle UPDATE operations
        if self._update_data:
            return Response([self._update_data])

        # Handle SELECT operations
        if self.table_name == "dashboard_events":
            # ✅ Return events for the dashboard
            dashboard_id = self._filters.get("dashboard_id")
            events = self.data.get("dashboard_events", {}).get(dashboard_id, [])

            # Filter by event_type if specified
            if "event_type" in self._filters:
                event_type = self._filters["event_type"]
                events = [e for e in events if e.get("event_type") == event_type]

            return Response(events)

        if self.table_name == "dashboards":
            # Return dashboard data
            dashboard_id = self._filters.get("id")
            if dashboard_id and dashboard_id in self.data.get("dashboards", {}):
                return Response([self.data["dashboards"][dashboard_id]])
            return Response([])

        # For playlist_stats table with specific filters
        if self.table_name == "playlist_stats":
            matching_rows = []

            # Filter by playlist_url
            if "playlist_url" in self._filters:
                url = self._filters["playlist_url"]
                if url in self.data.get("playlist_stats", {}):
                    matching_rows.append(self.data["playlist_stats"][url])

            # Filter by dashboard_id
            if "dashboard_id" in self._filters:
                dashboard_id = self._filters["dashboard_id"]
                for row in self.data.get("playlist_stats", {}).values():
                    if row.get("dashboard_id") == dashboard_id:
                        matching_rows.append(row)

            # Apply limit
            if self._limit_count:
                matching_rows = matching_rows[: self._limit_count]

            # Return single row if .single() was called
            if self._single:
                return Response(matching_rows[0] if matching_rows else {})

            return Response(matching_rows)

        # For playlist_jobs table
        if self.table_name == "playlist_jobs":
            matching_rows = []

            # Filter by id
            if "id" in self._filters:
                job_id = self._filters["id"]
                jobs = self.data.get("playlist_jobs", {})
                if job_id in jobs:
                    matching_rows.append(jobs[job_id])

            # Filter by playlist_url
            if "playlist_url" in self._filters:
                url = self._filters["playlist_url"]
                jobs = self.data.get("playlist_jobs", {})
                for job in jobs.values():
                    if job.get("playlist_url") == url:
                        matching_rows.append(job)

            # Apply limit
            if self._limit_count:
                matching_rows = matching_rows[: self._limit_count]

            # ✅ Return single row if .single() was called
            if self._single:
                return Response(matching_rows[0] if matching_rows else {})

            return Response(matching_rows)

        # For other tables
        return Response([])


class MockSupabase:
    """Mock Supabase client that simulates database operations."""

    def __init__(self, data):
        self.data = data
        self.call_count = 0

    def table(self, table_name):
        """Return a mock table handler."""
        self.call_count += 1
        return MockSupabaseTable(table_name, self.data)


# ============================================================================
# Test Data Helpers
# ============================================================================


def create_test_dataframe(num_videos: int = 5) -> pl.DataFrame:
    """Create a test Polars DataFrame matching expected structure."""
    return pl.DataFrame(
        {
            "Rank": list(range(1, num_videos + 1)),
            "Title": [f"Test Video {i}" for i in range(1, num_videos + 1)],
            "Views": [10000 - (i * 1000) for i in range(num_videos)],
            "Likes": [300 - (i * 50) for i in range(num_videos)],
            "Comments": [50 - (i * 5) for i in range(num_videos)],
            "Duration": [240, 300, 180, 360, 120][:num_videos],
        }
    )


def create_test_playlist_row(
    playlist_url: str = "https://www.youtube.com/playlist?list=PLtest123",
    dashboard_id: str = "test-dash-abc123",
    num_videos: int = 5,
) -> dict:
    """
    Create a complete test playlist_stats row matching the new schema.

    ✅ UPDATED: Includes dashboard_id, view_count, share_count columns
    """
    df = create_test_dataframe(num_videos)

    return {
        "id": 1,
        "playlist_url": playlist_url,
        "title": "Test Playlist",
        "channel_name": "Test Channel",
        "channel_thumbnail": "https://example.com/test.jpg",
        "df_json": df.write_json(),  # Serialize DataFrame to JSON
        "summary_stats": json.dumps(
            {
                "total_views": 50000,
                "total_likes": 1500,
                "total_dislikes": 100,
                "total_comments": 250,
                "avg_engagement": 3.2,
                "actual_playlist_count": num_videos,
                "processed_video_count": num_videos,
            }
        ),
        "processed_date": "2024-01-01",
        "processed_on": "2024-01-01T12:00:00Z",
        "dashboard_id": dashboard_id,  # ✅ NEW: Must be present
        "view_count": 5,  # ✅ NEW: Event counter
        "share_count": 2,  # ✅ NEW: Event counter
    }


def create_test_job_row(
    job_id: int = 1,
    playlist_url: str = "https://youtube.com/playlist?list=PL123",
    status: str = "complete",
    dashboard_id: str = "test-dash-123",
) -> dict:
    """
    Create a complete test playlist_jobs row.

    For testing worker job processing
    """
    return {
        "id": job_id,
        "playlist_url": playlist_url,
        "status": status,
        "progress": 100 if status == "complete" else 0,
        "dashboard_id": dashboard_id,
        "created_at": "2024-01-01T12:00:00Z",
        "started_at": "2024-01-01T12:01:00Z",
        "completed_at": "2024-01-01T12:05:00Z" if status == "complete" else None,
        "error": None,
        "retry_count": 0,
    }


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_supabase():
    """Mock Supabase client for testing."""
    mock_data = {
        "playlist_stats": {
            TEST_PLAYLIST_URL: {
                "id": 1,
                "playlist_url": TEST_PLAYLIST_URL,
                "title": "Sample Playlist",
                "channel_name": "Test Channel",
                "channel_thumbnail": "https://example.com/thumb.jpg",
                "video_count": 10,
                "df_json": make_cached_row()["df_json"],
                "summary_stats": make_cached_row()["summary_stats"],
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
            }
        },
        "dashboards": {
            "test-dash-abc123": {
                "id": "test-dash-abc123",
                "playlist_url": TEST_PLAYLIST_URL,
                "title": "Sample Playlist",
                "created_at": datetime.utcnow().isoformat(),
                "df_json": make_cached_row()["df_json"],
                "summary_stats": make_cached_row()["summary_stats"],
            }
        },
        "dashboard_events": {},  # ✅ Add this for event tracking
        "analysis_jobs": {},
        "newsletter_signups": [],
    }

    return MockSupabase(mock_data)


@pytest.fixture
def mock_supabase_empty():
    """Provide a mock Supabase client with no data (for cache miss tests)."""
    return MockSupabase({"playlist_stats": {}})


@pytest.fixture
def test_playlist_row():
    """Provide a test playlist_stats row with all new schema fields."""
    return create_test_playlist_row()


@pytest.fixture
def test_dashboard_id():
    """Provide a test dashboard ID."""
    return "test-dash-abc123"


@pytest.fixture
def test_playlist_url():
    """Provide a test playlist URL."""
    return "https://www.youtube.com/playlist?list=PLtest123"


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

        # --- 3️⃣ Video details ---
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


@pytest.fixture
def mock_supabase_with_jobs():
    """
    Provide a mock Supabase client with both playlist_stats AND playlist_jobs.

    For testing worker that needs to update job status.
    """
    test_url = "https://youtube.com/playlist?list=PL123"
    test_dashboard_id = "test-dash-abc123"

    data = {
        "playlist_stats": {
            test_url: create_test_playlist_row(
                playlist_url=test_url,
                dashboard_id=test_dashboard_id,
                num_videos=5,
            )
        },
        "playlist_jobs": {
            1: create_test_job_row(
                job_id=1,
                playlist_url=test_url,
                status="complete",
                dashboard_id=test_dashboard_id,
            )
        },
    }

    return MockSupabase(data)


def create_auth_session_cookie(
    email: str = "test@viralvibes.com",
    name: str = "Test User",
    secret_key: str = "test-secret-key",
) -> str:
    """
    Create a signed session cookie containing auth data.

    This mimics what FastHTML's OAuth does after successful Google login.
    The session cookie must match the format expected by Starlette's SessionMiddleware.
    """
    serializer = URLSafeTimedSerializer(secret_key, salt="cookie-session")

    session_data = {
        "auth": {
            "email": email,
            "name": name,
            "picture": f"https://example.com/{email.split('@')[0]}.jpg",
            "ident": f"test_google_{hash(email)}",
        }
    }

    return serializer.dumps(session_data)


@pytest.fixture
def authenticated_client(client, monkeypatch):
    """
    Test client that bypasses authentication checks.

    Instead of trying to inject session data, we patch the routes
    to skip the auth check entirely during tests.
    """
    import main

    # Store original route handler
    original_submit_job = main.submit_job

    # Create wrapper that skips auth check
    def patched_submit_job(playlist_url: str, req, sess):
        # Create fake authenticated session
        fake_sess = {
            "auth": {
                "email": "test@viralvibes.com",
                "name": "Test User",
                "picture": "https://example.com/test-avatar.jpg",
                "ident": "test_google_123456",
            }
        }
        # Call original with fake authenticated session
        return original_submit_job(playlist_url, req, fake_sess)

    # Apply the patch
    monkeypatch.setattr(main, "submit_job", patched_submit_job)

    return client


@pytest.fixture
def authenticated_session():
    """
    Return mock authenticated session dict.

    Use when testing controllers directly (not via HTTP):
        def test_controller(authenticated_session):
            result = some_controller(sess=authenticated_session)
    """
    return {
        "auth": {
            "email": "test@viralvibes.com",
            "name": "Test User",
            "picture": "https://example.com/test-avatar.jpg",
            "ident": "test_google_123456",
        }
    }
