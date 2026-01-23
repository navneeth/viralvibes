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
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Dict, List
from unittest.mock import AsyncMock, MagicMock

import polars as pl
import pytest
from dotenv import load_dotenv
from itsdangerous import URLSafeTimedSerializer
from utils import compute_dashboard_id

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

# ============================================================================
# ✅ CONSTANTS (Add before fixtures that use them)
# ============================================================================

# Use a real playlist URL from constants for testing
try:
    from constants import KNOWN_PLAYLISTS

    TEST_PLAYLIST_URL = KNOWN_PLAYLISTS[0]["url"]
except (ImportError, IndexError, KeyError):
    # Fallback if constants not available
    TEST_PLAYLIST_URL = (
        "https://www.youtube.com/playlist?list=PLrAXtmErZgOeiKm4sgNOknGvNjby9efdf"
    )

# ============================================================================
# Minimal contract mapping
# ============================================================================

DEFAULT_EXPECTED_EXPORTS: Dict[str, List[str]] = {
    "worker.worker": ["Worker", "handle_job"],
    "db": [
        "upsert_playlist_stats",
        "get_cached_playlist_stats",
        "get_dashboard_event_counts",
        "record_dashboard_event",
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


# ============================================================================
# ✅ HELPER FUNCTIONS (Add before fixtures that use them)
# ============================================================================


def make_cached_row():
    """
    Legacy helper for cached data structure.

    Creates a minimal playlist_stats row structure for testing.
    """
    df = pl.DataFrame(
        [
            {
                "Rank": 1,
                "Title": "Sample Video",
                "Views": 100,
                "Likes": 10,
                "Dislikes": 1,
                "Comments": 5,
                "Engagement Rate (%)": 16.0,
                "Controversy": 18.18,
                "Views Formatted": "100",
                "Likes Formatted": "10",
                "Engagement Rate Formatted": "16.00%",
            }
        ]
    )

    return {
        "df_json": df.write_json(),
        "title": "Sample Playlist",
        "channel_name": "Tester",
        "channel_thumbnail": "/img.png",
        "summary_stats": {
            "total_views": 100,
            "total_likes": 10,
            "actual_playlist_count": 1,
            "processed_video_count": 1,
            "avg_engagement": 5.0,
        },
        "video_count": 1,
    }


class Response:
    """Mock Supabase response object."""

    def __init__(self, data):
        self.data = data


# ============================================================================
# ✅ AUTOUSE FIXTURE
# ============================================================================


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

    def __init__(self, table_name: str, mock_supabase: "MockSupabase"):
        self.table_name = table_name
        self.mock_supabase = mock_supabase
        self.data = {}
        self._filters = {}
        self._single = False
        self._insert_data = None
        self._update_data = None
        self._delete_filters = {}
        self._limit_count = None
        self._order_col = None
        self._order_desc = False
        self._select_cols = None

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

    def execute(self) -> SimpleNamespace:
        """Execute the query and return results."""
        # Handle INSERT operations
        if self._insert_data:
            # ✅ Handle dashboard_events inserts
            if self.table_name == "dashboard_events":
                # Initialize storage if needed
                if "dashboard_events" not in self.data:
                    self.data["dashboard_events"] = {}

                # Get dashboard_id from payload
                dashboard_id = self._insert_data.get("dashboard_id")

                if dashboard_id:
                    # Initialize list for this dashboard
                    if dashboard_id not in self.data["dashboard_events"]:
                        self.data["dashboard_events"][dashboard_id] = []

                    # Create event record
                    event_data = {
                        **self._insert_data,
                        "id": len(self.data["dashboard_events"][dashboard_id]) + 1,
                        "created_at": datetime.utcnow().isoformat(),
                    }

                    # Store event
                    self.data["dashboard_events"][dashboard_id].append(event_data)

                    return Response([event_data])

                return Response([self._insert_data])

            # Handle other inserts...
            return Response([{**self._insert_data, "id": 1}])

        # Handle UPDATE operations
        if self._update_data:
            return Response([self._update_data])

        # Handle SELECT operations
        if self.table_name == "dashboard_events":
            if self._insert_data:
                # Insert event
                self.mock_supabase.data.setdefault("dashboard_events", []).append(
                    self._insert_data
                )
                return SimpleNamespace(data=[self._insert_data])

            if self._filters.get("dashboard_id"):
                dashboard_id = self._filters["dashboard_id"]
                events = [
                    e
                    for e in self.mock_supabase.data.get("dashboard_events", [])
                    if e.get("dashboard_id") == dashboard_id
                ]
                return SimpleNamespace(data=events)

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
            "Dislikes": [10 - i for i in range(num_videos)],  # ✅ NEW
            "Comments": [50 - (i * 5) for i in range(num_videos)],
            "Duration": [240, 300, 180, 360, 120][:num_videos],  # ✅ NEW (in seconds)
            "Engagement Rate (%)": [6.0 - (i * 0.5) for i in range(num_videos)],
            "Controversy": [9.0 + (i * 2.0) for i in range(num_videos)],
            "Views Formatted": [f"{10000 - (i * 1000):,}" for i in range(num_videos)],
            "Likes Formatted": [f"{300 - (i * 50):,}" for i in range(num_videos)],
            "Comments Formatted": [f"{50 - (i * 5):,}" for i in range(num_videos)],
            "Engagement Rate Formatted": [
                f"{6.0 - (i * 0.5):.2f}%" for i in range(num_videos)
            ],
        }
    )


def create_test_playlist_row(
    playlist_url: str = TEST_PLAYLIST_URL,  # ✅ REMOVED dashboard_id parameter
    num_videos: int = 5,
) -> dict:
    """
    Create a complete test playlist_stats row matching the new schema.

    ✅ UPDATED: Includes dashboard_id, view_count, share_count columns
    """
    # ✅ Compute dashboard_id from URL using real hash function
    dashboard_id = compute_dashboard_id(playlist_url)

    df = create_test_dataframe(num_videos)

    # Calculate metrics from DataFrame
    total_views = int(df["Views"].sum())
    total_likes = int(df["Likes"].sum())
    total_dislikes = int(df["Dislikes"].sum()) if "Dislikes" in df.columns else 0
    total_comments = int(df["Comments"].sum())
    avg_engagement = (
        (total_likes + total_comments) / total_views if total_views > 0 else 0
    )
    avg_duration_seconds = (
        int(df["Duration"].mean()) if "Duration" in df.columns else 270
    )

    return {
        "id": 1,
        "playlist_url": playlist_url,
        "dashboard_id": dashboard_id,  # ✅ Computed from URL
        "title": "Sample Playlist",  # ✅ Matches test assertion
        "channel_name": "Test Channel",
        "channel_thumbnail": "https://example.com/test.jpg",
        # ✅ Add all missing DB fields
        "view_count": total_views,  # Total video views
        "like_count": total_likes,
        "dislike_count": total_dislikes,
        "comment_count": total_comments,
        "video_count": num_videos,
        "processed_video_count": num_videos,
        # ✅ Calculated metrics
        "engagement_rate": avg_engagement,
        "controversy_score": 0.0,
        # ✅ Duration as PostgreSQL INTERVAL string
        "avg_duration": str(timedelta(seconds=avg_duration_seconds)),
        # ✅ Timestamps
        "processed_on": datetime.utcnow().isoformat(),
        "processed_date": datetime.utcnow().date().isoformat(),
        # ✅ JSON fields
        "df_json": df.write_json(),
        "summary_stats": json.dumps(
            {
                "total_views": total_views,
                "total_likes": total_likes,
                "total_dislikes": total_dislikes,
                "total_comments": total_comments,
                "avg_engagement": avg_engagement,
                "actual_playlist_count": num_videos,
                "processed_video_count": num_videos,
            }
        ),
        # ✅ Event counters
        "share_count": 0,  # Denormalized from dashboard_events
    }


def create_test_job_row(
    job_id: int = 1,
    playlist_url: str = TEST_PLAYLIST_URL,
    status: str = "complete",
    progress: float = None,  # ✅ REMOVED dashboard_id parameter
) -> dict:
    """
    Create a complete test playlist_jobs row.

    For testing worker job processing
    """
    if progress is None:
        progress = 1.0 if status == "complete" else 0.0

    return {
        "id": job_id,
        "playlist_url": playlist_url,
        "status": status,
        "progress": progress,  # ✅ Float (0.0-1.0)
        # ✅ Add all missing DB fields
        "retry_scheduled": False,
        "retry_after": None,
        "retry_count": 0,
        "error_stage": None,
        "status_message": "Processing complete" if status == "complete" else "Pending",
        "result_source": "fresh" if status == "complete" else None,
        "error": None,
        "error_trace": None,
        # ✅ Timestamps
        "created_at": "2024-01-01T12:00:00Z",
        "updated_at": (
            "2024-01-01T12:05:00Z" if status in ["complete", "failed"] else None
        ),
        "started_at": "2024-01-01T12:01:00Z" if status != "pending" else None,
        "finished_at": "2024-01-01T12:05:00Z" if status == "complete" else None,
    }


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_supabase():
    """✅ FIXED: Mock Supabase client with computed dashboard_id."""
    test_playlist_row = create_test_playlist_row(
        playlist_url=TEST_PLAYLIST_URL,
        num_videos=5,
    )

    mock_data = {
        "playlist_stats": {
            # ✅ Key by dashboard_id for efficient lookup
            test_playlist_row["dashboard_id"]: test_playlist_row,
        },
        "dashboards": {
            # ✅ Also support legacy dashboards table lookup
            test_playlist_row["dashboard_id"]: {
                "id": test_playlist_row["dashboard_id"],
                "playlist_url": TEST_PLAYLIST_URL,
                "title": "Sample Playlist",
                "created_at": datetime.utcnow().isoformat(),
                "df_json": test_playlist_row["df_json"],
                "summary_stats": test_playlist_row["summary_stats"],
            }
        },
        "dashboard_events": {},  # ✅ For event tracking
        "playlist_jobs": {},  # ✅ For job tracking
        "analysis_jobs": {},
        "newsletter_signups": [],
    }

    return MockSupabase(mock_data)


@pytest.fixture
def mock_supabase_empty():
    """Provide a mock Supabase client with no data (for cache miss tests)."""
    return MockSupabase(
        {
            "playlist_stats": {},
            "dashboard_events": {},
            "playlist_jobs": {},
            "dashboards": {},
        }
    )


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
    return TEST_PLAYLIST_URL


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
    test_url = TEST_PLAYLIST_URL

    # ✅ Create test data WITHOUT passing dashboard_id
    test_playlist_row = create_test_playlist_row(
        playlist_url=test_url,
        num_videos=5,
    )

    test_job_row = create_test_job_row(
        job_id=1,
        playlist_url=test_url,
        status="complete",
    )

    data = {
        "playlist_stats": {
            # ✅ Key by computed dashboard_id
            test_playlist_row["dashboard_id"]: test_playlist_row,
        },
        "playlist_jobs": {
            1: test_job_row,
        },
        "dashboard_events": {},
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
def client():
    """Unauthenticated test client."""
    from starlette.testclient import TestClient
    import main

    return TestClient(main.app)


@pytest.fixture
def authenticated_client(client, monkeypatch):
    """
    Test client with authenticated session.

    Works by wrapping the ASGI app to inject session data into the
    request scope BEFORE SessionMiddleware processes it.

    This is the standard pattern used in Starlette/FastHTML test suites.
    """
    import main
    from starlette.types import ASGIApp, Receive, Scope, Send

    # Fake authenticated session
    fake_session = {
        "auth": {
            "email": "test@viralvibes.com",
            "name": "Test User",
            "picture": "https://example.com/test-avatar.jpg",
            "ident": "test_google_123456",
        }
    }

    # Get the original app
    original_app = main.app

    class SessionInjectingMiddleware:
        """ASGI middleware that injects session into every request."""

        def __init__(self, app: ASGIApp):
            self.app = app

        async def __call__(self, scope: Scope, receive: Receive, send: Send):
            if scope["type"] == "http":
                # Inject session into scope BEFORE any middleware runs
                scope["session"] = fake_session.copy()

            # Call the wrapped app
            await self.app(scope, receive, send)

    # Wrap the original app
    wrapped_app = SessionInjectingMiddleware(original_app)

    # Patch main.app so any code that imports it gets the wrapped version
    monkeypatch.setattr(main, "app", wrapped_app)

    # Create new test client with wrapped app
    from starlette.testclient import TestClient

    return TestClient(wrapped_app)


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
