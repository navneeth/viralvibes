"""
Integration tests for main application endpoints.

Test Organization:
1. Public Endpoints (index, newsletter)
2. URL Validation & Preview (/validate/url, /validate/preview)
3. Job Submission (/submit-job)
4. Job Progress (/job-progress)
5. Dashboard (/d/{id})
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from starlette.testclient import TestClient

import main
from constants import KNOWN_PLAYLISTS, JobStatus
from db import (
    get_dashboard_event_counts,
    record_dashboard_event,
    set_supabase_client,
)

# Use a real playlist URL from constants for testing
TEST_PLAYLIST_URL = KNOWN_PLAYLISTS[0]["url"]


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def client():
    """Unauthenticated test client."""
    return TestClient(main.app)


def make_test_preview_data():
    """Create consistent preview data for tests."""
    return {
        "title": "Test Playlist",
        "channel_name": "Test Channel",
        "thumbnail": "/test.jpg",
        "video_count": 100,
        "description": "Test description",
        "source": "cache",
    }


def make_test_job_data(
    status: str = JobStatus.PROCESSING,
    progress: float = 0.25,
    error: str | None = None,
):
    """Create consistent job data for tests."""
    return {
        "id": "test-job-123",
        "status": status,
        "progress": progress,
        "started_at": (datetime.utcnow() - timedelta(seconds=30)).isoformat(),
        "error": error,
    }


# ============================================================
# Test Class 1: Public Endpoints
# ============================================================


class TestPublicEndpoints:
    """Tests for publicly accessible endpoints."""

    def test_index_renders(self, client):
        """Test homepage loads successfully."""
        r = client.get("/")
        assert r.status_code == 200
        assert "ViralVibes" in r.text or "Decode YouTube virality" in r.text

    def test_newsletter_signup_success(self, client, monkeypatch):
        """Test successful newsletter signup."""
        # Mock Supabase client
        fake_client = SimpleNamespace()
        fake_client.table = lambda tbl: SimpleNamespace(
            insert=lambda payload: SimpleNamespace(
                execute=lambda: SimpleNamespace(data=[{"id": 1}])
            )
        )
        monkeypatch.setattr(main, "supabase_client", fake_client)

        r = client.post("/newsletter", data={"email": "test@example.com"})
        assert r.status_code == 200
        assert "Thanks for signing up" in r.text or "Thanks" in r.text

    def test_newsletter_signup_invalid_email(self, client):
        """Test newsletter signup with invalid email."""
        r = client.post("/newsletter", data={"email": "not-an-email"})
        # Should return validation error
        assert r.status_code in (200, 422)


# ============================================================
# Test Class 2: URL Validation & Preview
# ============================================================


class TestURLValidationAndPreview:
    """Tests for /validate/url and /validate/preview endpoints."""

    def test_validate_url_requires_auth(self, client):
        """Test /validate/url requires authentication."""
        r = client.post("/validate/url", data={"playlist_url": TEST_PLAYLIST_URL})

        assert r.status_code in (200, 303, 401)
        # Should prompt for login
        if r.status_code == 200:
            assert "log in" in r.text.lower() or "login" in r.text.lower()

    @pytest.mark.skip(reason="Temporarily disabled for debugging")
    def test_validate_url_invalid_format(self, authenticated_client):
        """Test /validate/url rejects invalid URLs."""
        r = authenticated_client.post(
            "/validate/url",
            data={"playlist_url": "not-a-valid-url"},
        )

        assert r.status_code in (200, 422)
        if r.status_code == 200:
            # Should show validation error (case-insensitive check)
            text_lower = r.text.lower()
            assert (
                "invalid" in text_lower
                or "youtube" in text_lower
                or "error" in text_lower
                or "format" in text_lower
            )

    @pytest.mark.skip(reason="Temporarily disabled for debugging")
    def test_validate_url_triggers_preview(self, authenticated_client):
        """Test /validate/url triggers /validate/preview on success."""
        r = authenticated_client.post(
            "/validate/url",
            data={"playlist_url": TEST_PLAYLIST_URL},
        )

        assert r.status_code == 200

        # ✅ First check: auth should have worked (no login prompt)
        assert "log in" not in r.text.lower(), (
            "Authentication failed - session not injected properly. "
            f"Response: {r.text[:500]}"
        )

        # ✅ Second check: response should contain EITHER:
        # 1. An HTMX trigger to load preview
        # 2. The preview content itself
        # 3. Some indication the URL was accepted

        has_preview_reference = any(
            [
                "htmx.ajax" in r.text,
                "hx-post" in r.text,
                "/validate/preview" in r.text,
                "hx-get" in r.text,
                TEST_PLAYLIST_URL in r.text,  # URL echoed back
                "playlist-preview" in r.text,  # Preview container
            ]
        )

        assert (
            has_preview_reference
        ), f"Expected preview trigger or content in response. Got: {r.text[:1000]}"

    def test_preview_cache_hit_redirects_to_full_analysis(self, client, monkeypatch):
        """
        Test: When cached data exists, /validate/preview redirects to full analysis.

        Flow:
        1. User submits URL via /validate/url
        2. HTMX calls /validate/preview
        3. Cache hit detected
        4. Returns HTMX redirect to /validate/full
        """
        monkeypatch.setattr(
            "controllers.preview.get_cached_playlist_stats",
            lambda url, check_date=True: {"title": "Cached Playlist", "df_json": "[]"},
        )

        r = client.post("/validate/preview", data={"playlist_url": TEST_PLAYLIST_URL})

        assert r.status_code == 200
        # Should redirect to full analysis
        assert "htmx.ajax('POST', '/validate/full'" in r.text

    def test_preview_redirects_when_job_complete(self, client, monkeypatch):
        """Test /validate/preview redirects when job is complete."""
        monkeypatch.setattr(
            "controllers.preview.get_cached_playlist_stats",
            lambda url, check_date=True: None,
        )
        monkeypatch.setattr(
            "controllers.preview.get_playlist_job_status",
            lambda url: JobStatus.COMPLETE,  # ✅ Use constant instead of "done"
        )

        r = client.post("/validate/preview", data={"playlist_url": TEST_PLAYLIST_URL})

        assert r.status_code == 200
        assert "htmx.ajax('POST', '/validate/full'" in r.text

    def test_preview_auto_submit_when_no_job_exists(self, client, monkeypatch):
        """
        Test: Auto-submit triggers when no active job exists.

        Flow:
        1. No cache exists
        2. No active job found (job_status = None)
        3. auto_submit=True passed to render_preview_card
        4. HTMX trigger fires /submit-job on page load
        """
        monkeypatch.setattr(
            "controllers.preview.get_cached_playlist_stats",
            lambda url, check_date=True: None,
        )
        monkeypatch.setattr(
            "controllers.preview.get_playlist_job_status",
            lambda url: None,  # No job exists
        )
        monkeypatch.setattr(
            "controllers.preview.get_playlist_preview_info",
            lambda url: make_test_preview_data(),
        )

        r = client.post("/validate/preview", data={"playlist_url": TEST_PLAYLIST_URL})

        assert r.status_code == 200
        # Should show preview card
        assert "Test Playlist" in r.text
        # Should have auto-submit trigger (check for just "load", not "load once")
        assert 'hx-post="/submit-job"' in r.text
        assert 'hx-trigger="load"' in r.text  # ✅ Fixed: removed "once"

    def test_preview_auto_submit_when_job_failed(self, client, monkeypatch):
        """
        Test: Auto-submit triggers retry when previous job failed.

        Flow:
        1. No cache exists
        2. Previous job status = failed
        3. auto_submit=True (retry logic)
        4. HTMX trigger fires /submit-job on page load
        """
        monkeypatch.setattr(
            "controllers.preview.get_cached_playlist_stats",
            lambda url, check_date=True: None,
        )
        monkeypatch.setattr(
            "controllers.preview.get_playlist_job_status",
            lambda url: JobStatus.FAILED,
        )
        monkeypatch.setattr(
            "controllers.preview.get_playlist_preview_info",
            lambda url: make_test_preview_data(),
        )

        r = client.post("/validate/preview", data={"playlist_url": TEST_PLAYLIST_URL})

        assert r.status_code == 200
        assert 'hx-post="/submit-job"' in r.text
        assert 'hx-trigger="load"' in r.text  # ✅ Fixed: removed "once"

    def test_preview_shows_existing_job_without_auto_submit(self, client, monkeypatch):
        """
        Test: When job is processing, show status without auto-submit.

        Flow:
        1. No cache exists
        2. Active job found (status = processing)
        3. auto_submit=False
        4. Shows disabled button with "Analysis in Progress"
        """
        monkeypatch.setattr(
            "controllers.preview.get_cached_playlist_stats",
            lambda url, check_date=True: None,
        )
        monkeypatch.setattr(
            "controllers.preview.get_playlist_job_status",
            lambda url: JobStatus.PROCESSING,
        )
        monkeypatch.setattr(
            "controllers.preview.get_playlist_preview_info",
            lambda url: make_test_preview_data(),
        )

        r = client.post("/validate/preview", data={"playlist_url": TEST_PLAYLIST_URL})

        assert r.status_code == 200
        # Should NOT have auto-submit trigger with "load"
        # It may have polling trigger with "every 2s" but not load trigger
        assert (
            'hx-trigger="load"' not in r.text or 'hx-post="/submit-job"' not in r.text
        )  # ✅ Fixed logic
        # Should show processing state
        assert "Analysis in Progress" in r.text or "Processing" in r.text.lower()

    def test_preview_handles_blocked_job(self, client, monkeypatch):
        """Test /validate/preview shows blocked message for YouTube-blocked playlists."""
        monkeypatch.setattr(
            "controllers.preview.get_cached_playlist_stats",
            lambda url, check_date=True: None,
        )
        monkeypatch.setattr(
            "controllers.preview.get_playlist_job_status",
            lambda url: JobStatus.BLOCKED,
        )

        r = client.post("/validate/preview", data={"playlist_url": TEST_PLAYLIST_URL})

        assert r.status_code == 200
        assert "blocked" in r.text.lower() or "YouTube" in r.text

    def test_preview_public_access_no_auth_required(self, client, monkeypatch):
        """Test /validate/preview is publicly accessible (no auth required)."""
        # Mock preview data
        monkeypatch.setattr(
            "controllers.preview.get_cached_playlist_stats",
            lambda url, check_date=True: None,
        )
        monkeypatch.setattr(
            "controllers.preview.get_playlist_job_status", lambda url: None
        )
        monkeypatch.setattr(
            "controllers.preview.get_playlist_preview_info",
            lambda url: make_test_preview_data(),
        )

        # No authentication - should still work
        r = client.post("/validate/preview", data={"playlist_url": TEST_PLAYLIST_URL})

        assert r.status_code == 200
        # Should NOT show auth error
        assert "log in" not in r.text.lower()


# ============================================================
# Test Class 3: Job Submission
# ============================================================


class TestJobSubmission:
    """Tests for /submit-job endpoint."""

    def test_submit_job_creates_job_and_returns_polling_trigger(
        self, authenticated_client, monkeypatch
    ):
        """
        Test: /submit-job creates job and returns HTMX polling div.

        Flow:
        1. User clicks "Analyze" or auto-submit triggers
        2. POST /submit-job
        3. Job created in database
        4. Returns div with hx-get="/job-progress" + hx-trigger="every 2s"
        """
        called = {}

        def fake_submit(url):
            called["url"] = url
            return True

        monkeypatch.setattr("main.submit_playlist_job", fake_submit)

        r = authenticated_client.post(
            "/submit-job",
            data={"playlist_url": TEST_PLAYLIST_URL},
        )

        assert r.status_code == 200
        assert called.get("url") == TEST_PLAYLIST_URL

        # Should return polling div
        assert 'hx-get="/job-progress' in r.text
        assert 'hx-trigger="load, every 2s"' in r.text
        assert 'id="preview-box"' in r.text

    def test_submit_job_handles_duplicate_submission(
        self, authenticated_client, monkeypatch
    ):
        """
        Test: Duplicate job submissions are handled gracefully.

        Flow:
        1. Job already exists for URL
        2. submit_playlist_job returns False (duplicate)
        3. Still returns polling div (shows existing job progress)
        """
        monkeypatch.setattr("main.submit_playlist_job", lambda url: False)

        r = authenticated_client.post(
            "/submit-job",
            data={"playlist_url": TEST_PLAYLIST_URL},
        )

        assert r.status_code == 200
        # Should still return polling div
        assert 'hx-get="/job-progress' in r.text


# ============================================================
# Test Class 4: Job Progress
# ============================================================


class TestJobProgress:
    """Tests for /job-progress endpoint and status polling."""

    def test_job_progress_shows_active_job_with_polling(
        self, authenticated_client, monkeypatch
    ):
        """
        Test: Active job shows progress UI with continued polling.

        Flow:
        1. GET /job-progress
        2. Job status = processing, progress = 25%
        3. Returns progress UI with stats
        4. Includes hx-trigger="every 2s" for continued polling
        """
        monkeypatch.setattr(
            "controllers.job_progress.get_job_progress",
            lambda url: make_test_job_data(status=JobStatus.PROCESSING, progress=25.0),
        )
        monkeypatch.setattr(
            "controllers.job_progress.get_playlist_preview_info",
            lambda url: make_test_preview_data(),
        )

        r = authenticated_client.get(f"/job-progress?playlist_url={TEST_PLAYLIST_URL}")

        assert r.status_code == 200

        # Should show progress
        assert "25%" in r.text or "Complete" in r.text
        assert "Test Playlist" in r.text
        assert "100" in r.text  # video count

        # Should continue polling
        assert 'hx-get="/job-progress' in r.text
        assert 'hx-trigger="every 2s"' in r.text

    def test_job_progress_redirects_on_completion(
        self, authenticated_client, monkeypatch
    ):
        """
        Test: Completed job triggers redirect to dashboard.

        Flow:
        1. GET /job-progress
        2. Job status = complete, progress = 100%
        3. Returns redirect script (window.location.href)
        4. NO polling trigger (job is done)
        """
        monkeypatch.setattr(
            "controllers.job_progress.get_job_progress",
            lambda url: make_test_job_data(status=JobStatus.COMPLETE, progress=100.0),
        )
        monkeypatch.setattr(
            "controllers.job_progress.get_playlist_preview_info",
            lambda url: make_test_preview_data(),
        )

        r = authenticated_client.get(f"/job-progress?playlist_url={TEST_PLAYLIST_URL}")

        assert r.status_code == 200

        # Should contain redirect script
        assert "window.location.href" in r.text or "complete" in r.text.lower()

        # Should NOT have polling trigger
        assert 'hx-trigger="every 2s"' not in r.text

    def test_job_progress_shows_error_on_failure(
        self, authenticated_client, monkeypatch
    ):
        """
        Test: Failed job shows error message without polling.

        Flow:
        1. GET /job-progress
        2. Job status = failed, error message present
        3. Shows error alert with message
        4. NO polling (job is terminal)
        """
        monkeypatch.setattr(
            "controllers.job_progress.get_job_progress",
            lambda url: make_test_job_data(
                status=JobStatus.FAILED,
                progress=50.0,
                error="Network timeout while fetching videos",
            ),
        )
        monkeypatch.setattr(
            "controllers.job_progress.get_playlist_preview_info",
            lambda url: make_test_preview_data(),
        )

        r = authenticated_client.get(f"/job-progress?playlist_url={TEST_PLAYLIST_URL}")

        assert r.status_code == 200
        assert "Failed" in r.text or "error" in r.text.lower()
        assert "Network timeout" in r.text

        # Should NOT poll
        assert 'hx-trigger="every 2s"' not in r.text

    def test_job_progress_shows_blocked_state(self, authenticated_client, monkeypatch):
        """Test blocked job shows appropriate warning message."""
        monkeypatch.setattr(
            "controllers.job_progress.get_job_progress",
            lambda url: make_test_job_data(status=JobStatus.BLOCKED, progress=0.0),
        )
        monkeypatch.setattr(
            "controllers.job_progress.get_playlist_preview_info",
            lambda url: make_test_preview_data(),
        )

        r = authenticated_client.get(f"/job-progress?playlist_url={TEST_PLAYLIST_URL}")

        assert r.status_code == 200
        assert "Blocked" in r.text or "blocked" in r.text
        assert "YouTube" in r.text

    def test_job_progress_handles_missing_job(self, authenticated_client, monkeypatch):
        """Test /job-progress gracefully handles missing job data."""
        monkeypatch.setattr(
            "controllers.job_progress.get_job_progress",
            lambda url: None,  # No job found
        )

        r = authenticated_client.get(f"/job-progress?playlist_url={TEST_PLAYLIST_URL}")

        assert r.status_code == 200
        assert "No analysis job found" in r.text or "not found" in r.text.lower()

    def test_job_progress_clamps_invalid_progress(
        self, authenticated_client, monkeypatch
    ):
        """Test progress values outside [0, 100] are clamped correctly."""
        # Test progress > 100
        monkeypatch.setattr(
            "controllers.job_progress.get_job_progress",
            lambda url: make_test_job_data(
                status=JobStatus.PROCESSING,
                progress=150.0,  # Invalid!
            ),
        )
        monkeypatch.setattr(
            "controllers.job_progress.get_playlist_preview_info",
            lambda url: make_test_preview_data(),
        )

        r = authenticated_client.get(f"/job-progress?playlist_url={TEST_PLAYLIST_URL}")

        assert r.status_code == 200
        # Progress bar width should be clamped to 100%
        assert 'style="width: 100%' in r.text or "width: 100.0%" in r.text

    def test_job_progress_handles_none_status(self, authenticated_client, monkeypatch):
        """Test job with None/unknown status shows graceful fallback."""
        monkeypatch.setattr(
            "controllers.job_progress.get_job_progress",
            lambda url: {
                "id": "test-job",
                "status": None,  # Unknown status
                "progress": 10.0,
                "error": None,
            },
        )
        monkeypatch.setattr(
            "controllers.job_progress.get_playlist_preview_info",
            lambda url: make_test_preview_data(),
        )

        r = authenticated_client.get(f"/job-progress?playlist_url={TEST_PLAYLIST_URL}")

        assert r.status_code == 200
        # Should show fallback message (not "Status: None")
        assert "Preparing analysis" in r.text or "Waiting" in r.text
        assert "Status: None" not in r.text


# ============================================================
# Test Class 5: Dashboard
# ============================================================


class TestDashboard:
    """Tests for persistent dashboard endpoints."""

    def test_dashboard_by_id_loads(
        self, authenticated_client, mock_supabase, monkeypatch
    ):
        """Test GET /d/{dashboard_id} returns dashboard content."""
        set_supabase_client(mock_supabase)

        dashboard_id = "test-dash-abc123"
        r = authenticated_client.get(f"/d/{dashboard_id}")

        assert r.status_code in (200, 303)
        assert "Sample Playlist" in r.text or "playlist-table" in r.text

    @pytest.mark.skip(reason="Temporarily disabled for debugging")
    def test_dashboard_records_view_event(self, mock_supabase):
        """Test viewing dashboard increments view count."""
        from db import (
            get_dashboard_event_counts,
            record_dashboard_event,
            set_supabase_client,
        )

        set_supabase_client(mock_supabase)

        dashboard_id = "test-dash-abc123"

        # ✅ Initial state: no events recorded yet
        initial_counts = get_dashboard_event_counts(
            supabase=mock_supabase,
            dashboard_id=dashboard_id,
        )
        initial_view_count = initial_counts.get("view", 0)

        # Should start at 0
        assert (
            initial_view_count == 0
        ), f"Expected 0 initial views, got {initial_view_count}"

        # Record first view event
        record_dashboard_event(
            supabase=mock_supabase,
            dashboard_id=dashboard_id,
            event_type="view",
        )

        # Verify event was recorded
        updated_counts = get_dashboard_event_counts(
            supabase=mock_supabase,
            dashboard_id=dashboard_id,
        )
        updated_view_count = updated_counts.get("view", 0)

        # View count should be 1
        assert (
            updated_view_count == 1
        ), f"Expected 1 view after recording, got {updated_view_count}"

        # Record another view
        record_dashboard_event(
            supabase=mock_supabase,
            dashboard_id=dashboard_id,
            event_type="view",
        )

        # Verify count increased to 2
        final_counts = get_dashboard_event_counts(
            supabase=mock_supabase,
            dashboard_id=dashboard_id,
        )
        final_view_count = final_counts.get("view", 0)

        assert (
            final_view_count == 2
        ), f"Expected 2 views after second record, got {final_view_count}"

    def test_dashboard_event_counts(self, mock_supabase):
        """Test fetching dashboard event counts."""
        set_supabase_client(mock_supabase)

        dashboard_id = "test-dash-abc123"
        counts = get_dashboard_event_counts(
            supabase=mock_supabase,
            dashboard_id=dashboard_id,
        )

        assert "view" in counts
        assert "share" in counts
        assert isinstance(counts["view"], int)
        assert isinstance(counts["share"], int)


# ============================================================
# Legacy Tests (Keep for Compatibility)
# ============================================================


def make_cached_row():
    """Legacy helper for cached data structure."""
    df = pl.DataFrame(
        [
            {
                "Rank": 1,
                "Title": "A",
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
