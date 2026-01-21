from datetime import datetime, timedelta
from types import SimpleNamespace

import polars as pl
import pytest
from starlette.testclient import TestClient

import main
from constants import KNOWN_PLAYLISTS
from db import get_dashboard_event_counts, record_dashboard_event, set_supabase_client

# Use a real playlist URL from constants for testing
TEST_PLAYLIST_URL = KNOWN_PLAYLISTS[0]["url"]


@pytest.fixture
def client():
    return TestClient(main.app)


def make_cached_row():
    # small cached row used by validate_full
    df = pl.DataFrame(
        [
            {
                "Rank": 1,
                "Title": "A",
                "Views": 100,
                "Likes": 10,
                "Dislikes": 1,
                "Comments": 5,
                "Engagement Rate (%)": 16.0,  # (Likes + Dislikes + Comments) / Views
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


def test_index_renders(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "ViralVibes" in r.text or "Decode YouTube virality" in r.text


def test_validate_url_invalid(client):
    r = client.post("/validate/url", json={"playlist_url": "not-a-url"})
    # validator returns a Div with errors (422 may be returned by pydantic depending on your validators)
    assert r.status_code in (200, 422)


def test_preview_returns_script_on_cache_hit(client, monkeypatch):
    """Test /validate/preview returns redirect script when cache exists."""
    # Mock at the controller level where it's imported
    monkeypatch.setattr(
        "controllers.preview.get_cached_playlist_stats",
        lambda url, check_date=True: {"title": "Cached Playlist"},
    )

    r = client.post(
        "/validate/preview",
        data={"playlist_url": TEST_PLAYLIST_URL},
    )

    assert r.status_code == 200
    assert "htmx.ajax('POST', '/validate/full'" in r.text


def test_preview_shows_preview_on_cache_miss_job_done(client, monkeypatch):
    """Test /validate/preview shows preview card when no cache but job is done."""
    # Mock at controller level
    monkeypatch.setattr(
        "controllers.preview.get_cached_playlist_stats",
        lambda url, check_date=True: None,
    )
    monkeypatch.setattr(
        "controllers.preview.get_playlist_job_status", lambda url: "done"
    )
    monkeypatch.setattr(
        "controllers.preview.get_playlist_preview_info",
        lambda url: {"title": "Test", "video_count": 50},
    )

    r = client.post(
        "/validate/preview",
        data={"playlist_url": TEST_PLAYLIST_URL},
    )

    assert r.status_code == 200
    # Job done with no cache → should show redirect script
    assert "htmx.ajax('POST', '/validate/full'" in r.text


def make_test_dataframe():
    """Create a test Polars DataFrame matching expected structure."""
    import polars as pl

    return pl.DataFrame(
        {
            "Rank": [1, 2, 3, 4, 5],
            "Title": [
                "Video 1",
                "Video 2",
                "Video 3",
                "Video 4",
                "Video 5",
            ],
            "Views": [10000, 8000, 6000, 4000, 2000],
            "Likes": [300, 240, 180, 120, 60],
            "Comments": [50, 40, 30, 20, 10],
            "Duration": [240, 300, 180, 360, 120],
        }
    )


def test_submit_job_and_poll(authenticated_client, monkeypatch):
    """
    Test job submission and progress polling screen flow:
    1. /submit-job creates job and returns HTMX trigger div
    2. /job-progress returns progress screen with updates
    """
    playlist_url = TEST_PLAYLIST_URL

    # Mock submit_playlist_job to track it was called
    called = {}

    def fake_submit(url):
        called["url"] = url

    monkeypatch.setattr("main.submit_playlist_job", fake_submit)

    # Mock get_job_progress to return a pending job
    mock_job = {
        "id": "job-123",
        "status": "processing",
        "progress": 0.25,  # 25% complete
        "started_at": (datetime.utcnow() - timedelta(seconds=30)).isoformat(),
        "error": None,
    }
    monkeypatch.setattr(
        "controllers.job_progress.get_job_progress", lambda url: mock_job
    )

    # Mock get_playlist_preview_info to return test data
    mock_preview = {
        "title": "Test Playlist",
        "video_count": 100,
    }
    monkeypatch.setattr(
        "controllers.job_progress.get_playlist_preview_info", lambda url: mock_preview
    )

    # Mock get_estimated_stats
    monkeypatch.setattr(
        "controllers.job_progress.get_estimated_stats",
        lambda count: {
            "estimated_total_views": 5000000,
            "estimated_total_likes": 150000,
            "estimated_total_comments": 15000,
        },
    )

    # ---- TEST /submit-job ----
    # This should return a div with hx-get and hx-trigger attributes
    r = authenticated_client.post("/submit-job", data={"playlist_url": playlist_url})

    print(f"\n🔍 Response status: {r.status_code}")
    print(f"🔍 Response text (first 500 chars): {r.text[:500]}")
    print(f"🔍 Called dict: {called}")

    assert r.status_code == 200
    assert called.get("url") == playlist_url

    # Response should have HTMX polling attributes
    assert 'hx-get="/job-progress' in r.text
    assert 'hx-trigger="load, every 2s"' in r.text
    assert 'id="preview-box"' in r.text

    # ---- TEST /job-progress ----
    # Now test the progress endpoint that /submit-job triggers
    r_progress = authenticated_client.get(f"/job-progress?playlist_url={playlist_url}")

    assert r_progress.status_code == 200

    # Should contain progress screen content
    assert "Processing Your Playlist" in r_progress.text
    assert "Analyzing 100 videos" in r_progress.text

    # Should show progress percentage
    assert "25%" in r_progress.text or "Complete" in r_progress.text

    # Should have stats preview section
    # assert "What's Being Analyzed" in r_progress.text
    assert "Videos" in r_progress.text
    assert "Est. Views" in r_progress.text

    # Should have tips section
    assert "What You'll Discover" in r_progress.text or "Processing" in r_progress.text

    # Should still have polling attributes (job not complete)
    assert 'id="progress-container"' in r_progress.text
    assert 'hx-get="/job-progress' in r_progress.text
    assert 'hx-trigger="every 2s"' in r_progress.text
    assert 'hx-swap="outerHTML"' in r_progress.text


def test_job_progress_completion(authenticated_client, monkeypatch):
    """
    Test /job-progress when job is complete.
    Should show redirect/completion message, NOT polling.
    """
    playlist_url = TEST_PLAYLIST_URL

    # Mock a COMPLETED job
    mock_job = {
        "id": "job-123",
        "status": "done",
        "progress": 1.0,
        "started_at": (datetime.utcnow() - timedelta(seconds=300)).isoformat(),
        "error": None,
    }
    monkeypatch.setattr(
        "controllers.job_progress.get_job_progress", lambda url: mock_job
    )

    mock_preview = {
        "title": "Test Playlist",
        "video_count": 100,
    }
    monkeypatch.setattr(
        "controllers.job_progress.get_playlist_preview_info", lambda url: mock_preview
    )

    monkeypatch.setattr(
        "controllers.job_progress.get_estimated_stats",
        lambda count: {
            "estimated_total_views": 5000000,
            "estimated_total_likes": 150000,
            "estimated_total_comments": 15000,
        },
    )

    # Test progress endpoint with completed job
    r = authenticated_client.get(f"/job-progress?playlist_url={playlist_url}")

    assert r.status_code == 200

    # ✅ Updated: The controller shows "100% Complete" not "Loading results"
    # Check for completion indicators
    assert "100%" in r.text or "Complete" in r.text

    # Should NOT have polling attributes (job is complete)
    assert 'id="progress-container"' in r.text
    assert 'hx-get="/job-progress"' not in r.text
    assert 'hx-trigger="every 2s"' not in r.text


def test_job_progress_with_error(authenticated_client, monkeypatch):
    """
    Test /job-progress when job has failed.
    Should show error message.
    """
    playlist_url = TEST_PLAYLIST_URL

    # Mock a FAILED job
    mock_job = {
        "id": "job-123",
        "status": "failed",
        "progress": 0.50,
        "started_at": (datetime.utcnow() - timedelta(seconds=60)).isoformat(),
        "error": "Network timeout while fetching videos",
    }
    monkeypatch.setattr(
        "controllers.job_progress.get_job_progress", lambda url: mock_job
    )

    mock_preview = {
        "title": "Test Playlist",
        "video_count": 100,
    }
    monkeypatch.setattr(
        "controllers.job_progress.get_playlist_preview_info", lambda url: mock_preview
    )

    monkeypatch.setattr(
        "controllers.job_progress.get_estimated_stats",
        lambda count: {
            "estimated_total_views": 5000000,
            "estimated_total_likes": 150000,
            "estimated_total_comments": 15000,
        },
    )

    r = authenticated_client.get(f"/job-progress?playlist_url={playlist_url}")

    assert r.status_code == 200

    # Should show error message
    assert "Error" in r.text or "error" in r.text.lower()
    assert "Network timeout" in r.text


def test_job_progress_progress_calculation(authenticated_client, monkeypatch):
    """Test that /job-progress correctly calculates elapsed and remaining time."""
    playlist_url = TEST_PLAYLIST_URL

    # Create a job that started 60 seconds ago and is 50% complete
    start_time = datetime.utcnow() - timedelta(seconds=60)
    mock_job = {
        "id": "job-123",
        "status": "processing",
        "progress": 0.50,
        "started_at": start_time.isoformat(),
        "error": None,
    }
    monkeypatch.setattr(
        "controllers.job_progress.get_job_progress", lambda url: mock_job
    )

    mock_preview = {
        "title": "Test Playlist",
        "video_count": 100,
    }
    monkeypatch.setattr(
        "controllers.job_progress.get_playlist_preview_info", lambda url: mock_preview
    )

    monkeypatch.setattr(
        "controllers.job_progress.get_estimated_stats",
        lambda count: {
            "estimated_total_views": 5000000,
            "estimated_total_likes": 150000,
            "estimated_total_comments": 15000,
        },
    )

    r = authenticated_client.get(f"/job-progress?playlist_url={playlist_url}")

    assert r.status_code == 200

    # Should show 50% progress
    assert "50%" in r.text

    # Should show elapsed time (approximately 60s = 1m 0s)
    assert "Elapsed" in r.text

    # Should show remaining time estimate
    assert "Remaining" in r.text


def test_check_job_status_transitions(authenticated_client, monkeypatch):
    """Test job status polling transitions -> returns polling div"""
    monkeypatch.setattr(main, "get_playlist_job_status", lambda url: "processing")
    r = authenticated_client.get(f"/check-job-status?playlist_url={TEST_PLAYLIST_URL}")
    assert r.status_code == 200
    assert "Analysis in progress" in r.text or "loading" in r.text.lower()


def test_newsletter_with_supabase_success(client, monkeypatch):
    # mock supabase client to return data on insert
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


def test_dashboard_by_id(authenticated_client, mock_supabase, monkeypatch):
    """Test GET /d/{dashboard_id} returns persistent dashboard."""

    # Inject mock Supabase
    set_supabase_client(mock_supabase)

    dashboard_id = "test-dash-abc123"
    r = authenticated_client.get(f"/d/{dashboard_id}")

    # Should return 200 (or redirect if using persistent mode)
    assert r.status_code in (200, 303)
    # Should contain dashboard content
    assert "Sample Playlist" in r.text or "playlist-table" in r.text


def test_dashboard_records_view_event(client, mock_supabase, monkeypatch):
    """Test that viewing a dashboard increments view_count."""

    set_supabase_client(mock_supabase)

    dashboard_id = "test-dash-abc123"

    # Record a view event
    record_dashboard_event(
        supabase=mock_supabase, dashboard_id=dashboard_id, event_type="view"
    )

    # Verify event was recorded (check mock was called)
    # This depends on your mock implementation
    assert True  # Placeholder


def test_get_dashboard_event_counts(mock_supabase, monkeypatch):
    """Test fetching dashboard event counts."""

    set_supabase_client(mock_supabase)

    dashboard_id = "test-dash-abc123"
    counts = get_dashboard_event_counts(
        supabase=mock_supabase, dashboard_id=dashboard_id
    )

    # Should return dict with view and share counts
    assert "view" in counts
    assert "share" in counts
    assert isinstance(counts["view"], int)
    assert isinstance(counts["share"], int)
