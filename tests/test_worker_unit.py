import asyncio
import json
from datetime import datetime
from types import SimpleNamespace

import pytest

import worker.worker as wk
from worker.worker import JobResult, Worker
from constants import JobStatus

# Reuse existing conftest helpers
from tests.conftest import create_test_dataframe


@pytest.mark.asyncio
async def test_worker_process_one_success(monkeypatch, mock_supabase_with_jobs):
    """
    Test worker processes a job successfully.

    Uses mock_supabase_with_jobs fixture from conftest
    """
    job = {
        "id": 1,
        "playlist_url": "https://youtube.com/playlist?list=PL123",
        "status": "pending",
        "created_at": "2024-01-01T12:00:00Z",
    }

    # Mock YouTube service response
    async def fake_get_playlist_data(url, progress_callback=None, max_expanded=20):
        """Return test playlist data."""
        df = create_test_dataframe(num_videos=5)

        return (
            df,
            "My Playlist",
            "Test Channel",
            "https://example.com/thumb.jpg",
            {
                "total_views": 50000,
                "total_likes": 1500,
                "total_comments": 250,
                "actual_playlist_count": 5,
                "avg_engagement": 3.2,
            },
        )

    # Mock YouTube service
    mock_yt_service = SimpleNamespace(
        get_playlist_data=fake_get_playlist_data,
    )

    # ✅ Mock database upsert (make it async)
    async def fake_upsert(stats):
        from db import UpsertResult

        return UpsertResult(
            source="fresh",
            df_json=stats["df_json"],
            summary_stats_json=json.dumps(stats.get("summary_stats", {})),
            dashboard_id="test-dash-abc123",
            error=None,
            raw_row={
                "id": 1,
                "dashboard_id": "test-dash-abc123",
                "playlist_url": job["playlist_url"],
                "title": "My Playlist",
                "channel_name": "Test Channel",
                "processed_video_count": 5,
            },
        )

    # ✅ Mock mark_job_status (the worker calls this to update job)
    async def fake_mark_job_status(job_id, status, updates=None):
        """Mock job status update."""
        # Update the mock database
        if "playlist_jobs" in mock_supabase_with_jobs.data:
            if job_id in mock_supabase_with_jobs.data["playlist_jobs"]:
                mock_supabase_with_jobs.data["playlist_jobs"][job_id]["status"] = status
                if updates:
                    mock_supabase_with_jobs.data["playlist_jobs"][job_id].update(
                        updates
                    )
        return True

    # ✅ Apply all mocks
    monkeypatch.setattr(wk, "yt_service", mock_yt_service)
    monkeypatch.setattr(wk, "upsert_playlist_stats", fake_upsert)
    monkeypatch.setattr(wk, "mark_job_status", fake_mark_job_status)

    # ✅ Add job to mock database (worker expects it to exist)
    if "playlist_jobs" not in mock_supabase_with_jobs.data:
        mock_supabase_with_jobs.data["playlist_jobs"] = {}

    mock_supabase_with_jobs.data["playlist_jobs"][1] = job.copy()

    # ✅ Create worker with mocked services
    worker = Worker(supabase=mock_supabase_with_jobs, yt=mock_yt_service)

    # Process job
    result = await worker.process_one(job, is_retry=False)

    # ✅ Assertions
    assert isinstance(result, JobResult), f"Expected JobResult, got {type(result)}"
    assert result.job_id == 1, f"Expected job_id=1, got {result.job_id}"

    # ✅ Check status - worker should have marked job as complete
    assert result.status == JobStatus.DONE or result.status == "complete", (
        f"Expected status='complete' or 'done', got {result.status!r}. "
        f"Full result: {result}"
    )

    # ✅ Verify no error
    assert result.error is None, f"Expected no error, got {result.error}"

    # ✅ Verify result source
    assert (
        result.result_source == "fresh"
    ), f"Expected source='fresh', got {result.result_source}"

    # ✅ Verify job was updated in database
    updated_job = mock_supabase_with_jobs.data["playlist_jobs"][1]
    assert updated_job["status"] in [
        JobStatus.DONE,
        "complete",
    ], f"Job status not updated in database. Got: {updated_job['status']}"


@pytest.mark.asyncio
async def test_worker_process_one_handles_youtube_error(
    monkeypatch, mock_supabase_with_jobs
):
    """Test worker handles YouTube API errors gracefully."""
    job = {
        "id": 2,
        "playlist_url": "https://youtube.com/playlist?list=PLInvalid",
        "status": "pending",
        "created_at": "2024-01-01T12:00:00Z",
    }

    # ✅ Mock YouTube service to raise error
    async def fake_get_playlist_data_error(
        url, progress_callback=None, max_expanded=20
    ):
        raise Exception("YouTube API quota exceeded")

    mock_yt_service = SimpleNamespace(
        get_playlist_data=fake_get_playlist_data_error,
    )

    # ✅ Mock mark_job_status
    async def fake_mark_job_status(job_id, status, updates=None):
        if "playlist_jobs" in mock_supabase_with_jobs.data:
            if job_id in mock_supabase_with_jobs.data["playlist_jobs"]:
                mock_supabase_with_jobs.data["playlist_jobs"][job_id]["status"] = status
                if updates:
                    mock_supabase_with_jobs.data["playlist_jobs"][job_id].update(
                        updates
                    )
        return True

    monkeypatch.setattr(wk, "yt_service", mock_yt_service)
    monkeypatch.setattr(wk, "mark_job_status", fake_mark_job_status)

    # ✅ Add job to database
    if "playlist_jobs" not in mock_supabase_with_jobs.data:
        mock_supabase_with_jobs.data["playlist_jobs"] = {}

    mock_supabase_with_jobs.data["playlist_jobs"][2] = job.copy()

    worker = Worker(supabase=mock_supabase_with_jobs, yt=mock_yt_service)

    # Process job (should handle error)
    result = await worker.process_one(job, is_retry=False)

    # ✅ Assertions
    assert isinstance(result, JobResult)
    assert result.job_id == 2
    assert result.status == JobStatus.FAILED or result.status == "failed"
    assert result.error is not None
    assert "quota exceeded" in result.error.lower() or "YouTube" in result.error


@pytest.mark.asyncio
async def test_worker_process_one_handles_database_error(
    monkeypatch, mock_supabase_with_jobs
):
    """Test worker handles database errors during upsert."""
    job = {
        "id": 3,
        "playlist_url": "https://youtube.com/playlist?list=PL789",
        "status": "pending",
        "created_at": "2024-01-01T12:00:00Z",
    }

    # ✅ Mock successful YouTube fetch
    async def fake_get_playlist_data(url, progress_callback=None, max_expanded=20):
        df = create_test_dataframe(num_videos=5)
        return (
            df,
            "My Playlist",
            "Test Channel",
            "https://example.com/thumb.jpg",
            {"total_views": 1000},
        )

    # ✅ Mock database error during upsert
    async def fake_upsert_error(stats):
        raise Exception("Database connection lost")

    # ✅ Mock mark_job_status
    async def fake_mark_job_status(job_id, status, updates=None):
        if "playlist_jobs" in mock_supabase_with_jobs.data:
            if job_id in mock_supabase_with_jobs.data["playlist_jobs"]:
                mock_supabase_with_jobs.data["playlist_jobs"][job_id]["status"] = status
                if updates:
                    mock_supabase_with_jobs.data["playlist_jobs"][job_id].update(
                        updates
                    )
        return True

    mock_yt_service = SimpleNamespace(get_playlist_data=fake_get_playlist_data)

    monkeypatch.setattr(wk, "yt_service", mock_yt_service)
    monkeypatch.setattr(wk, "upsert_playlist_stats", fake_upsert_error)
    monkeypatch.setattr(wk, "mark_job_status", fake_mark_job_status)

    # ✅ Add job to database
    if "playlist_jobs" not in mock_supabase_with_jobs.data:
        mock_supabase_with_jobs.data["playlist_jobs"] = {}

    mock_supabase_with_jobs.data["playlist_jobs"][3] = job.copy()

    worker = Worker(supabase=mock_supabase_with_jobs, yt=mock_yt_service)

    # Process job (should handle error)
    result = await worker.process_one(job, is_retry=False)

    # ✅ Assertions
    assert isinstance(result, JobResult)
    assert result.job_id == 3
    assert result.status == JobStatus.FAILED or result.status == "failed"
    assert result.error is not None
    assert "Database" in result.error or "connection" in result.error.lower()
