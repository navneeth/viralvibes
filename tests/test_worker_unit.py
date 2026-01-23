import asyncio
import json
from datetime import datetime
from types import SimpleNamespace

import pytest

import worker.worker as wk
from worker.worker import JobResult, Worker
from constants import JobStatus
from db import UpsertResult

# Reuse existing conftest helpers
from tests.conftest import create_test_dataframe


@pytest.mark.asyncio
async def test_worker_process_one_success(monkeypatch, mock_supabase_with_jobs):
    """
    Test worker processes a job successfully.
    """
    job = {
        "id": 1,
        "playlist_url": "https://youtube.com/playlist?list=PL123",
        "status": "pending",
        "created_at": "2024-01-01T12:00:00Z",
    }

    # ✅ Mock YouTube service response
    async def fake_get_playlist_data(url, progress_callback=None, max_expanded=20):
        """Return test playlist data."""
        # Simulate progress callback
        if progress_callback:
            await progress_callback(0.5, "Processing videos...")

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

    # ✅ Mock YouTube service
    mock_yt_service = SimpleNamespace(
        get_playlist_data=fake_get_playlist_data,
    )

    # ✅ Mock database upsert
    async def fake_upsert(stats):
        return UpsertResult(
            source="fresh",
            df_json=stats.get("df_json", "[]"),
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

    # ✅ Mock mark_job_status - THIS IS CRITICAL
    job_status_updates = {}  # Track status changes

    async def fake_mark_job_status(job_id, status, updates=None):
        """Mock job status update."""
        job_status_updates[job_id] = {"status": status, "updates": updates}

        # Update the mock database
        if "playlist_jobs" in mock_supabase_with_jobs.data:
            if job_id in mock_supabase_with_jobs.data["playlist_jobs"]:
                mock_supabase_with_jobs.data["playlist_jobs"][job_id]["status"] = status
                if updates:
                    mock_supabase_with_jobs.data["playlist_jobs"][job_id].update(
                        updates
                    )

        return True

    # ✅ Mock update_progress - Worker calls this during processing
    async def fake_update_progress(job_id, processed, total):
        """Mock progress update."""
        if "playlist_jobs" in mock_supabase_with_jobs.data:
            if job_id in mock_supabase_with_jobs.data["playlist_jobs"]:
                progress = processed / total if total > 0 else 0.0
                mock_supabase_with_jobs.data["playlist_jobs"][job_id][
                    "progress"
                ] = progress
        return True

    # ✅ Apply all mocks to worker.worker module
    monkeypatch.setattr(wk, "yt_service", mock_yt_service)
    monkeypatch.setattr(wk, "upsert_playlist_stats", fake_upsert)
    monkeypatch.setattr(wk, "mark_job_status", fake_mark_job_status)
    monkeypatch.setattr(wk, "update_progress", fake_update_progress)

    # ✅ Add job to mock database
    if "playlist_jobs" not in mock_supabase_with_jobs.data:
        mock_supabase_with_jobs.data["playlist_jobs"] = {}

    mock_supabase_with_jobs.data["playlist_jobs"][1] = job.copy()

    # ✅ Create worker
    worker = Worker(supabase=mock_supabase_with_jobs, yt=mock_yt_service)

    # 🔍 DEBUG: Print what we're about to process
    print(f"\n🔍 Processing job: {job}")

    # Process job
    result = await worker.process_one(job, is_retry=False)

    # 🔍 DEBUG: Print result and status updates
    print(f"🔍 Result: {result}")
    print(f"🔍 Status updates: {job_status_updates}")
    print(f"🔍 Mock DB job: {mock_supabase_with_jobs.data['playlist_jobs'].get(1)}")

    # ✅ Assertions
    assert isinstance(result, JobResult), f"Expected JobResult, got {type(result)}"
    assert result.job_id == 1, f"Expected job_id=1, got {result.job_id}"

    # ✅ Check that mark_job_status was called
    assert 1 in job_status_updates, "mark_job_status should have been called for job 1"

    # ✅ Check the status that was set
    final_status = job_status_updates[1]["status"]
    assert final_status in [
        JobStatus.DONE,
        "complete",
        "done",
    ], f"Expected final status to be 'done' or 'complete', got {final_status!r}"

    # ✅ Check JobResult fields
    assert result.status in [JobStatus.DONE, "complete", "done", None], (
        f"Expected status='complete' or 'done' or None, got {result.status!r}. "
        f"Full result: {result}"
    )

    # ✅ If status is None, at least error should be None (success case)
    if result.status is None:
        assert (
            result.error is None
        ), "If status is None but job succeeded, error must also be None"
        # Check that the job was actually marked as complete in the database
        assert mock_supabase_with_jobs.data["playlist_jobs"][1]["status"] in [
            JobStatus.DONE,
            "complete",
            "done",
        ]


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

    # ✅ Track status updates
    job_status_updates = {}

    async def fake_mark_job_status(job_id, status, updates=None):
        job_status_updates[job_id] = {"status": status, "updates": updates}

        if "playlist_jobs" in mock_supabase_with_jobs.data:
            if job_id in mock_supabase_with_jobs.data["playlist_jobs"]:
                mock_supabase_with_jobs.data["playlist_jobs"][job_id]["status"] = status
                if updates:
                    mock_supabase_with_jobs.data["playlist_jobs"][job_id].update(
                        updates
                    )
        return True

    async def fake_update_progress(job_id, processed, total):
        return True

    monkeypatch.setattr(wk, "yt_service", mock_yt_service)
    monkeypatch.setattr(wk, "mark_job_status", fake_mark_job_status)
    monkeypatch.setattr(wk, "update_progress", fake_update_progress)

    # ✅ Add job to database
    if "playlist_jobs" not in mock_supabase_with_jobs.data:
        mock_supabase_with_jobs.data["playlist_jobs"] = {}

    mock_supabase_with_jobs.data["playlist_jobs"][2] = job.copy()

    worker = Worker(supabase=mock_supabase_with_jobs, yt=mock_yt_service)

    # Process job (should handle error)
    result = await worker.process_one(job, is_retry=False)

    # 🔍 DEBUG
    print(f"\n🔍 Error handling result: {result}")
    print(f"🔍 Status updates: {job_status_updates}")

    # ✅ Assertions
    assert isinstance(result, JobResult)
    assert result.job_id == 2

    # ✅ Check that job was marked as failed in database
    assert 2 in job_status_updates, "mark_job_status should have been called"
    assert (
        job_status_updates[2]["status"] == JobStatus.FAILED
        or job_status_updates[2]["status"] == "failed"
    )

    # ✅ JobResult may have None status, but error should be set OR database should be updated
    if result.status is None:
        # At minimum, check database was updated
        assert mock_supabase_with_jobs.data["playlist_jobs"][2]["status"] in [
            JobStatus.FAILED,
            "failed",
        ], "Job should be marked as failed in database"
    else:
        assert result.status == JobStatus.FAILED or result.status == "failed"

    # ✅ Error message should contain details (either in result or in database)
    assert (
        result.error is not None
        or job_status_updates[2]["updates"].get("error") is not None
    )


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

    # ✅ Track status updates
    job_status_updates = {}

    async def fake_mark_job_status(job_id, status, updates=None):
        job_status_updates[job_id] = {"status": status, "updates": updates}

        if "playlist_jobs" in mock_supabase_with_jobs.data:
            if job_id in mock_supabase_with_jobs.data["playlist_jobs"]:
                mock_supabase_with_jobs.data["playlist_jobs"][job_id]["status"] = status
                if updates:
                    mock_supabase_with_jobs.data["playlist_jobs"][job_id].update(
                        updates
                    )
        return True

    async def fake_update_progress(job_id, processed, total):
        return True

    mock_yt_service = SimpleNamespace(get_playlist_data=fake_get_playlist_data)

    monkeypatch.setattr(wk, "yt_service", mock_yt_service)
    monkeypatch.setattr(wk, "upsert_playlist_stats", fake_upsert_error)
    monkeypatch.setattr(wk, "mark_job_status", fake_mark_job_status)
    monkeypatch.setattr(wk, "update_progress", fake_update_progress)

    # ✅ Add job to database
    if "playlist_jobs" not in mock_supabase_with_jobs.data:
        mock_supabase_with_jobs.data["playlist_jobs"] = {}

    mock_supabase_with_jobs.data["playlist_jobs"][3] = job.copy()

    worker = Worker(supabase=mock_supabase_with_jobs, yt=mock_yt_service)

    # Process job (should handle error)
    result = await worker.process_one(job, is_retry=False)

    # 🔍 DEBUG
    print(f"\n🔍 DB error handling result: {result}")
    print(f"🔍 Status updates: {job_status_updates}")

    # ✅ Assertions
    assert isinstance(result, JobResult)
    assert result.job_id == 3

    # ✅ Check database was updated
    assert 3 in job_status_updates, "mark_job_status should have been called"
    assert job_status_updates[3]["status"] in [JobStatus.FAILED, "failed"]

    # ✅ Verify error was captured somewhere
    if result.status is None:
        assert mock_supabase_with_jobs.data["playlist_jobs"][3]["status"] in [
            JobStatus.FAILED,
            "failed",
        ]
    else:
        assert result.status in [JobStatus.FAILED, "failed"]

    assert (
        result.error is not None
        or job_status_updates[3]["updates"].get("error") is not None
    )
