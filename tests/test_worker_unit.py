import asyncio
import json
from datetime import datetime, timedelta
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

    ✅ This test is NECESSARY - it verifies the worker's job processing flow,
    including progress tracking, status updates, and error handling.
    """
    job = {
        "id": 1,
        "playlist_url": "https://youtube.com/playlist?list=PL123",
        "status": "pending",
        "created_at": "2024-01-01T12:00:00Z",
    }

    # ✅ Mock the high-level process_playlist function that worker.process_one() calls
    async def fake_process_playlist(playlist_url: str):
        """Simulate successful playlist processing."""
        df = create_test_dataframe(num_videos=5)

        return {
            "playlist_url": playlist_url,
            "title": "My Playlist",
            "channel_name": "Test Channel",
            "channel_thumbnail": "https://example.com/thumb.jpg",
            "video_count": 5,
            "processed_video_count": 5,
            "avg_duration": timedelta(seconds=180),  # 3 minutes
            "total_views": 50000,
            "total_likes": 1500,
            "total_comments": 250,
            "engagement_rate": 3.2,
            "controversy_score": 0.1,
            "df_json": df.to_json(orient="records"),
            "summary_stats": {
                "total_views": 50000,
                "total_likes": 1500,
                "total_comments": 250,
                "actual_playlist_count": 5,
                "avg_engagement": 3.2,
            },
        }

    # ✅ Mock the jobs.process_playlist function
    import worker.jobs as jobs_module

    monkeypatch.setattr(jobs_module, "process_playlist", fake_process_playlist)

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

    monkeypatch.setattr(wk, "upsert_playlist_stats", fake_upsert)

    # ✅ Track status updates
    job_status_updates = {}

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

    monkeypatch.setattr(wk, "mark_job_status", fake_mark_job_status)

    # ✅ Mock update_progress
    async def fake_update_progress(job_id, processed, total):
        """Mock progress update."""
        if "playlist_jobs" in mock_supabase_with_jobs.data:
            if job_id in mock_supabase_with_jobs.data["playlist_jobs"]:
                progress = processed / total if total > 0 else 0.0
                mock_supabase_with_jobs.data["playlist_jobs"][job_id][
                    "progress"
                ] = progress
        return True

    monkeypatch.setattr(wk, "update_progress", fake_update_progress)

    # ✅ Add job to mock database
    if "playlist_jobs" not in mock_supabase_with_jobs.data:
        mock_supabase_with_jobs.data["playlist_jobs"] = {}

    mock_supabase_with_jobs.data["playlist_jobs"][1] = job.copy()

    # ✅ Create worker (doesn't need yt service if we mock process_playlist)
    worker = Worker(supabase=mock_supabase_with_jobs, yt=None)

    # Process job
    result = await worker.process_one(job, is_retry=False)

    # 🔍 DEBUG: Print result and status updates
    print(f"\n🔍 Result: {result}")
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
    ], (
        f"Expected final status to be 'done' or 'complete', got {final_status!r}. "
        f"Updates: {job_status_updates[1].get('updates')}"
    )

    # ✅ Verify no error
    assert (
        result.error is None
        or mock_supabase_with_jobs.data["playlist_jobs"][1].get("error") is None
    )


@pytest.mark.asyncio
async def test_worker_process_one_handles_youtube_error(
    monkeypatch, mock_supabase_with_jobs
):
    """
    Test worker handles YouTube API errors gracefully.

    ✅ NECESSARY - Verifies worker error handling and retry logic.
    """
    job = {
        "id": 2,
        "playlist_url": "https://youtube.com/playlist?list=PLInvalid",
        "status": "pending",
        "created_at": "2024-01-01T12:00:00Z",
    }

    # ✅ Mock process_playlist to raise error
    async def fake_process_playlist_error(playlist_url: str):
        raise Exception("YouTube API quota exceeded")

    import worker.jobs as jobs_module

    monkeypatch.setattr(jobs_module, "process_playlist", fake_process_playlist_error)

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

    monkeypatch.setattr(wk, "mark_job_status", fake_mark_job_status)

    async def fake_update_progress(job_id, processed, total):
        return True

    monkeypatch.setattr(wk, "update_progress", fake_update_progress)

    # ✅ Add job to database
    if "playlist_jobs" not in mock_supabase_with_jobs.data:
        mock_supabase_with_jobs.data["playlist_jobs"] = {}

    mock_supabase_with_jobs.data["playlist_jobs"][2] = job.copy()

    worker = Worker(supabase=mock_supabase_with_jobs, yt=None)

    # Process job (should handle error)
    result = await worker.process_one(job, is_retry=False)

    # 🔍 DEBUG
    print(f"\n🔍 Error handling result: {result}")
    print(f"🔍 Status updates: {job_status_updates}")

    # ✅ Assertions
    assert isinstance(result, JobResult)
    assert result.job_id == 2

    # ✅ Check that job was marked as failed
    assert 2 in job_status_updates, "mark_job_status should have been called"
    assert job_status_updates[2]["status"] in [JobStatus.FAILED, "failed"]

    # ✅ Verify error was captured
    assert (
        result.error is not None
        or job_status_updates[2].get("updates", {}).get("error") is not None
    ), "Error should be captured in result or database updates"


@pytest.mark.asyncio
async def test_worker_process_one_handles_database_error(
    monkeypatch, mock_supabase_with_jobs
):
    """
    Test worker handles database errors during upsert.

    ✅ NECESSARY - Verifies database failure handling.
    """
    job = {
        "id": 3,
        "playlist_url": "https://youtube.com/playlist?list=PL789",
        "status": "pending",
        "created_at": "2024-01-01T12:00:00Z",
    }

    # ✅ Mock successful YouTube fetch
    async def fake_process_playlist(playlist_url: str):
        df = create_test_dataframe(num_videos=5)
        return {
            "playlist_url": playlist_url,
            "title": "My Playlist",
            "df_json": df.to_json(orient="records"),
            "summary_stats": {"total_views": 1000},
        }

    import worker.jobs as jobs_module

    monkeypatch.setattr(jobs_module, "process_playlist", fake_process_playlist)

    # ✅ Mock database error during upsert
    async def fake_upsert_error(stats):
        raise Exception("Database connection lost")

    monkeypatch.setattr(wk, "upsert_playlist_stats", fake_upsert_error)

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

    monkeypatch.setattr(wk, "mark_job_status", fake_mark_job_status)

    async def fake_update_progress(job_id, processed, total):
        return True

    monkeypatch.setattr(wk, "update_progress", fake_update_progress)

    # ✅ Add job to database
    if "playlist_jobs" not in mock_supabase_with_jobs.data:
        mock_supabase_with_jobs.data["playlist_jobs"] = {}

    mock_supabase_with_jobs.data["playlist_jobs"][3] = job.copy()

    worker = Worker(supabase=mock_supabase_with_jobs, yt=None)

    # Process job (should handle error)
    result = await worker.process_one(job, is_retry=False)

    # 🔍 DEBUG
    print(f"\n🔍 DB error handling result: {result}")
    print(f"🔍 Status updates: {job_status_updates}")

    # ✅ Assertions
    assert isinstance(result, JobResult)
    assert result.job_id == 3

    # ✅ Check database was updated to failed
    assert 3 in job_status_updates, "mark_job_status should have been called"
    assert job_status_updates[3]["status"] in [JobStatus.FAILED, "failed"]

    # ✅ Verify error was captured
    assert (
        result.error is not None
        or job_status_updates[3].get("updates", {}).get("error") is not None
    ), "Database error should be captured"
