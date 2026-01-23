import asyncio
import json
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

import db  # ✅ Import db module to patch it
import worker.jobs as jobs_module
import worker.worker as wk
from constants import JobStatus
from db import UpsertResult

# Reuse existing conftest helpers
from tests.conftest import create_test_dataframe
from worker.worker import JobResult, Worker


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

    # ✅ Create a mock YouTube service that returns data without network calls
    async def fake_get_playlist_data(url, progress_callback=None, max_expanded=20):
        """Return test playlist data without making API calls."""
        if progress_callback:
            await progress_callback(0.5, "Processing videos...")

        df = create_test_dataframe(num_videos=5)
        print(
            f"🔍 fake_get_playlist_data: created df type={type(df)}, shape={df.shape if hasattr(df, 'shape') else 'no shape'}"
        )

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

    # ✅ Patch the YouTube service in BOTH modules
    monkeypatch.setattr(wk, "yt_service", mock_yt_service)
    monkeypatch.setattr(jobs_module, "yt_service", mock_yt_service)

    # ✅ Mock database upsert - patch in db module
    # NOTE: upsert_playlist_stats is NOT async, so the fake shouldn't be either
    def fake_upsert(stats):
        """Mock upsert that returns a complete UpsertResult."""
        # Worker passes raw "df" (Polars DataFrame) and "summary_stats" (dict)
        # We need to serialize them like the real function does

        print(f"\n{'=' * 80}")
        print(f"🔍 fake_upsert called!")
        print(f"🔍 Received stats keys: {list(stats.keys())}")

        df = stats.get("df")
        summary_stats = stats.get("summary_stats", {})

        print(f"🔍 df type: {type(df)}")
        print(f"🔍 df is None: {df is None}")
        if df is not None and hasattr(df, "shape"):
            print(f"🔍 df shape: {df.shape}")
        if df is not None and hasattr(df, "is_empty"):
            print(f"🔍 df.is_empty(): {df.is_empty()}")

        # Serialize DataFrame to JSON
        df_json = None
        if df is not None:
            try:
                df_json = df.write_json()
                print(
                    f"🔍 ✅ Successfully serialized df to JSON, length: {len(df_json)}"
                )
            except Exception as e:
                print(f"🔍 ❌ Error serializing df: {e}")
                import traceback

                traceback.print_exc()
                df_json = None

        # Serialize summary_stats to JSON
        summary_stats_json = json.dumps(summary_stats) if summary_stats else "{}"
        print(f"🔍 summary_stats_json length: {len(summary_stats_json)}")

        print(f"{'=' * 80}")

        result = UpsertResult(
            source="fresh",
            df_json=df_json,  # ✅ Serialized DataFrame (or None)
            summary_stats_json=summary_stats_json,  # ✅ Serialized summary_stats
            error=None,
            raw_row={
                "id": 1,
                "dashboard_id": "test-dash-abc123",
                "playlist_url": job["playlist_url"],
                "title": stats.get("title", "My Playlist"),
                "channel_name": stats.get("channel_name", "Test Channel"),
                "processed_video_count": stats.get("processed_video_count", 5),
            },
        )

        # Debug: show what the result looks like
        result_dict = vars(result)
        print(f"🔍 result_dict keys: {list(result_dict.keys())}")
        print(
            f"🔍 result_dict df_json type: {type(result_dict.get('df_json'))}, truthy: {bool(result_dict.get('df_json'))}"
        )
        print(
            f"🔍 result_dict summary_stats_json type: {type(result_dict.get('summary_stats_json'))}, truthy: {bool(result_dict.get('summary_stats_json'))}"
        )

        print(f"🔍 Returning UpsertResult with df_json: {result.df_json is not None}")

        return result

    # ✅ Patch upsert in BOTH db module (where it's defined) AND worker module (where it's imported)
    # Since worker does: from db import upsert_playlist_stats, we need to patch both locations
    monkeypatch.setattr(db, "upsert_playlist_stats", fake_upsert)
    monkeypatch.setattr(wk, "upsert_playlist_stats", fake_upsert)

    # ✅ Track status updates
    job_status_updates = {}

    async def fake_mark_job_status(job_id, status, updates=None):
        """Mock job status update."""
        print(f"🔍 mark_job_status: job_id={job_id}, status={status}")
        job_status_updates[job_id] = {"status": status, "updates": updates}

        if "playlist_jobs" in mock_supabase_with_jobs.data:
            if job_id in mock_supabase_with_jobs.data["playlist_jobs"]:
                mock_supabase_with_jobs.data["playlist_jobs"][job_id]["status"] = status
                if updates:
                    mock_supabase_with_jobs.data["playlist_jobs"][job_id].update(
                        updates
                    )

        return True

    # ✅ Patch mark_job_status in worker.worker module (where it's defined)
    monkeypatch.setattr(wk, "mark_job_status", fake_mark_job_status)

    # ✅ Mock update_progress
    async def fake_update_progress(job_id, processed, total):
        if "playlist_jobs" in mock_supabase_with_jobs.data:
            if job_id in mock_supabase_with_jobs.data["playlist_jobs"]:
                progress = processed / total if total > 0 else 0.0
                mock_supabase_with_jobs.data["playlist_jobs"][job_id][
                    "progress"
                ] = progress
        return True

    # ✅ Patch update_progress in worker.worker module (where it's defined)
    monkeypatch.setattr(wk, "update_progress", fake_update_progress)

    # ✅ Add job to mock database
    if "playlist_jobs" not in mock_supabase_with_jobs.data:
        mock_supabase_with_jobs.data["playlist_jobs"] = {}

    mock_supabase_with_jobs.data["playlist_jobs"][1] = job.copy()

    # ✅ Create worker WITH mock YouTube service
    worker = Worker(supabase=mock_supabase_with_jobs, yt=mock_yt_service)

    # Process job
    print("\n" + "=" * 80)
    print("🚀 Starting worker.process_one()")
    print("=" * 80)

    result = await worker.process_one(job, is_retry=False)

    # 🔍 DEBUG
    print("\n" + "=" * 80)
    print("📊 RESULTS:")
    print("=" * 80)
    print(f"Result: {result}")
    print(f"Status updates: {job_status_updates}")
    print("=" * 80 + "\n")

    # ✅ Assertions
    assert isinstance(result, JobResult)
    assert result.job_id == 1

    assert 1 in job_status_updates

    final_status = job_status_updates[1]["status"]
    assert final_status in [JobStatus.DONE, "complete", "done"], (
        f"Expected 'done', got {final_status!r}. "
        f"Updates: {job_status_updates[1].get('updates')}"
    )

    final_error = job_status_updates[1].get("updates", {}).get("error")
    assert (
        result.error is None and final_error is None
    ), f"Got error: {result.error or final_error}"


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

    async def fake_get_playlist_data_error(
        url, progress_callback=None, max_expanded=20
    ):
        raise Exception("YouTube API quota exceeded")

    mock_yt_service = SimpleNamespace(get_playlist_data=fake_get_playlist_data_error)

    monkeypatch.setattr(wk, "yt_service", mock_yt_service)
    monkeypatch.setattr(jobs_module, "yt_service", mock_yt_service)

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

    # ✅ Patch in worker.worker, not db
    monkeypatch.setattr(wk, "mark_job_status", fake_mark_job_status)

    async def fake_update_progress(job_id, processed, total):
        return True

    # ✅ Patch in worker.worker, not db
    monkeypatch.setattr(wk, "update_progress", fake_update_progress)

    if "playlist_jobs" not in mock_supabase_with_jobs.data:
        mock_supabase_with_jobs.data["playlist_jobs"] = {}

    mock_supabase_with_jobs.data["playlist_jobs"][2] = job.copy()

    worker = Worker(supabase=mock_supabase_with_jobs, yt=mock_yt_service)
    result = await worker.process_one(job, is_retry=False)

    assert isinstance(result, JobResult)
    assert result.job_id == 2
    assert 2 in job_status_updates
    assert job_status_updates[2]["status"] in [JobStatus.FAILED, "failed"]
    assert (
        result.error is not None
        or job_status_updates[2].get("updates", {}).get("error") is not None
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

    async def fake_get_playlist_data(url, progress_callback=None, max_expanded=20):
        df = create_test_dataframe(num_videos=5)
        return (
            df,
            "My Playlist",
            "Test Channel",
            "https://example.com/thumb.jpg",
            {"total_views": 1000},
        )

    mock_yt_service = SimpleNamespace(get_playlist_data=fake_get_playlist_data)

    monkeypatch.setattr(wk, "yt_service", mock_yt_service)
    monkeypatch.setattr(jobs_module, "yt_service", mock_yt_service)

    async def fake_upsert_error(stats):
        raise Exception("Database connection lost")

    # ✅ Patch in db module (where upsert is defined)
    monkeypatch.setattr(db, "upsert_playlist_stats", fake_upsert_error)

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

    # ✅ Patch in worker.worker (where mark_job_status is defined)
    monkeypatch.setattr(wk, "mark_job_status", fake_mark_job_status)

    async def fake_update_progress(job_id, processed, total):
        return True

    # ✅ Patch in worker.worker (where update_progress is defined)
    monkeypatch.setattr(wk, "update_progress", fake_update_progress)

    if "playlist_jobs" not in mock_supabase_with_jobs.data:
        mock_supabase_with_jobs.data["playlist_jobs"] = {}

    mock_supabase_with_jobs.data["playlist_jobs"][3] = job.copy()

    worker = Worker(supabase=mock_supabase_with_jobs, yt=mock_yt_service)
    result = await worker.process_one(job, is_retry=False)

    assert isinstance(result, JobResult)
    assert result.job_id == 3
    assert 3 in job_status_updates
    assert job_status_updates[3]["status"] in [JobStatus.FAILED, "failed"]
    assert (
        result.error is not None
        or job_status_updates[3].get("updates", {}).get("error") is not None
    )
