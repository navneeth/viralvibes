"""
Progress callback tolerance tests.

Tests that the worker's progress tracking mechanism:
1. Handles various callback argument patterns from YouTube service
2. Correctly updates job progress in database
3. Doesn't crash the job on callback errors
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

import worker.worker as wk
from worker.worker import Worker


@pytest.fixture
def fake_df():
    """Sample DataFrame for testing."""
    return pl.DataFrame(
        {
            "video_id": ["vid1", "vid2", "vid3"],
            "title": ["Video 1", "Video 2", "Video 3"],
            "views": [1000, 2000, 3000],
            "likes": [100, 200, 300],
            "published_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )


@pytest.fixture
def patch_upsert(monkeypatch):
    """Mock upsert_playlist_stats to avoid database writes."""

    async def fake_upsert(*args, **kwargs):
        return {"id": "dashboard-123"}

    monkeypatch.setattr("worker.worker.upsert_playlist_stats", fake_upsert)


@pytest.mark.asyncio
async def test_progress_updates_tracked_in_database(
    mock_supabase_with_jobs, fake_df, patch_upsert, monkeypatch
):
    """
    CRITICAL: Test that progress updates actually write to database.

    This is the PRIMARY function of progress callbacks - ensuring
    users can see real-time job progress.
    """
    progress_updates = []

    async def fake_get_playlist_data(url, progress_callback=None):
        """Simulate YouTube service making progress callbacks."""
        if progress_callback:
            # Simulate processing 10 videos with incremental updates
            await progress_callback(3, 10)  # 30% done
            await progress_callback(7, 10)  # 70% done
            await progress_callback(10, 10)  # 100% done

        return (
            fake_df,
            "Test Playlist",
            "Test Channel",
            "thumb.jpg",
            {"total_views": 1000},
        )

    # Track actual update_progress calls
    def tracked_update_progress(job_id, processed, total):
        progress_updates.append({"processed": processed, "total": total})

    monkeypatch.setattr(
        wk, "yt_service", AsyncMock(get_playlist_data=fake_get_playlist_data)
    )
    monkeypatch.setattr(wk, "update_progress", tracked_update_progress)

    # Get job from mock database
    jobs = mock_supabase_with_jobs.table("playlist_jobs").select("*").limit(1).execute()
    job = (
        jobs.data[0]
        if jobs.data
        else {"id": 1, "playlist_url": "https://youtube.com/test"}
    )

    worker = Worker(supabase=mock_supabase_with_jobs)
    result = await worker.process_one(job)

    # ✅ Verify progress was tracked
    assert len(progress_updates) == 3
    assert progress_updates[0] == {"processed": 3, "total": 10}
    assert progress_updates[1] == {"processed": 7, "total": 10}
    assert progress_updates[2] == {"processed": 10, "total": 10}

    # ✅ Verify job completed successfully (worker returns "complete", not "done")
    assert result.status == "complete"


@pytest.mark.asyncio
async def test_progress_callback_handles_multiple_argument_patterns(
    mock_supabase_with_jobs, fake_df, patch_upsert, monkeypatch
):
    """
    FUNCTIONAL: Test progress callback adapts to different argument patterns.

    YouTube service may call progress callback with:
    - (processed, total)
    - (processed, total, metadata)
    - {"processed": N, "total": M}

    Worker must handle all patterns without crashing.
    """
    progress_updates = []

    async def fake_get_playlist_data(url, progress_callback=None):
        """Simulate various callback patterns."""
        if progress_callback:
            # Pattern 1: Two arguments
            await progress_callback(2, 10)

            # Pattern 2: Three arguments with metadata
            await progress_callback(5, 10, {"video_id": "xyz123"})

            # Pattern 3: Dict argument
            await progress_callback(
                {"processed": 8, "total": 10, "status": "analyzing"}
            )

        return (
            fake_df,
            "Multi-Pattern Test",
            "Channel",
            "thumb.jpg",
            {"total_views": 500},
        )

    def tracked_update(job_id, processed, total):
        progress_updates.append((processed, total))

    monkeypatch.setattr(
        wk, "yt_service", AsyncMock(get_playlist_data=fake_get_playlist_data)
    )
    monkeypatch.setattr(wk, "update_progress", tracked_update)

    jobs = mock_supabase_with_jobs.table("playlist_jobs").select("*").limit(1).execute()
    job = (
        jobs.data[0]
        if jobs.data
        else {"id": 1, "playlist_url": "https://youtube.com/test"}
    )

    worker = Worker(supabase=mock_supabase_with_jobs)
    result = await worker.process_one(job)

    # ✅ All three patterns should be parsed correctly
    assert len(progress_updates) == 3
    assert progress_updates[0] == (2, 10)
    assert progress_updates[1] == (5, 10)
    assert progress_updates[2] == (8, 10)

    # ✅ Job should complete despite varied patterns
    assert result.status == "complete"


@pytest.mark.asyncio
async def test_progress_callback_errors_dont_crash_job(
    mock_supabase_with_jobs, fake_df, patch_upsert, monkeypatch
):
    """
    FAULT TOLERANCE: Test that progress callback errors don't kill the job.

    If progress tracking fails (DB issue, invalid data, etc.),
    the job should still complete - progress is non-critical.
    """
    callback_errors = []

    async def fake_get_playlist_data(url, progress_callback=None):
        """Simulate callbacks that cause errors."""
        if progress_callback:
            # These should all fail gracefully
            try:
                await progress_callback(None, None)  # Invalid: None values
            except Exception as e:
                callback_errors.append(("none_values", str(e)))

            try:
                await progress_callback("not_a_number", "also_not")  # Invalid: strings
            except Exception as e:
                callback_errors.append(("invalid_types", str(e)))

            # At least one valid callback to ensure job processes
            await progress_callback(5, 10)

        return fake_df, "Error Test", "Channel", "thumb.jpg", {"total_views": 100}

    # Make update_progress fail on invalid data but succeed on valid
    def strict_update_progress(job_id, processed, total):
        if not isinstance(processed, int) or not isinstance(total, int):
            raise ValueError(f"Invalid progress values: {processed}, {total}")
        # Valid data - just pass

    monkeypatch.setattr(
        wk, "yt_service", AsyncMock(get_playlist_data=fake_get_playlist_data)
    )
    monkeypatch.setattr(wk, "update_progress", strict_update_progress)

    jobs = mock_supabase_with_jobs.table("playlist_jobs").select("*").limit(1).execute()
    job = (
        jobs.data[0]
        if jobs.data
        else {"id": 1, "playlist_url": "https://youtube.com/test"}
    )

    worker = Worker(supabase=mock_supabase_with_jobs)
    result = await worker.process_one(job)

    # ✅ Job should complete despite progress callback errors
    assert result.status == "complete"


@pytest.mark.asyncio
async def test_progress_updates_reflect_actual_video_processing(
    mock_supabase_with_jobs, patch_upsert, monkeypatch
):
    """
    HIGH VALUE: Test progress updates correlate with actual work being done.

    Progress should increment as videos are fetched, not arbitrarily.
    This ensures users see accurate real-time progress.
    """
    progress_history = []
    videos_processed = []

    async def fake_get_playlist_data(url, progress_callback=None):
        """Simulate realistic video processing with progress updates."""

        # Simulate fetching 5 videos incrementally
        for i in range(1, 6):
            video_id = f"video_{i}"
            videos_processed.append(video_id)

            if progress_callback:
                await progress_callback(i, 5)  # Update after each video

        # Return realistic dataframe
        df = pl.DataFrame(
            {
                "video_id": [f"video_{i}" for i in range(1, 6)],
                "title": [f"Video {i}" for i in range(1, 6)],
                "views": [1000 * i for i in range(1, 6)],
                "likes": [100 * i for i in range(1, 6)],
                "published_at": ["2024-01-01"] * 5,
            }
        )

        return df, "Progressive Test", "Channel", "thumb.jpg", {"total_views": 15000}

    def track_progress(job_id, processed, total):
        progress_history.append(
            {
                "videos_done": len(videos_processed),
                "progress_reported": processed,
                "total": total,
            }
        )

    monkeypatch.setattr(
        wk, "yt_service", AsyncMock(get_playlist_data=fake_get_playlist_data)
    )
    monkeypatch.setattr(wk, "update_progress", track_progress)

    jobs = mock_supabase_with_jobs.table("playlist_jobs").select("*").limit(1).execute()
    job = (
        jobs.data[0]
        if jobs.data
        else {"id": 1, "playlist_url": "https://youtube.com/test"}
    )

    worker = Worker(supabase=mock_supabase_with_jobs)
    result = await worker.process_one(job)

    # ✅ Progress updates should match actual work
    assert len(progress_history) == 5

    for i, update in enumerate(progress_history, start=1):
        assert update["videos_done"] == i
        assert update["progress_reported"] == i
        assert update["total"] == 5

    # ✅ Job should complete successfully
    assert result.status == "complete"
    assert len(videos_processed) == 5


@pytest.mark.asyncio
async def test_no_progress_callback_still_completes_job(
    mock_supabase_with_jobs, fake_df, patch_upsert, monkeypatch
):
    """
    EDGE CASE: Test job completes even if no progress callbacks are made.

    Some YouTube service implementations might not support progress tracking.
    The job should still work without it.
    """

    async def fake_get_playlist_data(url, progress_callback=None):
        """Service that doesn't call progress_callback."""
        # Intentionally don't call progress_callback
        return fake_df, "No Progress", "Channel", "thumb.jpg", {"total_views": 1000}

    monkeypatch.setattr(
        wk, "yt_service", AsyncMock(get_playlist_data=fake_get_playlist_data)
    )

    jobs = mock_supabase_with_jobs.table("playlist_jobs").select("*").limit(1).execute()
    job = (
        jobs.data[0]
        if jobs.data
        else {"id": 1, "playlist_url": "https://youtube.com/test"}
    )

    worker = Worker(supabase=mock_supabase_with_jobs)
    result = await worker.process_one(job)

    # ✅ Job should complete successfully without progress updates
    assert result.status == "complete"
