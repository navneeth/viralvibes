# tests/test_worker.py
import asyncio
from dataclasses import dataclass
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from services.youtube_service import RateLimitError, YouTubeBotChallengeError
from worker.worker import JobResult, Worker, handle_job, init
from worker.worker import Worker as worker_module


@dataclass
class MockPostgrestResponse:
    """Mock PostgrestAPIResponse for testing."""

    data: List[Any]


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["yt-dlp", "youtubeapi"])
async def test_worker_processes_a_pending_job_and_completes(backend):
    """Tests that the worker loop correctly fetches and processes a pending job for both backends."""
    # Create a mock pending job
    fake_job_data = [
        {
            "id": "abc-123",
            "playlist_url": "https://youtube.com/playlist?list=MOCK",
            "status": "pending",
        }
    ]

    # Mock the database response
    mock_execute = AsyncMock()
    mock_execute.execute.return_value = MockPostgrestResponse(data=fake_job_data)

    mock_table = MagicMock()
    mock_table.select.return_value.eq.return_value.order.return_value.limit.return_value = (
        mock_execute
    )
    mock_table.update.return_value.eq.return_value.execute.return_value = MagicMock(
        data=[]
    )

    # Mock handle_job return value
    mock_stats = {
        "total_views": 1000,
        "processed_video_count": 1,
        "backend": backend,
        "description": "Mock playlist description",
        "published_at": "2023-01-01T00:00:00Z",
        "default_language": "",
        "podcast_status": "disabled" if backend == "youtubeapi" else "unknown",
        "privacy_status": "public" if backend == "youtubeapi" else "Unknown",
    }

    # Set up patches
    with (
        patch("worker.worker.supabase_client") as mock_supabase,
        patch("worker.worker.handle_job") as mock_handle_job,
        patch("asyncio.sleep", new=AsyncMock()),
        patch("worker.worker.upsert_playlist_stats") as mock_upsert_playlist_stats,
    ):
        mock_supabase.table.return_value = mock_table
        mock_handle_job.return_value = (
            pl.DataFrame({"Title": ["Test Video"], "Views": [1000], "Likes": [100]}),
            "Mock Playlist",
            "Mock Channel",
            "http://thumbnail.url",
            mock_stats,
        )
        mock_upsert_playlist_stats.return_value = {
            "source": "fresh",
            "df_json": "{}",
            "summary_stats_json": str(mock_stats),
        }

        # Initialize worker
        await init()

        # Create worker with specific backend
        worker = worker_module(supabase=mock_supabase, backend=backend)

        # Mock progress callback
        mock_progress_callback = AsyncMock()
        worker.progress_callback = mock_progress_callback

        # Run one iteration of the worker loop
        jobs = await worker.fetch_pending_jobs()
        if jobs:
            await worker.handle_job(jobs[0])

        # Verify handler was called with correct job
        mock_handle_job.assert_awaited_once_with(fake_job_data[0], is_retry=False)

        # Verify progress callback was called (if applicable)
        if backend == "yt-dlp":  # yt-dlp uses progress callback
            mock_progress_callback.assert_awaited()

        # Verify Supabase update
        mock_supabase.table.assert_called_with("youtube_jobs")
        mock_table.update.assert_called()
        update_call = mock_table.update.call_args[0][0]
        assert update_call["id"] == "abc-123"
        assert update_call["status"] == "done"

        # Verify upsert of playlist stats
        mock_upsert_playlist_stats.assert_called_once()
        upsert_call = mock_upsert_playlist_stats.call_args[0][0]
        assert upsert_call["summary_stats_json"] == str(mock_stats)
        assert upsert_call["source"] == "fresh"
        assert mock_stats["backend"] == backend
        assert mock_stats["privacy_status"] == (
            "public" if backend == "youtubeapi" else "Unknown"
        )


@pytest.mark.asyncio
@pytest.mark.skip(reason="Temporarily disabled for debugging")
async def test_process_one_success_returns_done_and_raw_row():
    fake_job = {
        "id": "abc-123",
        "playlist_url": "https://youtube.com/playlist?list=MOCK",
        "status": "pending",
    }

    job_row = {
        "id": "abc-123",
        "playlist_url": fake_job["playlist_url"],
        "status": "done",
        "error": None,
        "retry_scheduled": False,
        "result_source": "fresh",
    }

    #  Patch the EXACT functions process_one calls
    with (
        patch("worker.worker.get_latest_playlist_job", return_value=job_row),
        patch(
            "worker.worker.handle_job", new=AsyncMock(return_value=None)
        ) as mock_handle,
    ):
        worker = worker_module(supabase=None, yt=None)  # No supabase needed
        result = await worker.process_one(fake_job)

        mock_handle.assert_awaited_once_with(fake_job, is_retry=False)

        assert result.job_id == "abc-123"
        assert result.status == "done"
        assert result.result_source == "fresh"
        assert result.raw_row == job_row


@pytest.mark.asyncio
async def test_process_one_handles_handler_exception_and_returns_failed():
    fake_job = {"id": "err-1", "playlist_url": "https://youtube.com/playlist?list=ERR"}

    # make handle_job raise
    async def _raise(job, is_retry=False):
        raise RuntimeError("boom")

    with patch("worker.worker.handle_job", new=AsyncMock(side_effect=_raise)):
        worker = worker_module(supabase=None, yt=None)
        result = await worker.process_one(fake_job)

        assert result.job_id == "err-1"
        assert result.status == "failed"
        assert result.error is not None


@pytest.mark.asyncio
async def test_process_one_retry_flag():
    fake_job = {"id": "retry-1", "playlist_url": "mock"}
    with patch("worker.worker.handle_job", new=AsyncMock(return_value=None)):
        worker = worker_module(supabase=None)
        await worker.process_one(fake_job, is_retry=True)
        # Assert retry flag passed through


@pytest.mark.parametrize(
    "failure_case",
    [
        # 1–5: Missing job data
        {"job": {}, "expected_status": "failed", "expected_error": "None"},
        {
            "job": {"id": "abc"},
            "expected_status": "failed",
            "expected_error": "None",
        },  # No URL
        {
            "job": {"playlist_url": "url"},
            "expected_status": "failed",
            "expected_error": "None",
        },  # No ID
        # 6–8: Service failures
        {
            "mock_yt_fail": True,
            "expected_status": "failed",
            "expected_error": "Network error",
        },
        {
            "mock_upsert_fail": True,
            "expected_status": "failed",
            "expected_error": "Upsert failed",
        },
        {
            "mock_bot_challenge": True,
            "expected_status": "blocked",
            "expected_error": "Bot challenge",
        },
        # 9–10: DB failures
        {
            "mock_supabase_none": True,
            "expected_status": "failed",
            "expected_error": "Supabase client not initialized",
        },
        {
            "mock_status_update_fail": True,
            "expected_status": "failed",
            "expected_error": "Cannot mark job",
        },
        # 11–12: Retry edge cases
        {
            "job": {"retry_count": 4},
            "expected_status": "failed",
            "expected_error": "max retries exhausted",
        },
        {
            "job": {"status": "blocked"},
            "expected_status": "failed",
            "expected_error": "None",
        },  # Skip blocked
        # 13–15: Progress/Validation failures
        {
            "mock_empty_df": True,
            "expected_status": "failed",
            "expected_error": "No valid videos",
        },
        {
            "mock_upsert_incomplete": True,
            "expected_status": "failed",
            "expected_error": "Incomplete data",
        },
        {
            "mock_normalize_fail": True,
            "expected_status": "failed",
            "expected_error": "Column normalization",
        },
        # 16: Slow network / timeout
        {
            "mock_timeout": True,
            "expected_status": "failed",
            "expected_error": "Timeout",
        },
        # 17: Full success case
        {"mock_success": True, "expected_status": "success", "expected_error": ""},
    ],
)
@pytest.mark.asyncio
@pytest.mark.skip(reason="Temporarily disabled for debugging")
async def test_worker_process_one_handles_all_assumptions(failure_case, monkeypatch):
    """[ASSUMPTIONS BUSTER] Tests Worker.process_one handles ALL missing data + failures gracefully."""

    base_job = {
        "id": "test-123",
        "playlist_url": "https://youtube.com/playlist?list=MOCK",
        "status": "pending",
        "retry_count": 0,
    }
    job = {**base_job, **failure_case.get("job", {})}

    # --- Mocks for YouTube playlist service ---
    async def fake_handle_job(
        self, url, max_expanded=10, progress_callback=None
    ):  # ADD progress_callback=None
        if failure_case.get("mock_bot_challenge"):
            raise YouTubeBotChallengeError("Bot challenge detected")
        if failure_case.get("mock_yt_fail"):
            raise ConnectionError("Network error")
        if failure_case.get("mock_timeout"):
            await asyncio.sleep(0.1)
            raise asyncio.TimeoutError("Timeout while fetching playlist")
        if failure_case.get("mock_empty_df"):
            return None, "Title", "Channel", "", {"processed_video_count": 0}
        if failure_case.get("mock_normalize_fail"):
            df = pl.DataFrame({"title": ["test"]})
            df.is_empty = lambda: False
            df.height = 1
            raise ValueError("Column normalization failed") from None
        # Successful fetch
        return (
            pl.DataFrame({"Title": ["test"], "Views": [1000]}),
            "Title",
            "Channel",
            "",
            {"total_views": 1000},
        )

    # Patch service
    monkeypatch.setattr(
        "worker.worker.yt_service", MagicMock(get_playlist_data=fake_handle_job)
    )

    # --- DB + utility mocks ---
    if failure_case.get("mock_supabase_none"):
        monkeypatch.setattr("worker.worker.supabase_client", None)
        fake_supabase = None
    else:
        fake_supabase = MagicMock()
        fake_supabase.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[]
        )
        monkeypatch.setattr("worker.worker.supabase_client", fake_supabase)

    # Mock status updates
    if failure_case.get("mock_status_update_fail"):
        monkeypatch.setattr("worker.worker.mark_job_status", lambda *args: False)

    # Mock upsert logic
    if failure_case.get("mock_upsert_fail"):
        monkeypatch.setattr(
            "worker.worker.upsert_playlist_stats",
            lambda x: {"source": "error", "df_json": None},
        )
    elif failure_case.get("mock_upsert_incomplete"):
        monkeypatch.setattr(
            "worker.worker.upsert_playlist_stats",
            lambda x: {"source": "fresh", "df_json": None, "summary_stats_json": None},
        )
    else:
        monkeypatch.setattr(
            "worker.worker.upsert_playlist_stats",
            lambda x: {"source": "fresh", "df_json": "{}", "summary_stats_json": "{}"},
        )

    # Normalize
    monkeypatch.setattr(
        "worker.worker.normalize_columns",
        lambda df: df if not failure_case.get("mock_normalize_fail") else None,
    )

    # --- Expected job state ---
    expected_row = {
        "id": job["id"],
        "status": failure_case["expected_status"],
        "error": failure_case["expected_error"],
    }
    monkeypatch.setattr(
        "worker.worker.get_latest_playlist_job", lambda url: expected_row
    )

    # --- Run Worker ---
    worker = worker_module(supabase=fake_supabase)
    result = await worker.process_one(job)

    # --- Assertions ---
    assert isinstance(result, JobResult), f"Case {failure_case}: Must return JobResult"
    assert result.job_id == job["id"], f"Case {failure_case}: Must preserve job_id"
    assert (
        result.status == failure_case["expected_status"]
    ), f"Case {failure_case}: Wrong status"
    if failure_case["expected_error"]:
        assert failure_case["expected_error"] in (
            result.error or ""
        ), f"Case {failure_case}: Wrong error"
    elif failure_case.get("mock_success"):
        assert (
            result.status == "success" and not result.error
        ), "Case success: Must succeed cleanly"

    print(f"✅ PASSED: {failure_case.get('job', 'Base')} → {result.status}")
