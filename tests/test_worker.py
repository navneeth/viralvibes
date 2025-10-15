# tests/test_worker.py
import asyncio
from dataclasses import dataclass
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker.worker import Worker as worker_module
from worker.worker import handle_job, init


@dataclass
class MockPostgrestResponse:
    """Mock PostgrestAPIResponse for testing."""

    data: List[Any]


@pytest.mark.asyncio
async def test_worker_processes_a_pending_job_and_completes():
    """Tests that the worker loop correctly fetches and processes a pending job."""
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

    # Set up patches
    with (
        patch("worker.worker.supabase_client") as mock_supabase,
        patch("worker.worker.handle_job") as mock_handle_job,
        patch("asyncio.sleep", new=AsyncMock()),
    ):
        mock_supabase.table.return_value = mock_table
        mock_handle_job.return_value = None
        # Initialize worker
        await init()

        # Run one iteration of the worker loop
        jobs = await worker_module(supabase=mock_supabase).fetch_pending_jobs()
        if jobs:
            await worker_module(supabase=None).handle_job(jobs[0])

        # Verify handler was called with correct job
        mock_handle_job.assert_awaited_once_with(fake_job_data[0])


@pytest.mark.asyncio
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
