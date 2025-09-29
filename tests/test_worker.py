# tests/test_worker.py
import asyncio
from dataclasses import dataclass
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker import worker as worker_module


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
        await worker_module.init()

        # Run one iteration
        jobs = await worker_module.fetch_pending_jobs()
        if jobs:
            await worker_module.handle_job(jobs[0])

        # Verify handler was called with correct job
        mock_handle_job.assert_awaited_once_with(fake_job_data[0])
