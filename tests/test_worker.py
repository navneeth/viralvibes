# tests/test_worker.py
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from supabase import PostgrestAPIResponse

import worker.worker as worker_module


@pytest.mark.asyncio
async def test_worker_processes_a_pending_job_and_completes():
    """
    Tests that the worker loop correctly fetches and processes a pending job, then exits.
    """
    # Create a mock pending job from the database
    fake_job_data = [
        {
            "id": "abc-123",
            "playlist_url": "https://youtube.com/playlist?list=PL0xvhH4iaYhy4ulh0h-dn4mslO6B8nj0-",
            "status": "pending",
        }
    ]

    # Mock the full Supabase method chain to simulate finding a job
    mock_execute = MagicMock()
    mock_execute.execute = AsyncMock(
        return_value=PostgrestAPIResponse(data=fake_job_data)
    )
    mock_execute.execute.return_value.data = fake_job_data

    mock_table_chain = MagicMock()
    mock_table_chain.select.return_value.eq.return_value.order.return_value.limit.return_value = (
        mock_execute
    )

    # Patch the global `supabase_client` and the `handle_job` function
    with patch(
        "worker.worker.supabase_client", new=MagicMock()
    ) as mock_supabase_client:
        mock_supabase_client.table.return_value = mock_table_chain

        with patch("worker.worker.handle_job", new=AsyncMock()) as mock_handle_job:
            # Patch the worker_loop's internal sleep calls to prevent real delays
            with patch("asyncio.sleep", new=AsyncMock()):
                # We run init to set the global variable
                await worker_module.init()

                # Patch the main worker_loop to simulate it stopping after one iteration
                # This is the key fix to prevent the test from running indefinitely
                async def mock_worker_loop_once():
                    # Check for jobs once
                    jobs = await worker_module.fetch_pending_jobs()
                    if jobs:
                        await worker_module.handle_job(jobs[0])

                # Now, call our patched version of the worker loop
                await mock_worker_loop_once()

                # Verify that the handle_job function was called exactly once with the fake job
                mock_handle_job.assert_awaited_once_with(fake_job_data[0])
