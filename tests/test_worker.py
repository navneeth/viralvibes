# tests/test_worker.py
import asyncio
from unittest.mock import AsyncMock, patch

import pytest

import worker.worker as worker_module


@pytest.mark.asyncio
async def test_worker_task_inserts_to_db():
    fake_url = "https://youtube.com/playlist?list=FAKE123"

    # Mock process_playlist to return fake stats
    fake_stats = {
        "playlist_name": "Fake Playlist",
        "view_count": 1000,
        "like_count": 100,
        "dislike_count": 10,
        "comment_count": 5,
        "video_count": 3,
        "avg_duration": None,
        "engagement_rate": 0.1,
        "controversy_score": 0.05,
    }

    # Put the fake playlist URL in the queue
    await worker_module.playlist_queue.put(fake_url)

    # Patch process_playlist and supabase_client
    with patch("worker.worker.process_playlist", new=AsyncMock(return_value=fake_stats)) as mock_job, \
         patch("worker.worker.supabase_client") as mock_db:

        # Create a mock table().insert().execute() chain
        mock_table = mock_db.table.return_value
        mock_table.insert.return_value.execute.return_value = None

        # Run the worker task
        result = await worker_module.worker_task()

        # Check it processed successfully
        assert result is True

        # Verify the job function was called with the playlist URL
        mock_job.assert_awaited_once_with(fake_url)

        # Verify it attempted to insert into the DB
        mock_db.table.assert_called_once_with("playlist_stats")
        mock_table.insert.assert_called_once()
        mock_table.insert.return_value.execute.assert_called_once()
