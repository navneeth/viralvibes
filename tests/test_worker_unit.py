import asyncio
import json
from datetime import datetime
from types import SimpleNamespace

import pytest

import worker.worker as wk
from worker.worker import JobResult, Worker

# Reuse existing conftest helpers
from tests.conftest import create_test_dataframe


@pytest.mark.asyncio
async def test_worker_process_one_success(monkeypatch, mock_supabase_with_jobs):
    """
    Test worker processes a job successfully.

    Uses mock_supabase_with_jobs fixture from conftest
    """
    job = {"id": 1, "playlist_url": "https://youtube.com/playlist?list=PL123"}

    # Mock YouTube service response
    async def fake_get_playlist_data(url, progress_callback=None):
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
    monkeypatch.setattr(
        wk, "yt_service", SimpleNamespace(get_playlist_data=fake_get_playlist_data)
    )

    # Mock database upsert
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
            },
        )

    monkeypatch.setattr(wk, "upsert_playlist_stats", fake_upsert)

    # Use the fixture that includes job data
    worker = Worker(supabase=mock_supabase_with_jobs, yt=wk.yt_service)

    # Process job
    result = await worker.process_one(job, is_retry=False)

    # Assertions
    assert isinstance(result, JobResult)
    assert result.job_id == 1
    assert result.status == "complete"
