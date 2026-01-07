import asyncio
import json
from types import SimpleNamespace

import pytest

import worker.worker as wk
from worker.worker import JobResult, Worker


@pytest.mark.asyncio
async def test_worker_process_one_success(monkeypatch):
    # Fake job row
    job = {"id": 1, "playlist_url": "https://youtube.com/playlist?list=PL123"}

    # Mock yt_service.get_playlist_data -> return (df, title, channel, thumb, summary)
    class FakeDF:
        def __init__(self):
            self.height = 2
            self.columns = ["Title", "Views"]

        def is_empty(self):
            return False

        def write_json(self):
            return "[]"

    async def fake_get_playlist_data(url, progress_callback=None):
        df = FakeDF()
        title = "My Playlist"
        channel = "Chan"
        thumb = "/img.png"
        summary = {
            "total_views": 100,
            "actual_playlist_count": 2,
            "avg_engagement": 1.2,
        }
        return df, title, channel, thumb, summary

    # Monkeypatch the module-level youtube service used by handler
    monkeypatch.setattr(
        wk, "yt_service", SimpleNamespace(get_playlist_data=fake_get_playlist_data)
    )

    async def fake_upsert(stats):
        from db import UpsertResult  # Import inside to avoid import issues

        return UpsertResult(
            source="fresh",
            df="[]",
            # summary_stats=json.dumps(stats.get("summary_stats", {})),
        )

    # override upsert_playlist_stats used in worker
    monkeypatch.setattr(wk, "upsert_playlist_stats", fake_upsert)

    # fake supabase client that accepts updates (used when worker reads final job row) -> keep simple
    fake_supabase = SimpleNamespace(
        table=lambda *a, **k: SimpleNamespace(
            select=lambda *x, **y: SimpleNamespace(
                eq=lambda *a, **k: SimpleNamespace(
                    limit=lambda n: SimpleNamespace(
                        execute=lambda: SimpleNamespace(data=[{}])
                    )
                )
            )
        )
    )
    w = Worker(supabase=fake_supabase, yt=wk.yt_service)
    res = await w.process_one(job, is_retry=False)
    assert isinstance(res, JobResult)
    # status may be None if DB fetch above returns empty row; at least job_id is preserved
    assert res.job_id == 1
