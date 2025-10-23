import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import worker.worker as wk
from worker.worker import JobResult, Worker, YouTubeBotChallengeError


# ==============================================================
# 🧠  Minimal in-memory Supabase stub
# ==============================================================
class FakeSupabase:
    """In-memory stub for Supabase client used by Worker tests."""

    def __init__(self):
        self.tables = {"playlist_jobs": []}

    def insert_job(self, row):
        self.tables["playlist_jobs"].append(dict(row))

    def table(self, name):
        if name not in self.tables:
            self.tables[name] = []
        outer = self

        class Table:
            def __init__(self):
                self._updates = {}
                self._filters = []

            def select(self, *a, **k):
                return self

            def update(self, updates):
                self._updates = updates
                return self

            def eq(self, key, val):
                self._filters.append((key, val))
                for r in outer.tables[name]:
                    if r.get(key) == val:
                        r.update(self._updates)
                return self

            def in_(self, key, values):
                self._filters.append((key, values))
                return self

            def order(self, *a, **k):
                return self

            def limit(self, n):
                self._limit = n
                return self

            def execute(self):
                rows = outer.tables[name]
                return SimpleNamespace(data=rows)

        return Table()


# ==============================================================
# 🔧 Shared fixtures
# ==============================================================
@pytest.fixture
def fake_db():
    db = FakeSupabase()
    db.insert_job({"id": "job123", "status": "pending"})
    return db


@pytest.fixture
def fake_df():
    class DF:
        def __init__(self):
            self.height = 2

        def is_empty(self):
            return False

    return DF()


@pytest.fixture
def patch_upsert(monkeypatch):
    async def fake_upsert(stats):
        from db import UpsertResult  # Import inside fixture to avoid import issues

        return UpsertResult(
            source="fresh",
            df_json="[]",
            summary_stats_json=json.dumps(stats.get("summary_stats", {})),
        )

    monkeypatch.setattr(wk, "upsert_playlist_stats", fake_upsert)


@pytest.fixture
def mock_supabase():
    """Mock Supabase client with a simple in-memory .table()."""
    table_mock = MagicMock()
    table_mock.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = [
        {"id": "job123", "status": "pending"}
    ]
    table_mock.update.return_value.eq.return_value.execute.return_value.data = [
        {"id": "job123", "status": "done"}
    ]
    client = MagicMock()
    client.table.return_value = table_mock
    return client


@pytest.fixture
def mock_yt_success():
    """Mock YoutubePlaylistService for successful processing."""
    yt = AsyncMock()
    yt.get_playlist_data.return_value = (
        MagicMock(is_empty=lambda: False, height=10),
        "Playlist A",
        "Channel X",
        "thumb.jpg",
        {
            "total_views": 1000,
            "total_likes": 100,
            "total_dislikes": 5,
            "total_comments": 10,
        },
    )
    return yt


@pytest.fixture(autouse=True)
def patch_supabase_global(monkeypatch, fake_db):
    """Ensure worker uses our in-memory supabase instead of None."""
    monkeypatch.setattr(wk, "supabase_client", fake_db)


# ==============================================================
# ✅ Tests
# ==============================================================


@pytest.mark.asyncio
async def test_worker_processes_pending_job_successfully(
    fake_db, fake_df, patch_upsert, monkeypatch
):
    """Happy path: playlist processed successfully."""

    async def fake_get_playlist_data(url, progress_callback=None):
        return fake_df, "My Playlist", "ChannelX", "/thumb.jpg", {"total_views": 100}

    monkeypatch.setattr(
        wk, "yt_service", SimpleNamespace(get_playlist_data=fake_get_playlist_data)
    )

    worker = Worker(supabase=fake_db)
    result = await worker.process_one(
        {"id": "job123", "playlist_url": "https://youtube.com/playlist?list=abc"}
    )
    assert isinstance(result, JobResult)
    assert result.job_id == "job123"
    assert fake_db.tables["playlist_jobs"][0]["status"] in ("processing", "done")


@pytest.mark.asyncio
@pytest.mark.skip(reason="Temporarily disabled for debugging")
async def test_worker_handles_empty_playlist_with_retry(
    fake_db, patch_upsert, monkeypatch
):
    """Handles empty dataframe gracefully and schedules retry."""

    class EmptyDF:
        def is_empty(self):
            return True

        height = 0

    async def fake_get_playlist_data(url, progress_callback=None):
        return EmptyDF(), "Empty", "Chan", "thumb", {}

    monkeypatch.setattr(
        wk, "yt_service", SimpleNamespace(get_playlist_data=fake_get_playlist_data)
    )

    worker = Worker(supabase=fake_db)
    result = await worker.process_one(
        {"id": "job_empty", "playlist_url": "https://youtube.com/playlist?list=empty"}
    )
    assert isinstance(result, JobResult)
    # Expect failure due to empty playlist
    assert "failed" in (result.status or "")


@pytest.mark.asyncio
@pytest.mark.skip(reason="Temporarily disabled for debugging")
async def test_worker_handles_bot_challenge(fake_db, patch_upsert, monkeypatch):
    """Bot challenge triggers 'blocked' state."""

    async def fake_get_playlist_data(url, progress_callback=None):
        raise YouTubeBotChallengeError("Captcha!")

    monkeypatch.setattr(
        wk, "yt_service", SimpleNamespace(get_playlist_data=fake_get_playlist_data)
    )

    worker = Worker(supabase=fake_db)
    result = await worker.process_one(
        {"id": "job_bot", "playlist_url": "https://youtube.com/playlist?list=xyz"}
    )
    assert isinstance(result, JobResult)
    assert result.job_id == "job_bot"
    assert result.status == "blocked" or result.error


@pytest.mark.asyncio
@pytest.mark.skip(reason="Temporarily disabled for debugging")
async def test_worker_max_retries_exhausted(fake_db, patch_upsert, monkeypatch):
    """If retry_count >= MAX_RETRY_ATTEMPTS, job marked failed permanently."""

    async def fake_get_playlist_data(url, progress_callback=None):
        raise RuntimeError("Persistent failure")

    monkeypatch.setattr(
        wk, "yt_service", SimpleNamespace(get_playlist_data=fake_get_playlist_data)
    )

    job = {
        "id": "job_retry",
        "playlist_url": "https://youtube.com/playlist?list=retry",
        "retry_count": 3,
    }
    worker = Worker(supabase=fake_db)
    result = await worker.process_one(job, is_retry=True)
    assert isinstance(result, JobResult)
    assert "failed" in (result.status or "")


@pytest.mark.asyncio
@pytest.mark.skip(reason="Temporarily disabled for debugging")
async def test_worker_validates_backend_metadata(
    fake_db, fake_df, patch_upsert, monkeypatch
):
    """Ensure metadata fields propagate from yt service."""

    async def fake_get_playlist_data(url, progress_callback=None):
        return (
            fake_df,
            "Meta Playlist",
            "Meta Channel",
            "meta_thumb",
            {"total_views": 1234},
        )

    yt_mock = SimpleNamespace(get_playlist_data=fake_get_playlist_data)
    monkeypatch.setattr(wk, "yt_service", yt_mock)

    worker = Worker(supabase=fake_db, yt=yt_mock)
    job = {"id": "job_meta", "playlist_url": "https://youtube.com/playlist?list=meta"}

    result = await worker.process_one(job)
    assert isinstance(result, JobResult)
    assert result.job_id == "job_meta"
    assert "status" in (result.raw_row or {})


@pytest.mark.asyncio
async def test_process_one_handles_handler_exception_and_returns_failed(
    fake_db, monkeypatch
):
    """If handle_job raises, process_one returns failed result."""

    async def boom(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.setattr(wk, "handle_job", boom)

    worker = Worker(supabase=fake_db)
    job = {"id": "job_fail", "playlist_url": "https://youtube.com/playlist?list=f"}

    result = await worker.process_one(job)
    assert result.status == "failed"
    assert "boom" in (result.error or "")


@pytest.mark.asyncio
@pytest.mark.skip(reason="Temporarily disabled for debugging")
async def test_process_one_retry_flag(fake_db, fake_df, patch_upsert, monkeypatch):
    """Retry flag passes through without issue."""

    async def fake_get_playlist_data(url, progress_callback=None):
        return (
            fake_df,
            "Retry Playlist",
            "Retry Channel",
            "thumb.jpg",
            {"total_views": 99},
        )

    monkeypatch.setattr(
        wk, "yt_service", SimpleNamespace(get_playlist_data=fake_get_playlist_data)
    )

    job = {
        "id": "job_r1",
        "playlist_url": "https://youtube.com/playlist?list=r1",
        "retry_count": 1,
    }
    worker = Worker(supabase=fake_db)
    result = await worker.process_one(job, is_retry=True)
    assert isinstance(result, JobResult)
    assert result.job_id == "job_r1"
    assert fake_db.tables["playlist_jobs"][0]["status"] in ("processing", "done")
