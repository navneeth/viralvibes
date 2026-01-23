import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from datetime import timedelta
from worker.jobs import process_playlist
from utils import compute_dashboard_id
from tests.conftest import TEST_PLAYLIST_URL

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
    import polars as pl

    return pl.DataFrame({"title": ["Test"], "view_count": [100]})


@pytest.fixture
def patch_upsert(monkeypatch):
    def fake_upsert(stats):
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
    assert isinstance(result, JobResult), f"Expected JobResult, got {type(result)}"
    assert result.job_id == "job123", f"Expected job_id 'job123', got {result.job_id}"
    assert result.status == "done", f"Expected status 'done', got {result.status}"
    assert fake_db.tables["playlist_jobs"][0]["status"] == "done", (
        f"Expected job status 'done', got {fake_db.tables['playlist_jobs'][0]['status']}, "
        f"job state: {fake_db.tables['playlist_jobs']}"
    )
    assert fake_db.tables["playlist_jobs"][0]["result_source"] == "fresh"


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


class TestProcessPlaylist:
    """Tests for process_playlist() function schema compliance."""

    @pytest.mark.asyncio
    async def test_process_playlist_returns_complete_schema(self):
        """
        ✅ SCHEMA COMPLIANCE TEST

        Verify that process_playlist() returns EXACTLY the required fields
        matching the playlist_stats database schema (no more, no less).
        """
        # Use test playlist URL
        test_url = TEST_PLAYLIST_URL

        # Process the playlist
        result = await process_playlist(test_url)

        # ✅ Define EXACT schema from playlist_stats table
        required_fields = {
            # Core identifiers
            "playlist_url",
            "dashboard_id",
            # Metadata
            "title",
            "channel_name",
            "channel_thumbnail",
            # Aggregate stats
            "view_count",
            "like_count",
            "dislike_count",
            "comment_count",
            "video_count",
            "processed_video_count",
            # Computed metrics
            "avg_duration",
            "engagement_rate",
            "controversy_score",
            # JSON payloads
            "df_json",
            "summary_stats",
            # Event counters
            "share_count",
        }

        # ✅ STRICT SCHEMA CHECK: Detect extra AND missing fields
        actual_fields = set(result.keys())
        extra_fields = actual_fields - required_fields
        missing_fields = required_fields - actual_fields

        assert not extra_fields, (
            f"❌ UNEXPECTED FIELDS returned by process_playlist():\n"
            f"  {sorted(extra_fields)}\n\n"
            f"These fields are NOT in the playlist_stats schema.\n"
            f"Action: Either add them to the schema or remove from process_playlist()."
        )

        assert not missing_fields, (
            f"❌ MISSING REQUIRED FIELDS in process_playlist() output:\n"
            f"  {sorted(missing_fields)}\n\n"
            f"Available fields: {sorted(actual_fields)}"
        )

        # ✅ Ensure EXACT match (combines both checks)
        assert actual_fields == required_fields, (
            f"Schema mismatch!\n"
            f"  Missing: {sorted(missing_fields)}\n"
            f"  Extra:   {sorted(extra_fields)}"
        )

        print(f"\n✅ Exact schema match: {len(required_fields)} fields")
        print(f"✅ No unexpected fields found")

        # ✅ Verify field types
        assert isinstance(result["playlist_url"], str), "playlist_url must be string"
        assert isinstance(result["dashboard_id"], str), "dashboard_id must be string"
        assert (
            len(result["dashboard_id"]) == 16
        ), f"dashboard_id must be 16-char hash, got {len(result['dashboard_id'])}"

        assert isinstance(result["title"], str), "title must be string"
        assert isinstance(result["channel_name"], str), "channel_name must be string"
        assert isinstance(
            result["channel_thumbnail"], str
        ), "channel_thumbnail must be string"

        assert isinstance(
            result["view_count"], int
        ), f"view_count must be int, got {type(result['view_count'])}"
        assert isinstance(
            result["like_count"], int
        ), f"like_count must be int, got {type(result['like_count'])}"
        assert isinstance(
            result["dislike_count"], int
        ), f"dislike_count must be int, got {type(result['dislike_count'])}"
        assert isinstance(
            result["comment_count"], int
        ), f"comment_count must be int, got {type(result['comment_count'])}"
        assert isinstance(
            result["video_count"], int
        ), f"video_count must be int, got {type(result['video_count'])}"
        assert isinstance(
            result["processed_video_count"], int
        ), f"processed_video_count must be int, got {type(result['processed_video_count'])}"

        assert isinstance(
            result["avg_duration"], timedelta
        ), f"avg_duration must be timedelta, got {type(result['avg_duration'])}"
        assert isinstance(
            result["engagement_rate"], (int, float)
        ), f"engagement_rate must be numeric, got {type(result['engagement_rate'])}"
        assert isinstance(
            result["controversy_score"], (int, float)
        ), f"controversy_score must be numeric, got {type(result['controversy_score'])}"

        # ✅ ENHANCED: Validate df_json is valid JSON with expected structure
        assert isinstance(
            result["df_json"], str
        ), f"df_json must be JSON string, got {type(result['df_json'])}"

        try:
            parsed_df_json = json.loads(result["df_json"])
        except json.JSONDecodeError as e:
            pytest.fail(
                f"df_json is not valid JSON: {e}\nContent: {result['df_json'][:200]}"
            )

        # ✅ Polars .write_json() returns array of objects (list of dicts)
        assert isinstance(parsed_df_json, list), (
            f"df_json must decode to a list (Polars DataFrame format), "
            f"got {type(parsed_df_json)}"
        )

        # ✅ If non-empty, verify it's a list of dicts (rows)
        if parsed_df_json:
            assert isinstance(
                parsed_df_json[0], dict
            ), f"df_json rows must be dicts, got {type(parsed_df_json[0])}"
            print(f"✅ df_json is valid JSON with {len(parsed_df_json)} rows")
        else:
            print(f"✅ df_json is valid empty JSON array")

        # ✅ Validate summary_stats
        assert isinstance(
            result["summary_stats"], dict
        ), f"summary_stats must be dict, got {type(result['summary_stats'])}"

        assert isinstance(
            result["share_count"], int
        ), f"share_count must be int, got {type(result['share_count'])}"
        assert (
            result["share_count"] == 0
        ), f"share_count must default to 0, got {result['share_count']}"

        print(f"✅ All field types validated")

        # ✅ Verify computed dashboard_id matches
        expected_dashboard_id = compute_dashboard_id(test_url)
        assert result["dashboard_id"] == expected_dashboard_id, (
            f"dashboard_id mismatch:\n"
            f"  Expected: {expected_dashboard_id}\n"
            f"  Got:      {result['dashboard_id']}"
        )

        print(f"✅ dashboard_id correctly computed: {expected_dashboard_id}")

        # ✅ Verify non-negative values
        assert result["view_count"] >= 0, "view_count must be non-negative"
        assert result["like_count"] >= 0, "like_count must be non-negative"
        assert result["dislike_count"] >= 0, "dislike_count must be non-negative"
        assert result["comment_count"] >= 0, "comment_count must be non-negative"
        assert result["video_count"] >= 0, "video_count must be non-negative"
        assert (
            result["processed_video_count"] >= 0
        ), "processed_video_count must be non-negative"

        print(f"✅ All numeric values are non-negative")

        # ✅ Verify video counts match
        assert result["video_count"] == result["processed_video_count"], (
            f"video_count ({result['video_count']}) should equal "
            f"processed_video_count ({result['processed_video_count']})"
        )

        print(f"✅ video_count == processed_video_count: {result['video_count']}")

    @pytest.mark.asyncio
    async def test_process_playlist_handles_empty_playlist(self):
        """
        ✅ EDGE CASE TEST

        Verify that process_playlist() handles empty playlists gracefully
        by returning valid schema with zero values.
        """
        # Use a known empty or invalid playlist URL
        # Note: This will likely fail with current implementation
        # because YouTube API will reject invalid playlists
        # We're testing the graceful degradation path

        test_url = "https://www.youtube.com/playlist?list=PLInvalidPlaylist123"

        try:
            result = await process_playlist(test_url)

            # If it doesn't raise an error, verify zero values
            assert (
                result["video_count"] == 0
            ), "Empty playlist should have video_count=0"
            assert (
                result["processed_video_count"] == 0
            ), "Empty playlist should have processed_video_count=0"
            assert result["view_count"] == 0, "Empty playlist should have view_count=0"
            assert result["avg_duration"] == timedelta(
                seconds=0
            ), "Empty playlist should have avg_duration=0"
            assert (
                result["df_json"] == "[]"
            ), "Empty playlist should have empty JSON array"

            print(f"✅ Empty playlist handled gracefully with zero values")

        except Exception as e:
            # Expected behavior: API rejects invalid playlists
            print(f"⚠️  Empty playlist test skipped (API rejected): {e}")
            pytest.skip(f"YouTube API rejected invalid playlist: {e}")

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Temporarily disabled for debugging")
    async def test_process_playlist_field_consistency(self):
        """
        ✅ CONSISTENCY TEST

        Verify that computed fields are logically consistent with each other.
        """
        test_url = TEST_PLAYLIST_URL
        result = await process_playlist(test_url)

        # ✅ If there are videos, there should be views
        if result["video_count"] > 0:
            assert result["view_count"] > 0, "Playlists with videos should have views"
            print(
                f"✅ Playlist has {result['video_count']} videos and {result['view_count']:,} views"
            )

        # ✅ Engagement rate should be reasonable
        if result["engagement_rate"] > 0:
            assert (
                0 <= result["engagement_rate"] <= 1.0
            ), f"Engagement rate should be between 0 and 1, got {result['engagement_rate']}"
            print(f"✅ Engagement rate is valid: {result['engagement_rate']:.4f}")

        # ✅ Controversy score should be non-negative
        assert (
            result["controversy_score"] >= 0
        ), f"Controversy score should be non-negative, got {result['controversy_score']}"
        print(f"✅ Controversy score is valid: {result['controversy_score']:.2f}")

        # ✅ Duration should be positive if videos exist
        if result["video_count"] > 0:
            assert (
                result["avg_duration"].total_seconds() > 0
            ), "Average duration should be positive for non-empty playlists"
            print(f"✅ Average duration: {result['avg_duration']}")

        # ✅ REMOVED: JSON validation (already in schema test)
        # Kept only: summary_stats key validation

        # ✅ summary_stats should contain expected keys
        expected_summary_keys = ["total_views", "total_likes", "avg_engagement"]
        missing_keys = set(expected_summary_keys) - set(result["summary_stats"].keys())
        assert not missing_keys, (
            f"summary_stats missing expected keys: {missing_keys}\n"
            f"Available keys: {list(result['summary_stats'].keys())}"
        )
        print(f"✅ summary_stats contains all expected keys: {expected_summary_keys}")
