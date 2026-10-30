import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


@pytest.mark.parametrize("response_value", [True, False])
def test_coverage_refresh_uses_explicit_client(monkeypatch, response_value):
    import db

    client = MagicMock()
    client.rpc.return_value.execute.return_value = SimpleNamespace(data=response_value)
    monkeypatch.setattr(db, "supabase_client", None)
    assert db.refresh_creator_sync_coverage(client=client) is response_value
    client.rpc.assert_called_once_with(
        "refresh_creator_sync_coverage", {"p_min_subscribers": 10000}
    )


def test_coverage_refresh_failure_is_nonfatal():
    import db

    client = MagicMock()
    client.rpc.side_effect = RuntimeError("refresh failed")
    assert db.refresh_creator_sync_coverage(client=client) is False


@pytest.mark.parametrize("exhausted", [False, True])
def test_failed_sync_keeps_retry_fields_consistent(monkeypatch, exhausted):
    import db

    retry_count = db.CREATOR_WORKER_MAX_RETRIES if exhausted else 0
    query = MagicMock()
    query.select.return_value = query
    query.eq.return_value = query
    query.single.return_value = query
    query.update.return_value = query
    query.execute.side_effect = [
        SimpleNamespace(data={"retry_count": retry_count}),
        SimpleNamespace(data=[{"id": 7}]),
    ]
    client = MagicMock()
    client.table.return_value = query
    monkeypatch.setattr(db, "supabase_client", client)

    before = datetime.now(timezone.utc)
    assert db.mark_creator_sync_failed(7, "API failure") is True
    after = datetime.now(timezone.utc)

    payload = query.update.call_args.args[0]
    assert payload["retry_at"] == payload["next_retry_at"]
    assert payload["retry_count"] == retry_count + 1
    if exhausted:
        assert payload["status"] == "failed"
        assert payload["next_retry_at"] is None
    else:
        assert payload["status"] == "pending"
        retry_time = datetime.fromisoformat(payload["next_retry_at"])
        assert db.CREATOR_WORKER_RETRY_BASE <= (retry_time - before).total_seconds()
        assert (retry_time - after).total_seconds() <= db.CREATOR_WORKER_RETRY_BASE


def test_mark_creator_sync_processing_claims_only_pending_jobs(monkeypatch):
    import db

    query = MagicMock()
    query.update.return_value = query
    query.eq.return_value = query
    query.select.return_value = query
    query.execute.return_value = SimpleNamespace(data=[{"id": 7}])
    client = MagicMock()
    client.table.return_value = query
    monkeypatch.setattr(db, "supabase_client", client)

    assert db.mark_creator_sync_processing(7) is True
    query.eq.assert_any_call("id", 7)
    query.eq.assert_any_call("status", "pending")
    query.select.assert_called_once_with("id")


def test_mark_creator_sync_processing_returns_false_when_claim_was_lost(monkeypatch):
    import db

    query = MagicMock()
    query.update.return_value = query
    query.eq.return_value = query
    query.select.return_value = query
    query.execute.return_value = SimpleNamespace(data=[])
    client = MagicMock()
    client.table.return_value = query
    monkeypatch.setattr(db, "supabase_client", client)

    assert db.mark_creator_sync_processing(7) is False


def test_creator_snapshot_update_rejects_older_syncs(monkeypatch):
    import worker.creator_worker as creator_worker

    query = MagicMock()
    query.update.return_value = query
    query.eq.return_value = query
    query.or_.return_value = query
    query.select.return_value = query
    query.execute.return_value = SimpleNamespace(data=[])
    client = MagicMock()
    client.table.return_value = query
    monkeypatch.setattr(creator_worker, "supabase_client", client)
    snapshot_started_at = datetime(2026, 10, 23, tzinfo=timezone.utc)

    creator_worker._update_creator_snapshot(
        "creator-1", {"current_view_count": 10}, snapshot_started_at
    )

    query.eq.assert_called_once_with("id", "creator-1")
    query.or_.assert_called_once_with(
        "last_synced_at.is.null,last_synced_at.lt.2026-10-23T00:00:00+00:00"
    )
    query.select.assert_called_once_with("id")


def test_sync_job_stops_when_another_worker_claimed_it(monkeypatch):
    import worker.creator_worker as creator_worker

    client = MagicMock()
    monkeypatch.setattr(creator_worker, "supabase_client", client)
    monkeypatch.setattr(creator_worker, "mark_creator_sync_processing", lambda job_id: False)

    result = asyncio.run(creator_worker.handle_sync_job(7, "creator-1"))

    assert result is False
    client.table.assert_not_called()


@pytest.mark.parametrize(
    ("fresh_subscribers", "fresh_views", "fresh_videos", "valid_stats"),
    [
        (1200, 25_000, 115, True),
        (0, 25_000, 115, False),
    ],
)
def test_sync_rollover_stores_fresh_counts_only_for_valid_stats(
    monkeypatch, fresh_subscribers, fresh_views, fresh_videos, valid_stats
):
    import worker.creator_worker as creator_worker

    old_snapshot_at = datetime.now(timezone.utc) - timedelta(days=35)
    creator = {
        "channel_id": "UCcreator",
        "channel_name": "Creator",
        "primary_category": "Music",
        "current_subscribers": 1100,
        "current_view_count": 20_000,
        "current_video_count": 110,
        "prev_subscribers": 1000,
        "prev_view_count": 10_000,
        "prev_video_count": 100,
        "prev_snapshot_at": old_snapshot_at.isoformat(),
        "recent_upload": {"published_at": "2026-10-20T12:00:00Z"},
        "avg_days_between_uploads": 4.25,
    }
    query = MagicMock()
    query.select.return_value = query
    query.eq.return_value = query
    query.execute.return_value = SimpleNamespace(data=[creator])
    client = MagicMock()
    client.table.return_value = query
    monkeypatch.setattr(creator_worker, "supabase_client", client)
    monkeypatch.setattr(creator_worker, "mark_creator_sync_processing", lambda _job_id: True)
    monkeypatch.setattr(creator_worker, "mark_creator_sync_completed", lambda _job_id: True)
    monkeypatch.setattr(creator_worker, "_needs_category_fetch", lambda _category: False)
    failed_jobs = []
    monkeypatch.setattr(
        creator_worker,
        "mark_creator_sync_failed",
        lambda job_id, reason: failed_jobs.append((job_id, reason)) or True,
    )

    async def _fetch_channel_data(_channel_id):
        return {
            "channel_name": "Creator",
            "current_subscribers": fresh_subscribers,
            "current_view_count": fresh_views,
            "current_video_count": fresh_videos,
            "topic_categories": [],
            "channel_age_days": 900,
        }

    async def _fetch_recent_video_intelligence(*_args, **_kwargs):
        return creator_worker._empty_recent_video_intelligence()

    monkeypatch.setattr(creator_worker, "_fetch_channel_data", _fetch_channel_data)
    monkeypatch.setattr(
        creator_worker,
        "_fetch_recent_video_intelligence",
        _fetch_recent_video_intelligence,
    )
    monkeypatch.setattr(creator_worker, "_compute_quality_grade", lambda *_args: "A")
    monkeypatch.setattr(
        creator_worker.ContactExtractorService,
        "build_db_update_payload",
        lambda _payload: {},
    )
    monkeypatch.setattr(
        creator_worker.schema_detector, "filter_payload", lambda payload: (payload, [])
    )
    captured = {}

    def _capture_snapshot(_creator_id, payload, _snapshot_started_at):
        captured.update(payload)
        return SimpleNamespace(data=[{"id": _creator_id}])

    monkeypatch.setattr(creator_worker, "_update_creator_snapshot", _capture_snapshot)

    result = asyncio.run(creator_worker.handle_sync_job(7, "creator-1"))

    if valid_stats:
        assert result is True
        assert captured["subscribers_change_30d"] == 200
        assert captured["views_change_30d"] == 15_000
        assert captured["videos_change_30d"] == 15
        assert captured["growth_window_days"] >= 34.99
        assert captured["prev_subscribers"] == captured["current_subscribers"] == fresh_subscribers
        assert captured["prev_view_count"] == captured["current_view_count"] == fresh_views
        assert captured["prev_video_count"] == captured["current_video_count"] == fresh_videos
        assert captured["recent_upload"] == creator["recent_upload"]
        assert captured["avg_days_between_uploads"] == 4.25
        assert not failed_jobs
    else:
        assert result is False
        assert not any(
            field in captured
            for field in (
                "prev_subscribers",
                "prev_view_count",
                "prev_video_count",
                "prev_snapshot_at",
            )
        )
        assert failed_jobs and failed_jobs[0][0] == 7
        assert captured["growth_window_days"] is None


def test_creator_state_read_retries_without_optional_columns_after_schema_cache_error(monkeypatch):
    import worker.creator_worker as creator_worker

    class _MissingOptionalColumnError(Exception):
        code = "PGRST204"

    query = MagicMock()
    query.select.return_value = query
    query.eq.return_value = query
    query.execute.side_effect = [
        _MissingOptionalColumnError("recent_upload missing from schema cache"),
        SimpleNamespace(data=[{"channel_id": "UCcreator"}]),
    ]
    client = MagicMock()
    client.table.return_value = query
    monkeypatch.setattr(creator_worker, "supabase_client", client)
    monkeypatch.setattr(
        creator_worker.schema_detector,
        "available_columns",
        set(creator_worker._CREATOR_STATE_REQUIRED_COLUMNS)
        | set(creator_worker._CREATOR_STATE_OPTIONAL_COLUMNS),
    )

    result = creator_worker._fetch_creator_state("creator-1")

    assert result == {"channel_id": "UCcreator"}
    selected_columns = [call.args[0] for call in query.select.call_args_list]
    assert "recent_upload" in selected_columns[0]
    assert "avg_days_between_uploads" in selected_columns[0]
    assert "recent_upload" not in selected_columns[1]
    assert "avg_days_between_uploads" not in selected_columns[1]


def test_creator_state_read_omits_optional_columns_when_schema_lacks_them(monkeypatch):
    import worker.creator_worker as creator_worker

    query = MagicMock()
    query.select.return_value = query
    query.eq.return_value = query
    query.execute.return_value = SimpleNamespace(data=[{"channel_id": "UCcreator"}])
    client = MagicMock()
    client.table.return_value = query
    monkeypatch.setattr(creator_worker, "supabase_client", client)
    monkeypatch.setattr(
        creator_worker.schema_detector,
        "available_columns",
        set(creator_worker._CREATOR_STATE_REQUIRED_COLUMNS),
    )

    result = creator_worker._fetch_creator_state("creator-1")

    assert result == {"channel_id": "UCcreator"}
    selected_columns = query.select.call_args.args[0]
    assert "recent_upload" not in selected_columns
    assert "avg_days_between_uploads" not in selected_columns
