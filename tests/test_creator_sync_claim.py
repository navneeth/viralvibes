import asyncio
from datetime import datetime, timezone
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
