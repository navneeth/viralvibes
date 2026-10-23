import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock


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
