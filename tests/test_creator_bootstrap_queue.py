from types import SimpleNamespace

import worker.bootstrap_creators as bootstrap
import worker.creator_worker as creator_worker
import db


class _CreatorQuery:
    def __init__(self, rows, filters=None):
        self.rows = rows
        self.filters = filters if filters is not None else []

    def select(self, *args, **kwargs):
        return self

    def is_(self, *args, **kwargs):
        self.filters.append(("is", args, kwargs))
        return self

    def in_(self, *args, **kwargs):
        return self

    def lt(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def execute(self):
        return SimpleNamespace(data=self.rows)


def test_unsynced_queue_uses_explicit_client_when_worker_global_is_empty(monkeypatch):
    creator_id = "creator-unsynced"
    client = SimpleNamespace(
        table=lambda _name: _CreatorQuery([{"id": creator_id, "channel_id": "UC123"}])
    )
    queued = []
    bulk_calls = []
    monkeypatch.setattr(creator_worker, "supabase_client", None)
    monkeypatch.setattr(
        creator_worker,
        "queue_creator_sync_bulk",
        lambda ids, source, **kwargs: (
            bulk_calls.append((ids, source, kwargs)) or queued.extend(ids) or len(ids),
            0,
        ),
    )

    result = creator_worker._queue_unsynced_creators(client=client)

    assert result == 1
    assert queued == [creator_id]
    assert bulk_calls == [
        ([creator_id], "bootstrap_unsynced", {"client": client}),
    ]


def test_unsynced_query_excludes_archived_creators(monkeypatch):
    filters = []
    client = SimpleNamespace(
        table=lambda _name: _CreatorQuery(
            [{"id": "creator", "channel_id": "UC123"}], filters=filters
        )
    )
    monkeypatch.setattr(creator_worker, "supabase_client", client)
    monkeypatch.setattr(creator_worker, "queue_creator_sync_bulk", lambda *_a, **_kw: (0, 0))

    creator_worker._queue_unsynced_creators()

    assert ("is", ("archived_at", "null"), {}) in filters


def test_stale_queue_uses_explicit_client_when_worker_global_is_empty(monkeypatch):
    creator_id = "creator-stale"
    client = SimpleNamespace(
        table=lambda _name: _CreatorQuery(
            [{"id": creator_id, "channel_id": "UC123", "last_synced_at": "2026-01-01"}]
        )
    )
    queued = []
    bulk_calls = []
    monkeypatch.setattr(creator_worker, "supabase_client", None)
    monkeypatch.setattr(
        creator_worker,
        "queue_creator_sync_bulk",
        lambda ids, source, **kwargs: (
            bulk_calls.append((ids, source, kwargs)) or queued.extend(ids) or len(ids),
            0,
        ),
    )

    result = creator_worker._queue_creators_for_extended_refresh(client=client)

    assert result == 1
    assert queued == [creator_id]
    assert bulk_calls == [
        ([creator_id], "scheduled_refresh", {"client": client}),
    ]


def test_bootstrap_passes_initialized_client_to_queue_helpers(monkeypatch):
    client = object()
    calls = []
    monkeypatch.setattr(bootstrap, "init_supabase", lambda: client)
    monkeypatch.setattr(
        bootstrap, "_queue_unsynced_creators", lambda **kwargs: calls.append(kwargs) or 1
    )
    monkeypatch.setattr(
        bootstrap,
        "_queue_creators_for_extended_refresh",
        lambda **kwargs: calls.append(kwargs) or 1,
    )
    monkeypatch.setattr(bootstrap, "queue_invalid_creators_for_retry", lambda **kwargs: 0)
    monkeypatch.setattr(bootstrap, "refresh_category_stats_cache", lambda: 0)
    monkeypatch.setattr(
        bootstrap,
        "refresh_hero_stats_cache",
        lambda: {"success": True, "materialized_views": []},
    )
    monkeypatch.setattr(
        bootstrap.sys,
        "argv",
        ["bootstrap", "--unsynced-batch", "5000", "--no-stats", "--no-categories"],
    )

    bootstrap.main()

    assert calls == [
        {"batch_size": 5000, "client": client},
        {"days_since_last_sync": 7, "client": client},
    ]


class _BulkJobQuery:
    def __init__(self, existing=None):
        self.existing = existing or []
        self.inserted = None
        self.filters = []

    def select(self, *args, **kwargs):
        return self

    def in_(self, *args, **kwargs):
        self.filters.append(("in", args, kwargs))
        return self

    def eq(self, *args, **kwargs):
        self.filters.append(("eq", args, kwargs))
        return self

    def insert(self, payload):
        self.inserted = payload
        return self

    def execute(self):
        return SimpleNamespace(data=self.inserted if self.inserted is not None else self.existing)


def test_bulk_enqueue_uses_explicit_client_when_db_global_is_unset(monkeypatch):
    creator_id = "creator-explicit-client"
    pending_query = _BulkJobQuery()
    insert_query = _BulkJobQuery()
    table_calls = []

    class Client:
        def table(self, name):
            table_calls.append(name)
            if name == db.CREATOR_SYNC_JOBS_TABLE:
                return pending_query if len(table_calls) == 1 else insert_query
            raise AssertionError(f"Unexpected table: {name}")

    client = Client()
    monkeypatch.setattr(db, "supabase_client", None)

    queued, skipped = db.queue_creator_sync_bulk(
        [creator_id], source="bootstrap_unsynced", client=client
    )

    assert (queued, skipped) == (1, 0)
    assert table_calls == [db.CREATOR_SYNC_JOBS_TABLE, db.CREATOR_SYNC_JOBS_TABLE]
    assert insert_query.inserted == [
        {
            "creator_id": creator_id,
            "status": "pending",
            "source": "bootstrap_unsynced",
            "job_type": "sync_stats",
        }
    ]
