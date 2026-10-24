from types import SimpleNamespace

import worker.bootstrap_creators as bootstrap
import worker.creator_worker as creator_worker


class _CreatorQuery:
    def __init__(self, rows):
        self.rows = rows

    def select(self, *args, **kwargs):
        return self

    def is_(self, *args, **kwargs):
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
    monkeypatch.setattr(creator_worker, "supabase_client", None)
    monkeypatch.setattr(
        creator_worker,
        "queue_creator_sync_bulk",
        lambda ids, source: (queued.extend(ids) or len(ids), 0),
    )

    result = creator_worker._queue_unsynced_creators(client=client)

    assert result == 1
    assert queued == [creator_id]


def test_stale_queue_uses_explicit_client_when_worker_global_is_empty(monkeypatch):
    creator_id = "creator-stale"
    client = SimpleNamespace(
        table=lambda _name: _CreatorQuery(
            [{"id": creator_id, "channel_id": "UC123", "last_synced_at": "2026-01-01"}]
        )
    )
    queued = []
    monkeypatch.setattr(creator_worker, "supabase_client", None)
    monkeypatch.setattr(
        creator_worker,
        "queue_creator_sync_bulk",
        lambda ids, source: (queued.extend(ids) or len(ids), 0),
    )

    result = creator_worker._queue_creators_for_extended_refresh(client=client)

    assert result == 1
    assert queued == [creator_id]


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
