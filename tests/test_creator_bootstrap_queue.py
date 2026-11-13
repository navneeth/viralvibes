from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import worker.bootstrap_creators as bootstrap
import worker.creator_worker as creator_worker
import db


VALID_CHANNEL_ID = "UC" + "A" * 22


@pytest.mark.parametrize("mode", ["normal", "targeted"])
def test_pending_job_fetch_preserves_fifo_and_optionally_targets_pilot(monkeypatch, mode):
    fixed_now = datetime(2026, 11, 3, 12, tzinfo=timezone.utc)
    monkeypatch.setattr(creator_worker, "datetime", SimpleNamespace(now=lambda tz: fixed_now))
    query = MagicMock()
    for method in ("select", "eq", "or_", "order", "limit", "in_"):
        getattr(query, method).return_value = query
    jobs = [{"id": 7, "creator_id": "approved-creator"}]
    query.execute.return_value = SimpleNamespace(data=jobs)
    client = MagicMock()
    client.table.return_value = query
    monkeypatch.setattr(creator_worker, "supabase_client", client)
    if mode == "targeted":
        result = creator_worker._fetch_pending_jobs(
            1, creator_ids=["approved-creator"], source="unknown_activity_refresh"
        )
    else:
        result = creator_worker._fetch_pending_jobs(1)

    assert result == jobs
    query.select.assert_called_once_with("id,creator_id,source,retry_count,job_type,input_query")
    query.order.assert_called_once_with("created_at", desc=False)
    query.limit.assert_called_once_with(1)
    query.eq.assert_any_call("status", "pending")
    ready_filter = f"next_retry_at.is.null,next_retry_at.lte.{fixed_now.isoformat()}"
    query.or_.assert_called_once_with(ready_filter)
    if mode == "targeted":
        query.in_.assert_called_once_with("creator_id", ["approved-creator"])
        query.eq.assert_any_call("source", "unknown_activity_refresh")
    else:
        query.in_.assert_not_called()
        assert query.eq.call_count == 1


@pytest.mark.parametrize("raise_on_error", [False, True])
def test_pending_job_fetch_can_propagate_errors(monkeypatch, raise_on_error):
    query = MagicMock()
    for method in ("select", "eq", "or_", "order", "limit"):
        getattr(query, method).return_value = query
    query.execute.side_effect = RuntimeError("database unavailable")
    client = MagicMock()
    client.table.return_value = query
    monkeypatch.setattr(creator_worker, "supabase_client", client)

    if raise_on_error:
        with pytest.raises(RuntimeError, match="database unavailable"):
            creator_worker._fetch_pending_jobs(1, raise_on_error=True)
    else:
        assert creator_worker._fetch_pending_jobs(1) == []


def test_strict_pending_job_fetch_rejects_uninitialized_client(monkeypatch):
    monkeypatch.setattr(creator_worker, "supabase_client", None)

    with pytest.raises(RuntimeError, match="not initialized"):
        creator_worker._fetch_pending_jobs(1, raise_on_error=True)


def test_empty_pilot_allowlist_never_fetches_general_queue(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(creator_worker, "supabase_client", client)

    assert creator_worker._fetch_pending_jobs(1, creator_ids=[]) == []
    client.table.assert_not_called()


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

    def filter(self, *args, **kwargs):
        self.filters.append(("filter", args, kwargs))
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
        table=lambda _name: _CreatorQuery([{"id": creator_id, "channel_id": VALID_CHANNEL_ID}])
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
            [{"id": "creator", "channel_id": VALID_CHANNEL_ID}], filters=filters
        )
    )
    monkeypatch.setattr(creator_worker, "supabase_client", client)
    monkeypatch.setattr(creator_worker, "queue_creator_sync_bulk", lambda *_a, **_kw: (0, 0))

    creator_worker._queue_unsynced_creators()

    assert ("is", ("archived_at", "null"), {}) in filters
    assert (
        "filter",
        ("channel_id", "match", r"^UC[a-zA-Z0-9_-]{22}$"),
        {},
    ) in filters


def test_stale_queue_uses_explicit_client_when_worker_global_is_empty(monkeypatch):
    creator_id = "creator-stale"
    filters = []
    client = SimpleNamespace(
        table=lambda _name: _CreatorQuery(
            [
                {
                    "id": creator_id,
                    "channel_id": VALID_CHANNEL_ID,
                    "last_synced_at": "2026-01-01",
                }
            ],
            filters=filters,
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
    assert ("filter", ("channel_id", "match", r"^UC[a-zA-Z0-9_-]{22}$"), {}) in filters


def test_unsynced_queue_skips_invalid_channel_ids(monkeypatch):
    client = SimpleNamespace(
        table=lambda _name: _CreatorQuery(
            [
                {"id": "valid", "channel_id": VALID_CHANNEL_ID},
                {"id": "numeric", "channel_id": 167689828},
                {"id": "text", "channel_id": "SixTONES,artist,keywords"},
            ]
        )
    )
    bulk_calls = []
    monkeypatch.setattr(creator_worker, "supabase_client", client)
    monkeypatch.setattr(
        creator_worker,
        "queue_creator_sync_bulk",
        lambda ids, source, **kwargs: bulk_calls.append((ids, source)) or (len(ids), 0),
    )

    assert creator_worker._queue_unsynced_creators() == 1
    assert bulk_calls == [(["valid"], "bootstrap_unsynced")]


def test_stale_queue_skips_invalid_channel_ids(monkeypatch):
    client = SimpleNamespace(
        table=lambda _name: _CreatorQuery(
            [
                {
                    "id": "valid",
                    "channel_id": VALID_CHANNEL_ID,
                    "last_synced_at": "2026-01-01",
                },
                {
                    "id": "invalid",
                    "channel_id": "SixTONES,artist,keywords",
                    "last_synced_at": "2026-01-01",
                },
            ]
        )
    )
    bulk_calls = []
    monkeypatch.setattr(creator_worker, "supabase_client", client)
    monkeypatch.setattr(
        creator_worker,
        "queue_creator_sync_bulk",
        lambda ids, source, **kwargs: bulk_calls.append((ids, source)) or (len(ids), 0),
    )

    assert creator_worker._queue_creators_for_extended_refresh() == 1
    assert bulk_calls == [(["valid"], "scheduled_refresh")]


def test_playlist_creator_rejects_invalid_id_before_database_access(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(db, "supabase_client", client)

    result = db.get_or_create_creator_from_playlist(
        channel_id="SixTONES,artist,keywords",
        channel_name="SixTONES",
        channel_url="https://www.youtube.com/channel/SixTONES",
    )

    assert result is None
    client.table.assert_not_called()


def test_handle_creator_rejects_invalid_id_before_database_access(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(db, "supabase_client", client)

    result = db.add_creator_by_handle(
        handle="@example",
        channel_id="167689828",
        channel_name="Example",
        custom_url="example",
    )

    assert result is None
    client.table.assert_not_called()


def test_auto_retry_skips_invalid_channel_ids(monkeypatch):
    client = MagicMock()
    query = MagicMock()
    for method in ("select", "in_", "is_", "lt", "limit", "filter"):
        getattr(query, method).return_value = query
    query.not_.is_.return_value = query
    query.execute.side_effect = [
        SimpleNamespace(
            data=[
                {"id": "valid-failed", "channel_id": VALID_CHANNEL_ID},
                {"id": "numeric", "channel_id": "167689828"},
            ]
        ),
        SimpleNamespace(
            data=[
                {"id": "valid-unsynced", "channel_id": "UC" + "B" * 22},
                {"id": "text", "channel_id": "SixTONES,artist,keywords"},
            ]
        ),
    ]
    client.table.return_value = query
    bulk_calls = []
    monkeypatch.setattr(db, "supabase_client", client)
    monkeypatch.setattr(
        db,
        "queue_creator_sync_bulk",
        lambda ids, source: bulk_calls.append((ids, source)) or (len(ids), 0),
    )

    assert db.queue_invalid_creators_for_retry(hours_since_last_sync=24, batch_size=50) == 2
    assert bulk_calls == [(["valid-failed", "valid-unsynced"], "auto_retry")]
    assert query.filter.call_count == 2
    query.filter.assert_any_call("channel_id", "match", r"^UC[a-zA-Z0-9_-]{22}$")


@pytest.mark.parametrize("skip_stats", [False, True])
@pytest.mark.parametrize("snapshot_mode", ["off", "preview", "apply"])
@pytest.mark.parametrize("snapshot_rpc_error", [False, True])
def test_bootstrap_passes_initialized_client_to_queue_helpers(
    monkeypatch, skip_stats, snapshot_mode, snapshot_rpc_error
):
    client = MagicMock()
    calls = []
    events = []
    coverage_calls = []
    snapshot_response = MagicMock()
    snapshot_response.execute.return_value = SimpleNamespace(data=[{"creator_id": "enrolled"}])
    if snapshot_rpc_error:
        snapshot_response.execute.side_effect = RuntimeError("snapshot RPC unavailable")
    client.rpc.side_effect = lambda *args: events.append("snapshot") or snapshot_response
    monkeypatch.setattr(bootstrap, "init_supabase", lambda: client)
    monkeypatch.setattr(
        bootstrap,
        "archive_permanently_failed_creators",
        lambda: events.append("archive") or 2,
    )
    monkeypatch.setattr(
        bootstrap,
        "_queue_unsynced_creators",
        lambda **kwargs: events.append("unsynced") or calls.append(kwargs) or 1,
    )
    monkeypatch.setattr(
        bootstrap,
        "_queue_creators_for_extended_refresh",
        lambda **kwargs: events.append("stale") or calls.append(kwargs) or 1,
    )
    monkeypatch.setattr(
        bootstrap,
        "queue_invalid_creators_for_retry",
        lambda **kwargs: events.append("retry") or 0,
    )
    monkeypatch.setattr(bootstrap, "refresh_category_stats_cache", lambda: 0)
    monkeypatch.setattr(
        bootstrap,
        "refresh_creator_sync_coverage",
        lambda **kwargs: coverage_calls.append(kwargs) or False,
    )
    monkeypatch.setattr(
        bootstrap,
        "refresh_hero_stats_cache",
        lambda: {"success": True, "materialized_views": []},
    )
    monkeypatch.setattr(
        bootstrap.sys,
        "argv",
        ["bootstrap", "--unsynced-batch", "5000", "--no-categories"]
        + (["--no-stats"] if skip_stats else [])
        + (["--snapshot-refresh-batch", "40"] if snapshot_mode != "off" else [])
        + (["--snapshot-refresh-apply"] if snapshot_mode == "apply" else []),
    )

    if snapshot_rpc_error and snapshot_mode != "off":
        with pytest.raises(RuntimeError, match="snapshot RPC unavailable"):
            bootstrap.main()
    else:
        bootstrap.main()

    expected_events = ["archive"] + (["snapshot"] if snapshot_mode != "off" else [])
    if not (snapshot_rpc_error and snapshot_mode != "off"):
        expected_events += ["unsynced", "stale", "retry"]
    assert events == expected_events
    if snapshot_mode == "off":
        client.rpc.assert_not_called()
    else:
        client.rpc.assert_called_once_with(
            "enqueue_video_snapshot_refreshes",
            {"p_limit": 40, "p_dry_run": snapshot_mode != "apply", "p_max_queue": 250},
        )
        snapshot_response.execute.assert_called_once_with()
    if snapshot_rpc_error and snapshot_mode != "off":
        assert calls == []
        return
    assert calls == [
        {"batch_size": 5000, "client": client},
        {"days_since_last_sync": 7, "client": client},
    ]
    assert coverage_calls == ([] if skip_stats else [{"client": client}])


@pytest.mark.parametrize(
    "options",
    [
        ["--snapshot-refresh-batch", "41"],
        ["--snapshot-refresh-batch", "-1"],
        ["--snapshot-refresh-apply"],
    ],
)
def test_bootstrap_rejects_invalid_snapshot_options(monkeypatch, options):
    monkeypatch.setattr(bootstrap.sys, "argv", ["bootstrap"] + options)
    with pytest.raises(SystemExit):
        bootstrap._parse_args()


def test_bootstrap_skips_queueing_when_terminal_archival_fails(monkeypatch):
    client = MagicMock()
    queue_unsynced = MagicMock()
    queue_stale = MagicMock()
    queue_retry = MagicMock()
    monkeypatch.setattr(bootstrap, "init_supabase", lambda: client)
    monkeypatch.setattr(
        bootstrap,
        "archive_permanently_failed_creators",
        MagicMock(side_effect=RuntimeError("archival unavailable")),
    )
    monkeypatch.setattr(bootstrap, "_queue_unsynced_creators", queue_unsynced)
    monkeypatch.setattr(bootstrap, "_queue_creators_for_extended_refresh", queue_stale)
    monkeypatch.setattr(bootstrap, "queue_invalid_creators_for_retry", queue_retry)
    monkeypatch.setattr(
        bootstrap.sys,
        "argv",
        [
            "bootstrap",
            "--no-stats",
            "--no-categories",
            "--snapshot-refresh-batch",
            "40",
            "--snapshot-refresh-apply",
        ],
    )

    with pytest.raises(RuntimeError, match="archival unavailable"):
        bootstrap.main()

    queue_unsynced.assert_not_called()
    queue_stale.assert_not_called()
    queue_retry.assert_not_called()
    client.rpc.assert_not_called()


def test_queue_creator_sync_rejects_archived_creator(monkeypatch):
    creator_query = MagicMock()
    for method in ("select", "eq", "limit"):
        getattr(creator_query, method).return_value = creator_query
    creator_query.execute.return_value = SimpleNamespace(
        data=[{"archived_at": "2026-11-11T00:00:00+00:00"}]
    )
    client = MagicMock()
    client.table.return_value = creator_query
    monkeypatch.setattr(db, "supabase_client", client)

    assert db.queue_creator_sync("archived-creator") is False
    client.table.assert_called_once_with(db.CREATOR_TABLE)
    creator_query.insert.assert_not_called()


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
