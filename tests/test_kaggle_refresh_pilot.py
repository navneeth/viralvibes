import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ReadTimeout

import kaggle_worker
import worker.creator_worker as creator_worker


CREATOR_ID = "6488b4d0-96e9-4cb3-b7da-36cc086f8e75"


@pytest.fixture
def pilot_client(monkeypatch):
    client = MagicMock()
    client.rpc.return_value.execute.return_value = SimpleNamespace(data=[])
    monkeypatch.setattr(creator_worker, "supabase_client", client)
    monkeypatch.setattr(kaggle_worker, "init", AsyncMock())
    return client


def _fake_pool(monkeypatch):
    pool = SimpleNamespace(
        slots=[object()],
        all_exhausted_event=SimpleNamespace(is_set=lambda: False),
        current_resolver=lambda: object(),
        summary=lambda: "pilot",
        record_job=MagicMock(),
        _index=0,
        current_slot=SimpleNamespace(jobs_processed=0, api_key="test-key"),
        mark_exhausted_and_rotate=MagicMock(return_value=False),
    )
    monkeypatch.setattr(kaggle_worker.KeyPool, "from_env", lambda: pool)
    monkeypatch.setattr(kaggle_worker.stop_event, "is_set", lambda: False)
    monkeypatch.setattr(kaggle_worker, "_display_final_report", lambda *args: None)
    return pool


def test_pilot_defaults_to_dry_run_after_normal_queue_drains(monkeypatch, pilot_client):
    _fake_pool(monkeypatch)
    events = []

    def enqueue_rpc(name, params):
        events.append(("enqueue", name, params))
        return SimpleNamespace(execute=lambda: SimpleNamespace(data=[{"creator_id": CREATOR_ID}]))

    pilot_client.rpc.side_effect = enqueue_rpc
    fetch = MagicMock(side_effect=lambda **kwargs: events.append(("fetch", kwargs)) or [])
    monkeypatch.setattr(kaggle_worker, "_fetch_pending_jobs", fetch)

    asyncio.run(kaggle_worker.run(max_empty_polls=1, refresh_creator_ids=[CREATOR_ID]))

    assert events[0] == ("fetch", {"batch_size": 1, "raise_on_error": True})
    assert events[1] == (
        "enqueue",
        "enqueue_unknown_activity_refreshes",
        {"p_creator_ids": [CREATOR_ID], "p_dry_run": True, "p_max_queue": 1000},
    )
    fetch.assert_called_once_with(batch_size=1, raise_on_error=True)
    kaggle_worker.init.assert_awaited_once_with()


def test_auto_pilot_defaults_to_dry_run_after_normal_queue_drains(monkeypatch, pilot_client):
    _fake_pool(monkeypatch)
    auto_id = "14076ba5-f130-4f86-9b53-ecf3b93ed077"
    pilot_client.rpc.return_value.execute.return_value = SimpleNamespace(
        data=[{"creator_id": auto_id, "current_subscribers": 149000000, "dry_run": True}]
    )
    fetch = MagicMock(return_value=[])
    monkeypatch.setattr(kaggle_worker, "_fetch_pending_jobs", fetch)

    asyncio.run(kaggle_worker.run(max_empty_polls=1, refresh_auto=True))

    pilot_client.rpc.assert_called_once_with(
        "enqueue_unknown_activity_refresh_batch",
        {"p_limit": 5, "p_dry_run": True, "p_max_queue": 1000},
    )
    fetch.assert_called_once_with(batch_size=1, raise_on_error=True)
    kaggle_worker.init.assert_awaited_once_with()


def test_auto_pilot_respects_remaining_budget_and_targets_returned_ids(monkeypatch, pilot_client):
    pool = _fake_pool(monkeypatch)
    auto_id = "14076ba5-f130-4f86-9b53-ecf3b93ed077"
    second_id = "1c540d99-99fc-447b-8ee0-5c2dee9006e7"
    pilot_client.rpc.return_value.execute.return_value = SimpleNamespace(
        data=[
            {"creator_id": auto_id, "current_subscribers": 149000000, "dry_run": False},
            {"creator_id": second_id, "current_subscribers": 137000000, "dry_run": False},
        ]
    )
    fetch = MagicMock(
        side_effect=[
            [{"id": 1, "creator_id": "regular-fifo-job"}],
            [],
            [{"id": 2, "creator_id": auto_id}],
            [{"id": 3, "creator_id": second_id}],
        ]
    )
    monkeypatch.setattr(kaggle_worker, "_fetch_pending_jobs", fetch)
    handle = AsyncMock(return_value=True)
    monkeypatch.setattr(kaggle_worker, "handle_sync_job", handle)
    monkeypatch.setattr("db.refresh_creator_sync_coverage", MagicMock())

    asyncio.run(
        kaggle_worker.run(max_jobs=3, max_empty_polls=1, refresh_auto=True, refresh_dry_run=False)
    )

    pilot_client.rpc.assert_called_once_with(
        "enqueue_unknown_activity_refresh_batch",
        {"p_limit": 2, "p_dry_run": False, "p_max_queue": 1000},
    )
    assert fetch.call_args_list[2].kwargs == {
        "batch_size": 1,
        "creator_ids": [auto_id, second_id],
        "source": "unknown_activity_refresh",
        "raise_on_error": True,
    }
    assert [call.kwargs["creator_id"] for call in handle.await_args_list] == [
        "regular-fifo-job",
        auto_id,
        second_id,
    ]
    assert pool.record_job.call_count == 3


def test_auto_and_explicit_pilot_modes_are_mutually_exclusive(pilot_client):
    with pytest.raises(ValueError, match="not both"):
        asyncio.run(
            kaggle_worker.run(
                refresh_creator_ids=[CREATOR_ID],
                refresh_auto=True,
            )
        )

    pilot_client.rpc.assert_not_called()


@pytest.mark.parametrize("dry_run", [True, False])
def test_pilot_rpc_timeout_preserves_regular_run_finalization(
    monkeypatch, pilot_client, caplog, dry_run
):
    _fake_pool(monkeypatch)
    fetch = MagicMock(side_effect=[[{"id": 1, "creator_id": CREATOR_ID}], []])
    monkeypatch.setattr(kaggle_worker, "_fetch_pending_jobs", fetch)
    handle = AsyncMock(return_value=True)
    monkeypatch.setattr(kaggle_worker, "handle_sync_job", handle)
    coverage = MagicMock()
    monkeypatch.setattr("db.refresh_creator_sync_coverage", coverage)
    report = MagicMock()
    monkeypatch.setattr(kaggle_worker, "_display_final_report", report)
    pilot_client.rpc.return_value.execute.side_effect = ReadTimeout("pilot read timed out")

    asyncio.run(
        kaggle_worker.run(
            max_jobs=3,
            max_empty_polls=1,
            refresh_creator_ids=[CREATOR_ID],
            refresh_dry_run=dry_run,
        )
    )

    handle.assert_awaited_once()
    assert fetch.call_count == 2
    pilot_client.rpc.assert_called_once()
    pilot_client.rpc.return_value.execute.assert_called_once()
    coverage.assert_called_once()
    report.assert_called_once()
    assert report.call_args.args[0] == 1
    assert "stopping refresh phase without retry" in caplog.text
    assert ("Enqueue outcome is unknown" in caplog.text) is (not dry_run)


def test_apply_pilot_runs_after_normal_fifo_with_remaining_capacity(monkeypatch, pilot_client):
    pool = _fake_pool(monkeypatch)
    second_id = "14076ba5-f130-4f86-9b53-ecf3b93ed077"
    third_id = "1c540d99-99fc-447b-8ee0-5c2dee9006e7"
    events = []

    def enqueue_rpc(name, params):
        events.append(("enqueue", name, params))
        return SimpleNamespace(
            execute=lambda: SimpleNamespace(
                data=[{"creator_id": CREATOR_ID}, {"creator_id": second_id}]
            )
        )

    pilot_client.rpc.side_effect = enqueue_rpc
    fetch_results = [
        [{"id": 1, "creator_id": "regular-fifo-job"}],
        [],
        [{"id": 2, "creator_id": CREATOR_ID}],
        [{"id": 3, "creator_id": second_id}],
        [],
    ]

    def fetch(**kwargs):
        events.append(("fetch", kwargs))
        return fetch_results.pop(0)

    fetch_mock = MagicMock(side_effect=fetch)
    monkeypatch.setattr(kaggle_worker, "_fetch_pending_jobs", fetch_mock)
    handle = AsyncMock(return_value=True)
    monkeypatch.setattr(kaggle_worker, "handle_sync_job", handle)
    coverage = MagicMock()
    views = MagicMock()
    monkeypatch.setattr("db.refresh_creator_sync_coverage", coverage)
    monkeypatch.setattr("db.refresh_hero_stats_cache", views)

    asyncio.run(
        kaggle_worker.run(
            max_jobs=3,
            max_empty_polls=1,
            refresh_creator_ids=[CREATOR_ID, second_id, third_id],
            refresh_dry_run=False,
        )
    )

    assert events[0] == ("fetch", {"batch_size": 1, "raise_on_error": True})
    assert events[1] == ("fetch", {"batch_size": 1, "raise_on_error": True})
    assert events[2] == (
        "enqueue",
        "enqueue_unknown_activity_refreshes",
        {
            "p_creator_ids": [CREATOR_ID, second_id],
            "p_dry_run": False,
            "p_max_queue": 1000,
        },
    )
    assert fetch_mock.call_args_list[2].kwargs == {
        "batch_size": 1,
        "creator_ids": [CREATOR_ID, second_id],
        "source": "unknown_activity_refresh",
        "raise_on_error": True,
    }
    assert [call.kwargs["creator_id"] for call in handle.await_args_list] == [
        "regular-fifo-job",
        CREATOR_ID,
        second_id,
    ]
    assert len(fetch_results) == 1  # max_jobs was reached before a final empty poll
    assert pool.record_job.call_count == 3
    coverage.assert_called_once()
    views.assert_not_called()


def test_apply_pilot_caps_processing_at_five_and_preserves_maintenance(monkeypatch, pilot_client):
    ids = [
        CREATOR_ID,
        "14076ba5-f130-4f86-9b53-ecf3b93ed077",
        "1c540d99-99fc-447b-8ee0-5c2dee9006e7",
        "4a9591a1-a84c-44d9-8b98-bbe0bc6ae963",
        "d899ca2d-e87e-4e7a-ad74-cbc1ceca0dc8",
    ]
    pool = _fake_pool(monkeypatch)
    pilot_client.rpc.return_value.execute.return_value = SimpleNamespace(
        data=[{"creator_id": creator_id} for creator_id in ids]
    )
    fetch = MagicMock(
        side_effect=[
            [],
            *[
                [{"id": index, "creator_id": creator_id}]
                for index, creator_id in enumerate(ids, start=1)
            ],
            [],
        ]
    )
    handle = AsyncMock(return_value=True)
    monkeypatch.setattr(kaggle_worker, "_fetch_pending_jobs", fetch)
    monkeypatch.setattr(kaggle_worker, "handle_sync_job", handle)
    coverage = MagicMock()
    views = MagicMock()
    monkeypatch.setattr("db.refresh_creator_sync_coverage", coverage)
    monkeypatch.setattr("db.refresh_hero_stats_cache", views)

    asyncio.run(
        kaggle_worker.run(
            max_jobs=100,
            max_empty_polls=1,
            refresh_creator_ids=ids,
            refresh_dry_run=False,
        )
    )

    assert fetch.call_count == 7
    assert handle.await_count == pool.record_job.call_count == 5
    assert pilot_client.rpc.call_count == 1
    coverage.assert_called_once()
    views.assert_not_called()


@pytest.mark.parametrize("phase", ["normal", "pilot"])
def test_fetch_failure_retries_without_queue_phase_transition(monkeypatch, pilot_client, phase):
    _fake_pool(monkeypatch)
    monkeypatch.setattr(kaggle_worker.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr("db.refresh_creator_sync_coverage", MagicMock())
    pilot_client.rpc.return_value.execute.return_value = SimpleNamespace(
        data=[{"creator_id": CREATOR_ID}]
    )
    job = {"id": 1, "creator_id": CREATOR_ID}
    results = (
        [RuntimeError("read failed"), [job]]
        if phase == "normal"
        else [[], RuntimeError("read failed"), [job]]
    )
    fetch = MagicMock(side_effect=results)
    monkeypatch.setattr(kaggle_worker, "_fetch_pending_jobs", fetch)
    handle = AsyncMock(return_value=True)
    monkeypatch.setattr(kaggle_worker, "handle_sync_job", handle)

    asyncio.run(
        kaggle_worker.run(
            max_jobs=1,
            max_empty_polls=1,
            refresh_creator_ids=[CREATOR_ID],
            refresh_dry_run=False,
        )
    )

    handle.assert_awaited_once()
    assert pilot_client.rpc.call_count == (1 if phase == "pilot" else 0)
    assert fetch.call_count == len(results)
    kaggle_worker.asyncio.sleep.assert_awaited_once_with(30)
    assert all(call.kwargs["raise_on_error"] for call in fetch.call_args_list)


def test_repeated_fetch_failures_abort_without_enqueue(monkeypatch, pilot_client, caplog):
    _fake_pool(monkeypatch)
    monkeypatch.setattr(kaggle_worker.asyncio, "sleep", AsyncMock())
    fetch = MagicMock(side_effect=RuntimeError("database unavailable"))
    monkeypatch.setattr(kaggle_worker, "_fetch_pending_jobs", fetch)

    asyncio.run(kaggle_worker.run(max_empty_polls=1, refresh_creator_ids=[CREATOR_ID]))

    pilot_client.rpc.assert_not_called()
    assert fetch.call_count == kaggle_worker._MAX_FETCH_FAILURES
    assert "not empty" in caplog.text


def test_fetch_error_breaks_consecutive_empty_polls(monkeypatch, pilot_client):
    _fake_pool(monkeypatch)
    monkeypatch.setattr(kaggle_worker.asyncio, "sleep", AsyncMock())
    fetch = MagicMock(side_effect=[[], RuntimeError("read failed"), [], []])
    monkeypatch.setattr(kaggle_worker, "_fetch_pending_jobs", fetch)

    asyncio.run(kaggle_worker.run(max_empty_polls=2, refresh_creator_ids=[CREATOR_ID]))

    assert fetch.call_count == 4
    pilot_client.rpc.assert_called_once()


def test_pilot_does_not_return_to_general_fifo(monkeypatch, pilot_client):
    _fake_pool(monkeypatch)
    monkeypatch.setattr("db.refresh_creator_sync_coverage", MagicMock())
    pilot_client.rpc.return_value.execute.return_value = SimpleNamespace(
        data=[{"creator_id": CREATOR_ID}]
    )
    fetch = MagicMock(
        side_effect=[
            [],
            [{"id": 1, "creator_id": CREATOR_ID}],
            [],
        ]
    )
    monkeypatch.setattr(kaggle_worker, "_fetch_pending_jobs", fetch)
    handle = AsyncMock(return_value=True)
    monkeypatch.setattr(kaggle_worker, "handle_sync_job", handle)

    asyncio.run(
        kaggle_worker.run(
            max_jobs=2,
            max_empty_polls=1,
            refresh_creator_ids=[CREATOR_ID],
            refresh_dry_run=False,
        )
    )

    handle.assert_awaited_once()
    assert all(
        call.kwargs["creator_ids"] == [CREATOR_ID]
        and call.kwargs["source"] == "unknown_activity_refresh"
        for call in fetch.call_args_list[1:]
    )
    assert fetch.call_count == 3
    pilot_client.rpc.assert_called_once()


def test_normal_run_preserves_fifo_and_maintenance(monkeypatch, pilot_client):
    _fake_pool(monkeypatch)
    fetch = MagicMock(side_effect=[[{"id": 1, "creator_id": CREATOR_ID}], []])
    monkeypatch.setattr(kaggle_worker, "_fetch_pending_jobs", fetch)
    handle = AsyncMock(return_value=True)
    monkeypatch.setattr(kaggle_worker, "handle_sync_job", handle)
    coverage = MagicMock()
    monkeypatch.setattr("db.refresh_creator_sync_coverage", coverage)

    asyncio.run(kaggle_worker.run(max_jobs=2, max_empty_polls=1))

    handle.assert_awaited_once()
    coverage.assert_called_once()
    pilot_client.rpc.assert_not_called()
    assert fetch.call_count == 2
    assert all(call.kwargs == {"batch_size": 1} for call in fetch.call_args_list)


def test_normal_run_retains_existing_fetch_error_behavior(monkeypatch, pilot_client):
    _fake_pool(monkeypatch)
    sleep = AsyncMock()
    monkeypatch.setattr(kaggle_worker.asyncio, "sleep", sleep)
    query = MagicMock()
    for method in ("select", "eq", "or_", "order", "limit"):
        getattr(query, method).return_value = query
    query.execute.side_effect = RuntimeError("database unavailable")
    pilot_client.table.return_value = query

    asyncio.run(kaggle_worker.run(max_empty_polls=1))

    query.execute.assert_called_once()
    pilot_client.rpc.assert_not_called()
    sleep.assert_not_awaited()


def test_normal_run_does_not_enqueue_pilot(monkeypatch, pilot_client):
    _fake_pool(monkeypatch)

    asyncio.run(kaggle_worker.run(max_jobs=0))

    pilot_client.rpc.assert_not_called()
    kaggle_worker.init.assert_awaited_once_with()


@pytest.mark.parametrize("ids", [[], [CREATOR_ID] * 6, ["invalid-id"]])
def test_invalid_pilot_allowlist_fails_before_enqueue(pilot_client, ids):
    with pytest.raises(ValueError):
        asyncio.run(kaggle_worker.run(refresh_creator_ids=ids))
    pilot_client.rpc.assert_not_called()


def test_worker_initialization_retains_startup_maintenance(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(creator_worker, "init_supabase", lambda: client)
    monkeypatch.setattr(creator_worker, "supabase_client", None)
    monkeypatch.setattr(creator_worker, "youtube_resolver", None)
    monkeypatch.setattr(creator_worker, "get_creator_worker_api_key", lambda: "test-key")
    monkeypatch.setattr(creator_worker, "YouTubeResolver", lambda api_key: object())
    monkeypatch.setattr(
        creator_worker.schema_detector, "detect_schema_sync", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        creator_worker.schema_detector,
        "get_status",
        lambda: {"total_columns": 0, "missing_count": 0},
    )
    monkeypatch.delenv("CREATOR_WORKER_SKIP_DIAGNOSIS", raising=False)
    reset = MagicMock()
    diagnose = MagicMock()
    enqueue = MagicMock(return_value=0)
    monkeypatch.setattr(creator_worker, "_reset_stuck_processing_jobs", reset)
    monkeypatch.setattr(creator_worker, "_diagnose_creator_db_state", diagnose)
    monkeypatch.setattr(creator_worker, "_queue_unsynced_creators", enqueue)

    asyncio.run(creator_worker.init())

    reset.assert_called_once()
    diagnose.assert_called_once()
    enqueue.assert_called_once()
