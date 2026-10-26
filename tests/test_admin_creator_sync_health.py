from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from xml.etree.ElementTree import fromstring

import pytest

import routes.admin as admin_routes
from constants import (
    CREATOR_SYNC_COVERAGE_MAX_AGE_HOURS,
    CREATOR_SYNC_PROGRESS_GRACE_HOURS,
    CREATOR_SYNC_READY_WARNING_HOURS,
    CREATOR_SYNC_STALL_HOURS,
)
from routes.admin import _fetch_creator_sync_health
from views.admin import _CreatorSyncHealthSection


NOW = datetime(2026, 10, 26, 12, tzinfo=timezone.utc)


def _ago(hours):
    return (NOW - timedelta(hours=hours)).isoformat()


def _row(**changes):
    row = {
        "ready_jobs": 4,
        "retry_delayed_jobs": 3,
        "processing_jobs": 1,
        "oldest_ready_at": _ago(3),
        "oldest_processing_at": _ago(1),
        "last_successful_sync_at": _ago(0.5),
        "eligible_creators_without_job": 12,
        "oldest_eligible_since": _ago(96),
        "coverage_sampled_at": _ago(1),
        "coverage_min_subscribers": 10000,
    }
    return row | changes


def _fetch(row=None):
    client = MagicMock()
    client.rpc.return_value.execute.return_value.data = [_row() if row is None else row]
    return _fetch_creator_sync_health(client, NOW)


def _render(data):
    root = fromstring(str(_CreatorSyncHealthSection(data)))
    rows = {
        "".join(row[0].itertext()): "".join(row[1].itertext())
        for row in root.iter("div")
        if len(row) == 2 and all(child.tag == "span" for child in row)
    }
    return " ".join(root.itertext()), rows


def test_creator_sync_health_reads_aggregate_and_converts_ages():
    client = MagicMock()
    client.rpc.return_value.execute.return_value.data = [_row()]
    result = _fetch_creator_sync_health(client, NOW)

    assert result == {
        "creator_sync_health_available": True,
        "creator_sync_ready_jobs": 4,
        "creator_sync_delayed_jobs": 3,
        "creator_sync_processing_jobs": 1,
        "creator_sync_oldest_ready_secs": 3 * 3600,
        "creator_sync_oldest_processing_secs": 3600,
        "creator_sync_unqueued_stale": 12,
        "creator_sync_oldest_unqueued_secs": 96 * 3600,
        "creator_sync_last_success_secs": 1800,
        "creator_sync_coverage_available": True,
        "creator_sync_coverage_age_secs": 3600,
        "creator_sync_coverage_min_subscribers": 10000,
    }
    client.rpc.assert_called_once_with("get_creator_sync_health_v2", {})


@pytest.mark.parametrize("rows", [[], [_row(), _row()]])
def test_invalid_cardinality_is_unavailable(rows):
    client = MagicMock()
    client.rpc.return_value.execute.return_value.data = rows
    assert not _fetch_creator_sync_health(client, NOW)["creator_sync_health_available"]


@pytest.mark.parametrize(
    "changes",
    [
        {"ready_jobs": -1},
        {"retry_delayed_jobs": -1},
        {"processing_jobs": -1},
        {"ready_jobs": 1.5},
        {"ready_jobs": True},
        {"ready_jobs": None},
        {"oldest_ready_at": "invalid"},
        {"oldest_ready_at": None},
        {"oldest_processing_at": None},
        {"last_successful_sync_at": "invalid"},
    ],
)
def test_invalid_queue_metrics_are_unavailable(changes):
    result = _fetch(_row(**changes))
    assert not result["creator_sync_health_available"]
    assert result["creator_sync_ready_jobs"] is None


@pytest.mark.parametrize(
    "changes",
    [
        {"eligible_creators_without_job": -1},
        {"coverage_sampled_at": "invalid"},
        {"coverage_min_subscribers": -1},
        {"oldest_eligible_since": None},
        {"oldest_eligible_since": "invalid"},
        {"coverage_sampled_at": None},
    ],
)
def test_bad_or_missing_coverage_does_not_hide_queue_health(changes):
    result = _fetch(_row(**changes))
    assert result["creator_sync_health_available"]
    assert not result["creator_sync_coverage_available"]
    assert result["creator_sync_unqueued_stale"] is None
    text, rows = _render(result)
    assert "Healthy" in text
    assert rows["Eligible creators without a job"] == "Unavailable"


def test_zero_coverage_does_not_require_an_oldest_timestamp():
    result = _fetch(_row(eligible_creators_without_job=0, oldest_eligible_since=None))
    assert result["creator_sync_coverage_available"]


def test_read_timeout_is_not_retried(monkeypatch):
    monkeypatch.setattr(
        admin_routes, "_db_execute_readonly", lambda fn: pytest.fail("wrong policy")
    )
    client = MagicMock()
    import httpx

    client.rpc.return_value.execute.side_effect = httpx.ReadTimeout("slow query")
    result = _fetch_creator_sync_health(client, NOW)
    assert not result["creator_sync_health_available"]
    assert client.rpc.return_value.execute.call_count == 1


def test_creator_sync_health_section_renders_each_count():
    text, rows = _render(_fetch())
    assert "Healthy" in text
    assert rows["Ready jobs"] == "4"
    assert rows["Retry-delayed jobs"] == "3"
    assert rows["Processing jobs"] == "1"
    assert rows["Oldest processing age"] == "1h 0m"
    assert rows["Eligible creators without a job"] == "12"
    assert rows["Coverage minimum subscribers"] == "10,000"


@pytest.mark.parametrize(
    "wait_hours,success_hours,status",
    [
        (1, None, "Healthy"),
        (CREATOR_SYNC_READY_WARNING_HOURS, 25, "Healthy"),
        (CREATOR_SYNC_READY_WARNING_HOURS + 0.01, 1, "Behind"),
        (CREATOR_SYNC_STALL_HOURS, None, "Behind"),
        (CREATOR_SYNC_STALL_HOURS + 1, None, "Stalled"),
        (CREATOR_SYNC_STALL_HOURS + 1, CREATOR_SYNC_PROGRESS_GRACE_HOURS, "Behind"),
        (CREATOR_SYNC_STALL_HOURS + 1, CREATOR_SYNC_PROGRESS_GRACE_HOURS + 1, "Stalled"),
    ],
)
def test_health_depends_on_daily_schedule_wait_and_progress(wait_hours, success_hours, status):
    result = _fetch(
        _row(
            oldest_ready_at=_ago(wait_hours),
            last_successful_sync_at=_ago(success_hours) if success_hours is not None else None,
        )
    )
    text, _ = _render(result)
    assert status in text


def test_large_stale_coverage_does_not_change_idle_status():
    result = _fetch(
        _row(
            ready_jobs=0,
            processing_jobs=0,
            oldest_ready_at=None,
            oldest_processing_at=None,
            eligible_creators_without_job=927000,
            last_successful_sync_at=None,
            coverage_sampled_at=_ago(CREATOR_SYNC_COVERAGE_MAX_AGE_HOURS + 1),
        )
    )
    text, rows = _render(result)
    assert "Idle" in text
    assert "(stale)" in rows["Coverage updated"]
    assert rows["Eligible creators without a job"] == "927,000"


def test_stuck_processing_without_ready_jobs_is_not_healthy():
    result = _fetch(
        _row(
            ready_jobs=0,
            oldest_ready_at=None,
            oldest_processing_at=_ago(CREATOR_SYNC_STALL_HOURS + 1),
            last_successful_sync_at=None,
        )
    )
    text, _ = _render(result)
    assert "Stalled" in text


def test_failed_query_renders_every_metric_unavailable():
    client = MagicMock()
    client.rpc.side_effect = RuntimeError("RPC unavailable")
    result = _fetch_creator_sync_health(client, NOW)
    text, rows = _render(result)
    assert "Health metrics unavailable" in text
    assert set(rows) == {
        "Ready jobs",
        "Retry-delayed jobs",
        "Processing jobs",
        "Oldest ready wait",
        "Oldest processing age",
        "Eligible creators without a job",
        "Oldest eligible wait",
        "Coverage updated",
        "Coverage minimum subscribers",
        "Last successful sync",
    }
    assert all(value == "Unavailable" for value in rows.values())
