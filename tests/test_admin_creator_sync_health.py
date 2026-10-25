from datetime import datetime, timezone
from unittest.mock import MagicMock

from routes.admin import _fetch_creator_sync_health
from views.admin import _CreatorSyncHealthSection


def test_creator_sync_health_reads_aggregate_and_converts_ages():
    now = datetime(2026, 10, 24, 12, 0, tzinfo=timezone.utc)
    client = MagicMock()
    client.rpc.return_value.execute.return_value.data = [
        {
            "ready_jobs": 4,
            "retry_delayed_jobs": 3,
            "stale_creators_without_job": 12,
            "oldest_unqueued_stale_at": "2026-10-20T12:00:00+00:00",
            "last_successful_sync_at": "2026-10-24T11:30:00+00:00",
        }
    ]

    result = _fetch_creator_sync_health(client, now)

    assert result == {
        "creator_sync_health_available": True,
        "creator_sync_ready_jobs": 4,
        "creator_sync_delayed_jobs": 3,
        "creator_sync_unqueued_stale": 12,
        "creator_sync_oldest_unqueued_secs": 4 * 86400,
        "creator_sync_last_success_secs": 30 * 60,
    }
    client.rpc.assert_called_once_with("get_creator_sync_health", {"p_stale_days": 7})


def test_creator_sync_health_failure_is_unavailable_not_zero():
    client = MagicMock()
    client.rpc.side_effect = RuntimeError("RPC unavailable")

    result = _fetch_creator_sync_health(client, datetime.now(timezone.utc))

    assert result["creator_sync_health_available"] is False
    assert result["creator_sync_ready_jobs"] is None
    assert result["creator_sync_unqueued_stale"] is None


def test_creator_sync_health_section_renders_status_and_counts():
    html = str(
        _CreatorSyncHealthSection(
            {
                "creator_sync_health_available": True,
                "creator_sync_ready_jobs": 4,
                "creator_sync_delayed_jobs": 3,
                "creator_sync_unqueued_stale": 12,
                "creator_sync_oldest_unqueued_secs": 4 * 86400,
                "creator_sync_last_success_secs": 30 * 60,
            }
        )
    )

    assert "Creator Sync Health" in html
    assert "Behind" in html
    assert "Eligible creators without a job" in html
    assert "12" in html
    assert "Retry-delayed jobs" in html


def test_creator_sync_health_section_reports_healthy_and_stalled_states():
    healthy_html = str(
        _CreatorSyncHealthSection(
            {
                "creator_sync_health_available": True,
                "creator_sync_ready_jobs": 0,
                "creator_sync_delayed_jobs": 2,
                "creator_sync_unqueued_stale": 0,
                "creator_sync_oldest_unqueued_secs": None,
                "creator_sync_last_success_secs": 60,
            }
        )
    )
    stalled_html = str(
        _CreatorSyncHealthSection(
            {
                "creator_sync_health_available": True,
                "creator_sync_ready_jobs": 0,
                "creator_sync_delayed_jobs": 0,
                "creator_sync_unqueued_stale": 1,
                "creator_sync_oldest_unqueued_secs": 86400,
                "creator_sync_last_success_secs": 86401,
            }
        )
    )

    assert "Healthy" in healthy_html
    assert "Stalled" in stalled_html


def test_creator_sync_health_section_does_not_render_failed_queries_as_zero():
    html = str(
        _CreatorSyncHealthSection(
            {
                "creator_sync_health_available": False,
                "creator_sync_ready_jobs": None,
                "creator_sync_delayed_jobs": None,
                "creator_sync_unqueued_stale": None,
                "creator_sync_oldest_unqueued_secs": None,
                "creator_sync_last_success_secs": None,
            }
        )
    )

    assert "Unavailable" in html
    assert "Health metrics unavailable" in html
    assert "Ready jobs</span><span" in html
    assert html.count("Unavailable") >= 5
