from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from services import video_snapshot_demand as demand

CREATOR_ID = "6488b4d0-96e9-4cb3-b7da-36cc086f8e75"
USER_ID = "14076ba5-f130-4f86-9b53-ecf3b93ed077"


@pytest.mark.parametrize("flag, enabled", [("", False), ("false", False), ("true", True)])
def test_demand_is_default_off(monkeypatch, flag, enabled):
    monkeypatch.setenv("VIDEO_SNAPSHOT_DEMAND_ENABLED", flag)
    assert demand.video_snapshot_demand_enabled() is enabled


def test_request_is_scoped_and_uses_only_admission_rpc(monkeypatch):
    monkeypatch.setenv("VIDEO_SNAPSHOT_DEMAND_ENABLED", "true")
    client = MagicMock()
    client.rpc.return_value.execute.return_value = SimpleNamespace(data="queued")
    monkeypatch.setattr("db.supabase_client", client)

    assert demand.request_video_snapshot(CREATOR_ID, USER_ID) == "queued"
    client.rpc.assert_called_once_with(
        "request_creator_video_snapshot",
        {
            "p_creator_id": CREATOR_ID,
            "p_user_id": USER_ID,
        },
    )
    client.table.assert_not_called()


@pytest.mark.parametrize("scenario", ["disabled", "invalid", "rpc_failed"])
def test_demand_safely_rejects_or_handles_failure(monkeypatch, scenario):
    monkeypatch.setenv(
        "VIDEO_SNAPSHOT_DEMAND_ENABLED", "false" if scenario == "disabled" else "true"
    )
    client = MagicMock()
    client.rpc.side_effect = RuntimeError("RPC missing")
    monkeypatch.setattr("db.supabase_client", client)

    result = demand.request_video_snapshot("bad" if scenario == "invalid" else CREATOR_ID, USER_ID)

    assert (
        result
        == {"disabled": "disabled", "invalid": "invalid", "rpc_failed": "unavailable"}[scenario]
    )
    if scenario != "rpc_failed":
        client.rpc.assert_not_called()


def test_demand_only_requests_missing_or_stale_nonarchived_snapshots():
    now = datetime.now(timezone.utc)
    assert demand.needs_video_snapshot({}) is True
    assert demand.needs_video_snapshot({"archived_at": now.isoformat()}) is False
    assert (
        demand.needs_video_snapshot(
            {"recent_videos": [], "recent_videos_fetched_at": now.isoformat()}
        )
        is False
    )
    assert (
        demand.needs_video_snapshot(
            {
                "recent_videos": [],
                "recent_videos_fetched_at": (now - timedelta(days=8)).isoformat(),
            }
        )
        is True
    )
    assert (
        demand.needs_video_snapshot(
            {
                "recent_videos": [],
                "recent_videos_fetched_at": (now + timedelta(days=8)).isoformat(),
            }
        )
        is True
    )


@pytest.mark.parametrize(
    "scenario, status_code",
    [
        ("anonymous", 401),
        ("no_auth", 401),
        ("cross_origin", 403),
        ("no_hx", 403),
        ("allowed", 200),
    ],
)
def test_demand_endpoint_requires_auth_and_same_origin(monkeypatch, scenario, status_code):
    from routes.creators import request_creator_video_snapshot_route

    request = SimpleNamespace(
        headers={"HX-Request": "true", "origin": "http://testserver"},
        base_url="http://testserver/",
    )
    session = {"auth": "trusted-session", "user_id": USER_ID}
    if scenario == "anonymous":
        session = {}
    elif scenario == "no_auth":
        del session["auth"]
    elif scenario == "cross_origin":
        request.headers["origin"] = "https://other.example"
    elif scenario == "no_hx":
        del request.headers["HX-Request"]
    admission = MagicMock(return_value="queued")
    monkeypatch.setattr(demand, "request_video_snapshot", admission)

    response = request_creator_video_snapshot_route(request, session, CREATOR_ID)

    assert response.status_code == status_code
    if scenario == "allowed":
        admission.assert_called_once_with(CREATOR_ID, USER_ID)
        assert response.headers["cache-control"] == "no-store"
    else:
        admission.assert_not_called()


def test_demand_route_rejects_get_and_anonymous_post():
    import main
    from starlette.testclient import TestClient

    client = TestClient(main.app)
    url = f"/creator/{CREATOR_ID}/videos/request"
    assert client.get(url).status_code == 405
    assert client.post(url).status_code == 401


def test_health_discovers_bounded_demand_cohort_without_manual_allowlist(monkeypatch):
    import worker.creator_worker as worker

    monkeypatch.delenv("CREATOR_RECENT_VIDEO_ALLOWLIST", raising=False)
    monkeypatch.setattr(
        worker.schema_detector,
        "available_columns",
        {
            "recent_videos",
            "recent_videos_fetched_at",
            "recent_videos_requested_at",
        },
    )
    client = MagicMock()
    query = client.table.return_value
    query.select.return_value = query
    query.not_.is_.return_value = query
    query.order.return_value = query
    query.limit.return_value = query
    query.execute.return_value = SimpleNamespace(
        data=[
            {
                "id": str(index),
                "recent_videos": [],
                "recent_videos_fetched_at": datetime.now(timezone.utc).isoformat(),
            }
            for index in range(26)
        ]
    )
    monkeypatch.setattr(worker, "supabase_client", client)

    health = worker.get_recent_video_snapshot_health()

    assert health is not None
    assert health["scope"] == "recent_demand"
    assert health["inspected"] == health["empty"] == 25
    assert health["has_more"] is True
    assert health["configured"] is None
    query.limit.assert_called_once_with(26)
    query.not_.is_.assert_called_once_with("recent_videos_requested_at", "null")
    query.order.assert_called_once_with("recent_videos_requested_at", desc=True)
    query.update.assert_not_called()
