from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from routes.admin import _fetch_admin_data
from views.admin import _QueueSection, _UsersSection


def test_recent_active_users_are_limited_to_30_days_and_sorted_by_login(monkeypatch):
    import routes.admin as admin

    fixed_now = datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc)

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz else fixed_now.replace(tzinfo=None)

    monkeypatch.setattr(admin, "datetime", FrozenDateTime)

    client = MagicMock()
    query = client.table.return_value
    query.select.return_value = query
    query.gte.return_value = query
    query.order.return_value = query
    query.limit.return_value = query
    query.execute.return_value.data = [{"id": "u1", "name": "Alex", "email": "alex@example.com"}]
    monkeypatch.setattr(admin._db, "supabase_client", client)

    data = _fetch_admin_data()

    query.select.assert_any_call("id, name, email, created_at, last_login_at")
    cutoff_30d = (fixed_now - timedelta(days=30)).isoformat()
    query.gte.assert_any_call("last_login_at", cutoff_30d)
    query.order.assert_any_call("last_login_at", desc=True)
    query.limit.assert_any_call(20)
    assert data["recent_active_users"] == query.execute.return_value.data


def test_admin_data_counts_persisted_user_creator_requests(monkeypatch):
    import routes.admin as admin

    client = MagicMock()
    query = client.table.return_value
    for method in (
        "select",
        "eq",
        "neq",
        "gte",
        "lt",
        "gt",
        "is_",
        "in_",
        "order",
        "limit",
        "ilike",
        "or_",
    ):
        getattr(query, method).return_value = query
    query.not_ = query
    query.execute.return_value.count = 26
    query.execute.return_value.data = []
    monkeypatch.setattr(admin._db, "supabase_client", client)
    monkeypatch.setattr(admin, "_fetch_creator_sync_health", lambda *_: {})

    data = _fetch_admin_data()

    assert data["creator_add_requests"] == 26
    client.table.assert_any_call("creator_add_request_history")
    query.select.assert_any_call("job_id", count="exact")


def test_queue_section_shows_creator_request_count_or_unavailable():
    base_data = {"queue_pending": 0, "queue_processing": 0, "queue_failed": 0}

    html = str(_QueueSection(base_data | {"creator_add_requests": 26}))
    assert "Creator Requests" in html
    assert "26" in html
    assert "accepted rows, all-time" in html

    unavailable_html = str(_QueueSection(base_data | {"creator_add_requests": None}))
    assert "Unavailable" in unavailable_html


def test_users_section_labels_login_recency_and_renders_outreach_email():
    html = str(
        _UsersSection(
            {
                "recent_active_users": [
                    {
                        "name": "Alex User",
                        "email": "alex@example.com",
                        "created_at": "2026-01-02T00:00:00Z",
                        "last_login_at": "2026-10-22T00:00:00Z",
                    }
                ]
            }
        )
    )

    assert "Recently Active for Outreach" in html
    assert "login recency, not login frequency" in html
    assert "alex@example.com" in html
