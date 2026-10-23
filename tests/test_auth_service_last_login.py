from types import SimpleNamespace
from unittest.mock import MagicMock

from auth import auth_service
from auth.auth_service import ViralVibesAuth


def _make_auth_info():
    return SimpleNamespace(
        email="alex@example.com",
        name="Alex User",
        given_name="Alex",
        email_verified=True,
        picture=None,
        access_token=None,
        refresh_token=None,
        expires_at=None,
    )


def _make_auth_service(monkeypatch, existing_user):
    service = object.__new__(ViralVibesAuth)
    service.supabase_client = MagicMock()
    users_query = service.supabase_client.table.return_value
    users_query.update.return_value = users_query
    users_query.eq.return_value = users_query
    users_query.or_.return_value = users_query
    service._get_user_by_email = lambda email: existing_user
    monkeypatch.setattr(auth_service, "_db_is_admin", lambda user_id: False)
    return service


def test_first_oauth_login_is_saved_as_last_login(monkeypatch):
    service = _make_auth_service(monkeypatch, existing_user=None)
    users_query = service.supabase_client.table.return_value
    users_query.insert.return_value.execute.return_value.data = [{"id": "user-1"}]

    service.get_auth(_make_auth_info(), "google-user-1", {}, "/login")

    inserted_user = users_query.insert.call_args.args[0]
    assert inserted_user["last_login_at"]


def test_existing_user_login_update_is_monotonic(monkeypatch):
    service = _make_auth_service(monkeypatch, existing_user={"id": "user-1"})
    users_query = service.supabase_client.table.return_value

    service.get_auth(_make_auth_info(), "google-user-1", {}, "/login")

    updated_user = users_query.update.call_args.args[0]
    login_at = updated_user["last_login_at"]
    condition = users_query.or_.call_args.args[0]
    assert f"last_login_at.lt.{login_at}" in condition
    assert "last_login_at.is.null" in condition