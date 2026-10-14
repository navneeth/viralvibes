from types import SimpleNamespace

import pytest


class _LegacyQuery:
    def __getattr__(self, name):
        if name == "not_":
            return self
        return lambda *_args, **_kwargs: self

    def execute(self):
        return SimpleNamespace(data=[], count=0)


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("57014: canceling statement due to statement timeout"),
        RuntimeError("PGRST003: timed out acquiring connection from connection pool"),
        type("ReadTimeout", (Exception,), {})("read timed out"),
    ],
)
def test_ranked_search_timeout_does_not_run_legacy_fallback(monkeypatch, error):
    import db

    class RpcCall:
        def execute(self):
            raise error

    class Client:
        def __init__(self):
            self.table_calls = []

        def rpc(self, *_args, **_kwargs):
            return RpcCall()

        def table(self, name):
            self.table_calls.append(name)
            return _LegacyQuery()

    client = Client()
    monkeypatch.setattr(db, "supabase_client", client)
    monkeypatch.setattr(db, "_db_execute", lambda operation: operation())
    monkeypatch.setattr(
        db,
        "_db_execute_readonly_without_read_timeout",
        lambda operation: operation(),
    )

    result = db.get_creators(
        search="volleyball",
        sort="subscribers",
        limit=50,
        offset=0,
        return_count=True,
    )

    assert result.creators == []
    assert result.total_count == 0
    assert result.degraded is True
    assert client.table_calls == []


def test_ranked_search_non_timeout_error_keeps_legacy_fallback(monkeypatch):
    import db

    class RpcCall:
        def execute(self):
            raise RuntimeError("function search_creators_ranked does not exist")

    class Client:
        def __init__(self):
            self.table_calls = []

        def rpc(self, *_args, **_kwargs):
            return RpcCall()

        def table(self, name):
            self.table_calls.append(name)
            return _LegacyQuery()

    client = Client()
    monkeypatch.setattr(db, "supabase_client", client)
    monkeypatch.setattr(db, "_db_execute", lambda operation: operation())
    monkeypatch.setattr(
        db,
        "_db_execute_readonly_without_read_timeout",
        lambda operation: operation(),
    )

    result = db.get_creators(
        search="volleyball",
        sort="subscribers",
        limit=50,
        offset=0,
        return_count=True,
    )

    assert result.creators == []
    assert result.degraded is False
    assert client.table_calls == ["creators"]
