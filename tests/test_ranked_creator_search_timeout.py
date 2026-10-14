import pytest


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
        def rpc(self, *_args, **_kwargs):
            return RpcCall()

        def table(self, *_args, **_kwargs):
            pytest.fail("ranked-search timeout must not launch the legacy table scan")

    monkeypatch.setattr(db, "supabase_client", Client())
    monkeypatch.setattr(db, "_db_execute", lambda operation: operation())

    attempted, result = db._get_ranked_creator_search(
        search="volleyball",
        sort="subscribers",
        limit=50,
        offset=0,
        return_count=True,
    )

    assert attempted is True
    assert result.creators == []
    assert result.total_count == 0
    assert result.degraded is True


def test_ranked_search_non_timeout_error_keeps_legacy_fallback(monkeypatch):
    import db

    class RpcCall:
        def execute(self):
            raise RuntimeError("function search_creators_ranked does not exist")

    class Client:
        def rpc(self, *_args, **_kwargs):
            return RpcCall()

    monkeypatch.setattr(db, "supabase_client", Client())
    monkeypatch.setattr(db, "_db_execute", lambda operation: operation())

    attempted, result = db._get_ranked_creator_search(
        search="volleyball",
        sort="subscribers",
        limit=50,
        offset=0,
        return_count=True,
    )

    assert attempted is False
    assert result.creators == []
    assert result.degraded is False
