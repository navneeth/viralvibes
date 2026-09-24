"""Regression tests for db retry-scope predicates.

Guards the retry-scope decisions documented in db.py so future changes
cannot silently drop coverage for known-transient HTTP/2 transport errors
observed in production (Vercel + Supabase logs), and cannot silently
broaden the write-safe predicate to include errors that may fire after
the server has partially received a request.
"""

import pytest

from db import _is_cf_upstream_5xx, _is_transient_disconnect, _is_transient_transport_readonly


def _named_exc(name: str):
    """Build a throwaway exception class with a chosen __name__.

    db.py matches httpx/httpcore transient errors by class name (not
    isinstance) to survive vendored subclass differences across deploy
    targets; tests mirror that by faking the class name.
    """
    return type(name, (Exception,), {})


_RemoteProtocolError = _named_exc("RemoteProtocolError")
_WriteError = _named_exc("WriteError")
_WriteTimeout = _named_exc("WriteTimeout")
_ReadError = _named_exc("ReadError")
_ReadTimeout = _named_exc("ReadTimeout")
_ConnectError = _named_exc("ConnectError")
_ConnectTimeout = _named_exc("ConnectTimeout")


# ── _is_transient_disconnect: STRICT set (safe for reads AND writes) ─────


@pytest.mark.parametrize(
    "exc",
    [
        _RemoteProtocolError("Server disconnected"),
        _ConnectError("connection refused"),
        _ConnectTimeout("connect timeout"),
        RuntimeError("dictionary changed size during iteration"),
        RuntimeError("deque mutated during iteration"),
        KeyError(3),
    ],
)
def test_strict_matches_pre_dispatch_errors(exc):
    assert _is_transient_disconnect(exc) is True


@pytest.mark.parametrize(
    "exc",
    [
        _WriteError("[Errno 32] Broken pipe"),
        _WriteTimeout("write timeout"),
        _ReadError("read error"),
        _ReadTimeout("read timeout"),
        Exception("[Errno 32] Broken pipe"),
        ValueError("bad payload"),
        KeyError("user_id"),
        KeyError(2),
        RuntimeError("unrelated"),
    ],
)
def test_strict_does_not_match_post_dispatch_or_unrelated(exc):
    assert _is_transient_disconnect(exc) is False


def test_strict_matches_when_cause_is_transient():
    root = _RemoteProtocolError("Server disconnected")
    wrapped = Exception("query failed")
    wrapped.__cause__ = root
    assert _is_transient_disconnect(wrapped) is True


# ── _is_transient_transport_readonly: broader set (idempotent reads only)


@pytest.mark.parametrize(
    "exc",
    [
        _RemoteProtocolError("Server disconnected"),
        _ConnectError("connection refused"),
        _WriteError("[Errno 32] Broken pipe"),
        _WriteTimeout("write timeout"),
        _ReadError("read error"),
        _ReadTimeout("read timeout"),
    ],
)
def test_readonly_matches_broader_transport_errors(exc):
    assert _is_transient_transport_readonly(exc) is True


@pytest.mark.parametrize(
    "exc",
    [
        Exception("[Errno 32] Broken pipe"),
        ValueError("bad payload"),
        KeyError("user_id"),
        RuntimeError("unrelated"),
    ],
)
def test_readonly_does_not_match_unrelated(exc):
    assert _is_transient_transport_readonly(exc) is False


def test_readonly_matches_when_cause_is_write_error():
    root = _WriteError("[Errno 32] Broken pipe")
    wrapped = Exception("query failed")
    wrapped.__cause__ = root
    assert _is_transient_transport_readonly(wrapped) is True


def test_find_creator_by_normalized_handle_uses_readonly_retry(monkeypatch):
    """Transient read timeouts on creator handle lookups must use the readonly retry policy."""

    class FakeTable:
        def select(self, *_args, **_kwargs):
            return self

        def ilike(self, *_args, **_kwargs):
            return self

        def limit(self, *_args, **_kwargs):
            return self

        def execute(self):
            return type("Resp", (), {"data": []})()

    class FakeClient:
        def rpc(self, *args, **kwargs):
            raise _ReadTimeout("read timeout")

        def table(self, *_args, **_kwargs):
            return FakeTable()

    used = {"readonly": 0}

    def readonly_wrapper(fn):
        used["readonly"] += 1
        return fn()

    monkeypatch.setattr("db.supabase_client", FakeClient())
    monkeypatch.setattr(
        "db._db_execute",
        lambda fn: (_ for _ in ()).throw(
            AssertionError("write-safe retry path should not be used")
        ),
    )
    monkeypatch.setattr("db._db_execute_readonly", readonly_wrapper)

    result = __import__("db")._find_creator_by_normalized_handle("alejoigoa")

    assert result is None
    # RPC + "alejoigoa" + "@alejoigoa" fallback attempts; all are readonly reads.
    assert used["readonly"] == 3


# ── Transient upstream gateway 5xx (CF + Kong) wrapped in postgrest.APIError

_APIError = _named_exc("APIError")


def _apierror(code):
    """Fake postgrest.APIError with a .code attribute like the real class."""
    exc = _APIError("origin unreachable")
    exc.code = code  # type: ignore[attr-defined]
    return exc


@pytest.mark.parametrize(
    "code",
    [
        502,  # Kong bad gateway to PostgREST
        503,  # Supabase overload
        504,  # Kong timeout to PostgREST (observed in production)
        520,  # Cloudflare unknown
        521,  # Cloudflare origin down
        522,  # Cloudflare connection timeout
        523,  # Cloudflare origin unreachable
        524,  # Cloudflare timeout waiting for response
    ],
)
def test_upstream_gateway_matches_all_known_codes(code):
    assert _is_cf_upstream_5xx(_apierror(code)) is True


@pytest.mark.parametrize(
    "exc",
    [
        _apierror(200),
        _apierror(400),
        _apierror(404),
        _apierror(500),  # generic 500 is not a gateway signal
        _apierror(525),  # SSL handshake, not upstream unreachable
        _apierror(None),
        _apierror("not-a-number"),
        _apierror("PGRST116"),  # PostgREST application error code
        _apierror("42883"),  # Postgres SQL state
        _named_exc("APIError")("no code attribute"),
        Exception("522 in message but wrong type"),
    ],
)
def test_upstream_gateway_rejects_unrelated(exc):
    assert _is_cf_upstream_5xx(exc) is False


@pytest.mark.parametrize("code", [502, 503, 504, 520, 522, 524])
def test_readonly_matches_upstream_gateway(code):
    assert _is_transient_transport_readonly(_apierror(code)) is True


@pytest.mark.parametrize("code", [502, 503, 504, 522, 524])
def test_strict_does_not_match_upstream_gateway(code):
    # Gateway 5xx may represent post-dispatch state (esp. 504/524) so the
    # write-safe predicate must NOT match; only the read-only predicate does.
    assert _is_transient_disconnect(_apierror(code)) is False
