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


# ── Cloudflare 5xx from Supabase's origin (postgrest.APIError) ───────────

_APIError = _named_exc("APIError")


def _apierror(code):
    """Fake postgrest.APIError with a .code attribute like the real class."""
    exc = _APIError("origin unreachable")
    exc.code = code  # type: ignore[attr-defined]
    return exc


@pytest.mark.parametrize("code", [520, 521, 522, 523, 524])
def test_cf_upstream_5xx_matches_all_known_codes(code):
    assert _is_cf_upstream_5xx(_apierror(code)) is True


@pytest.mark.parametrize(
    "exc",
    [
        _apierror(200),
        _apierror(400),
        _apierror(404),
        _apierror(500),  # generic 500 is not a CF upstream signal
        _apierror(525),  # SSL handshake, not upstream unreachable
        _apierror(None),
        _apierror("not-a-number"),
        _named_exc("APIError")("no code attribute"),
        Exception("522 in message but wrong type"),
    ],
)
def test_cf_upstream_5xx_rejects_unrelated(exc):
    assert _is_cf_upstream_5xx(exc) is False


def test_readonly_matches_cf_upstream_5xx():
    assert _is_transient_transport_readonly(_apierror(522)) is True


def test_strict_does_not_match_cf_upstream_5xx():
    # CF 5xx may represent post-dispatch state (esp. 520, 524) so the
    # write-safe predicate must NOT match; only the read-only predicate does.
    assert _is_transient_disconnect(_apierror(522)) is False
