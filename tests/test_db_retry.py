"""Regression tests for db._is_transient_disconnect.

Guards the retry-scope decisions documented in db.py so future changes
cannot silently drop coverage for known-transient HTTP/2 transport errors
observed in production (Vercel + Supabase logs).
"""

import pytest

from db import _is_transient_disconnect


# ── Types that must be treated as transient ──────────────────────────────


class _FakeRemoteProtocolError(Exception):
    pass


_FakeRemoteProtocolError.__name__ = "RemoteProtocolError"


class _FakeWriteError(Exception):
    pass


_FakeWriteError.__name__ = "WriteError"


class _FakeConnectError(Exception):
    pass


_FakeConnectError.__name__ = "ConnectError"


@pytest.mark.parametrize(
    "exc",
    [
        _FakeRemoteProtocolError("Server disconnected"),
        _FakeWriteError("[Errno 32] Broken pipe"),
        _FakeConnectError("connection refused"),
        BrokenPipeError("[Errno 32] Broken pipe"),
        ConnectionResetError("peer reset"),
        Exception("[Errno 32] Broken pipe"),  # matched by string fallback
        RuntimeError("dictionary changed size during iteration"),
        RuntimeError("deque mutated during iteration"),
        KeyError(3),  # HTTP/2 client stream ids are odd
    ],
)
def test_transient_disconnect_matches(exc):
    assert _is_transient_disconnect(exc) is True


# ── Types that must NOT be treated as transient ───────────────────────────


@pytest.mark.parametrize(
    "exc",
    [
        ValueError("bad payload"),
        KeyError("user_id"),  # non-integer key, application bug
        KeyError(2),  # even integer, not an HTTP/2 stream id
        RuntimeError("unrelated"),
        Exception("timeout: statement canceled"),  # 57014 handled elsewhere
    ],
)
def test_non_transient_not_matched(exc):
    assert _is_transient_disconnect(exc) is False


def test_matches_when_cause_is_transient():
    root = _FakeWriteError("[Errno 32] Broken pipe")
    wrapped = Exception("query failed")
    wrapped.__cause__ = root
    assert _is_transient_disconnect(wrapped) is True
