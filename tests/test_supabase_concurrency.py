"""Regression tests for the outbound-concurrency semaphore + env-var parsing.

These tests lock in the fixes for review of PR #639
(bound Supabase client timeout and cap outbound concurrency):

- _read_positive_env falls back safely on malformed / out-of-range input
  instead of crashing at module import.
- The per-attempt semaphore acquisition in _db_execute / _db_execute_readonly
  releases its slot during tenacity retry backoff, so sleeping retries do
  not starve fresh healthy requests.
"""

import logging
import threading
import time

import pytest

import db


# ── _read_positive_env: input validation ──────────────────────────────────


def test_read_positive_env_uses_default_when_unset(monkeypatch):
    monkeypatch.delenv("TEST_VAR", raising=False)
    assert db._read_positive_env("TEST_VAR", 5.0) == 5.0


def test_read_positive_env_parses_valid_float(monkeypatch):
    monkeypatch.setenv("TEST_VAR", "7.5")
    assert db._read_positive_env("TEST_VAR", 5.0) == 7.5


def test_read_positive_env_parses_valid_int(monkeypatch):
    monkeypatch.setenv("TEST_VAR", "42")
    assert db._read_positive_env("TEST_VAR", 5, cast=int) == 42


@pytest.mark.parametrize("bad", ["not-a-number", "", "1.2.3", "twenty"])
def test_read_positive_env_falls_back_on_malformed(monkeypatch, caplog, bad):
    monkeypatch.setenv("TEST_VAR", bad)
    with caplog.at_level(logging.WARNING, logger="vv_db"):
        assert db._read_positive_env("TEST_VAR", 9.0) == 9.0
    assert any("not a valid" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("bad", ["0", "-1", "0.0", "-0.5"])
def test_read_positive_env_falls_back_below_min(monkeypatch, caplog, bad):
    monkeypatch.setenv("TEST_VAR", bad)
    with caplog.at_level(logging.WARNING, logger="vv_db"):
        assert db._read_positive_env("TEST_VAR", 10.0, min_value=1.0) == 10.0
    assert any("below the safe minimum" in r.getMessage() for r in caplog.records)


# ── Semaphore is released across retry backoffs ───────────────────────────


class _TransientDisconnect(Exception):
    """Fake exception the strict retry predicate matches by class name."""

    pass


_TransientDisconnect.__name__ = "RemoteProtocolError"


def _bounded_wait(sem: threading.Semaphore, timeout_s: float) -> bool:
    """Threading.Semaphore.acquire with a timeout; True if slot obtained."""
    return sem.acquire(timeout=timeout_s)


def test_semaphore_released_during_retry_backoff(monkeypatch):
    """A tenacity-driven retry must NOT hold the concurrency slot while sleeping.

    Fills the semaphore with a mock _db_execute call that fails once (forcing
    a backoff), and asserts a second concurrent caller can grab a slot while
    the first is still in its retry sleep.
    """
    # Make the semaphore small so we can saturate it in one call, and shrink
    # the backoff so the test stays fast.
    monkeypatch.setattr(db, "_supabase_request_semaphore", threading.BoundedSemaphore(1))

    # First call: fails once with a retriable transport error, then succeeds.
    call_count = {"n": 0}

    def _flaky_fn():
        call_count["n"] += 1
        if call_count["n"] == 1:
            # Set __cause__ so the strict predicate matches via cause chain too.
            raise _TransientDisconnect("Server disconnected")
        return "ok"

    # Second caller polls whether it can grab a slot during the first call's
    # retry backoff.  Runs in a thread so the first call can proceed.
    second_got_slot = threading.Event()
    first_finished = threading.Event()

    def _second_caller():
        # Poll for up to 3s; if per-attempt semaphore works, we should get
        # a slot well before then.
        got = _bounded_wait(db._supabase_request_semaphore, timeout_s=3.0)
        if got:
            second_got_slot.set()
            db._supabase_request_semaphore.release()

    t = threading.Thread(target=_second_caller, daemon=True)

    def _first_caller():
        try:
            db._db_execute(_flaky_fn)
        finally:
            first_finished.set()

    t2 = threading.Thread(target=_first_caller, daemon=True)
    # Start the first caller which will acquire the slot, fail, release
    # during backoff sleep, retry, and succeed.
    t2.start()
    # Give the first caller a moment to start executing and hit its retry.
    time.sleep(0.05)
    # Start the polling second caller now — it should see the slot free
    # while the first is sleeping in tenacity backoff.
    t.start()
    t.join(timeout=3.5)
    t2.join(timeout=5.0)
    assert first_finished.is_set(), "first caller never completed"
    assert second_got_slot.is_set(), (
        "second caller could not acquire the slot during the first caller's "
        "retry backoff — the semaphore is being held across sleeps"
    )
