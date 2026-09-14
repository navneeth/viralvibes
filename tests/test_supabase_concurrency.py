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

    Verified with a controlled barrier that makes the ordering unambiguous:

    - The flaky function raises on attempt #1 (retry backoff begins).
    - Attempt #2 blocks on ``proceed`` and cannot return until we release it.
    - The second caller waits for ``first_attempt_raised`` before touching the
      semaphore, so any successful acquire is provably during the backoff
      sleep — the retry attempt has not yet completed.
    - If the semaphore is held across the sleep (the bug this test guards
      against), the second caller times out and ``proceed`` is never set,
      so the whole test hangs to the outer join timeout instead of quietly
      passing on eventual acquisition.
    """
    sem = threading.BoundedSemaphore(1)
    monkeypatch.setattr(db, "_supabase_request_semaphore", sem)

    first_attempt_raised = threading.Event()
    proceed = threading.Event()
    second_acquired_during_backoff = threading.Event()
    first_finished = threading.Event()
    call_count = {"n": 0}

    def _flaky_fn():
        call_count["n"] += 1
        if call_count["n"] == 1:
            # Signal that we are about to enter tenacity's backoff sleep.
            first_attempt_raised.set()
            raise _TransientDisconnect("Server disconnected")
        # Attempt #2 waits on `proceed` — will only be set by the second
        # caller after it has acquired the semaphore.  This makes the
        # "acquired during backoff, not after retry finished" property
        # provable rather than probabilistic.
        assert proceed.wait(timeout=3.0), "second caller never signalled proceed"
        return "ok"

    def _second_caller():
        # Only touch the semaphore once we know the retry is in its sleep.
        assert first_attempt_raised.wait(timeout=2.0), "first attempt never raised"
        # Acquire with a bounded wait.  If the slot is held across backoff,
        # this blocks forever (the retry can't finish because it's waiting
        # on `proceed`, which we only set after acquiring); the timeout
        # keeps the test bounded either way.
        got = sem.acquire(timeout=2.0)
        try:
            # Must acquire BEFORE the first caller finishes — that's what
            # "released during backoff" means.  Guard against a phantom win
            # in case tenacity's backoff was extremely short and the retry
            # somehow completed before we got here.
            if got and not first_finished.is_set():
                second_acquired_during_backoff.set()
        finally:
            if got:
                sem.release()
            # Always release the retry so the test can finish.
            proceed.set()

    def _first_caller():
        try:
            db._db_execute(_flaky_fn)
        finally:
            first_finished.set()

    t_first = threading.Thread(target=_first_caller, daemon=True)
    t_second = threading.Thread(target=_second_caller, daemon=True)
    t_first.start()
    t_second.start()

    t_second.join(timeout=5.0)
    t_first.join(timeout=5.0)

    assert first_finished.is_set(), "first caller never completed"
    assert second_acquired_during_backoff.is_set(), (
        "second caller could not acquire the slot DURING the first caller's "
        "retry backoff — the semaphore is being held across sleeps"
    )
