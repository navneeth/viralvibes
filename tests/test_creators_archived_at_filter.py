"""Regression tests for the ``creators.archived_at`` ghost-column fix.

Background
----------
``db.queue_invalid_creators_for_retry``, ``db.archive_permanently_failed_creators``,
and ``worker/creator_worker.py``'s stale-check all assume ``creators.archived_at``
exists.  No tracked migration ever created the column, so every call to those
paths was returning Postgres 42703 ("column does not exist") and the broad
``try/except`` was swallowing the failure.  Side effects:

* permanently-failed creators were re-queued every bootstrap pass;
* ``archive_permanently_failed_creators`` never persisted a single timestamp.

Migration 063 adds the column.  This test suite pins the three invariants
that must stay true going forward so the column cannot be silently dropped
again:

1. ``queue_invalid_creators_for_retry`` emits ``.is_("archived_at", "null")``
   on BOTH its sub-queries (failed-and-stale AND never-synced branches).
2. ``archive_permanently_failed_creators`` includes ``archived_at`` in the
   UPDATE payload for every archived row.
3. ``worker/creator_worker.py`` keeps the ``.is_("archived_at", "null")``
   filter on its stale-check SELECT.

The suite deliberately uses a lightweight fluent spy for (1) and (2) so the
assertions pin actual call arguments rather than a brittle source-text
substring.  Invariant (3) is pinned via a source-text check because the
worker module imports ``googleapiclient``, which is not available in every
dev environment and would make the test module uncollectable.
"""

from __future__ import annotations

import pathlib
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest


# ---------------------------------------------------------------------------
# Fluent spy — captures .is_(), .update(), and .not_.is_() calls per table.
# ---------------------------------------------------------------------------


class _TableSpy:
    """Fluent query builder that records calls and returns a pre-seeded response."""

    def __init__(self, spy: dict, table_name: str, data: Any):
        self._spy = spy
        self._table = table_name
        self._data = data

    # Chain terminators we want to assert on explicitly.

    def is_(self, col: str, val: Any) -> "_TableSpy":
        self._spy.setdefault("is_calls", []).append((self._table, col, val))
        return self

    def update(self, payload: Dict[str, Any]) -> "_TableSpy":
        self._spy.setdefault("update_payloads", []).append((self._table, dict(payload)))
        return self

    def execute(self):
        self._spy.setdefault("executed", []).append(self._table)
        return SimpleNamespace(data=self._data)

    # ``.not_.is_(...)`` chain used by queue_invalid_creators_for_retry.

    @property
    def not_(self) -> "_NotProxy":
        return _NotProxy(self)

    # Every other filter method (.eq, .gt, .lt, .in_, .select, .limit, .gte,
    # .order, .offset, ...) is a fluent no-op that returns self so the chain
    # composes.  __getattr__ is only called when attribute lookup fails
    # normally, so recorded fields like ``_spy`` are not affected.

    def __getattr__(self, name: str):
        def _fluent(*_a, **_kw):
            return self

        return _fluent


class _NotProxy:
    """Thin proxy so ``spy.not_.is_(...)`` records as a ``not_is`` call."""

    def __init__(self, parent: _TableSpy):
        self._parent = parent

    def is_(self, col: str, val: Any) -> _TableSpy:
        self._parent._spy.setdefault("not_is_calls", []).append((self._parent._table, col, val))
        return self._parent


class _RoutingClient:
    """Supabase client stand-in that returns a per-table spy with seeded data."""

    def __init__(self, spy: dict, data_by_table: Optional[Dict[str, Any]] = None):
        self._spy = spy
        self._data = data_by_table or {}

    def table(self, name: str) -> _TableSpy:
        self._spy.setdefault("tables_touched", []).append(name)
        return _TableSpy(self._spy, name, self._data.get(name, []))


# ---------------------------------------------------------------------------
# Invariant 1: queue_invalid_creators_for_retry filters BOTH branches.
# ---------------------------------------------------------------------------


def test_queue_invalid_filters_on_archived_at_in_both_branches(monkeypatch):
    """Both sub-queries (failed-and-stale, never-synced) must exclude archived rows."""
    import db

    spy: dict = {}
    # Empty responses mean the function skips the bulk-queue step and returns 0,
    # which is fine: we only care that the SELECTs were composed correctly.
    monkeypatch.setattr(db, "supabase_client", _RoutingClient(spy))
    # queue_creator_sync_bulk also talks to Supabase; neutralise it so the
    # assertion focuses on the two SELECTs we actually care about.
    monkeypatch.setattr(db, "queue_creator_sync_bulk", lambda *_a, **_kw: (0, 0))

    db.queue_invalid_creators_for_retry(hours_since_last_sync=24, batch_size=50)

    is_calls = spy.get("is_calls", [])
    archived_filters = [c for c in is_calls if c[1] == "archived_at" and c[2] == "null"]
    assert len(archived_filters) == 2, (
        "queue_invalid_creators_for_retry MUST filter out archived rows in both "
        "the failed-and-stale branch and the never-synced branch, otherwise "
        "permanently-failed creators get re-queued every bootstrap pass.  "
        f"Expected 2 .is_(archived_at, null) calls, got {len(archived_filters)}.  "
        f"Full is_calls: {is_calls!r}"
    )
    # Both calls must target the creators table — a stray filter on the jobs
    # table would be a different bug entirely.
    tables = {c[0] for c in archived_filters}
    assert tables == {
        db.CREATOR_TABLE
    }, f"archived_at filter must target the creators table only, got {tables!r}"


# ---------------------------------------------------------------------------
# Invariant 2: archive_permanently_failed_creators writes archived_at.
# ---------------------------------------------------------------------------


def test_archive_permanently_failed_writes_archived_at(monkeypatch):
    """The UPDATE payload MUST include archived_at as the terminal state marker."""
    import db

    spy: dict = {}
    # Seed one failed job so the function proceeds past the "nothing to do"
    # guard and reaches the UPDATE step.
    fake_jobs: List[Dict[str, Any]] = [
        {"creator_id": "11111111-1111-1111-1111-111111111111", "retry_count": 5},
    ]
    # The UPDATE's execute() needs non-empty data so the function increments
    # archived_count — otherwise the archive is silently a no-op.
    fake_update_result: List[Dict[str, Any]] = [
        {"id": "11111111-1111-1111-1111-111111111111"},
    ]
    monkeypatch.setattr(
        db,
        "supabase_client",
        _RoutingClient(
            spy,
            {
                db.CREATOR_SYNC_JOBS_TABLE: fake_jobs,
                db.CREATOR_TABLE: fake_update_result,
            },
        ),
    )

    archived = db.archive_permanently_failed_creators(max_retries=3)

    assert archived == 1, (
        f"Expected exactly one archived row, got {archived}.  The spy pipeline "
        "probably didn't deliver the seeded fake_update_result through to the "
        "function's result.data check."
    )

    updates = spy.get("update_payloads", [])
    assert (
        len(updates) == 1
    ), f"Expected one UPDATE per archived creator, got {len(updates)}: {updates!r}"

    update_table, payload = updates[0]
    assert update_table == db.CREATOR_TABLE
    assert "archived_at" in payload, (
        "UPDATE payload MUST include archived_at as the terminal state "
        "marker — without it, the next bootstrap pass can't tell this row "
        "has already been archived and will re-queue it.  Payload keys: "
        f"{sorted(payload.keys())!r}"
    )
    # sync_status must remain within the DB CHECK allowlist.  "archived" is
    # NOT a permitted value — see db.py L1794-1803 context.
    assert payload.get("sync_status") == "failed", (
        "sync_status in the archive payload must stay 'failed' (DB CHECK "
        f"constraint) — got {payload.get('sync_status')!r}"
    )


# ---------------------------------------------------------------------------
# Invariant 3: worker/creator_worker.py keeps the archived_at exclusion.
# ---------------------------------------------------------------------------


def test_worker_source_filters_on_archived_at():
    """worker/creator_worker.py MUST keep the archived_at exclusion filter.

    Pinned via source-text inspection because the worker module imports
    googleapiclient, which is not available in every dev environment and
    would make the test module uncollectable if imported here.
    """
    worker_src = pathlib.Path("worker/creator_worker.py").read_text(encoding="utf-8")
    needle = '.is_("archived_at", "null")'
    assert needle in worker_src, (
        f"worker/creator_worker.py must contain {needle!r} on its stale-check "
        "SELECT to exclude archived creators from the requeue path.  Removing "
        "this filter re-opens the ghost-column failure mode even though the "
        "column now exists."
    )


# ---------------------------------------------------------------------------
# Boundary: the no-client guard must not raise.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fn_name",
    ["queue_invalid_creators_for_retry", "archive_permanently_failed_creators"],
)
def test_no_client_returns_zero(monkeypatch, fn_name):
    """Guard against the module-load path where supabase_client failed to init."""
    import db

    monkeypatch.setattr(db, "supabase_client", None)
    fn = getattr(db, fn_name)
    assert fn() == 0
