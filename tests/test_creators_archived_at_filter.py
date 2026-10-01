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
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest


# ---------------------------------------------------------------------------
# Fluent spy — captures .is_(), .update(), and .not_.is_() calls per table.
# ---------------------------------------------------------------------------


class _TableSpy:
    """Fluent query builder that records calls per-execute so each SELECT is
    inspected on its own merits.

    ``.is_()`` and ``.not_.is_()`` calls are collected on the per-instance
    builder as a query is composed.  On ``.execute()`` the recorded filter
    list is snapshotted into ``spy["executed_queries"]`` as one entry per
    query, so assertions can verify each query *individually* contains the
    archived_at exclusion instead of relying on a global call count — the
    latter is defeated by a refactor that puts both filters in one branch
    and omits them from the other.
    """

    def __init__(self, spy: dict, table_name: str, data: Any):
        self._spy = spy
        self._table = table_name
        self._data = data
        self._my_is_calls: List[tuple] = []
        self._my_not_is_calls: List[tuple] = []

    # Chain terminators we want to assert on explicitly.

    def is_(self, col: str, val: Any) -> "_TableSpy":
        self._my_is_calls.append((col, val))
        self._spy.setdefault("is_calls", []).append((self._table, col, val))
        return self

    def update(self, payload: Dict[str, Any]) -> "_TableSpy":
        self._spy.setdefault("update_payloads", []).append((self._table, dict(payload)))
        return self

    def execute(self):
        # Snapshot THIS query's filter list so the test can assert the
        # archived_at exclusion is present on each query independently.
        self._spy.setdefault("executed_queries", []).append(
            {
                "table": self._table,
                "is_calls": list(self._my_is_calls),
                "not_is_calls": list(self._my_not_is_calls),
            }
        )
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
        self._parent._my_not_is_calls.append((col, val))
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
    """BOTH sub-queries must filter out archived rows, verified per-query."""
    import db

    spy: dict = {}
    monkeypatch.setattr(db, "supabase_client", _RoutingClient(spy))
    monkeypatch.setattr(db, "queue_creator_sync_bulk", lambda *_a, **_kw: (0, 0))

    db.queue_invalid_creators_for_retry(hours_since_last_sync=24, batch_size=50)

    # queue_invalid_creators_for_retry fires exactly two SELECTs: the
    # failed-and-stale branch and the never-synced branch.  Checking the
    # aggregate .is_() count across the whole function is defeated by a
    # future refactor that duplicates the filter in one branch and drops it
    # from the other — pin the invariant per-execute() instead.
    executed_queries = spy.get("executed_queries", [])
    creators_queries = [q for q in executed_queries if q["table"] == db.CREATOR_TABLE]
    assert len(creators_queries) == 2, (
        "Expected exactly two SELECTs against the creators table (failed-and-"
        f"stale + never-synced), got {len(creators_queries)}.  All executed "
        f"queries: {executed_queries!r}"
    )

    for idx, query in enumerate(creators_queries, 1):
        assert ("archived_at", "null") in query["is_calls"], (
            f"SELECT #{idx} on creators MUST include .is_(archived_at, null) "
            "to exclude permanently-failed creators from the requeue; without "
            "it, this branch re-queues archived rows every bootstrap pass.  "
            f"This query's filters: is_calls={query['is_calls']!r}, "
            f"not_is_calls={query['not_is_calls']!r}"
        )


# ---------------------------------------------------------------------------
# Invariant 2: archive_permanently_failed_creators writes archived_at.
# ---------------------------------------------------------------------------


def test_archive_permanently_failed_writes_archived_at(monkeypatch):
    """The UPDATE payload MUST carry a valid ISO archived_at AND guard idempotency."""
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

    # The value must be a parseable ISO 8601 timestamp — a non-empty string
    # alone is not enough protection against a future refactor that writes
    # a placeholder sentinel like "pending" or an empty string.
    archived_at_value = payload["archived_at"]
    assert isinstance(archived_at_value, str) and archived_at_value, (
        f"archived_at must be a non-empty ISO timestamp string, got " f"{archived_at_value!r}"
    )
    try:
        parsed = datetime.fromisoformat(archived_at_value)
    except ValueError as exc:
        pytest.fail(f"archived_at={archived_at_value!r} does not parse as ISO 8601: {exc}")
    # Must be timezone-aware — a naive datetime would make the terminal
    # marker ambiguous across timezones and break ordering/comparison
    # downstream.
    assert parsed.tzinfo is not None, (
        f"archived_at must be timezone-aware (UTC), got naive datetime " f"{archived_at_value!r}"
    )

    # sync_status must remain within the DB CHECK allowlist.  "archived" is
    # NOT a permitted value — see db.py context near the archive call.
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
    would make the test module uncollectable if imported here.  Comment-only
    lines are skipped so a stray mention of the filter in a docstring or
    code comment cannot satisfy the invariant.
    """
    worker_src = pathlib.Path("worker/creator_worker.py").read_text(encoding="utf-8")
    needle = '.is_("archived_at", "null")'

    code_lines = [line for line in worker_src.splitlines() if not line.lstrip().startswith("#")]
    code_src = "\n".join(code_lines)

    assert needle in code_src, (
        f"worker/creator_worker.py must contain {needle!r} as executable code "
        "(not just a comment) on its stale-check SELECT to exclude archived "
        "creators from the requeue path.  Removing this filter re-opens the "
        "ghost-column failure mode even though the column now exists."
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
