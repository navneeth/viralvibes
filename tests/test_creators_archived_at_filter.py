"""Regression tests for archived creators and terminal sync failures.

Migration 063 adds ``creators.archived_at``. These tests keep requeue queries
from selecting archived creators, verify the Python wrapper calls the atomic
terminal-archive RPC and propagates failures, and protect the worker's stale
creator filter. The SQL integration test for locking, recovery, and enqueue
protection is in ``db/tests/terminal_creator_sync_archive.sql``.
"""

from __future__ import annotations

import pathlib
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

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
        # Default operation; .update()/.insert()/.delete() override before
        # the chain reaches .execute() so each snapshot records WHICH kind of
        # query the filter chain was composed for.  Without this, assertions
        # that “every UPDATE carries filter X” collapse into “at least one
        # query carries filter X” and a future refactor that adds a guarded
        # SELECT but drops the guard from an UPDATE passes falsely.
        self._my_operation: str = "select"

    # Chain terminators we want to assert on explicitly.

    def is_(self, col: str, val: Any) -> "_TableSpy":
        self._my_is_calls.append((col, val))
        self._spy.setdefault("is_calls", []).append((self._table, col, val))
        return self

    def update(self, payload: Dict[str, Any]) -> "_TableSpy":
        self._my_operation = "update"
        self._spy.setdefault("update_payloads", []).append((self._table, dict(payload)))
        return self

    def insert(self, payload: Any) -> "_TableSpy":
        self._my_operation = "insert"
        return self

    def delete(self) -> "_TableSpy":
        self._my_operation = "delete"
        return self

    def execute(self):
        # Snapshot THIS query's filter list AND operation so the test can
        # assert the archived_at exclusion is present on each UPDATE query
        # independently — not just “somewhere in the function”.
        self._spy.setdefault("executed_queries", []).append(
            {
                "table": self._table,
                "operation": self._my_operation,
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

    def __init__(
        self,
        spy: dict,
        data_by_table: Optional[Dict[str, Any]] = None,
        responses_by_table: Optional[Dict[str, List[Any]]] = None,
    ):
        self._spy = spy
        self._data = data_by_table or {}
        self._responses = responses_by_table or {}

    def table(self, name: str) -> _TableSpy:
        self._spy.setdefault("tables_touched", []).append(name)
        responses = self._responses.get(name)
        data = responses.pop(0) if responses else self._data.get(name, [])
        return _TableSpy(self._spy, name, data)


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
# Invariant 2: terminal archival uses the atomic database RPC.
# ---------------------------------------------------------------------------


def test_archive_permanently_failed_calls_atomic_rpc(monkeypatch):
    import db

    client = MagicMock()
    client.rpc.return_value.execute.return_value = SimpleNamespace(data=7)
    monkeypatch.setattr(db, "supabase_client", client)

    assert db.archive_permanently_failed_creators(max_retries=4, limit=25) == 7
    client.rpc.assert_called_once_with(
        "archive_terminal_creator_syncs",
        {"p_max_retries": 4, "p_limit": 25},
    )


def test_archive_permanently_failed_propagates_rpc_errors(monkeypatch):
    import db

    client = MagicMock()
    client.rpc.return_value.execute.side_effect = RuntimeError("database unavailable")
    monkeypatch.setattr(db, "supabase_client", client)

    with pytest.raises(RuntimeError, match="database unavailable"):
        db.archive_permanently_failed_creators()


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


def test_queue_invalid_no_client_returns_zero(monkeypatch):
    """Guard against the module-load path where supabase_client failed to init."""
    import db

    monkeypatch.setattr(db, "supabase_client", None)
    assert db.queue_invalid_creators_for_retry() == 0


def test_terminal_archive_no_client_fails_closed(monkeypatch):
    import db

    monkeypatch.setattr(db, "supabase_client", None)
    with pytest.raises(RuntimeError, match="not available"):
        db.archive_permanently_failed_creators()
