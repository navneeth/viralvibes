"""Regression tests for the batched refresh_category_stats_cache() path.

Background
----------
pg_stat_statements showed 274 x 8.1 s calls per bootstrap Pass 4 to the
per-category ``get_category_box_stats(p_category)`` RPC — an N+1 pattern
whose 5.7 M shared-blocks-per-call cost was flat-lining the DB during
worker refreshes.

Migration 061 introduces ``get_all_category_box_stats()`` which computes
every synced primary_category in one GROUP BY heap pass. Migration 062
returns the complete result as one scalar JSON aggregate, avoiding the
PostgREST table-row cap. ``db.refresh_category_stats_cache`` calls that RPC
exactly once per pass and bulk-upserts the result.

Final contract pinned here
--------------------------
1. Exactly ONE ``.rpc()`` call, and its name is ``get_all_category_box_stats``.
2. The deprecated ``get_distinct_synced_categories`` RPC is NOT called
   (regression fence against the N+1 loop coming back).
3. The upsert payload rows match the cache-table schema
   (``category`` / ``stats_json`` / ``creator_count`` / ``refreshed_at``).
4. An empty RPC response returns 0 without raising, matching the
   pre-existing "no synced categories found" behaviour.
5. A malformed row is skipped, not fatal — a single bad row must not
   abort the whole refresh pass.
"""

from types import SimpleNamespace
from typing import Any

import pytest


class _SpyRPC:
    """Fluent no-op RPC handle that records the terminal execute() call.

    ``supabase.rpc("name", params).execute()`` returns a response with
    ``.data``; our spy makes that shape configurable per test.
    """

    def __init__(self, spy: dict, data: Any):
        self._spy = spy
        self._data = data

    def execute(self):
        self._spy["executed"] = True
        return SimpleNamespace(data=self._data)


class _SpyTable:
    """Fluent no-op table handle that records the upsert payload."""

    def __init__(self, spy: dict):
        self._spy = spy

    def upsert(self, rows, on_conflict=None):
        self._spy["upsert_rows"] = list(rows)
        self._spy["upsert_on_conflict"] = on_conflict
        return self

    def execute(self):
        self._spy["upsert_executed"] = True
        return SimpleNamespace(data=None)


class _SpyClient:
    """Fluent no-op Supabase client — records every rpc()/table() call."""

    def __init__(self, spy: dict, rpc_data: Any):
        self._spy = spy
        self._rpc_data = rpc_data

    def rpc(self, name, params=None):
        self._spy.setdefault("rpc_calls", []).append((name, params))
        return _SpyRPC(self._spy, self._rpc_data)

    def table(self, name):
        self._spy["table"] = name
        return _SpyTable(self._spy)


def _install_client(monkeypatch, spy: dict, rpc_data: Any):
    import db

    monkeypatch.setattr(db, "supabase_client", _SpyClient(spy, rpc_data))


# ---------------------------------------------------------------------------
# The core regression fence: one RPC, not N.
# ---------------------------------------------------------------------------


def _stats_row(category: str, count: int) -> dict:
    """Return one RPC row shaped like ``get_all_category_box_stats``'s output."""
    return {
        "category": category,
        "stats_json": {
            "count": count,
            "subscribers": {"min": 1, "p25": 2, "median": 3, "p75": 4, "max": 5},
            "views": {"min": 10, "p25": 20, "median": 30, "p75": 40, "max": 50},
            "engagement": {"min": 0.1, "p25": 0.2, "median": 0.3, "p75": 0.4, "max": 0.5},
            "monthly_uploads": {"min": 0, "p25": 1, "median": 2, "p75": 3, "max": 4},
        },
    }


def _aggregate_payload(*rows: dict, duration_ms: int = 12) -> dict:
    return {
        "categories": {row["category"]: row["stats_json"] for row in rows},
        "duration_ms": duration_ms,
    }


def test_refresh_uses_single_batched_rpc(monkeypatch):
    """Exactly one RPC per pass, and it must be the batched one."""
    from db import refresh_category_stats_cache

    spy: dict = {}
    _install_client(
        monkeypatch,
        spy,
        _aggregate_payload(_stats_row("Gaming", 42), _stats_row("Music", 7), _stats_row("Tech", 3)),
    )

    result = refresh_category_stats_cache()

    assert result == 3
    assert spy.get("executed"), "The batched RPC was never executed — did the code path exit early?"
    # This is THE regression fence against the N+1 pattern coming back.
    assert len(spy["rpc_calls"]) == 1, (
        f"expected exactly one RPC call per pass, got {len(spy['rpc_calls'])}: "
        f"{spy['rpc_calls']!r}"
    )
    assert spy["rpc_calls"][0][0] == "get_all_category_box_stats"


def test_refresh_does_not_call_deprecated_distinct_rpc(monkeypatch):
    """The N+1 pattern's DISTINCT-categories helper must no longer be called."""
    from db import refresh_category_stats_cache

    spy: dict = {}
    _install_client(monkeypatch, spy, _aggregate_payload(_stats_row("Gaming", 42)))

    refresh_category_stats_cache()

    called_names = [name for name, _ in spy.get("rpc_calls", [])]
    assert (
        "get_distinct_synced_categories" not in called_names
    ), f"the deprecated per-category N+1 loop appears to be back: called {called_names!r}"
    assert (
        "get_category_box_stats" not in called_names
    ), f"the per-category RPC must NOT be called from the refresh path: {called_names!r}"


# ---------------------------------------------------------------------------
# Upsert payload schema — regression fence against cache-table drift.
# ---------------------------------------------------------------------------


def test_upsert_payload_matches_cache_schema(monkeypatch):
    from db import refresh_category_stats_cache

    spy: dict = {}
    _install_client(
        monkeypatch,
        spy,
        _aggregate_payload(_stats_row("Gaming", 42), _stats_row("Music", 7)),
    )

    refresh_category_stats_cache()

    assert spy.get("upsert_executed"), "the bulk upsert was never executed"
    assert spy["table"] == "category_stats_cache"
    assert spy["upsert_on_conflict"] == "category"

    for row in spy["upsert_rows"]:
        assert set(row.keys()) == {"category", "stats_json", "creator_count", "refreshed_at"}
        assert isinstance(row["stats_json"], dict)
        assert row["creator_count"] == row["stats_json"]["count"], (
            "creator_count must be derived from stats_json['count'] so the cache "
            "column matches the payload"
        )

    categories = [row["category"] for row in spy["upsert_rows"]]
    assert set(categories) == {"Gaming", "Music"}, (
        "the aggregate includes each requested category; JSON object order is "
        f"not part of the RPC contract: got {categories!r}"
    )


# ---------------------------------------------------------------------------
# Boundary conditions.
# ---------------------------------------------------------------------------


def test_empty_rpc_response_returns_zero_without_raising(monkeypatch):
    """No synced categories -> log a warning, return 0, do not upsert."""
    from db import refresh_category_stats_cache

    spy: dict = {}
    _install_client(monkeypatch, spy, _aggregate_payload())

    result = refresh_category_stats_cache()

    assert result == 0
    assert "upsert_rows" not in spy, (
        "no rows to upsert should mean no upsert call — got " f"{spy.get('upsert_rows')!r}"
    )


@pytest.mark.parametrize(
    "bad_row",
    [
        None,  # non-mapping row
        42,  # scalar row
        {"category": None, "stats_json": {"count": 1}},  # missing category
        {"category": "Music", "stats_json": None},  # missing stats
        {"category": "Music", "stats_json": "not-a-dict"},  # wrong type
        {"category": "Music"},  # missing stats_json key
    ],
)
def test_malformed_row_is_skipped_not_fatal(monkeypatch, bad_row):
    """A single bad row must not abort the whole refresh pass."""
    from db import refresh_category_stats_cache

    spy: dict = {}
    _install_client(
        monkeypatch,
        spy,
        [bad_row, _stats_row("Gaming", 42)],
    )

    result = refresh_category_stats_cache()

    assert result == 1, (
        "the good row must still be upserted even though a sibling row was "
        f"malformed; got result={result}"
    )
    categories = [row["category"] for row in spy["upsert_rows"]]
    assert categories == ["Gaming"]


def test_supabase_client_none_returns_zero(monkeypatch):
    """Guard against the module-load path where supabase_client failed to init."""
    import db

    monkeypatch.setattr(db, "supabase_client", None)

    assert db.refresh_category_stats_cache() == 0


def test_refresh_routes_rpc_through_db_execute(monkeypatch):
    import db

    spy: dict = {}
    _install_client(monkeypatch, spy, _aggregate_payload(_stats_row("Gaming", 42)))
    calls = []

    def execute_with_recording(fn):
        calls.append(True)
        return fn()

    monkeypatch.setattr(db, "_db_execute", execute_with_recording)

    assert db.refresh_category_stats_cache() == 1
    assert calls == [True]


def test_rpc_exception_logs_and_returns_zero(monkeypatch):
    """Unexpected RPC errors must not propagate to the bootstrap driver."""
    import db

    class _RaisingClient:
        def rpc(self, name, params=None):
            class _R:
                def execute(self_inner):
                    raise RuntimeError("simulated RPC blowup")

            return _R()

    monkeypatch.setattr(db, "supabase_client", _RaisingClient())

    assert db.refresh_category_stats_cache() == 0
