"""Regression test for the /creators list count strategy.

`get_creators()` used to pass `count="exact"` on every filtered listing, which
made PostgREST run `SELECT count(*) FROM creators` alongside the paged SELECT.
On the live 800k+ row table with broad ILIKE + `sync_status IN (...)` this
was the single most expensive line of code in the app: 12.6 s p50, 63k calls
per day per pg_stat_statements evidence.

This test pins the fix: unless the fast MV-count shortcut fires, the count
kwarg must be `"estimated"`.  PostgREST returns the planner's row estimate
when running the exact count would be expensive, and falls back to exact
counts when the plan is cheap — so pagination stays correct for narrow
filters while broad browses no longer stall.
"""

from types import SimpleNamespace

import pytest


class _SpyQuery:
    """Fluent no-op query builder that records the terminal execute() call."""

    def __init__(self, spy: dict):
        self._spy = spy

    def __getattr__(self, name):
        # Every filter method (.eq, .gt, .in_, .or_, .order, .limit, .offset,
        # .not_, etc.) returns self so the chain composes.  __getattr__ also
        # catches attribute chains like ``query.not_.is_(...)``.
        def _fluent(*_a, **_kw):
            return self

        # `.not_` is accessed as an attribute, then .is_() is called on it —
        # return self for both so the chain flows through.
        return _fluent if not name.startswith("_") else self

    def execute(self):
        # No count returned; get_creators wraps the empty result in
        # CreatorsResult([], 0) via its own error path if data is None-ish.
        return SimpleNamespace(data=[], count=0)


class _SpyTable:
    def __init__(self, spy: dict):
        self._spy = spy

    def select(self, cols, count=None):
        self._spy["cols"] = cols
        self._spy["count"] = count
        return _SpyQuery(self._spy)


class _SpyClient:
    def __init__(self, spy: dict):
        self._spy = spy

    def table(self, name):
        self._spy["table"] = name
        return _SpyTable(self._spy)


@pytest.fixture
def spy_supabase(monkeypatch):
    """Patch db.supabase_client with a fluent spy that records the count kwarg."""
    import db

    spy: dict = {}
    monkeypatch.setattr(db, "supabase_client", _SpyClient(spy))
    # Skip the MV count shortcut so the code path we care about is exercised.
    monkeypatch.setattr(db, "_get_category_count_from_mv", lambda _c: None)
    # Skip the ranked-search RPC path — it returns before the count kwarg is set.
    monkeypatch.setattr(db, "_get_ranked_creator_search", lambda **_kw: (False, None))
    # Neutralise the retry/backoff wrapper so we execute the lambda directly.
    monkeypatch.setattr(db, "_db_execute_readonly", lambda fn: fn())
    return spy


def test_default_browse_uses_estimated_count(spy_supabase):
    """Filtered browse must use `count="estimated"`, never `"exact"`."""
    from db import get_creators

    get_creators(return_count=True, limit=50)

    assert spy_supabase["table"] == "creators"
    assert spy_supabase["count"] == "estimated", (
        f"expected count='estimated' to avoid the ~12s exact-count PostgREST "
        f"round trip, got count={spy_supabase['count']!r}"
    )


def test_return_count_false_omits_count_kwarg(spy_supabase):
    """When the caller does not need a total, no count header should be set."""
    from db import get_creators

    get_creators(return_count=False, limit=50)

    assert spy_supabase["count"] is None


def test_count_is_never_exact_on_filtered_browse(spy_supabase):
    """Adding filters must not regress the count strategy back to `"exact"`."""
    from db import get_creators

    get_creators(
        return_count=True,
        grade_filter="A+",
        country_filter="US",
        language_filter="en",
        limit=50,
    )

    assert spy_supabase["count"] != "exact"
    assert spy_supabase["count"] == "estimated"
