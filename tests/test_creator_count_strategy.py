"""Regression test for the /creators list count strategy.

Background
----------
pg_stat_statements shows the PostgREST /creators list is our worst offender:
63,114 calls x 12.6 s p50.  The exact-count query PostgREST bundles alongside
the paged SELECT is >99 % of that wall clock on broad filtered browses.

An earlier attempt swapped the default to ``count="estimated"``.  Review found
this silently regressed pagination correctness in routes/creators.py, which
computes ``total_pages`` from ``total_count`` and issues a redirect when
``page > total_pages``.  An approximate count would send users to phantom
pages or hide real ones.

Final contract pinned here
--------------------------
1. ``get_creators`` defaults to ``count="exact"`` when ``return_count`` is True
   (safe for redirect-based pagination).
2. Callers may opt into ``count_strategy="estimated"`` (or ``"planned"``) for
   endpoints that do not redirect on out-of-range pages.
3. ``return_count=False`` never emits a count header (unchanged).
4. Unknown strategy values fall back to ``"exact"`` (fail-safe).
"""

from types import SimpleNamespace

import pytest


class _SpyQuery:
    """Fluent no-op query builder that records the terminal execute() call."""

    def __init__(self, spy: dict):
        self._spy = spy

    def __getattr__(self, name):
        # ``get_creators`` chains ``query.not_.is_("channel_name", "null")``,
        # so the ``not_`` attribute must yield the query itself so the
        # subsequent ``.is_(...)`` lookup lands on the fluent builder.  Every
        # other public method (.eq, .gt, .in_, .or_, .order, .limit, .offset,
        # .lt, .gte, ...) is a fluent no-op that returns self.  Private names
        # fall back to returning self so the instance keeps working as a plain
        # object internally.
        if name == "not_":
            return self
        if name.startswith("_"):
            return self

        def _fluent(*_a, **_kw):
            return self

        return _fluent

    def execute(self):
        self._spy["executed"] = True
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
    """Filtered browse defaults to ``count="estimated"`` for the perf win.

    The /creators route no longer redirects based on count-arithmetic, so an
    approximate total is safe there.  Callers that need an exact count must
    opt in with ``count_strategy="exact"``.
    """
    from db import get_creators

    get_creators(return_count=True, limit=50)

    assert spy_supabase["table"] == "creators"
    assert spy_supabase.get("executed"), (
        "spy_query.execute() was never called — the code path exited early "
        "and the count assertion below would be meaningless"
    )
    assert spy_supabase["count"] == "estimated", (
        "default must be 'estimated' — exact COUNT(*) on the 800k-row "
        f"creators table takes ~12 s p50 per pg_stat_statements.  "
        f"Got count={spy_supabase['count']!r}"
    )


def test_return_count_false_omits_count_kwarg(spy_supabase):
    """When the caller does not need a total, no count header should be set."""
    from db import get_creators

    get_creators(return_count=False, limit=50)

    assert spy_supabase.get("executed")
    assert spy_supabase["count"] is None


def test_count_strategy_estimated_propagates(spy_supabase):
    """Opt-in ``count_strategy="estimated"`` must reach PostgREST unchanged."""
    from db import get_creators

    get_creators(return_count=True, count_strategy="estimated", limit=50)

    assert spy_supabase.get("executed")
    assert spy_supabase["count"] == "estimated"


def test_count_strategy_exact_propagates(spy_supabase):
    """Callers that opt out to ``count_strategy="exact"`` must reach PostgREST unchanged."""
    from db import get_creators

    get_creators(return_count=True, count_strategy="exact", limit=50)

    assert spy_supabase.get("executed")
    assert spy_supabase["count"] == "exact"


def test_count_strategy_planned_propagates(spy_supabase):
    """``count_strategy="planned"`` is a valid PostgREST mode and must pass through."""
    from db import get_creators

    get_creators(return_count=True, count_strategy="planned", limit=50)

    assert spy_supabase.get("executed")
    assert spy_supabase["count"] == "planned"


def test_unknown_count_strategy_falls_back_to_exact(spy_supabase):
    """An unrecognised strategy must not be forwarded to PostgREST verbatim."""
    from db import get_creators

    get_creators(return_count=True, count_strategy="wharrgarbl", limit=50)

    assert spy_supabase.get("executed")
    assert spy_supabase["count"] == "estimated"


def test_added_filters_do_not_regress_default(spy_supabase):
    """Adding filters must keep the default strategy at "estimated"."""
    from db import get_creators

    get_creators(
        return_count=True,
        grade_filter="A+",
        country_filter="US",
        language_filter="en",
        limit=50,
    )

    assert spy_supabase.get("executed")
    assert spy_supabase["count"] == "estimated"
