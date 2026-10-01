"""Regression tests for the anonymous facet gate on ``/creators``.

Background
----------
Facet filters (``grade`` / ``language`` / ``activity`` / ``age`` / ``country`` /
``category``) are authenticated-only.  Anonymous visitors who craft a filtered
URL get the filters silently stripped server-side so the slow 6-facet DB
path cannot be fired without a session.

This exists because of the Oct 2026 crawler incident: a bot enumerated every
``(grade \u00d7 language \u00d7 activity \u00d7 age \u00d7 country \u00d7 category)`` combination
on ``/creators``, which saturated the Supabase connection pool and triggered
cascading ReadTimeouts across every tenant request.

Contract pinned here
--------------------
1. Anonymous + facet params -> ``get_creators`` called with all facets
   defaulted to ``"all"`` (DB protection path).
2. Anonymous + facet params -> sign-up return URL preserves the attempted
   facets (so a post-login round trip lands on the view the user wanted).
3. Anonymous + facet params -> ``search`` is NOT stripped (deliberate
   product decision; search path has its own ranked RPC guardrails).
4. Authenticated + facet params -> ``get_creators`` called with the facets
   intact (regression fence against over-stripping).
5. Anonymous + default URL -> no gate noise, ``get_creators`` called with
   defaults, no return-URL contamination.
6. ``SignUpNudge``'s rendered ``href`` percent-encodes the whole nested
   ``return_url`` so inner ``?`` / ``&`` / ``+`` don't collide with the
   outer ``/login`` query string.
"""

from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest
from starlette.datastructures import QueryParams


# ---------------------------------------------------------------------------
# Fluent spy around get_creators + the request shim.
# ---------------------------------------------------------------------------


class _RequestStub:
    """Minimal stand-in for the Starlette ``Request`` passed into the route."""

    def __init__(self, query: Dict[str, str]):
        # Starlette's QueryParams supports .get(key, default) and dict() casts.
        self.query_params = QueryParams(query)
        # The route reads request.url.path for the country-extraction redirect
        # branch — not exercised here but needs to exist.
        self.url = MagicMock()
        self.url.path = "/creators"


def _install_route_patches(monkeypatch, captured: dict):
    """Patch the route's heavy collaborators.

    * ``get_creators`` records the kwargs it was called with and returns a
      ``CreatorsResult`` so the route path proceeds past the DB fan-out
      regardless of whether ``return_count`` is True or False.
    * Every other DB / RPC helper is neutralised to a cheap no-op so the
      ThreadPoolExecutor fan-out doesn't actually touch Supabase.
    * ``render_creators_page`` records the kwargs it was invoked with so the
      test can inspect ``anon_filters_stripped`` / ``anon_filter_return_url``.
    """
    import routes.creators as rc
    import db
    from db import CreatorsResult

    def _spy_get_creators(**kwargs):
        captured["get_creators_kwargs"] = dict(kwargs)
        # Return CreatorsResult unconditionally — the route handles both the
        # NamedTuple (return_count=True) and list (return_count=False) shapes,
        # but only the CreatorsResult path works for *both* branches because
        # the if-branch does ``creators_result.creators`` without an isinstance
        # check.
        return CreatorsResult(creators=[], total_count=0)

    def _spy_render(**kwargs):
        captured["render_kwargs"] = dict(kwargs)
        return "<html>stub</html>"

    monkeypatch.setattr(rc, "get_creators", _spy_get_creators)
    monkeypatch.setattr(rc, "render_creators_page", _spy_render)

    # Neutralise every other DB-side helper the route fans out to.
    monkeypatch.setattr(rc, "get_creator_hero_stats", lambda: {"total_creators": 0})
    monkeypatch.setattr(rc, "get_top_countries_with_counts", lambda limit=8: [])
    monkeypatch.setattr(rc, "get_top_languages_with_counts", lambda limit=5: [])
    monkeypatch.setattr(rc, "get_top_categories_with_counts", lambda limit=4: [])
    monkeypatch.setattr(rc, "get_user_favourite_creator_ids", lambda uid: set())
    monkeypatch.setattr(rc, "calculate_creator_stats", lambda creators: {})
    monkeypatch.setattr(rc, "find_creator_by_handle", lambda h: None)
    # Short-circuit the shadow-metric log so the test isn't noisy.
    monkeypatch.setattr(rc, "_log_search_intent_shadow", lambda *a, **k: None)
    # Keep the fan-out from touching a real Supabase client.
    monkeypatch.setattr(db, "supabase_client", None)


# ---------------------------------------------------------------------------
# Invariant 1 + 2 + 3: anonymous + facets -> stripped + preserved + search kept.
# ---------------------------------------------------------------------------


def test_anonymous_facet_filters_are_stripped_before_db_call(monkeypatch):
    """Anonymous visitors hitting a filtered URL must get every facet
    defaulted before get_creators() is called."""
    import routes.creators as rc

    captured: dict = {}
    _install_route_patches(monkeypatch, captured)

    request = _RequestStub(
        {
            "grade": "A+",
            "language": "es",
            "activity": "active",
            "age": "veteran",
            "country": "DE",
            "category": "Entertainment",
            "search": "mrbeast",  # search is NOT a facet; must stay intact
        }
    )

    rc.creators_route(request, is_authenticated=False, user_id=None)

    kwargs = captured.get("get_creators_kwargs", {})
    assert kwargs, "get_creators was never called \u2014 the spy chain broke"
    for facet in (
        "grade_filter",
        "language_filter",
        "activity_filter",
        "age_filter",
        "country_filter",
        "category_filter",
    ):
        assert kwargs[facet] == "all", (
            f"anonymous visitor's {facet!r} must be stripped to default before "
            f"the DB call so the slow 6-facet path can't be fired; got "
            f"{kwargs[facet]!r}"
        )
    # Search is intentionally NOT gated \u2014 the ranked search RPC has its own
    # trgm-indexed guardrails and we want /creators?search=X to stay open.
    assert (
        kwargs["search"] == "mrbeast"
    ), f"search must NOT be stripped by the anon gate; got {kwargs['search']!r}"


def test_anonymous_facet_strip_preserves_attempted_filters_in_return_url(monkeypatch):
    """When the server strips facets, the sign-up CTA must preserve the user's
    intent so a post-signup round-trip lands on the view they wanted."""
    import routes.creators as rc

    captured: dict = {}
    _install_route_patches(monkeypatch, captured)

    request = _RequestStub({"grade": "A+", "country": "US", "category": "Gaming"})

    rc.creators_route(request, is_authenticated=False, user_id=None)

    render_kwargs = captured.get("render_kwargs", {})
    assert render_kwargs.get("anon_filters_stripped") is True, (
        "the view must be told the strip fired so it can render the sign-up "
        "banner instead of a silent filter-removal"
    )
    return_url = render_kwargs.get("anon_filter_return_url")
    assert return_url and return_url.startswith("/creators?"), (
        f"return_url must round-trip back to /creators with query params; got " f"{return_url!r}"
    )
    # The user's attempted facet values must all be present in the return URL
    # so sign-in \u2192 land on filtered view works end-to-end.
    for facet_value in ("grade=A%2B", "country=US", "category=Gaming"):
        assert facet_value in return_url, (
            f"return_url must preserve {facet_value!r} so sign-up intent "
            f"survives the auth round-trip; got {return_url!r}"
        )


# ---------------------------------------------------------------------------
# Invariant 4: authenticated users keep their facets.
# ---------------------------------------------------------------------------


def test_authenticated_users_keep_their_facet_filters(monkeypatch):
    """Over-stripping on authenticated users would break the actual product;
    the gate must only fire for anonymous visitors."""
    import routes.creators as rc

    captured: dict = {}
    _install_route_patches(monkeypatch, captured)

    request = _RequestStub(
        {
            "grade": "A+",
            "country": "US",
            "category": "Gaming",
            "language": "en",
        }
    )

    rc.creators_route(
        request,
        is_authenticated=True,
        user_id="11111111-1111-1111-1111-111111111111",
    )

    kwargs = captured.get("get_creators_kwargs", {})
    assert kwargs["grade_filter"] == "A+"
    assert kwargs["country_filter"] == "US"
    assert kwargs["category_filter"] == "Gaming"
    assert kwargs["language_filter"] == "en"

    render_kwargs = captured.get("render_kwargs", {})
    assert render_kwargs.get("anon_filters_stripped") is False, (
        "the strip-flag must stay false for logged-in users so the sign-up "
        "banner never shadow-shows for them"
    )
    assert render_kwargs.get("anon_filter_return_url") is None


# ---------------------------------------------------------------------------
# Invariant 5: anonymous + no filters -> gate is a no-op.
# ---------------------------------------------------------------------------


def test_anonymous_default_browse_does_not_trigger_gate(monkeypatch):
    """The default anonymous browse (no filters, no search) must not look like
    an attempted filter: the sign-up banner must stay hidden."""
    import routes.creators as rc

    captured: dict = {}
    _install_route_patches(monkeypatch, captured)

    request = _RequestStub({})

    rc.creators_route(request, is_authenticated=False, user_id=None)

    kwargs = captured.get("get_creators_kwargs", {})
    for facet in (
        "grade_filter",
        "language_filter",
        "activity_filter",
        "age_filter",
        "country_filter",
        "category_filter",
    ):
        assert kwargs[facet] == "all"

    render_kwargs = captured.get("render_kwargs", {})
    assert render_kwargs.get("anon_filters_stripped") is False
    assert render_kwargs.get("anon_filter_return_url") is None


# ---------------------------------------------------------------------------
# Invariant 6: SignUpNudge's rendered href percent-encodes the return_url.
# ---------------------------------------------------------------------------


def test_signup_nudge_encodes_nested_return_url():
    """The whole ``return_url`` must be percent-encoded into the ``/login``
    href so inner ``?``, ``&``, and ``+`` don't collide with the outer
    query string.  Without this fix every facet past the first one is
    silently discarded during the auth round-trip and ``A%2B`` is decoded
    as ``A+`` / ``A `` by intermediate parsers."""
    from fasthtml.common import to_xml

    from components.buttons import SignUpNudge

    nudge = SignUpNudge(
        feature="filtered creator discovery",
        benefit="Narrow by country, language, grade, category and more.",
        return_url="/creators?grade=A%2B&country=US&category=Gaming",
    )
    html = to_xml(nudge)

    # Positive: the whole nested URL is encoded as one return_url value.
    # Expected encoded form: /creators%3Fgrade%3DA%252B%26country%3DUS%26category%3DGaming
    assert "return_url=%2Fcreators%3F" in html, (
        "SignUpNudge must percent-encode the nested return_url so its '?' "
        "becomes '%3F' in the /login href; raw interpolation lets the inner "
        "'?' collide with the outer query string.  Rendered href fragment: "
        f"{html[:400]!r}"
    )
    # The nested '&' separators must survive as '%26' so every facet past
    # the first one isn't silently lost.
    assert html.count("%26") >= 2, (
        "SignUpNudge must encode EACH nested '&' as '%26' so facets past "
        "the first survive the auth round-trip.  Rendered href fragment: "
        f"{html[:400]!r}"
    )
    # Negative: the raw separators MUST NOT appear inside the href value,
    # which would be the symptom of raw interpolation.  Guard against the
    # fragile fix of only escaping some separators but not others.
    _login_start = html.find("/login?")
    assert _login_start != -1, "SignUpNudge rendered no /login href"
    _href_end = html.find('"', _login_start)
    _login_href = html[_login_start:_href_end]
    # The ONLY '&' in a correctly-encoded href is the outer ampersand
    # between /login's own query params (there are none in SignUpNudge),
    # so finding any '&' in the href value proves nested '&' leaked.
    assert "&" not in _login_href, (
        "Raw '&' leaked into the /login href value, meaning the nested "
        f"return_url wasn't properly encoded.  Href: {_login_href!r}"
    )
    assert "?" not in _login_href[len("/login?") :], (
        "A nested '?' leaked into the /login href value, meaning the "
        f"return_url wasn't properly encoded.  Href: {_login_href!r}"
    )
