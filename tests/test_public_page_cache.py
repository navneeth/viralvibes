from types import SimpleNamespace

import pytest
from fasthtml.common import Div, P

import main


@pytest.fixture
def public_page_stubs(monkeypatch):
    monkeypatch.setattr(
        main,
        "NavComponent",
        lambda oauth, req, sess: Div(
            "Signed-in navigation" if sess and sess.get("auth") else "Anonymous navigation",
        ),
    )
    for name in ("country_detail_route", "category_detail_route"):
        monkeypatch.setattr(main, name, lambda *args: (P("Cached page content"), 100))
    for name in (
        "categories_explorer_route",
        "countries_explorer_route",
        "languages_explorer_route",
        "ranking_detail_route",
        "language_detail_route",
    ):
        monkeypatch.setattr(main, name, lambda *args: P("Cached page content"))


@pytest.mark.parametrize(
    "handler, arguments",
    [
        ("lists_country_detail", {"country_code": "US"}),
        ("lists_category_detail", {"category_slug": "music"}),
        ("lists_language_detail", {"language_code": "en"}),
        ("lists_categories_explorer", {}),
        ("lists_countries_explorer", {}),
        ("lists_languages_explorer", {}),
        ("rankings_category_country", {"category_slug": "gaming", "country_slug": "india"}),
    ],
)
@pytest.mark.parametrize("authenticated", [False, True])
def test_public_pages_cache_only_anonymous_complete_html(
    public_page_stubs, handler, arguments, authenticated
):
    session = {"auth": "trusted-session"} if authenticated else {}
    response = getattr(main, handler)(SimpleNamespace(query_params={}), session, **arguments)

    assert response.status_code == 200
    assert response.headers["cache-control"] == (
        "private, no-store" if authenticated else main._CC_STD
    )
    assert response.headers["vary"] == "Cookie"
    html = response.body.decode()
    assert html.startswith("<!DOCTYPE html>")
    assert "Cached page content" in html
    assert 'name="viewport"' in html
    assert 'rel="canonical"' in html
    assert 'src="/js/index.js"' in html
    assert ("Signed-in navigation" if authenticated else "Anonymous navigation") in html
    if authenticated:
        assert "s-maxage" not in response.headers["cache-control"]


def test_cache_helper_handles_no_session(public_page_stubs):
    response = main._public_cached_response(None, P("Public"), main._CC_STD)

    assert response.headers["cache-control"] == main._CC_STD
    assert response.headers["vary"] == "Cookie"


@pytest.mark.parametrize(
    "path",
    [
        "/lists/country/US",
        "/lists/category/music",
        "/lists/language/en",
        "/lists/categories",
        "/lists/countries",
        "/lists/languages",
        "/rankings/gaming/india",
        "/lists/country/US?page=2",
    ],
)
def test_anonymous_application_routes_emit_public_cache_headers(public_page_stubs, path):
    from starlette.testclient import TestClient

    response = TestClient(main.app).get(path, follow_redirects=False)

    assert response.status_code == 200
    assert response.headers["cache-control"] == main._CC_STD
    assert "cookie" in response.headers["vary"].lower()
    assert "Anonymous navigation" in response.text
    assert 'name="viewport"' in response.text


def test_authenticated_helper_preserves_head_and_body(public_page_stubs):
    from fasthtml.common import Meta, Titled

    response = main._public_cached_response(
        {"auth": "trusted-session"},
        Titled("Private page", P("Private content"), Meta(name="example", content="preserved")),
        main._CC_STD,
    )

    assert response.headers["cache-control"] == "private, no-store"
    assert "Private content" in response.body.decode()
    assert "<title>Private page</title>" in response.body.decode()
    assert 'name="example" content="preserved"' in response.body.decode()
