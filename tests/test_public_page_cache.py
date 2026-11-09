from base64 import b64encode
import json

import pytest
from fasthtml.common import P

import main


@pytest.fixture
def public_page_stubs(monkeypatch):
    monkeypatch.setattr("components.navigation._is_admin", lambda _: False)
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
    "handler, arguments, canonical",
    [
        ("lists_country_detail", {"country_code": "US"}, "/lists/country/us"),
        ("lists_category_detail", {"category_slug": "music"}, "/lists/category/music"),
        ("lists_language_detail", {"language_code": "en"}, "/lists/language/en"),
        ("lists_categories_explorer", {}, "/lists/categories"),
        ("lists_countries_explorer", {}, "/lists/countries"),
        ("lists_languages_explorer", {}, "/lists/languages"),
        (
            "rankings_category_country",
            {"category_slug": "gaming", "country_slug": "india"},
            "/rankings/gaming/india",
        ),
    ],
)
@pytest.mark.parametrize("authenticated", [False, True])
def test_public_pages_cache_only_anonymous_complete_html(
    public_page_stubs, handler, arguments, canonical, authenticated
):
    from starlette.testclient import TestClient
    from starlette.middleware.sessions import SessionMiddleware

    client = TestClient(main.app)
    if authenticated:
        configuration = next(
            item for item in main.app.user_middleware if item.cls is SessionMiddleware
        )
        middleware = SessionMiddleware(main.app, **configuration.kwargs)
        session = {
            "auth": {"email": "cache-test@example.com"},
            "user_id": "14076ba5-f130-4f86-9b53-ecf3b93ed077",
            "user_name": "Cache Test User",
            "avatar_url": "https://example.com/cache-test-avatar.jpg",
        }
        client.cookies.set(
            middleware.session_cookie,
            middleware.signer.sign(b64encode(json.dumps(session).encode())).decode(),
        )
    request_path = "/lists/country/US" if handler == "lists_country_detail" else canonical
    response = client.get(request_path, follow_redirects=False)

    assert response.status_code == 200
    assert response.headers["cache-control"] == (
        "private, no-store" if authenticated else main._CC_STD
    )
    assert "cookie" in {value.strip().lower() for value in response.headers["vary"].split(",")}
    html = response.text
    assert html.lower().startswith("<!doctype html>")
    assert "Cached page content" in html
    assert 'name="viewport"' in html
    assert f'rel="canonical" href="https://www.viralvibes.fyi{canonical}"' in html
    assert 'src="/js/index.js"' in html
    assert "htmx.org" in html
    if authenticated:
        assert 'href="/me/dashboards"' in html
        assert "https://example.com/cache-test-avatar.jpg" in html
        assert "s-maxage" not in response.headers["cache-control"]
    else:
        assert 'href="/me/dashboards"' not in html
        assert "/login?return_url=" in html


def test_cache_helper_handles_no_session(public_page_stubs):
    from fasthtml.common import fast_app
    from starlette.testclient import TestClient

    app, route = fast_app()

    @route("/")
    def public_page():
        return main._public_cached_response(None, P("Public"), main._CC_STD)

    response = TestClient(app).get("/")

    assert response.headers["cache-control"] == main._CC_STD
    assert "cookie" in response.headers["vary"].lower()
    assert "htmx.org" in response.text


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
    assert 'href="/me/dashboards"' not in response.text
    assert "/login?return_url=" in response.text
    assert 'name="viewport"' in response.text


def test_authenticated_helper_preserves_head_and_body(public_page_stubs):
    from fasthtml.common import Meta, Titled, fast_app
    from starlette.testclient import TestClient

    app, route = fast_app()

    @route("/private")
    def private_page():
        return main._public_cached_response(
            {"auth": "trusted-session"},
            Titled("Private page", P("Private content"), Meta(name="example", content="preserved")),
            main._CC_STD,
        )

    response = TestClient(app).get("/private")

    assert response.headers["cache-control"] == "private, no-store"
    assert "Private content" in response.text
    assert "<title>Private page</title>" in response.text
    assert 'name="example" content="preserved"' in response.text
    assert "htmx.org" in response.text
