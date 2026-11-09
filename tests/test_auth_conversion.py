from controllers.auth_routes import login_subheadline_for_return_url, safe_local_return_url
from components.navigation import _branded_login_href
from routes.pricing import _free_cta_target
import pytest


@pytest.fixture
def oauth_public_page_client():
    from fasthtml.common import P, fast_app
    from fasthtml.oauth import GoogleAppClient
    from starlette.testclient import TestClient
    from auth.auth_service import AUTH_SKIP_ROUTE_PATTERNS, ViralVibesAuth

    app, route = fast_app()
    app.router.routes.clear()
    ViralVibesAuth(
        app,
        GoogleAppClient("test-client", "test-secret"),
        skip=AUTH_SKIP_ROUTE_PATTERNS,
    )

    def content():
        return P("Handler reached")

    for path in (
        "/blog",
        "/blog/social-blade-alternative",
        "/blog/",
        "/blog/article/",
        "/rss.xml",
        "/creators/top",
        "/creators/top/music",
        "/creators/top/",
        "/rankings/gaming/india",
        "/rankings/gaming/india/more",
        "/robots.txt",
        "/sitemap.xml",
        "/creators",
        "/lists/country/us",
        "/admin",
        "/admin/users",
        "/me/favourites/export",
        "/me/dashboards",
        "/creators/request",
        "/creators/top/music/export",
        "/blog/article/edit",
        "/rankings/gaming/india/export",
        "/creators/@setindia",
        "/creators/@setindia/blueprint",
        "/billing/private-action",
    ):
        route(path)(content)

    return TestClient(app)


@pytest.mark.parametrize("user_agent", ["Mozilla/5.0", "Googlebot", "bingbot"])
@pytest.mark.parametrize(
    "path",
    [
        "/blog",
        "/blog/social-blade-alternative",
        "/blog/",
        "/blog/article/",
        "/blog?tag=research",
        "/rss.xml",
        "/creators/top",
        "/creators/top/music",
        "/creators/top/",
        "/rankings/gaming/india",
        "/rankings/gaming/india/more",
        "/robots.txt",
        "/sitemap.xml",
        "/creators",
        "/lists/country/us",
    ],
)
def test_approved_public_pages_bypass_real_oauth(oauth_public_page_client, path, user_agent):
    response = oauth_public_page_client.get(
        path,
        headers={"User-Agent": user_agent},
        follow_redirects=False,
    )

    assert response.status_code == 200
    assert "Handler reached" in response.text


@pytest.mark.parametrize(
    "path",
    [
        "/admin",
        "/admin/users",
        "/me/favourites/export",
        "/me/dashboards",
        "/creators/request",
        "/creators/top/music/export",
        "/blog/article/edit",
        "/rankings/gaming/india/export",
        "/billing/private-action",
        "/creators/@setindia",
        "/creators/@setindia/blueprint",
    ],
)
def test_oauth_protected_routes_and_handle_policy_remain_unchanged(oauth_public_page_client, path):
    response = oauth_public_page_client.get(path, follow_redirects=False)

    assert response.status_code in (302, 303, 307)
    assert response.headers["location"] == "/login"
    assert "Handler reached" not in response.text


def test_safe_local_return_url_accepts_same_site_paths():
    assert safe_local_return_url("/creators?sort=subscribers") == "/creators?sort=subscribers"


def test_safe_local_return_url_rejects_open_redirects():
    assert safe_local_return_url("https://evil.example/path") == "/"
    assert safe_local_return_url("//evil.example/path") == "/"
    assert safe_local_return_url("creators") == "/"
    assert safe_local_return_url("/login") == "/"


def test_login_subheadline_for_free_pricing_path():
    assert login_subheadline_for_return_url("/creators") == (
        "Sign in to start free. No credit card required."
    )


def test_free_pricing_cta_preserves_creator_intent():
    href, label = _free_cta_target(is_authenticated=False)
    assert href == "/login?return_url=/creators"
    assert label == "Start free with Google"


def test_nav_login_uses_branded_login_with_current_path():
    class URL:
        path = "/pricing"
        query = "utm_source=test"

    class Req:
        url = URL()

    assert _branded_login_href(Req()) == "/login?return_url=%2Fpricing%3Futm_source%3Dtest"
