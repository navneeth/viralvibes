import pytest

from components.navigation import _branded_login_href, _branded_signup_href
from controllers.auth_routes import login_subheadline_for_return_url, safe_local_return_url
from routes.pricing import _free_cta_target


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
        "/signup",
        "/validate/full",
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
        "/signup",
        "/validate/full",
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
    assert safe_local_return_url("/signup") == "/"


def test_login_subheadline_for_free_pricing_path():
    assert login_subheadline_for_return_url("/creators") == (
        "Sign in to continue to creator discovery. New here? Create a free account."
    )


def test_free_pricing_cta_preserves_creator_intent():
    href, label = _free_cta_target(is_authenticated=False)
    assert href == "/signup?return_url=/creators"
    assert label == "Create free account"


def test_free_plan_discloses_playlist_analysis_account_requirement():
    from fasthtml.common import to_xml

    from routes.pricing import _free_card

    html = to_xml(_free_card(is_authenticated=False))

    assert "Full playlist analysis (free account required)" in html
    assert "Create free account" in html
    assert "/signup?return_url=/creators" in html


def test_nav_login_uses_branded_login_with_current_path():
    class URL:
        path = "/pricing"
        query = "utm_source=test"

    class Req:
        url = URL()

    assert _branded_login_href(Req()) == "/login?return_url=%2Fpricing%3Futm_source%3Dtest"


def test_nav_signup_uses_branded_signup_with_current_path():
    class URL:
        path = "/pricing"
        query = "utm_source=test"

    class Req:
        url = URL()

    assert _branded_signup_href(Req()) == "/signup?return_url=%2Fpricing%3Futm_source%3Dtest"


def test_nav_auth_links_preserve_intended_return_url():
    class URL:
        path = "/signup"
        query = "return_url=%2Fcreators%3Fcountry%3DUS"

    class Req:
        url = URL()

    assert _branded_login_href(Req()) == ("/login?return_url=%2Fcreators%3Fcountry%3DUS")


def test_signup_entry_point_explains_creation_and_preserves_return_url():
    from starlette.testclient import TestClient

    import main

    response = TestClient(main.app).get("/signup?return_url=%2Fcreators")

    assert response.status_code == 200
    assert "Create your free account" in response.text
    assert "Continue with Google to create your account" in response.text
    assert "No credit card required" in response.text
    assert "Already have an account? Sign in" in response.text
    assert "state=%2Fcreators" in response.text


def test_auth_modal_uses_signup_entry_point():
    from fasthtml.common import to_xml

    from components.modals import AuthModal

    html = to_xml(AuthModal(return_url="/creators"))

    assert 'href="/signup?return_url=%2Fcreators"' in html


def test_legacy_signup_page_reuses_branded_auth_assets():
    from fasthtml.common import to_xml

    from controllers.auth_routes import build_login_page

    class OAuth:
        def login_link(self, req):
            return "/oauth/google"

    html = to_xml(
        build_login_page(
            OAuth(),
            req=None,
            return_url="/analysis",
            signup=True,
        )
    )

    assert "/static/ViralVibes_Logo.png" in html
    assert html.count("<path") == 4
    assert all(color in html for color in ("#4285F4", "#34A853", "#FBBC05", "#EA4335"))
    assert 'data-test="google-signin-btn"' in html
    assert "Privacy Policy" in html
    assert "state=%2Fanalysis" in html
