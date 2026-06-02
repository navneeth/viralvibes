from controllers.auth_routes import login_subheadline_for_return_url, safe_local_return_url
from components.navigation import _branded_login_href
from routes.pricing import _free_cta_target


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
