from controllers.auth_routes import login_subheadline_for_return_url, safe_local_return_url
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
    assert label == "Get started free"
