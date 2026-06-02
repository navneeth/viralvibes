import pytest
from starlette.testclient import TestClient

import main
from routes import stripe_checkout
from services import stripe_service


def test_ensure_stripe_api_key_reads_current_env(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_checkout")

    assert stripe_service.ensure_stripe_api_key() is True
    assert stripe_service.stripe.api_key == "sk_test_checkout"


@pytest.mark.asyncio
async def test_checkout_missing_stripe_secret_redirects_to_config_error(monkeypatch):
    called = False

    def fake_create(**kwargs):
        nonlocal called
        called = True
        raise AssertionError("Stripe should not be called without STRIPE_SECRET_KEY")

    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    monkeypatch.setattr(stripe_checkout, "get_price_for_plan", lambda plan, interval: "price_123")
    monkeypatch.setattr(stripe_checkout.stripe.checkout.Session, "create", fake_create)

    response = await stripe_checkout._do_checkout(
        {"user_id": "user_123", "user_email": "buyer@example.com"},
        "pro",
        "year",
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/pricing?error=config"
    assert called is False


def test_pricing_page_does_not_render_browser_title_as_visible_heading():
    client = TestClient(main.app)
    response = client.get("/pricing")

    assert response.status_code == 200
    assert "<title>Pricing - ViralVibes</title>" in response.text
    assert "<h1>Pricing - ViralVibes</h1>" not in response.text
