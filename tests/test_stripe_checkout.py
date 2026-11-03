import re
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient
from fasthtml.common import to_xml

import main
from db import get_latest_canceled_subscription
from routes import stripe_checkout
from services import stripe_service
from views.my_dashboards import render_my_dashboards_page


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


@pytest.mark.asyncio
async def test_checkout_rejects_product_id_as_price_before_calling_stripe(monkeypatch):
    called = False

    def fake_create(**kwargs):
        nonlocal called
        called = True
        raise AssertionError("Stripe should not receive a Product ID as a price")

    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_checkout")
    monkeypatch.setattr(
        stripe_checkout, "get_price_for_plan", lambda plan, interval: "prod_UNnVi76CIBJRPN"
    )
    monkeypatch.setattr(stripe_checkout.stripe.checkout.Session, "create", fake_create)

    response = await stripe_checkout._do_checkout(
        {"user_id": "user_123", "user_email": "buyer@example.com"},
        "pro",
        "year",
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/pricing?error=config"
    assert called is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("plan", "interval", "price_env"),
    [
        ("pro", "month", "STRIPE_PRICE_PRO_MONTHLY"),
        ("pro", "year", "STRIPE_PRICE_PRO_ANNUAL"),
        ("agency", "month", "STRIPE_PRICE_AGENCY_MONTHLY"),
        ("agency", "year", "STRIPE_PRICE_AGENCY_ANNUAL"),
    ],
)
async def test_checkout_creates_trial_session_for_each_paid_plan(
    monkeypatch, plan, interval, price_env
):
    """Every configured plan interval reaches Stripe with its price and 7-day trial."""
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_checkout")
    monkeypatch.setenv(price_env, "price_test_selected")
    mock_session = MagicMock()
    mock_session.url = "https://checkout.stripe.com/pay/session_123"
    create_session = MagicMock(return_value=mock_session)
    monkeypatch.setattr(stripe_checkout.stripe.checkout.Session, "create", create_session)

    response = await stripe_checkout._do_checkout(
        {"user_id": "user_123", "user_email": "buyer@example.com"},
        plan,
        interval,
    )

    assert response.loc == "https://checkout.stripe.com/pay/session_123"
    create_session.assert_called_once_with(
        mode="subscription",
        line_items=[{"price": "price_test_selected", "quantity": 1}],
        subscription_data={"trial_period_days": 7},
        customer_email="buyer@example.com",
        metadata={"user_id": "user_123"},
        success_url=f"{stripe_service.DOMAIN_URL}/billing/success?session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{stripe_service.DOMAIN_URL}/pricing",
    )
    assert stripe_service.get_plan_for_price("price_test_selected") == (plan, interval)


def test_billing_portal_missing_stripe_secret_redirects_to_error(monkeypatch):
    """Test billing portal redirects to error when STRIPE_SECRET_KEY is missing."""
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    monkeypatch.setattr(stripe_checkout, "get_stripe_customer_id", lambda user_id: "cus_123")

    response = stripe_checkout.billing_portal(
        MagicMock(), {"user_id": "user_123", "auth": True}  # req  # sess
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/me/dashboards"


def test_billing_portal_successful_with_stripe_secret_configured(monkeypatch):
    """Test billing portal redirects to Stripe when STRIPE_SECRET_KEY is configured."""
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_billing")

    # Mock the Stripe session creation
    mock_session = MagicMock()
    mock_session.url = "https://billing.stripe.com/session/123"

    monkeypatch.setattr(
        stripe_checkout.stripe.billing_portal.Session,
        "create",
        MagicMock(return_value=mock_session),
    )
    monkeypatch.setattr(stripe_checkout, "get_stripe_customer_id", lambda user_id: "cus_123")

    response = stripe_checkout.billing_portal(
        MagicMock(), {"user_id": "user_123", "auth": True}  # req  # sess
    )

    assert response.loc == "https://billing.stripe.com/session/123"


def test_pricing_page_does_not_render_browser_title_as_visible_heading():
    client = TestClient(main.app)
    response = client.get("/pricing")

    assert response.status_code == 200
    assert "<title>Pricing - ViralVibes</title>" in response.text
    assert "<h1>Pricing - ViralVibes</h1>" not in response.text


def test_billing_management_is_visible_and_linked_in_dashboards():
    dashboards = to_xml(
        render_my_dashboards_page(
            dashboards=[],
            user_name="Buyer",
            plan_info={
                "plan": "pro",
                "interval": "month",
                "status": "trialing",
            },
        )
    )

    assert re.search(
        r'<a\b(?=[^>]*href="/billing/portal")[^>]*>.*?Manage billing.*?</a>',
        dashboards,
        re.DOTALL,
    )


def test_free_user_dashboard_links_to_upgrade():
    dashboards = to_xml(render_my_dashboards_page(dashboards=[], user_name="Buyer"))

    assert re.search(
        r'<a\b(?=[^>]*href="/pricing")[^>]*>.*?Upgrade to Pro.*?</a>',
        dashboards,
        re.DOTALL,
    )


def test_canceled_subscription_is_rendered_in_billing_card():
    html = to_xml(
        render_my_dashboards_page(
            dashboards=[],
            user_name="Buyer",
            plan_info={
                "plan": "free",
                "interval": None,
                "status": "inactive",
            },
            billing_info={
                "plan": "pro",
                "interval": "month",
                "status": "canceled",
                "current_period_end": "2026-12-15T00:00:00+00:00",
            },
        )
    )

    assert "Pro" in html
    assert "Canceled" in html
    assert "Access until Dec 15, 2026" in html


def test_latest_canceled_subscription_is_available_for_billing_display(monkeypatch):
    subscription = {
        "plan": "pro",
        "interval": "month",
        "status": "canceled",
        "current_period_end": "2026-12-15T00:00:00+00:00",
    }
    subscription_query = MagicMock()
    user_query = MagicMock()
    status_query = MagicMock()
    order_query = MagicMock()
    limit_query = MagicMock()
    subscription_query.eq.return_value = user_query
    user_query.eq.return_value = status_query
    status_query.order.return_value = order_query
    order_query.limit.return_value = limit_query
    limit_query.execute.return_value = SimpleNamespace(data=[subscription])
    client = MagicMock()
    client.table.return_value.select.return_value = subscription_query
    monkeypatch.setattr("db.supabase_client", client)

    assert get_latest_canceled_subscription("user_123") == subscription
    client.table.assert_called_once_with("subscriptions")
    client.table.return_value.select.assert_called_once_with(
        "plan, interval, status, current_period_end"
    )
    subscription_query.eq.assert_called_once_with("user_id", "user_123")
    user_query.eq.assert_called_once_with("status", "canceled")


def test_price_lookups_follow_environment_changes(monkeypatch):
    env_values = {
        "STRIPE_PRICE_PRO_MONTHLY": "price_new_pro_month",
        "STRIPE_PRICE_PRO_ANNUAL": "price_new_pro_year",
        "STRIPE_PRICE_AGENCY_MONTHLY": "price_new_agency_month",
        "STRIPE_PRICE_AGENCY_ANNUAL": "price_new_agency_year",
    }
    for key, value in env_values.items():
        monkeypatch.setenv(key, value)

    assert stripe_service.get_price_for_plan("pro", "month") == "price_new_pro_month"
    assert stripe_service.get_plan_for_price("price_new_pro_month") == ("pro", "month")
