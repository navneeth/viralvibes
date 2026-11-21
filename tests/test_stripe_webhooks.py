from unittest.mock import Mock

from routes import stripe_webhooks


def _subscription(subscription_id="sub_123", price_id="price_retired"):
    return {
        "id": subscription_id,
        "customer": "cus_123",
        "status": "past_due",
        "current_period_end": 1_800_000_000,
        "items": {"data": [{"price": {"id": price_id}}]},
    }


def test_subscription_update_uses_stored_plan_for_retired_price(monkeypatch):
    persist = Mock()
    monkeypatch.setattr(stripe_webhooks, "get_user_id_by_stripe_customer", lambda _: "user_123")
    monkeypatch.setattr(stripe_webhooks, "get_plan_for_price", lambda _: ("free", ""))
    monkeypatch.setattr(
        stripe_webhooks,
        "get_subscription_plan_by_stripe_id",
        lambda _: {"plan": "pro", "interval": "year"},
    )
    monkeypatch.setattr(stripe_webhooks, "upsert_subscription", persist)

    stripe_webhooks._handle_subscription_upsert(_subscription())

    persist.assert_called_once_with(
        user_id="user_123",
        stripe_subscription_id="sub_123",
        stripe_price_id="price_retired",
        plan="pro",
        interval="year",
        status="past_due",
        current_period_end_ts=1_800_000_000,
    )


def test_unknown_price_without_prior_subscription_is_not_upserted(monkeypatch):
    persist = Mock()
    monkeypatch.setattr(stripe_webhooks, "get_user_id_by_stripe_customer", lambda _: "user_123")
    monkeypatch.setattr(stripe_webhooks, "get_plan_for_price", lambda _: ("free", ""))
    monkeypatch.setattr(stripe_webhooks, "get_subscription_plan_by_stripe_id", lambda _: None)
    monkeypatch.setattr(stripe_webhooks, "upsert_subscription", persist)

    stripe_webhooks._handle_subscription_upsert(_subscription())

    persist.assert_not_called()


def test_deleted_subscription_keeps_stored_agency_plan(monkeypatch):
    persist = Mock()
    monkeypatch.setattr(stripe_webhooks, "get_user_id_by_stripe_customer", lambda _: "user_123")
    monkeypatch.setattr(stripe_webhooks, "get_plan_for_price", lambda _: ("free", ""))
    monkeypatch.setattr(
        stripe_webhooks,
        "get_subscription_plan_by_stripe_id",
        lambda _: {"plan": "agency", "interval": "month"},
    )
    monkeypatch.setattr(stripe_webhooks, "upsert_subscription", persist)

    stripe_webhooks._handle_subscription_deleted(_subscription())

    persist.assert_called_once_with(
        user_id="user_123",
        stripe_subscription_id="sub_123",
        stripe_price_id="price_retired",
        plan="agency",
        interval="month",
        status="canceled",
        current_period_end_ts=1_800_000_000,
    )
