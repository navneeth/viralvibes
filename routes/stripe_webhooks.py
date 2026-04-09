# routes/stripe_webhooks.py
"""
Stripe webhook handler.

Stripe posts signed events to POST /webhook.  We verify the signature,
then fan out to the relevant handler.  Every response is plain HTTP —
no HTML, no session — because the caller is Stripe's infrastructure.

Handled events
--------------
checkout.session.completed
    Links a Stripe customer to the ViralVibes user who initiated checkout.
    Also activates the subscription immediately for the case where
    checkout mode="subscription" and the trial starts at session completion.

customer.subscription.created / updated
    Primary path: upserts plan/status/period-end into subscriptions table.

customer.subscription.deleted
    Marks the subscription canceled so gating kicks in.
"""

import logging

import stripe
from fasthtml.common import Request
from starlette.responses import JSONResponse

from db import get_user_id_by_stripe_customer, upsert_subscription
from services.stripe_service import WEBHOOK_SECRET, get_plan_for_price

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public entry point (called from main.py)
# ---------------------------------------------------------------------------


async def stripe_webhook(req: Request) -> JSONResponse:
    """Verify Stripe signature and dispatch to the appropriate handler."""
    payload = await req.body()
    sig_header = req.headers.get("stripe-signature", "")

    if not WEBHOOK_SECRET:
        logger.error("[Webhook] STRIPE_WEBHOOK_SECRET not configured")
        return JSONResponse({"error": "webhook not configured"}, status_code=500)

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, WEBHOOK_SECRET)
    except stripe.SignatureVerificationError:
        logger.warning("[Webhook] Invalid Stripe signature — request rejected")
        return JSONResponse({"error": "invalid signature"}, status_code=400)
    except Exception as exc:
        logger.exception("[Webhook] Failed to construct event: %s", exc)
        return JSONResponse({"error": "bad request"}, status_code=400)

    event_type: str = event["type"]
    logger.info("[Webhook] Received event: %s  id=%s", event_type, event["id"])

    handlers = {
        "checkout.session.completed": _handle_checkout_completed,
        "customer.subscription.created": _handle_subscription_upsert,
        "customer.subscription.updated": _handle_subscription_upsert,
        "customer.subscription.deleted": _handle_subscription_deleted,
    }

    handler = handlers.get(event_type)
    if handler:
        try:
            handler(event["data"]["object"])
        except Exception as exc:
            # Log but still return 200 — Stripe will retry on non-2xx
            logger.exception("[Webhook] Handler error for %s: %s", event_type, exc)

    # Always 200 for unhandled / successfully handled events
    return JSONResponse({"received": True})


# ---------------------------------------------------------------------------
# Event handlers
# ---------------------------------------------------------------------------


def _handle_checkout_completed(session: dict) -> None:
    """
    Link Stripe customer → ViralVibes user.

    checkout.session.completed fires when the user completes the Stripe-hosted
    checkout page.  The metadata["user_id"] we embed when creating the session
    (PR 3) is the bridge.  We write stripe_customer_id back to users here so the
    subscription events that follow can look up the user.
    """
    user_id: str | None = (session.get("metadata") or {}).get("user_id")
    customer_id: str | None = session.get("customer")

    if not user_id or not customer_id:
        logger.warning(
            "[Webhook] checkout.session.completed missing user_id or customer: %s / %s",
            user_id,
            customer_id,
        )
        return

    from db import link_stripe_customer  # imported here to avoid circular at module level

    link_stripe_customer(user_id, customer_id)
    logger.info("[Webhook] Linked customer %s → user %s", customer_id, user_id)


def _handle_subscription_upsert(subscription: dict) -> None:
    """Handle subscription.created and subscription.updated."""
    customer_id: str = subscription.get("customer", "")
    user_id = get_user_id_by_stripe_customer(customer_id)

    if not user_id:
        logger.warning("[Webhook] No user found for customer %s — skipping upsert", customer_id)
        return

    price_id = _extract_price_id(subscription)
    plan, interval = get_plan_for_price(price_id)
    status: str = subscription.get("status", "inactive")
    period_end_ts = subscription.get("current_period_end") or 0

    upsert_subscription(
        user_id=user_id,
        stripe_subscription_id=subscription["id"],
        stripe_price_id=price_id,
        plan=plan,
        interval=interval,
        status=status,
        current_period_end_ts=period_end_ts,
    )
    logger.info(
        "[Webhook] Upserted subscription %s: user=%s plan=%s/%s status=%s",
        subscription["id"],
        user_id,
        plan,
        interval,
        status,
    )


def _handle_subscription_deleted(subscription: dict) -> None:
    """Mark a canceled subscription so gating takes effect immediately."""
    customer_id: str = subscription.get("customer", "")
    user_id = get_user_id_by_stripe_customer(customer_id)

    if not user_id:
        logger.warning("[Webhook] No user found for customer %s — skipping cancel", customer_id)
        return

    price_id = _extract_price_id(subscription)
    plan, interval = get_plan_for_price(price_id)

    upsert_subscription(
        user_id=user_id,
        stripe_subscription_id=subscription["id"],
        stripe_price_id=price_id,
        plan=plan,
        interval=interval,
        status="canceled",
        current_period_end_ts=(subscription.get("current_period_end") or 0),
    )
    logger.info(
        "[Webhook] Marked subscription %s canceled for user %s",
        subscription["id"],
        user_id,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_price_id(subscription: dict) -> str:
    """Pull the first price ID out of subscription.items.data[0].price.id."""
    try:
        return subscription["items"]["data"][0]["price"]["id"]
    except (KeyError, IndexError, TypeError):
        return ""
