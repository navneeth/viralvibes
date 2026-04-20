# routes/stripe_checkout.py
"""
Stripe checkout flow.

All three handlers are called from main.py which owns the @rt decorators
and wraps HTML responses with NavComponent / Titled.

billing_checkout        POST /billing/checkout        — create Checkout session → redirect to Stripe
billing_checkout_begin  GET  /billing/checkout/begin  — resume checkout after login redirect
billing_success_content GET  /billing/success         — verify session_id, return page content Div
billing_portal          GET  /billing/portal          — open Customer Portal → redirect to Stripe
"""

import logging
from typing import Optional, Union

import stripe
from fasthtml.common import *
from monsterui.all import *

from db import get_stripe_customer_id, get_user_plan
from services.stripe_service import DOMAIN_URL, get_price_for_plan

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Checkout
# ---------------------------------------------------------------------------


async def _do_checkout(sess, plan: str, interval: str) -> Response:
    """Core Stripe session creation — caller must have already verified auth."""
    user_id: str = sess.get("user_id")
    price_id = get_price_for_plan(plan, interval)
    if not price_id:
        logger.error(
            "[Checkout] No price_id configured for plan=%s interval=%s — "
            "check STRIPE_PRICE_* env vars",
            plan,
            interval,
        )
        return RedirectResponse("/pricing?error=config", status_code=303)

    user_email: str = sess.get("user_email") or ""

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{"price": price_id, "quantity": 1}],
            subscription_data={"trial_period_days": 7},
            # Pre-fill email if available; omit rather than send empty string
            customer_email=user_email or None,
            # user_id embedded so the webhook can link customer → user
            metadata={"user_id": user_id},
            success_url=f"{DOMAIN_URL}/billing/success?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{DOMAIN_URL}/pricing",
        )
    except stripe.StripeError as exc:
        logger.exception("[Checkout] Stripe error creating session: %s", exc)
        return RedirectResponse("/pricing?error=stripe", status_code=303)

    return Redirect(session.url)


_PLAN_LABELS = {"pro": "Pro", "agency": "Agency"}
_INTERVAL_LABELS = {"year": "annual", "month": "monthly"}


def _checkout_login_subheadline(plan: str, interval: str) -> str:
    label = _PLAN_LABELS.get(plan, plan.capitalize())
    billing = _INTERVAL_LABELS.get(interval, interval)
    return f"Sign in to start your 7-day {label} trial ({billing} billing)"


async def billing_checkout(req, sess) -> Response:
    """POST /billing/checkout — create Checkout session → redirect to Stripe."""
    form = await req.form()
    plan: str = form.get("plan", "pro")
    interval: str = form.get("interval", "year")

    if not sess.get("auth") or not sess.get("user_id"):
        # Preserve plan context so the auth callback can complete checkout
        sess["intended_url"] = f"/billing/checkout/begin?plan={plan}&interval={interval}"
        sess["login_context"] = _checkout_login_subheadline(plan, interval)
        return RedirectResponse("/login", status_code=303)

    return await _do_checkout(sess, plan, interval)


async def billing_checkout_begin(req, sess, plan: str = "pro", interval: str = "year") -> Response:
    """GET /billing/checkout/begin — resume checkout after login redirects back with plan context."""
    if not sess.get("auth") or not sess.get("user_id"):
        sess["intended_url"] = f"/billing/checkout/begin?plan={plan}&interval={interval}"
        sess["login_context"] = _checkout_login_subheadline(plan, interval)
        return RedirectResponse("/login", status_code=303)

    return await _do_checkout(sess, plan, interval)


# ---------------------------------------------------------------------------
# Success
# ---------------------------------------------------------------------------


def billing_success_content(req, sess, session_id: str = "") -> Union[Div, Response]:
    """
    Show success page after Stripe checkout.

    The official pattern (per FastHTML stripe docs) is to verify from your own
    database — the webhook fires before Stripe redirects the browser, so by the
    time the user lands here the subscriptions row should already exist.
    If it hasn't arrived yet (rare race), we show a processing message rather
    than calling the Stripe API a second time.
    """
    user_id: Optional[str] = sess.get("user_id")
    if not sess.get("auth") or not user_id:
        return RedirectResponse("/login", status_code=303)

    if not session_id:
        return RedirectResponse("/pricing", status_code=303)

    plan_info = get_user_plan(user_id) or {}
    is_active = plan_info.get("status", "inactive") in ("active", "trialing")

    if not is_active:
        # Webhook hasn't arrived yet — show a friendly waiting state with
        # a meta-refresh so the page auto-retries after 3 s.
        logger.info(
            "[Success] subscription not yet active for user %s — showing retry page",
            user_id,
        )
        return Div(
            Meta(http_equiv="refresh", content=f"3;url=/billing/success?session_id={session_id}"),
            UkIcon("loader", cls="w-12 h-12 text-muted-foreground mx-auto mb-4 animate-spin"),
            H1("Setting up your subscription…", cls="text-2xl font-bold text-foreground mb-2"),
            P(
                "This usually takes a second. The page will refresh automatically.",
                cls="text-muted-foreground",
            ),
            cls="text-center py-24 max-w-lg mx-auto",
        )

    return Div(
        UkIcon("circle-check", cls="w-16 h-16 text-green-500 mx-auto mb-6"),
        H1("You're all set!", cls="text-3xl font-bold text-foreground mb-3"),
        P(
            "Your 7-day free trial has started. "
            "No charge until the trial ends — cancel any time.",
            cls="text-muted-foreground mb-8 max-w-md mx-auto",
        ),
        Div(
            A(
                "Go to my dashboards",
                href="/me/dashboards",
                cls=(ButtonT.primary, "inline-block px-8"),
            ),
            A(
                "Manage billing",
                href="/billing/portal",
                cls="ml-4 text-sm text-muted-foreground hover:underline",
            ),
            cls="flex items-center justify-center gap-4 flex-wrap",
        ),
        cls="text-center py-24 max-w-lg mx-auto",
    )


# ---------------------------------------------------------------------------
# Customer Portal
# ---------------------------------------------------------------------------


def billing_portal(req, sess) -> Response:
    """Open the Stripe Customer Portal for self-service billing management."""
    user_id: Optional[str] = sess.get("user_id")
    auth = sess.get("auth")
    if not auth or not user_id:
        return RedirectResponse("/login", status_code=303)

    customer_id = get_stripe_customer_id(user_id)
    if not customer_id:
        # User has no Stripe customer yet — send them to pricing to subscribe
        return RedirectResponse("/pricing", status_code=303)

    try:
        portal = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=f"{DOMAIN_URL}/me/dashboards",
        )
    except stripe.StripeError as exc:
        logger.exception("[Portal] Stripe error creating portal session: %s", exc)
        return RedirectResponse("/me/dashboards", status_code=303)

    return Redirect(portal.url)
