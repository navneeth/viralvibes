"""
Stripe integration — single source of truth for API config and plan metadata.

Environment variables required:
    STRIPE_SECRET_KEY           sk_live_... or sk_test_...
    STRIPE_WEBHOOK_SECRET       whsec_...
    STRIPE_PUBLISHABLE_KEY      pk_live_... or pk_test_...
    STRIPE_PRICE_PRO_MONTHLY    price_...
    STRIPE_PRICE_PRO_ANNUAL     price_...
    STRIPE_PRICE_AGENCY_MONTHLY price_...
    STRIPE_PRICE_AGENCY_ANNUAL  price_...
    DOMAIN_URL                  https://viralvibes.fyi  (no trailing slash)
"""

import os
import stripe

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
WEBHOOK_SECRET: str = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
PUBLISHABLE_KEY: str = os.environ.get("STRIPE_PUBLISHABLE_KEY", "")
DOMAIN_URL: str = os.environ.get("DOMAIN_URL", "http://localhost:5001").rstrip("/")

# ---------------------------------------------------------------------------
# Price → plan mapping
# ---------------------------------------------------------------------------
# Maps Stripe price_id → (plan_name, billing_interval)
# Used in the webhook handler to determine which plan to activate.
PRICE_TO_PLAN: dict[str, tuple[str, str]] = {
    k: v
    for k, v in {
        os.environ.get("STRIPE_PRICE_PRO_MONTHLY", ""): ("pro", "month"),
        os.environ.get("STRIPE_PRICE_PRO_ANNUAL", ""): ("pro", "year"),
        os.environ.get("STRIPE_PRICE_AGENCY_MONTHLY", ""): ("agency", "month"),
        os.environ.get("STRIPE_PRICE_AGENCY_ANNUAL", ""): ("agency", "year"),
    }.items()
    if k  # exclude entries where the env var isn't set yet
}

# Reverse map: (plan, interval) → price_id — used when creating checkout sessions
PLAN_TO_PRICE: dict[tuple[str, str], str] = {v: k for k, v in PRICE_TO_PLAN.items()}

# Plan hierarchy — used for gate comparisons (higher index = more access)
PLAN_RANK: dict[str, int] = {"free": 0, "pro": 1, "agency": 2}


def get_plan_for_price(price_id: str) -> tuple[str, str]:
    """Return (plan, interval) for a Stripe price_id, or ('free', '') if unknown."""
    return PRICE_TO_PLAN.get(price_id, ("free", ""))


def get_price_for_plan(plan: str, interval: str) -> str | None:
    """Return the Stripe price_id for a given (plan, interval) pair, or None."""
    return PLAN_TO_PRICE.get((plan, interval))
