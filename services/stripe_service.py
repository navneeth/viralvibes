"""
Stripe integration — single source of truth for API config and plan metadata.

Environment variables required:
    STRIPE_SECRET_KEY           sk_live_... or sk_test_...
                                restricted keys need Prices: read and Checkout Sessions: write
    STRIPE_WEBHOOK_SECRET       whsec_...
    STRIPE_PUBLISHABLE_KEY      pk_live_... or pk_test_...
    STRIPE_PRICE_PRO_MONTHLY    price_...
    STRIPE_PRICE_PRO_ANNUAL     price_...
    STRIPE_PRICE_AGENCY_MONTHLY price_... (legacy subscriptions only)
    STRIPE_PRICE_AGENCY_ANNUAL  price_... (legacy subscriptions only)
    STRIPE_LEGACY_PRICE_*       comma-separated retired Price IDs, by plan/interval
    DOMAIN_URL                  https://viralvibes.fyi  (no trailing slash)
"""

import logging as _logging
import os
from typing import Optional

import stripe

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "").strip()
WEBHOOK_SECRET: str = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
PUBLISHABLE_KEY: str = os.environ.get("STRIPE_PUBLISHABLE_KEY", "")
DOMAIN_URL: str = os.environ.get("DOMAIN_URL", "http://localhost:5001").rstrip("/")


# ---------------------------------------------------------------------------
# Price → plan mapping
# ---------------------------------------------------------------------------
# Maps current and explicitly retained retired Stripe Price IDs to
# (plan_name, billing_interval). Legacy variables accept comma-separated IDs.
def _configured_price_to_plan() -> dict[str, tuple[str, str]]:
    mapping: dict[str, tuple[str, str]] = {}
    price_vars = (
        ("STRIPE_LEGACY_PRICE_PRO_MONTHLY", "pro", "month", True),
        ("STRIPE_LEGACY_PRICE_PRO_ANNUAL", "pro", "year", True),
        ("STRIPE_LEGACY_PRICE_AGENCY_MONTHLY", "agency", "month", True),
        ("STRIPE_LEGACY_PRICE_AGENCY_ANNUAL", "agency", "year", True),
        ("STRIPE_PRICE_AGENCY_MONTHLY", "agency", "month", False),
        ("STRIPE_PRICE_AGENCY_ANNUAL", "agency", "year", False),
        ("STRIPE_PRICE_PRO_MONTHLY", "pro", "month", False),
        ("STRIPE_PRICE_PRO_ANNUAL", "pro", "year", False),
    )
    for env_name, plan, interval, multiple in price_vars:
        configured = os.environ.get(env_name, "")
        price_ids = configured.split(",") if multiple else [configured]
        for price_id in price_ids:
            price_id = price_id.strip()
            if price_id:
                mapping[price_id] = (plan, interval)
    return mapping


# Plan hierarchy — used for gate comparisons (higher index = more access)
PLAN_RANK: dict[str, int] = {"free": 0, "pro": 1, "agency": 2}

# ---------------------------------------------------------------------------
# Startup validation
# ---------------------------------------------------------------------------
_REQUIRED_PRICE_VARS = (
    "STRIPE_PRICE_PRO_MONTHLY",
    "STRIPE_PRICE_PRO_ANNUAL",
)

_log = _logging.getLogger(__name__)

for _var in _REQUIRED_PRICE_VARS:
    if not os.environ.get(_var):
        _log.warning(
            "[stripe_service] Missing env var %s — price→plan mapping will be incomplete", _var
        )


def ensure_stripe_api_key() -> bool:
    """Refresh Stripe's API key from env and report whether checkout can call Stripe.

    Side effect: mutates stripe.api_key with the current env value.
    This is the single point where Stripe key configuration is checked.
    """
    stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "").strip()
    if not stripe.api_key:
        _log.warning("[stripe_service] Missing env var STRIPE_SECRET_KEY — checkout is unavailable")
    return bool(stripe.api_key)


def get_plan_for_price(price_id: str) -> tuple[str, str]:
    """Return (plan, interval) for a Stripe price_id, or ('free', '') if unknown."""
    return _configured_price_to_plan().get(price_id, ("free", ""))


def get_price_for_plan(plan: str, interval: str) -> Optional[str]:
    """Return the Stripe price_id for a given (plan, interval) pair, or None."""
    return {v: k for k, v in _configured_price_to_plan().items()}.get((plan, interval))
