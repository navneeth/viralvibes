# services/plan_gate.py
"""
Plan-based feature gating.

Usage in a route handler:

    from services.plan_gate import gate_plan

    blocked = gate_plan(user_id, required="pro", redirect_url=str(req.url))
    if blocked:
        return blocked

`gate_plan` returns None when the user has sufficient access, or a
RedirectResponse to the pricing page when they don't.
"""

from fasthtml.common import RedirectResponse

from db import get_user_plan
from services.stripe_service import PLAN_RANK


def gate_plan(
    user_id: str | None,
    required: str,
    redirect_url: str = "/pricing",
) -> RedirectResponse | None:
    """
    Return a redirect to /pricing if the user's plan is below `required`,
    or None if access is allowed.

    Args:
        user_id:      ViralVibes user UUID (from sess["user_id"]).
        required:     Minimum plan name — "pro" or "agency".
        redirect_url: The URL to bounce back to after upgrading (passed as
                      ?from= query param so the pricing page can show context).

    Returns:
        RedirectResponse to pricing if insufficient, None if allowed.
    """
    if not user_id:
        return RedirectResponse("/login", status_code=303)

    plan_info = get_user_plan(user_id) or {}
    user_rank = PLAN_RANK.get(plan_info.get("plan", "free"), 0)

    if required not in PLAN_RANK:
        raise ValueError(f"[gate_plan] Unknown required plan: {required!r} — must be one of {list(PLAN_RANK)}")

    required_rank = PLAN_RANK[required]

    if user_rank >= required_rank:
        return None  # ✅ access granted

    from urllib.parse import quote_plus

    return RedirectResponse(
        f"/pricing?upgrade={required}&from={quote_plus(redirect_url)}",
        status_code=303,
    )
