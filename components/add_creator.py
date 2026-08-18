"""
AddCreatorForm — reusable HTMX form for submitting a missing YouTube creator.

Used in three contexts across the product:
  - "handle not found" banner above search results       (compact, pre-filled)
  - empty-state card when @handle returns no results     (card, pre-filled)
  - empty-state card when a name/filter search misses    (card, open input)

The form always posts to POST /creators/request and renders its response into
a #creator-add-result slot injected below the submit button.
"""

from __future__ import annotations

from typing import Literal
from urllib.parse import urlencode

from fasthtml.common import *
from monsterui.all import *

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_RESULT_SLOT_ID = "creator-add-result"

# Icon size used inside flex-gap containers (no margin-right needed).
_ICON_CLS = "size-4"

_BTN_BASE = (
    "flex items-center gap-1.5 font-semibold rounded-lg "
    "bg-primary text-primary-foreground hover:bg-primary/90 transition-colors shrink-0"
)
_BTN_SM = _BTN_BASE + " px-4 py-1.5 text-sm"
_BTN_MD = _BTN_BASE + " px-5 py-2.5 text-sm"

_LOGIN_BTN_BASE = (
    "inline-flex items-center gap-1.5 font-semibold rounded-lg "
    "border border-border hover:bg-accent transition-colors shrink-0 no-underline"
)
_LOGIN_BTN_SM = _LOGIN_BTN_BASE + " px-4 py-1.5 text-sm"
_LOGIN_BTN_MD = _LOGIN_BTN_BASE + " px-5 py-2.5 text-sm"

_INPUT_CLS = (
    "flex-1 px-3 py-2 text-sm rounded-lg border border-border "
    "bg-background focus:outline-none focus:ring-2 focus:ring-primary/40"
)


def _login_href(return_url: str) -> str:
    return f"/login?{urlencode({'return_url': return_url})}"


# ---------------------------------------------------------------------------
# Public component
# ---------------------------------------------------------------------------


def AddCreatorForm(
    is_authenticated: bool,
    *,
    prefill: str = "",
    return_url: str = "/creators",
    button_label: str = "",
    size: Literal["sm", "md"] = "sm",
    align: Literal["start", "center"] = "start",
) -> FT:
    """
    HTMX add-creator form / sign-in CTA.

    Args:
        is_authenticated: Whether the current user is logged in.
        prefill:          Pre-fill and hide the input (one-click submit).
                          When empty an open text ``<input>`` is shown instead.
        return_url:       Login redirect target for unauthenticated users.
        button_label:     Submit button text.  Defaults to ``"Add {prefill}"``
                          when *prefill* is set, otherwise ``"Submit"``.
        size:             ``"sm"`` (py-1.5) or ``"md"`` (py-2.5).
        align:            Form flex alignment — ``"start"`` or ``"center"``.
    """
    btn_cls = _BTN_MD if size == "md" else _BTN_SM
    login_btn_cls = _LOGIN_BTN_MD if size == "md" else _LOGIN_BTN_SM
    form_cls = f"flex flex-col items-{'center' if align == 'center' else 'start'} gap-0"
    result_slot = Div(id=_RESULT_SLOT_ID, cls="mt-2")

    # Resolve default button label
    label = button_label or (f"Add {prefill}" if prefill else "Submit")

    if not is_authenticated:
        # Unauthenticated — render a sign-in link (no form, no DB write)
        if prefill:
            return A(
                UkIcon("log-in", cls=_ICON_CLS),
                "Sign in with Google to add",
                href=_login_href(return_url),
                cls=login_btn_cls,
            )
        # Open-input variant: inline sentence link
        return P(
            A(
                "Sign in with Google",
                href=_login_href(return_url),
                cls="text-primary hover:underline font-medium",
            ),
            " to submit a creator by @handle.",
            cls="text-sm text-muted-foreground",
        )

    # Authenticated — render the HTMX form
    if prefill:
        # One-click: hidden input, single button
        return Form(
            Input(type="hidden", name="q", value=prefill),
            Button(
                UkIcon("plus-circle", cls=_ICON_CLS),
                label,
                type="submit",
                cls=btn_cls,
            ),
            result_slot,
            hx_post="/creators/request",
            hx_target=f"#{_RESULT_SLOT_ID}",
            cls=form_cls,
        )

    # Open input: text field + submit button side by side
    return Form(
        Div(
            Input(
                type="text",
                name="q",
                placeholder="@handle or channel ID…",
                autocomplete="off",
                cls=_INPUT_CLS,
            ),
            Button(
                label,
                type="submit",
                cls=btn_cls,
            ),
            cls="flex gap-2",
        ),
        result_slot,
        hx_post="/creators/request",
        hx_target=f"#{_RESULT_SLOT_ID}",
    )
