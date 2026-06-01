"""
Contact page — inquiry form, support channels, and FAQ.

Submissions land in ``contact_inquiries`` (migration 041) and a masked
record is logged at INFO level for runtime triage. Transactional-email
forwarding to ``CONTACT_EMAIL`` is layered on in PR 2c.

Spam controls are server-side only (no CAPTCHA, no UX friction):
    • Honeypot field — bots fill all visible-looking inputs; humans don't.
    • Submission-time floor — humans take >2s; replay protection caps at 24h.
    • Per-IP rate limit — at most 5 inquiries/hour from a single IP,
      enforced via ``count_contact_inquiries_from_ip`` (index-backed).

The form swaps via HTMX (``hx-post`` + ``hx-target=#contact-form-region``,
``hx-swap=outerHTML``) so success/error states replace only the form
region — nav and surrounding copy stay visible. ``method="post"`` and
``action="/contact"`` are retained for progressive enhancement so the
form still works without JavaScript; ``main.py`` wraps the bare fragment
in the standard page chrome when ``HX-Request`` is absent.
"""

import logging
import re
import time

from fasthtml.common import *
from monsterui.all import *

from components.page_layout import (
    FAQCard,
    LinkCard,
    PageSection,
    StaticPage,
)
from constants import CONTACT_EMAIL
from db import (
    CONTACT_INQUIRY_OPTIONS,
    CONTACT_INQUIRY_TYPES,
    count_contact_inquiries_from_ip,
    insert_contact_inquiry,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------

# Soft min/max enforced at the app layer; the DB CHECK constraints in
# migration 041 are the hard floor (1 char) / ceiling (10 000 chars).
_MIN_NAME_CHARS = 2
_MAX_NAME_CHARS = 80
_MIN_MESSAGE_CHARS = 10
_MAX_MESSAGE_CHARS = 10_000

# Spam controls — kept generous so a deliberate human can't trip them
# accidentally but a script-it-and-forget-it bot will.
_HONEYPOT_FIELD = "website"  # field name bots will fill in
_MIN_FILL_SECONDS = 2  # humans take longer to read the form
_MAX_FILL_SECONDS = 24 * 60 * 60  # stale forms (>24h) are replay attempts
_RATE_LIMIT_PER_HOUR = 5  # max submissions per IP per hour

# RFC 5322 is famously hairy; this catches the only failure that actually
# matters at this layer (no @ or no domain). Real validation happens when
# the operator hits Reply.
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _mask_email(email: str) -> str:
    """Return a privacy-preserving rendering of ``email`` for log lines.

    Keeps the first character of the local part and the full domain so an
    operator triaging logs can still recognise repeat senders, without
    putting harvestable addresses in runtime log storage.

    Examples:
        ``jane@example.com``    -> ``j***@example.com``
        ``a@example.com``       -> ``*@example.com``
        ``no-at-sign``          -> ``(no email)``
    """
    if not email:
        return "(no email)"
    local, sep, domain = email.partition("@")
    if not sep:
        return "(invalid email)"
    masked_local = (local[:1] + "***") if local else "*"
    return f"{masked_local}@{domain}"


def _get_client_ip(req) -> str | None:
    """Best-effort client IP — Vercel-aware ordering.

    On Vercel (the deployment target), ``req.client.host`` is the *edge
    proxy*, never the real visitor — ``X-Forwarded-For`` set by Vercel's
    edge is the only path to the actual client IP. The edge strips and
    rewrites the header on every hop, so trusting its first value is
    correct for this deployment. If this app is ever fronted by an
    untrusted proxy, the rate-limit logic in ``post_contact`` becomes
    spoofable and this helper must grow a trusted-upstream allowlist.

    Falls back to ``X-Real-IP`` (some CDNs set it) and then to the raw
    socket peer so local dev / direct hits still get a value.
    """
    xff = req.headers.get("x-forwarded-for") or req.headers.get("x-real-ip")
    if xff:
        first = xff.split(",")[0].strip()
        return first or None
    client = getattr(req, "client", None)
    return getattr(client, "host", None) if client else None


# ---------------------------------------------------------------------------
# Form rendering
# ---------------------------------------------------------------------------

_INPUT_CLS = (
    "w-full px-3 py-2.5 border border-border rounded-md bg-background text-foreground "
    "transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 "
    "focus:border-transparent placeholder:text-muted-foreground/60"
)
_INPUT_CLS_ERROR = (
    "w-full px-3 py-2.5 border border-red-500 rounded-md bg-background text-foreground "
    "transition-colors focus:outline-none focus:ring-2 focus:ring-red-500 "
    "focus:border-transparent placeholder:text-muted-foreground/60"
)
_LABEL_CLS = "block text-xs font-mono uppercase tracking-[0.14em] text-muted-foreground mb-2"
_ERROR_CLS = "text-xs text-red-600 mt-1.5"


def _field(label_text: str, control, *, error: str | None = None) -> Div:
    """A label + control pair with consistent spacing and optional error span."""
    attrs = getattr(control, "attrs", None) or {}
    target = attrs.get("name") or attrs.get("id") or ""
    children: list = [Label(label_text, For=target, cls=_LABEL_CLS), control]
    if error:
        children.append(P(error, cls=_ERROR_CLS, role="alert"))
    return Div(*children, cls="mb-5")


def _input_cls_for(field_name: str, errors: dict | None) -> str:
    return _INPUT_CLS_ERROR if errors and field_name in errors else _INPUT_CLS


def contact_form(
    *,
    values: dict | None = None,
    errors: dict | None = None,
) -> Form:
    """Render the contact form.

    ``values`` and ``errors`` are passed when the form is re-rendered after
    a validation failure so the user doesn't lose what they typed.
    """
    values = values or {}
    errors = errors or {}
    now_ts = int(time.time())

    # Honeypot — visually hidden, off the tab order, with autocomplete off
    # so password managers don't fill it. Bots that submit every input
    # they can see will trip it; humans never do.
    honeypot = Div(
        Label(
            "Leave this field blank",
            For=_HONEYPOT_FIELD,
        ),
        Input(
            type="text",
            id=_HONEYPOT_FIELD,
            name=_HONEYPOT_FIELD,
            value="",
            tabindex="-1",
            autocomplete="off",
        ),
        style="position:absolute;left:-9999px;width:1px;height:1px;overflow:hidden;",
        aria_hidden="true",
    )

    return Form(
        # Two-column row on md+ for compact name/email.
        Div(
            _field(
                "Name",
                Input(
                    type="text",
                    id="name",
                    name="name",
                    required=True,
                    value=values.get("name", ""),
                    cls=_input_cls_for("name", errors),
                    placeholder="Your name",
                    maxlength=str(_MAX_NAME_CHARS),
                ),
                error=errors.get("name"),
            ),
            _field(
                "Email",
                Input(
                    type="email",
                    id="email",
                    name="email",
                    required=True,
                    value=values.get("email", ""),
                    cls=_input_cls_for("email", errors),
                    placeholder="you@example.com",
                ),
                error=errors.get("email"),
            ),
            cls="grid grid-cols-1 md:grid-cols-2 gap-x-5",
        ),
        _field(
            "Inquiry Type",
            Select(
                *[
                    Option(label, value=slug, selected=(values.get("inquiry_type") == slug))
                    for slug, label in CONTACT_INQUIRY_OPTIONS
                ],
                id="inquiry_type",
                name="inquiry_type",
                cls=_input_cls_for("inquiry_type", errors),
            ),
            error=errors.get("inquiry_type"),
        ),
        _field(
            "Message",
            Textarea(
                values.get("message", ""),
                id="message",
                name="message",
                required=True,
                rows="5",
                cls=f"{_input_cls_for('message', errors)} resize-none",
                placeholder="Tell us more…",
                maxlength=str(_MAX_MESSAGE_CHARS),
            ),
            error=errors.get("message"),
        ),
        # Hidden fields powering the spam controls. Rendered server-side
        # so the client can't forge or omit them silently.
        honeypot,
        Input(type="hidden", name="ts", value=str(now_ts)),
        Button(
            "Send Message →",
            type="submit",
            cls=(
                "w-full px-4 py-2.5 bg-foreground text-background font-semibold rounded-md "
                "transition-transform hover:-translate-y-px hover:bg-foreground/90 "
                "focus:outline-none focus:ring-2 focus:ring-blue-500"
            ),
        ),
        # Progressive enhancement: action+method work without JS. With htmx
        # loaded, hx-* attributes take over and only the form region is
        # swapped — nav and surrounding copy stay in place.
        method="post",
        action="/contact",
        hx_post="/contact",
        hx_target="#contact-form-region",
        hx_swap="outerHTML",
        cls="space-y-1",
    )


def contact_form_region(
    *,
    values: dict | None = None,
    errors: dict | None = None,
    banner: str | None = None,
) -> Div:
    """The outer ``#contact-form-region`` wrapper htmx swaps into.

    ``banner`` is an optional inline error/notice shown above the form
    (used for rate-limit messages and the "please correct the highlighted
    fields" prompt).
    """
    children: list = []
    if banner:
        children.append(
            Div(
                P(banner, cls="text-sm text-red-700"),
                cls=(
                    "mb-5 px-4 py-3 border border-red-200 bg-red-50 "
                    "dark:border-red-900/40 dark:bg-red-950/30 rounded-md"
                ),
                role="alert",
            )
        )
    children.append(contact_form(values=values, errors=errors))
    return Div(*children, id="contact-form-region")


def _success_fragment() -> Div:
    """Replacement HTML shown after a successful (or silently dropped) POST."""
    return Div(
        Div(
            Span("✓", cls="text-2xl"),
            cls=(
                "inline-flex items-center justify-center w-12 h-12 rounded-full "
                "bg-blue-100 text-blue-600 mb-6"
            ),
        ),
        H2(
            "Thank you for reaching out!",
            cls="text-3xl font-bold tracking-tight text-foreground mb-3",
        ),
        P(
            "We've received your message and will get back to you within 24 business hours.",
            cls="text-muted-foreground mb-6",
        ),
        A(
            "← Back to contact page",
            href="/contact",
            cls="inline-flex items-center text-blue-600 hover:text-blue-700 font-semibold",
        ),
        cls="max-w-xl mx-auto px-4 py-16 text-center",
        id="contact-form-region",
    )


def _rate_limited_fragment(values: dict) -> Div:
    """Fragment shown when the per-IP cap is exceeded."""
    return contact_form_region(
        values=values,
        banner=(
            "You've sent several messages from this address recently. "
            "Please try again in an hour, or email us directly at "
            f"{CONTACT_EMAIL}."
        ),
    )


# ---------------------------------------------------------------------------
# Validation + spam checks
# ---------------------------------------------------------------------------


def _validate(values: dict) -> dict:
    """Server-side field validation. Returns ``{field: message}`` or ``{}``.

    Side-effect: silently normalises ``inquiry_type`` to ``"general"`` if
    the submitted value isn't in the allowlist — the ``<select>`` only
    emits known values, so a mismatch implies tampering and shouldn't be
    surfaced to a legitimate user.
    """
    errors: dict = {}

    name_len = len(values.get("name", ""))
    if name_len < _MIN_NAME_CHARS or name_len > _MAX_NAME_CHARS:
        errors["name"] = (
            f"Please enter your name ({_MIN_NAME_CHARS}\u2013{_MAX_NAME_CHARS} characters)."
        )

    if not _EMAIL_RE.match(values.get("email", "")):
        errors["email"] = "Please enter a valid email address."

    if values.get("inquiry_type") not in CONTACT_INQUIRY_TYPES:
        values["inquiry_type"] = "general"

    msg_len = len(values.get("message", ""))
    if msg_len < _MIN_MESSAGE_CHARS:
        errors["message"] = f"Please write a bit more (at least {_MIN_MESSAGE_CHARS} characters)."
    elif msg_len > _MAX_MESSAGE_CHARS:
        errors["message"] = f"Message is too long (max {_MAX_MESSAGE_CHARS:,} characters)."

    return errors


def _looks_like_bot(form, now_ts: int) -> tuple[bool, str | None]:
    """Honeypot and timestamp-floor checks.

    Returns ``(is_bot, reason)``. ``reason`` is a short tag used only for
    structured logging — the caller always responds with the success
    fragment when ``is_bot`` is True (we never tell a script its attempt
    failed). The ``"ts_missing"`` reason exists so a template regression
    that drops the hidden ``ts`` field shows up loudly in logs instead of
    silently swallowing every legitimate submission; the behaviour is
    still silent-drop, but operators get a distinct signal to act on.
    """
    if (form.get(_HONEYPOT_FIELD) or "").strip():
        return True, "honeypot"

    ts_raw = form.get("ts")
    if ts_raw is None or ts_raw == "":
        return True, "ts_missing"

    try:
        ts = int(ts_raw)
    except (TypeError, ValueError):
        return True, "ts_malformed"

    if ts <= 0:
        return True, "ts_nonpositive"

    delta = now_ts - ts
    if delta < _MIN_FILL_SECONDS:
        return True, "ts_too_fast"
    if delta > _MAX_FILL_SECONDS:
        return True, "ts_stale"

    return False, None


# ---------------------------------------------------------------------------
# Page content
# ---------------------------------------------------------------------------


def contact_page_content() -> Div:
    mailto = f"mailto:{CONTACT_EMAIL}"
    return StaticPage(
        "Let's talk.",
        # Centered welcome sets the tone before the cards.
        PageSection(
            "Direct Contact",
            "Pick the channel that fits your question — we route inquiries to the right team.",
            variant="centered",
            eyebrow="01 — Reach Us",
        ),
        # Contact cards with rotating variants for visual variety.
        Div(
            LinkCard(
                "Sales & Pricing",
                "Questions about plans, features, or pricing? Our sales team can help.",
                "Contact Sales →",
                mailto,
                variant="accent",
            ),
            LinkCard(
                "Support",
                "Need help using ViralVibes? Have a bug to report? We're here to help.",
                "Contact Support →",
                mailto,
                variant="tinted",
            ),
            LinkCard(
                "Privacy & Legal",
                "Questions about our privacy policy, data handling, or legal matters?",
                "Contact Legal →",
                mailto,
                variant="default",
            ),
            cls="grid grid-cols-1 md:grid-cols-3 gap-5",
        ),
        # Numbered section anchors the form as the page's main beat.
        PageSection(
            "Send Us a Message",
            "Fill out the form below and we'll get back to you as soon as possible. "
            "Most inquiries receive a reply within 24 business hours.",
            variant="numbered",
            number="02",
        ),
        contact_form_region(),
        # FAQ block — accent heading sets it apart from the form.
        PageSection(
            "Frequently Asked Questions",
            "Quick answers to the questions we hear most often.",
            variant="accent",
            eyebrow="03 — FAQ",
        ),
        Div(
            FAQCard(
                "How quickly do you respond?",
                "We aim to respond to all inquiries within 24 business hours. Sales questions are "
                "typically answered within 2–4 hours during business hours.",
            ),
            FAQCard(
                "Do you offer phone support?",
                "Currently, we support inquiries via email. For urgent sales questions, mention "
                "'URGENT' in your email and we'll prioritize your inquiry.",
                variant="default",
            ),
            FAQCard(
                "Can I schedule a demo?",
                f"Yes! Contact us at {CONTACT_EMAIL} and mention 'DEMO REQUEST'. "
                "We offer custom walkthroughs for teams evaluating ViralVibes.",
            ),
            FAQCard(
                "What about partnerships?",
                "We're always interested in partnership opportunities — integrations, reselling, "
                f"or co-marketing. Email {CONTACT_EMAIL} to discuss collaboration ideas.",
                variant="default",
            ),
            cls="grid grid-cols-1 md:grid-cols-2 gap-4",
        ),
        subtitle="Have a question? We'd love to hear from you. Get in touch with our team.",
        eyebrow="Contact",
    )


# ---------------------------------------------------------------------------
# POST handler
# ---------------------------------------------------------------------------


async def post_contact(req, sess) -> Div:
    """Process a contact-form submission. Always returns the form-region fragment.

    Flow:
        1. Read form + spam-control fields.
        2. Honeypot / timestamp-floor → silently return success.
        3. Validate fields → re-render form with inline errors on failure.
        4. Per-IP rate-limit → return form with banner on excess.
        5. Insert into ``contact_inquiries`` (PII lands here, not in logs).
        6. Log a masked record at INFO for runtime triage.
        7. Return the success fragment.

    The caller (``main.py``) is responsible for wrapping the fragment in
    page chrome when the request was a non-HTMX full-page POST.
    """
    form = await req.form()
    now_ts = int(time.time())

    values = {
        "name": (form.get("name") or "").strip(),
        "email": (form.get("email") or "").strip(),
        "inquiry_type": (form.get("inquiry_type") or "general").strip(),
        "message": (form.get("message") or "").strip()[:_MAX_MESSAGE_CHARS],
    }

    # 1. Bot? Pretend we accepted it so the script doesn't retry/learn.
    #    A ``ts_missing`` reason almost always means a template regression
    #    (the hidden field disappeared from contact_form) rather than a
    #    real bot — log at WARNING so it's visible in alerts; everything
    #    else stays at INFO.
    is_bot, reason = _looks_like_bot(form, now_ts)
    if is_bot:
        if reason == "ts_missing":
            logger.warning(
                "contact form: silent-drop reason=%s — likely template "
                "regression dropped the hidden ts field",
                reason,
            )
        else:
            logger.info("contact form: silent-drop reason=%s", reason)
        return _success_fragment()

    # 2. Field validation — re-render with inline errors if anything is off.
    errors = _validate(values)
    if errors:
        return contact_form_region(
            values=values,
            errors=errors,
            banner="Please correct the highlighted fields below.",
        )

    # 3. Per-IP rate limit. Counted after validation so typos don't burn quota.
    client_ip = _get_client_ip(req)
    if (
        client_ip
        and count_contact_inquiries_from_ip(client_ip, within_minutes=60) >= _RATE_LIMIT_PER_HOUR
    ):
        logger.warning(
            "contact form: rate-limited %s (>= %d/hr)",
            client_ip,
            _RATE_LIMIT_PER_HOUR,
        )
        return _rate_limited_fragment(values)

    # 4. Persist. The DB row is the system of record; the email forward
    #    (PR 2c) is a notification on top of it. If insert returns None
    #    the row didn't land — log it and still show success so a transient
    #    DB hiccup doesn't block the visitor.
    user_agent = (req.headers.get("user-agent") or "")[:500] or None
    inquiry_id = insert_contact_inquiry(
        name=values["name"],
        email=values["email"],
        inquiry_type=values["inquiry_type"],
        message=values["message"],
        client_ip=client_ip,
        user_agent=user_agent,
    )

    if inquiry_id is None:
        logger.error(
            "contact form: insert returned None for %s — submission lost",
            _mask_email(values["email"]),
        )
    else:
        logger.info(
            "contact form submission id=%d: type=%s name_present=%s email=%s chars=%d",
            inquiry_id,
            values["inquiry_type"],
            bool(values["name"]),
            _mask_email(values["email"]),
            len(values["message"]),
        )

    return _success_fragment()
