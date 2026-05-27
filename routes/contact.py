"""
Contact page — inquiry form, support channels, and FAQ.
"""

from fasthtml.common import *
from monsterui.all import *

from components.page_layout import (
    FAQCard,
    LinkCard,
    PageSection,
    StaticPage,
)
from constants import CONTACT_EMAIL


# ---------------------------------------------------------------------------
# Contact Form
# ---------------------------------------------------------------------------

_INPUT_CLS = (
    "w-full px-3 py-2.5 border border-border rounded-md bg-background text-foreground "
    "transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 "
    "focus:border-transparent placeholder:text-muted-foreground/60"
)
_LABEL_CLS = "block text-xs font-mono uppercase tracking-[0.14em] text-muted-foreground mb-2"


def _field(label_text: str, control) -> Div:
    """A label + control pair with consistent spacing."""
    attrs = getattr(control, "attrs", None) or {}
    target = attrs.get("name") or attrs.get("id") or ""
    return Div(
        Label(label_text, For=target, cls=_LABEL_CLS),
        control,
        cls="mb-5",
    )


def contact_form() -> Form:
    """Contact form for inquiries."""
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
                    cls=_INPUT_CLS,
                    placeholder="Your name",
                ),
            ),
            _field(
                "Email",
                Input(
                    type="email",
                    id="email",
                    name="email",
                    required=True,
                    cls=_INPUT_CLS,
                    placeholder="you@example.com",
                ),
            ),
            cls="grid grid-cols-1 md:grid-cols-2 gap-x-5",
        ),
        _field(
            "Inquiry Type",
            Select(
                Option("General inquiry", value="general"),
                Option("Sales / Pricing question", value="sales"),
                Option("Product feedback", value="feedback"),
                Option("Support / Bug report", value="support"),
                Option("Partnership opportunity", value="partnership"),
                Option("Career / Jobs", value="careers"),
                id="inquiry_type",
                name="inquiry_type",
                cls=_INPUT_CLS,
            ),
        ),
        _field(
            "Message",
            Textarea(
                id="message",
                name="message",
                required=True,
                rows="5",
                cls=f"{_INPUT_CLS} resize-none",
                placeholder="Tell us more…",
            ),
        ),
        Button(
            "Send Message →",
            type="submit",
            cls=(
                "w-full px-4 py-2.5 bg-foreground text-background font-semibold rounded-md "
                "transition-transform hover:-translate-y-px hover:bg-foreground/90 "
                "focus:outline-none focus:ring-2 focus:ring-blue-500"
            ),
        ),
        method="post",
        action="/contact",
        cls="space-y-1",
    )


# ---------------------------------------------------------------------------
# Contact Page Content
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
        contact_form(),
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


def post_contact(req, sess):
    """Handle contact form submission.

    Called from main.py's contact route handler.
    Extracts form data and processes the submission.
    """
    # TODO: Implement email sending or form processing
    # For now, just return a success message
    # Form data would be extracted via await req.form() if needed

    return Div(
        Div(
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
                "We've received your message and will get back to you as soon as possible.",
                cls="text-muted-foreground mb-6",
            ),
            A(
                "← Back to contact page",
                href="/contact",
                cls="inline-flex items-center text-blue-600 hover:text-blue-700 font-semibold",
            ),
            cls="max-w-xl mx-auto px-4 py-24 text-center",
        ),
    )
