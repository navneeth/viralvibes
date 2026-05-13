"""
Contact page — inquiry form, support channels, and FAQ.
"""

from fasthtml.common import *
from monsterui.all import *


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _contact_page(title: str, *sections) -> Div:
    """Wraps a contact page in a consistent container."""
    return Div(
        # Page header
        Div(
            H1(title, cls="text-3xl font-bold text-foreground mb-2"),
            P(
                "Have a question? We'd love to hear from you. Get in touch with our team.",
                cls="text-sm text-muted-foreground",
            ),
            cls="mb-10 pb-8 border-b border-border",
        ),
        # Sections
        Div(*sections, cls="space-y-12"),
        cls="max-w-3xl mx-auto px-4 py-16",
    )


def _section(heading: str, *paragraphs) -> Div:
    return Div(
        *([H2(heading, cls="text-xl font-semibold text-foreground mb-3")] if heading else []),
        *[P(text, cls="text-muted-foreground leading-relaxed mb-3") for text in paragraphs],
    )


def _contact_method(title: str, description: str, link_text: str, link: str) -> Div:
    """Creates a contact method card."""
    return Div(
        H3(title, cls="text-lg font-semibold text-foreground mb-2"),
        P(description, cls="text-muted-foreground leading-relaxed mb-3"),
        A(link_text, href=link, cls="text-blue-600 hover:text-blue-700 font-semibold"),
        cls="p-4 border border-border rounded-lg",
    )


# ---------------------------------------------------------------------------
# Contact Form
# ---------------------------------------------------------------------------


def contact_form() -> Form:
    """Contact form for inquiries."""
    return Form(
        Div(
            Label("Name", For="name", cls="block text-sm font-medium text-foreground mb-1"),
            Input(
                type="text",
                id="name",
                name="name",
                required=True,
                cls="w-full px-3 py-2 border border-border rounded-md bg-background text-foreground "
                "focus:outline-none focus:ring-2 focus:ring-blue-500",
                placeholder="Your name",
            ),
            cls="mb-4",
        ),
        Div(
            Label("Email", For="email", cls="block text-sm font-medium text-foreground mb-1"),
            Input(
                type="email",
                id="email",
                name="email",
                required=True,
                cls="w-full px-3 py-2 border border-border rounded-md bg-background text-foreground "
                "focus:outline-none focus:ring-2 focus:ring-blue-500",
                placeholder="your@email.com",
            ),
            cls="mb-4",
        ),
        Div(
            Label(
                "Inquiry Type",
                For="inquiry_type",
                cls="block text-sm font-medium text-foreground mb-1",
            ),
            Select(
                Option("General inquiry", value="general"),
                Option("Sales / Pricing question", value="sales"),
                Option("Product feedback", value="feedback"),
                Option("Support / Bug report", value="support"),
                Option("Partnership opportunity", value="partnership"),
                Option("Career / Jobs", value="careers"),
                id="inquiry_type",
                name="inquiry_type",
                cls="w-full px-3 py-2 border border-border rounded-md bg-background text-foreground "
                "focus:outline-none focus:ring-2 focus:ring-blue-500",
            ),
            cls="mb-4",
        ),
        Div(
            Label("Message", For="message", cls="block text-sm font-medium text-foreground mb-1"),
            Textarea(
                id="message",
                name="message",
                required=True,
                rows="5",
                cls="w-full px-3 py-2 border border-border rounded-md bg-background text-foreground "
                "focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none",
                placeholder="Tell us more...",
            ),
            cls="mb-4",
        ),
        Button(
            "Send Message",
            type="submit",
            cls="w-full px-4 py-2 bg-blue-600 text-white font-medium rounded-md "
            "hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500",
        ),
        method="post",
        action="/contact",
        cls="space-y-4",
    )


# ---------------------------------------------------------------------------
# Contact Page Content
# ---------------------------------------------------------------------------


def contact_page_content() -> Div:
    return _contact_page(
        "Get In Touch",
        _section(
            "Direct Contact",
            "Have questions? Reach out to our team directly:",
        ),
        Div(
            _contact_method(
                "Sales & Pricing",
                "Questions about plans, features, or pricing? Our sales team can help.",
                "Contact Sales →",
                "mailto:sales@viralvibes.app",
            ),
            _contact_method(
                "Support",
                "Need help using ViralVibes? Have a bug to report? We're here to help.",
                "Contact Support →",
                "mailto:support@viralvibes.app",
            ),
            _contact_method(
                "Privacy & Legal",
                "Questions about our privacy policy, data handling, or legal matters?",
                "Contact Legal →",
                "mailto:privacy@viralvibes.app",
            ),
            cls="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8",
        ),
        _section(
            "Send Us a Message",
            "Fill out the form below and we'll get back to you as soon as possible.",
        ),
        contact_form(),
        _section(
            "Frequently Asked Questions",
        ),
        Div(
            Div(
                H3("How quickly do you respond?", cls="text-lg font-semibold text-foreground mb-2"),
                P(
                    "We aim to respond to all inquiries within 24 business hours. Sales questions are typically "
                    "answered within 2-4 hours during business hours.",
                    cls="text-muted-foreground leading-relaxed",
                ),
                cls="p-4 border border-border rounded-lg",
            ),
            Div(
                H3("Do you offer phone support?", cls="text-lg font-semibold text-foreground mb-2"),
                P(
                    "Currently, we support inquiries via email. For urgent sales questions, mention 'URGENT' in "
                    "your email and we'll prioritize your inquiry.",
                    cls="text-muted-foreground leading-relaxed",
                ),
                cls="p-4 border border-border rounded-lg",
            ),
            Div(
                H3("Can I schedule a demo?", cls="text-lg font-semibold text-foreground mb-2"),
                P(
                    "Yes! Contact our sales team at sales@viralvibes.app and mention 'DEMO REQUEST'. "
                    "We offer custom walkthroughs for teams evaluating ViralVibes.",
                    cls="text-muted-foreground leading-relaxed",
                ),
                cls="p-4 border border-border rounded-lg",
            ),
            Div(
                H3("What about partnerships?", cls="text-lg font-semibold text-foreground mb-2"),
                P(
                    "We're always interested in partnership opportunities — integrations, reselling, or co-marketing. "
                    "Email partnerships@viralvibes.app to discuss collaboration ideas.",
                    cls="text-muted-foreground leading-relaxed",
                ),
                cls="p-4 border border-border rounded-lg",
            ),
            cls="space-y-4",
        ),
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
            H2("Thank you for reaching out!", cls="text-2xl font-bold text-foreground mb-2"),
            P(
                "We've received your message and will get back to you as soon as possible.",
                cls="text-muted-foreground mb-4",
            ),
            A(
                "Back to contact page",
                href="/contact",
                cls="text-blue-600 hover:text-blue-700 font-semibold",
            ),
            cls="max-w-3xl mx-auto px-4 py-16",
        ),
    )
