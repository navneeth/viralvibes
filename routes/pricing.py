"""Pricing page — Free / Pro / Agency tiers."""

from fasthtml.common import *
from monsterui.all import *

# ---------------------------------------------------------------------------
# Pricing data — single source of truth for all tier details
# ---------------------------------------------------------------------------
_PRICING = {
    "pro": {"monthly": "$19", "annual": "$15", "annual_total": "$180", "shortlists": "5 \u00d7 25"},
    "agency": {"monthly": "$49", "annual": "$39", "annual_total": "$468"},
    "free": {"dashboards": "3"},
}

# ---------------------------------------------------------------------------
# Billing toggle JS — annual is the default selected state
# ---------------------------------------------------------------------------
_TOGGLE_SCRIPT = Script(
    f"""
(function () {{
    var annual = true;
    var data = {{
        'pro-price':      ['{_PRICING["pro"]["annual"]}', '{_PRICING["pro"]["monthly"]}'],
        'agency-price':   ['{_PRICING["agency"]["annual"]}', '{_PRICING["agency"]["monthly"]}'],
        'pro-billing':    ['{_PRICING["pro"]["annual_total"]} billed annually', 'Billed monthly'],
        'agency-billing': ['{_PRICING["agency"]["annual_total"]} billed annually', 'Billed monthly'],
    }};
    function apply(announce) {{
        var i = annual ? 0 : 1;
        for (var id in data) {{ document.getElementById(id).textContent = data[id][i]; }}
        var thumb  = document.getElementById('toggle-thumb');
        var toggle = document.getElementById('billing-toggle');
        thumb.style.transform = annual ? 'translateX(0)' : 'translateX(-24px)';
        toggle.classList.toggle('bg-red-500', annual);
        toggle.classList.toggle('bg-muted-foreground/40', !annual);
        toggle.setAttribute('aria-pressed', String(annual));
        // Only update the sr-only live region on explicit user interaction
        if (announce) {{
            var label = document.getElementById('billing-period-text');
            if (label) label.textContent = annual ? 'Annual billing selected' : 'Monthly billing selected';
        }}
        // Update hidden interval fields in checkout forms
        var interval = annual ? 'year' : 'month';
        document.querySelectorAll('input[name="interval"]').forEach(function(el) {{
            el.value = interval;
        }});
    }}
    window.toggleBilling = function () {{ annual = !annual; apply(true); }};
    apply();
}}());
"""
)


# ---------------------------------------------------------------------------
# Shared micro-components
# ---------------------------------------------------------------------------


def _check(color: str = "green") -> UkIcon:
    return UkIcon("check", cls=f"w-4 h-4 text-{color}-500 mt-0.5 flex-shrink-0")


def _no() -> Span:
    return Span("—", cls="text-muted-foreground text-sm")


def _feature(text: str, bold: bool = False, color: str = "green") -> Li:
    return Li(
        Div(
            _check(color),
            Span(
                text,
                cls=f"text-sm {'text-foreground font-semibold' if bold else 'text-muted-foreground'}",
            ),
            cls="flex items-start gap-2.5",
        ),
    )


# ---------------------------------------------------------------------------
# Pricing cards
# ---------------------------------------------------------------------------


def _free_card() -> Div:
    return Div(
        Div(
            P(
                "Free",
                cls="text-sm font-semibold text-muted-foreground uppercase tracking-wider mb-2",
            ),
            Div(
                Span("$0", cls="text-5xl font-extrabold text-foreground"),
                Span("/mo", cls="text-muted-foreground text-sm ml-1 mb-2"),
                cls="flex items-end",
            ),
            P("No credit card required", cls="text-xs text-muted-foreground mt-1"),
            cls="mb-6",
        ),
        A(
            "Get started free",
            href="/login",
            cls=(
                "block w-full text-center font-semibold text-sm py-2.5 mb-8 rounded-lg "
                "border-2 border-border text-foreground hover:border-foreground transition-colors"
            ),
        ),
        Ul(
            _feature("Full creator lists & rankings"),
            _feature("Individual creator profiles"),
            _feature("Playlist analysis (unlimited)"),
            _feature("3 saved dashboards"),
            cls="space-y-3",
        ),
        cls="bg-background rounded-2xl border border-border p-8 flex flex-col h-full",
    )


def _pro_card() -> Div:
    return Div(
        # "Most popular" badge positioned above the card border
        Div(
            Span(
                "Most popular",
                cls="px-4 py-1 bg-red-500 text-white text-xs font-bold uppercase tracking-wider rounded-full",
            ),
            cls="absolute -top-3.5 left-1/2 -translate-x-1/2 whitespace-nowrap",
        ),
        Div(
            P("Pro", cls="text-sm font-semibold text-red-600 uppercase tracking-wider mb-2"),
            Div(
                Span(id="pro-price", cls="text-5xl font-extrabold text-foreground"),
                Span("/mo", cls="text-muted-foreground text-sm ml-1 mb-2"),
                cls="flex items-end",
            ),
            P(id="pro-billing", cls="text-xs text-muted-foreground mt-1"),
            cls="mb-6",
        ),
        Form(
            Input(type="hidden", name="plan", value="pro"),
            Input(type="hidden", name="interval", value="year"),
            Button(
                "Start Pro free for 7 days",
                type="submit",
                cls=(ButtonT.primary, "block w-full text-center font-semibold text-sm py-2.5 mb-8"),
            ),
            method="post",
            action="/billing/checkout",
        ),
        Ul(
            _feature("Everything in Free", bold=True, color="red"),
            _feature("Unlimited saved dashboards", color="red"),
            _feature("CSV & JSON export", color="red"),
            _feature(f"Saved shortlists ({_PRICING['pro']['shortlists']} creators)", color="red"),
            _feature("Daily data refresh", color="red"),
            _feature("Rising star digest (weekly email)", color="red"),
            cls="space-y-3",
        ),
        cls=(
            "bg-background rounded-2xl p-8 flex flex-col h-full relative "
            "ring-2 ring-red-500 shadow-xl shadow-red-500/10"
        ),
    )


def _agency_card() -> Div:
    return Div(
        Div(
            P(
                "Agency",
                cls="text-sm font-semibold text-muted-foreground uppercase tracking-wider mb-2",
            ),
            Div(
                Span(id="agency-price", cls="text-5xl font-extrabold text-foreground"),
                Span("/mo", cls="text-muted-foreground text-sm ml-1 mb-2"),
                cls="flex items-end",
            ),
            P(id="agency-billing", cls="text-xs text-muted-foreground mt-1"),
            cls="mb-6",
        ),
        Form(
            Input(type="hidden", name="plan", value="agency"),
            Input(type="hidden", name="interval", value="year"),
            Button(
                "Start Agency free for 7 days",
                type="submit",
                cls=(
                    "block w-full text-center font-semibold text-sm py-2.5 mb-8 rounded-lg "
                    "border-2 border-foreground text-foreground "
                    "hover:bg-foreground hover:text-background transition-colors"
                ),
            ),
            method="post",
            action="/billing/checkout",
        ),
        Ul(
            _feature("Everything in Pro", bold=True),
            _feature("Unlimited shortlists & creators"),
            _feature("Team seats (up to 5)"),
            _feature("Client-shareable report links"),
            _feature("Bulk export"),
            _feature("API access (coming soon)"),
            _feature("Priority support"),
            cls="space-y-3",
        ),
        cls="bg-background rounded-2xl border border-border p-8 flex flex-col h-full",
    )


# ---------------------------------------------------------------------------
# Billing period toggle UI
# ---------------------------------------------------------------------------


def _billing_toggle() -> Div:
    return Div(
        Span("Monthly", cls="text-sm font-medium text-muted-foreground"),
        Button(
            Span(
                id="toggle-thumb",
                cls="absolute top-0.5 right-0.5 w-5 h-5 bg-background rounded-full shadow transition-transform",
            ),
            id="billing-toggle",
            onclick="toggleBilling()",
            cls="relative w-12 h-6 bg-red-500 rounded-full transition-colors focus:outline-none ml-3",
            type="button",
            aria_label="Toggle billing period",
            aria_pressed="true",
        ),
        Div(
            Span("Annual", cls="text-sm font-semibold text-foreground"),
            Span(
                "Save 20%",
                cls=(
                    "ml-1.5 px-1.5 py-0.5 text-xs font-semibold rounded-full "
                    "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400"
                ),
            ),
            cls="flex items-center ml-3",
        ),
        # Visually-hidden live region — announces current state to screen readers only
        Span(
            id="billing-period-text",
            aria_live="polite",
            cls="sr-only",
        ),
        cls="flex items-center justify-center mb-14",
    )


# ---------------------------------------------------------------------------
# Feature comparison table
# ---------------------------------------------------------------------------


def _yes(color: str = "green") -> Div:
    return Div(UkIcon("check", cls=f"w-4 h-4 text-{color}-500 mx-auto"))


def _comparison_table() -> Div:
    def row(label, free, pro, agency):
        return Tr(
            Td(label, cls="py-3 pr-6 text-muted-foreground text-sm"),
            Td(free, cls="py-3 px-4 text-center"),
            Td(pro, cls="py-3 px-4 text-center"),
            Td(agency, cls="py-3 px-4 text-center"),
        )

    rows = [
        row("Creator lists & rankings", _yes(), _yes("red"), _yes()),
        row("Creator profiles", _yes(), _yes("red"), _yes()),
        row("Playlist analysis", _yes(), _yes("red"), _yes()),
        row(
            "Saved dashboards",
            Span(_PRICING["free"]["dashboards"], cls="text-xs text-muted-foreground"),
            Span("Unlimited", cls="text-xs font-semibold text-red-500"),
            Span("Unlimited", cls="text-xs"),
        ),
        row("CSV & JSON export", _no(), _yes("red"), _yes()),
        row(
            "Saved shortlists",
            _no(),
            Span(_PRICING["pro"]["shortlists"], cls="text-xs font-semibold text-red-500"),
            Span("Unlimited", cls="text-xs"),
        ),
        row("Daily data refresh", _no(), _yes("red"), _yes()),
        row("Rising star digest", _no(), _yes("red"), _yes()),
        row("Team seats", _no(), _no(), Span("Up to 5", cls="text-xs")),
        row("Client-shareable report links", _no(), _no(), _yes()),
        row("Bulk export", _no(), _no(), _yes()),
        row("API access", _no(), _no(), Span("Coming soon", cls="text-xs text-muted-foreground")),
    ]

    return Div(
        H2("Compare plans", cls="text-2xl font-bold text-foreground text-center mb-8"),
        Div(
            Table(
                Thead(
                    Tr(
                        Th(
                            "Feature",
                            cls="text-left py-3 pr-6 text-muted-foreground font-medium text-sm w-1/2",
                        ),
                        Th(
                            "Free",
                            cls="text-center py-3 px-4 text-foreground font-semibold text-sm",
                        ),
                        Th("Pro", cls="text-center py-3 px-4 text-red-600 font-semibold text-sm"),
                        Th(
                            "Agency",
                            cls="text-center py-3 px-4 text-foreground font-semibold text-sm",
                        ),
                        cls="border-b-2 border-border",
                    ),
                ),
                Tbody(*rows, cls="divide-y divide-border"),
                cls="w-full border-collapse",
            ),
            cls="overflow-x-auto",
        ),
        cls="mt-20",
    )


# ---------------------------------------------------------------------------
# Bottom CTA strip
# ---------------------------------------------------------------------------


def _bottom_cta() -> Div:
    return Div(
        P(
            "Still deciding?",
            cls="text-xs font-semibold text-muted-foreground uppercase tracking-widest mb-3",
        ),
        H3(
            "Start free \u2014 no credit card required.",
            cls="text-2xl font-bold text-foreground mb-2",
        ),
        P(
            "Browse 1M+ creators, run playlist analysis, and upgrade only when you\u2019re ready.",
            cls="text-muted-foreground mb-6",
        ),
        A(
            UkIcon("arrow-right", cls="w-4 h-4"),
            Span("Get started free"),
            href="/login",
            cls="inline-flex items-center gap-2 px-6 py-3 rounded-lg bg-red-500 hover:bg-red-600 text-white font-semibold transition-colors",
        ),
        cls="mt-20 bg-muted rounded-2xl px-8 py-10 text-center border border-border",
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def pricing_page_content() -> Div:
    """Full pricing page body — passed directly to Titled() route handler."""
    return Div(
        # ── Page header ────────────────────────────────────────────────────
        Div(
            P("Pricing", cls="text-sm font-semibold text-red-600 uppercase tracking-widest mb-3"),
            H1(
                "Simple, transparent pricing",
                cls="text-5xl font-extrabold text-foreground mb-4",
            ),
            P(
                "Start free. Upgrade when campaigns get serious.",
                cls="text-xl text-muted-foreground max-w-xl mx-auto",
            ),
            cls="text-center mb-12",
        ),
        # ── Annual / monthly toggle ────────────────────────────────────────
        _billing_toggle(),
        # ── Pricing cards ─────────────────────────────────────────────────
        # items-start so the Pro card badge overflow doesn't clip siblings
        Div(
            _free_card(),
            _pro_card(),
            _agency_card(),
            cls="grid grid-cols-1 md:grid-cols-3 gap-8 items-start",
        ),
        # ── Trust strip ───────────────────────────────────────────────────
        P(
            "All plans include a 7-day free trial \u00b7 Cancel anytime \u00b7 No hidden fees",
            cls="text-center text-sm text-muted-foreground mt-10",
        ),
        # ── Feature comparison table ──────────────────────────────────────
        _comparison_table(),
        # ── Bottom CTA ────────────────────────────────────────────────────
        _bottom_cta(),
        # ── Billing toggle script ─────────────────────────────────────────
        _TOGGLE_SCRIPT,
        cls="max-w-5xl mx-auto px-4 py-24",
    )
