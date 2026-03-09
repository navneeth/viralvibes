"""
Creator Lists view — curated, pre-filtered creator rankings.
Each tab is a different lens on the creators database.
"""

from fasthtml.common import *
from monsterui.all import *


# ─────────────────────────────────────────────────────────────────────────────
# Tab definitions
# Each entry: (id, label, icon, description, coming_soon)
# ─────────────────────────────────────────────────────────────────────────────
LISTS_TABS = [
    (
        "top-rated",
        "Top Rated",
        "star",
        "Creators with the highest quality scores — consistent upload cadence, strong engagement, and growing audiences.",
        False,
    ),
    (
        "most-active",
        "Most Active",
        "zap",
        "Channels uploading the most frequently. Great for finding creators who publish consistently.",
        False,
    ),
    (
        "by-country",
        "By Country",
        "globe",
        "Browse creators grouped by their home country. Discover regional talent around the world.",
        False,
    ),
    (
        "rising",
        "Rising Stars",
        "trending-up",
        "Channels with the fastest subscriber and view growth over the past 30 days.",
        False,
    ),
    (
        "veterans",
        "Veterans",
        "award",
        "Channels that have been publishing for 10+ years. Proven, durable creators.",
        False,
    ),
    (
        "new-channels",
        "New Channels",
        "sparkles",
        "Channels created in the last year. Early-stage creators to watch.",
        True,
    ),
]


def _placeholder_content(description: str, coming_soon: bool = False):
    """Renders the placeholder body for a single tab panel."""

    if coming_soon:
        return Div(
            Div(
                UkIcon("clock", cls="size-10 text-muted-foreground mb-3"),
                H3("Coming Soon", cls="text-lg font-semibold text-foreground mb-1"),
                P(
                    description,
                    cls="text-sm text-muted-foreground max-w-sm text-center",
                ),
                cls="flex flex-col items-center justify-center py-20 px-6",
            ),
            cls="min-h-64",
        )

    return Div(
        # Top bar — filter hint + count placeholder
        DivFullySpaced(
            P(
                description,
                cls="text-sm text-muted-foreground max-w-xl",
            ),
            Div(
                Span("— creators", cls="text-sm font-medium text-foreground"),
                cls="shrink-0 hidden sm:block",
            ),
            cls="mb-6 gap-4 flex-col sm:flex-row items-start sm:items-center",
        ),
        # Skeleton rows — 8 placeholder cards
        Div(
            *[_skeleton_row(i) for i in range(8)],
            cls="space-y-3",
        ),
        cls="min-h-64",
    )


def _skeleton_row(index: int):
    """A single animated skeleton placeholder for a creator row."""
    # Stagger opacity slightly so they don't pulse in perfect unison
    opacity = (
        "opacity-100"
        if index % 3 == 0
        else ("opacity-80" if index % 3 == 1 else "opacity-60")
    )
    return Div(
        # Rank number
        Div(
            Span(str(index + 1), cls="text-sm font-bold text-muted-foreground"),
            cls="w-8 shrink-0 text-center",
        ),
        # Avatar skeleton
        Div(cls="size-10 rounded-full bg-muted animate-pulse shrink-0"),
        # Name + meta skeletons
        Div(
            Div(cls="h-4 bg-muted animate-pulse rounded w-36 mb-1.5"),
            Div(cls="h-3 bg-muted animate-pulse rounded w-24"),
            cls="flex-1 min-w-0",
        ),
        # Stats skeletons (hidden on small screens)
        Div(
            Div(cls="h-4 bg-muted animate-pulse rounded w-16"),
            cls="hidden sm:block shrink-0",
        ),
        Div(
            Div(cls="h-4 bg-muted animate-pulse rounded w-12"),
            cls="hidden md:block shrink-0",
        ),
        # Grade badge skeleton
        Div(cls="h-6 bg-muted animate-pulse rounded-full w-8 shrink-0"),
        cls=f"flex items-center gap-3 p-3 rounded-xl border border-border bg-background {opacity}",
    )


def _tab_li(tab_id: str, label: str, icon: str, is_active: bool):
    """Renders a single tab <li> with icon + label."""
    active_cls = "uk-active" if is_active else ""
    return Li(
        A(
            UkIcon(icon, cls="size-4 shrink-0"),
            Span(label, cls="hidden sm:inline"),
            Span(label[0], cls="sm:hidden font-medium"),  # Initial on mobile
            href="#",
            cls="flex items-center gap-1.5 text-sm font-medium",
        ),
        cls=active_cls,
    )


def render_lists_page(active_tab: str = "top-rated") -> FT:
    """
    Renders the full /lists page with tabbed creator list navigation.
    Tabs use UIkit's switcher for zero-JS panel switching.
    """

    tab_items = []
    panel_items = []

    # Normalize active_tab to a known tab id; fall back to the first tab if invalid.
    valid_tab_ids = [tab_id for tab_id, *_ in LISTS_TABS]
    normalized_active_tab = (
        active_tab if active_tab in valid_tab_ids else valid_tab_ids[0]
    )

    for i, (tab_id, label, icon, description, coming_soon) in enumerate(LISTS_TABS):
        is_active = tab_id == normalized_active_tab
        tab_items.append(_tab_li(tab_id, label, icon, is_active))
        panel_items.append(Li(_placeholder_content(description, coming_soon)))

    return Div(
        # ── Page header ────────────────────────────────────────────────────────
        Div(
            H1(
                "Creator Lists",
                cls="text-2xl sm:text-3xl font-bold text-foreground",
            ),
            P(
                "Curated rankings of YouTube creators across different dimensions.",
                cls="text-muted-foreground mt-1 text-sm sm:text-base",
            ),
            cls="mb-6 pt-6",
        ),
        # ── Tab bar (horizontally scrollable on mobile) ────────────────────────
        Div(
            TabContainer(
                *tab_items,
                uk_switcher="connect: #lists-panels; animation: uk-animation-fade",
                alt=True,
                cls="flex-nowrap overflow-x-auto",
            ),
            cls="overflow-x-auto -mx-4 px-4 sm:mx-0 sm:px-0",
        ),
        # ── Tab panels ─────────────────────────────────────────────────────────
        Ul(
            *panel_items,
            id="lists-panels",
            cls="uk-switcher mt-6",
        ),
        cls="max-w-4xl mx-auto px-4 pb-16",
    )
