"""Editors' Shortlist rail — curated entry point into ``/creators/top``.

A horizontal card rail surfacing the five A+ category landing pages plus
the global A+ shortlist. Designed to live near the top of the two main
discovery surfaces (``/creators`` and ``/lists``) so anonymous visitors
and returning users alike have a one-click path into the editorial cut.

Design notes
------------
* Single accent treatment (no per-card colour) — restraint over noise.
* Mono numerals for counts so they read as data, not marketing.
* Horizontal scroll on mobile via ``overflow-x-auto`` so the rail never
  reflows into a tall column on small screens.
* Counts are optional. When omitted the cards still render with a
  generic CTA, which lets pages that don't want to take the small count
  query opt out cleanly.
"""

from __future__ import annotations

from fasthtml.common import A, Div, H3, P, Span

from utils import format_number

__all__ = ["EditorsShortlistRail"]

# Card order: "All A+" first as the broadest entry point, then categories
# in descending A+ population (matches what users will see in counts).
_RAIL_ITEMS: list[tuple[str | None, str, str]] = [
    # (slug, label, href)
    (None, "All A+ Tier", "/creators/top"),
    ("gaming", "Gaming", "/creators/top/gaming"),
    ("entertainment", "Entertainment", "/creators/top/entertainment"),
    ("music", "Music", "/creators/top/music"),
    ("education", "Education", "/creators/top/education"),
    ("howto-style", "Howto & Style", "/creators/top/howto-style"),
]


def _rail_card(
    *,
    label: str,
    href: str,
    count: int | None,
    is_all: bool,
) -> A:
    """Single card in the rail. Anchor element so the entire surface is clickable."""
    count_block = (
        Div(
            Span(
                format_number(count),
                cls="block text-3xl font-mono font-semibold text-foreground tabular-nums",
            ),
            P(
                "A+ rated creators",
                cls="text-xs text-muted-foreground mt-1",
            ),
            cls="mt-4",
        )
        if count is not None
        else Div(
            P(
                "View top creators",
                cls="text-sm text-foreground/80 mt-4",
            ),
        )
    )

    eyebrow = "Editors' Pick" if is_all else "Category"

    return A(
        Div(
            Span(
                eyebrow.upper(),
                cls=(
                    "text-[10px] font-mono tracking-[0.2em] "
                    "text-muted-foreground/70"
                ),
            ),
            H3(
                label,
                cls="text-lg font-semibold text-foreground mt-2 leading-tight",
            ),
            count_block,
            Div(
                Span(
                    "Browse →",
                    cls=(
                        "text-xs font-medium text-muted-foreground "
                        "group-hover:text-primary transition-colors"
                    ),
                ),
                cls="mt-6 pt-4 border-t border-border/60",
            ),
            cls="h-full flex flex-col",
        ),
        href=href,
        cls=(
            "group block snap-start shrink-0 "
            "w-[220px] sm:w-[240px] "
            "rounded-xl border border-border bg-card "
            "p-5 no-underline "
            "transition-all duration-200 "
            "hover:border-primary/40 hover:bg-primary/[0.03] "
            "hover:-translate-y-0.5 hover:shadow-sm"
        ),
    )


def EditorsShortlistRail(
    counts: dict[str, int] | None = None,
    *,
    headline: str = "Editors' Shortlist",
    subhead: str | None = "The A+ engagement tier — hand-graded, ranked by reach.",
    see_all_label: str = "See all A+ creators →",
    see_all_href: str = "/creators/top",
) -> Div:
    """Render the rail. Pass ``counts`` from ``get_aplus_category_counts()``.

    The rail intentionally returns a plain ``Div`` (no Container) so callers
    control the surrounding padding and section rhythm.
    """
    if counts is None:
        counts = {}

    cards = [
        _rail_card(
            label=label,
            href=href,
            count=counts.get(slug or "all"),
            is_all=slug is None,
        )
        for slug, label, href in _RAIL_ITEMS
    ]

    return Div(
        # Header strip — H2-equivalent + optional see-all CTA on one row.
        Div(
            Div(
                Span(
                    "EDITORIAL",
                    cls=(
                        "block text-[10px] font-mono tracking-[0.22em] "
                        "text-muted-foreground/70 mb-2"
                    ),
                ),
                H3(
                    headline,
                    cls="text-xl sm:text-2xl font-bold text-foreground tracking-tight",
                ),
                (
                    P(subhead, cls="text-sm text-muted-foreground mt-1")
                    if subhead
                    else None
                ),
            ),
            A(
                see_all_label,
                href=see_all_href,
                cls=(
                    "hidden sm:inline-flex items-center text-sm font-medium "
                    "text-muted-foreground hover:text-primary transition-colors "
                    "no-underline whitespace-nowrap"
                ),
            ),
            cls="flex items-end justify-between gap-4 mb-5",
        ),
        # Rail itself — horizontal scroll with snap.
        Div(
            *cards,
            cls=(
                "flex gap-4 overflow-x-auto snap-x snap-mandatory "
                "-mx-4 px-4 sm:mx-0 sm:px-0 pb-2"
            ),
        ),
        cls="my-10 sm:my-12",
    )
