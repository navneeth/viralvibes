"""
Static page layout primitives — with visual variants.

Shared building blocks for marketing/info pages (about, contact, legal,
privacy, terms, etc.). Each primitive supports several ``variant`` styles
so callers can produce editorial rhythm instead of a uniform stack:

    PageSection variants
    --------------------
    default   classic H2 + paragraphs
    split     heading left, body right (asymmetric editorial grid)
    accent    left vertical accent rule + tinted heading
    numbered  oversized display numeral, eyebrow above heading
    lead      first paragraph rendered as oversized pull-quote
    centered  narrow, centered column (good for spotlight callouts)

    InfoCard variants
    -----------------
    default   plain bordered card
    tinted    soft tinted background, no border
    accent    top accent rule
    minimal   no border, no background — just spacing

    FeatureGrid
    -----------
    Auto-rotates card variants across the grid so adjacent cards differ.
"""

from itertools import cycle
from typing import Iterable, Optional, Sequence, Union

from fasthtml.common import *


# ---------------------------------------------------------------------------
# Page wrapper
# ---------------------------------------------------------------------------


def StaticPage(
    title: str,
    *sections,
    subtitle: Optional[str] = None,
    eyebrow: Optional[str] = None,
    last_updated: Optional[str] = None,
    section_gap: str = "space-y-16",
    container_cls: str = "max-w-3xl mx-auto px-4 py-20",
) -> Div:
    """Centered, narrow-column page with an editorial header.

    The header gets a small mono eyebrow, a large gradient H1, an optional
    lede paragraph, and a short accent rule — instead of a plain underline.
    """
    if last_updated is not None:
        caption = P(
            Span("Last updated ", cls="text-muted-foreground"),
            Span(last_updated, cls="text-foreground font-medium"),
            cls="text-sm",
        )
    elif subtitle is not None:
        caption = P(
            subtitle,
            cls="text-base text-muted-foreground leading-relaxed max-w-2xl mt-3",
        )
    else:
        caption = None

    eyebrow_el = (
        P(
            eyebrow,
            cls="text-xs font-mono uppercase tracking-[0.18em] text-blue-600 mb-4",
        )
        if eyebrow
        else None
    )

    header_children = [
        c
        for c in (
            eyebrow_el,
            H1(
                title,
                cls=(
                    "text-4xl md:text-5xl font-bold tracking-tight "
                    "bg-gradient-to-br from-foreground via-foreground to-foreground/60 "
                    "bg-clip-text text-transparent"
                ),
            ),
            caption,
            # Short accent rule under header
            Div(cls="mt-8 h-px w-16 bg-gradient-to-r from-blue-600 to-transparent"),
        )
        if c is not None
    ]

    return Div(
        Div(*header_children, cls="mb-14"),
        Div(*sections, cls=section_gap),
        cls=container_cls,
    )


# ---------------------------------------------------------------------------
# Section primitives
# ---------------------------------------------------------------------------


# Shared heading styles
_H2_DEFAULT = "text-2xl font-semibold tracking-tight text-foreground mb-4"
_H2_ACCENT = "text-2xl font-semibold tracking-tight text-foreground mb-4"
_P_BODY = "text-muted-foreground leading-relaxed mb-3"
_P_LEAD = "text-xl md:text-2xl font-light text-foreground leading-snug mb-6"


def _render_paragraphs(paragraphs, cls=_P_BODY):
    return [P(text, cls=cls) for text in paragraphs]


def PageSection(
    heading: str,
    *paragraphs: str,
    variant: str = "default",
    eyebrow: Optional[str] = None,
    number: Optional[Union[int, str]] = None,
) -> Div:
    """A section with a heading and one or more paragraphs.

    Args:
        heading: Section heading text. Pass an empty string to omit.
        *paragraphs: Body paragraphs.
        variant: One of ``default``, ``split``, ``accent``, ``numbered``,
            ``lead``, ``centered``.
        eyebrow: Optional small uppercase label above the heading.
        number: Optional display number (used by the ``numbered`` variant
            and rendered as a large faded numeral).
    """
    if variant == "split":
        return _section_split(heading, paragraphs, eyebrow=eyebrow)
    if variant == "accent":
        return _section_accent(heading, paragraphs, eyebrow=eyebrow)
    if variant == "numbered":
        return _section_numbered(heading, paragraphs, number=number, eyebrow=eyebrow)
    if variant == "lead":
        return _section_lead(heading, paragraphs, eyebrow=eyebrow)
    if variant == "centered":
        return _section_centered(heading, paragraphs, eyebrow=eyebrow)
    return _section_default(heading, paragraphs, eyebrow=eyebrow)


def _eyebrow(text: Optional[str], cls_extra: str = "") -> Optional[P]:
    if not text:
        return None
    return P(
        text,
        cls=f"text-xs font-mono uppercase tracking-[0.18em] text-blue-600 mb-2 {cls_extra}",
    )


def _section_default(heading, paragraphs, eyebrow=None) -> Div:
    children = []
    eb = _eyebrow(eyebrow)
    if eb is not None:
        children.append(eb)
    if heading:
        children.append(H2(heading, cls=_H2_DEFAULT))
    children.extend(_render_paragraphs(paragraphs))
    return Div(*children)


def _section_split(heading, paragraphs, eyebrow=None) -> Div:
    """Heading column (4/12) + body column (8/12) — asymmetric editorial layout."""
    eb = _eyebrow(eyebrow)
    left_children = []
    if eb is not None:
        left_children.append(eb)
    if heading:
        left_children.append(H2(heading, cls=_H2_DEFAULT))
    return Div(
        Div(*left_children, cls="md:col-span-4"),
        Div(*_render_paragraphs(paragraphs), cls="md:col-span-8"),
        cls="grid grid-cols-1 md:grid-cols-12 gap-x-10 gap-y-4",
    )


def _section_accent(heading, paragraphs, eyebrow=None) -> Div:
    """Left vertical accent rule + slightly tinted background."""
    children = []
    eb = _eyebrow(eyebrow)
    if eb is not None:
        children.append(eb)
    if heading:
        children.append(H2(heading, cls=_H2_ACCENT))
    children.extend(_render_paragraphs(paragraphs))
    return Div(
        *children,
        cls=(
            "border-l-2 border-blue-600 pl-6 py-2 "
            "bg-gradient-to-r from-blue-50/50 to-transparent dark:from-blue-950/20 "
            "rounded-r-md"
        ),
    )


def _section_numbered(heading, paragraphs, number=None, eyebrow=None) -> Div:
    """Oversized display numeral to the left of the heading."""
    num_str = "" if number is None else str(number)
    eb = _eyebrow(eyebrow)
    right = []
    if eb is not None:
        right.append(eb)
    if heading:
        right.append(H2(heading, cls=_H2_DEFAULT))
    right.extend(_render_paragraphs(paragraphs))
    return Div(
        Div(
            num_str,
            cls=(
                "text-6xl md:text-7xl font-light tracking-tighter "
                "text-foreground/10 leading-none select-none md:col-span-2"
            ),
        ),
        Div(*right, cls="md:col-span-10"),
        cls="grid grid-cols-1 md:grid-cols-12 gap-x-6 items-start",
    )


def _section_lead(heading, paragraphs, eyebrow=None) -> Div:
    """First paragraph rendered as an oversized lede; remaining body normal."""
    eb = _eyebrow(eyebrow)
    children = []
    if eb is not None:
        children.append(eb)
    if heading:
        children.append(H2(heading, cls=_H2_DEFAULT))
    if paragraphs:
        children.append(P(paragraphs[0], cls=_P_LEAD))
        children.extend(_render_paragraphs(paragraphs[1:]))
    return Div(*children)


def _section_centered(heading, paragraphs, eyebrow=None) -> Div:
    """Narrow, centered column — good for spotlight moments."""
    eb = _eyebrow(eyebrow, cls_extra="mx-auto")
    children = []
    if eb is not None:
        children.append(eb)
    if heading:
        children.append(H2(heading, cls=f"{_H2_DEFAULT} text-center"))
    children.extend(P(text, cls=f"{_P_BODY} text-center") for text in paragraphs)
    return Div(*children, cls="max-w-xl mx-auto text-center")


def BulletSection(
    heading: str,
    items: Sequence[str],
    *,
    variant: str = "default",
) -> Div:
    """Heading (optional) + bulleted list. ``variant`` matches PageSection."""
    if variant == "split":
        return Div(
            Div(
                H2(heading, cls=_H2_DEFAULT) if heading else "",
                cls="md:col-span-4",
            ),
            Div(_bullets(items), cls="md:col-span-8"),
            cls="grid grid-cols-1 md:grid-cols-12 gap-x-10 gap-y-4",
        )

    children = []
    if heading:
        children.append(H2(heading, cls=_H2_DEFAULT))
    children.append(_bullets(items))
    if variant == "accent":
        return Div(
            *children,
            cls=(
                "border-l-2 border-blue-600 pl-6 py-2 "
                "bg-gradient-to-r from-blue-50/50 to-transparent dark:from-blue-950/20 "
                "rounded-r-md"
            ),
        )
    return Div(*children)


def _bullets(items: Sequence[str]) -> Ul:
    return Ul(
        *[
            Li(
                Span("→ ", cls="text-blue-600 font-semibold"),
                Span(item, cls="text-muted-foreground"),
                cls="leading-relaxed",
            )
            for item in items
        ],
        cls="space-y-2",
    )


# ---------------------------------------------------------------------------
# Card primitives
# ---------------------------------------------------------------------------


_CARD_BASE = "p-5 rounded-lg transition-colors"
_CARD_VARIANTS = {
    "default": "border border-border hover:border-foreground/30",
    "tinted": "bg-muted/40 hover:bg-muted/60",
    "accent": (
        "border-t-2 border-t-blue-600 border-x border-b border-border " "hover:border-foreground/30"
    ),
    "minimal": "px-0 py-0",
}


def InfoCard(title: str, *content, variant: str = "default") -> Div:
    """Card with an H3 title and arbitrary children.

    ``variant``: one of ``default``, ``tinted``, ``accent``, ``minimal``.
    """
    cls = _CARD_BASE + " " + _CARD_VARIANTS.get(variant, _CARD_VARIANTS["default"])
    return Div(
        H3(title, cls="text-lg font-semibold text-foreground mb-2"),
        *content,
        cls=cls,
    )


def FeatureGrid(
    items: Iterable[tuple[str, str]],
    *,
    cols: int = 2,
    gap: str = "gap-5",
    variants: Optional[Sequence[str]] = ("default", "tinted", "accent"),
) -> Div:
    """Grid of `InfoCard`s built from (title, description) pairs.

    Card variants rotate through ``variants`` so adjacent cards differ.
    Pass ``variants=None`` (or a single-element sequence) to disable rotation.
    """
    grid_cls = f"grid grid-cols-1 md:grid-cols-{cols} {gap}"
    variant_cycle = cycle(variants) if variants else cycle(["default"])
    return Div(
        *[
            InfoCard(
                title,
                P(desc, cls="text-muted-foreground leading-relaxed text-sm"),
                variant=next(variant_cycle),
            )
            for title, desc in items
        ],
        cls=grid_cls,
    )


def LinkCard(
    title: str,
    description: str,
    link_text: str,
    link: str,
    *,
    variant: str = "default",
) -> Div:
    """Card promoting a single link (e.g. a contact method)."""
    return InfoCard(
        title,
        P(description, cls="text-muted-foreground leading-relaxed mb-3 text-sm"),
        A(
            link_text,
            href=link,
            cls=(
                "inline-flex items-center gap-1 text-blue-600 hover:text-blue-700 "
                "font-semibold text-sm group"
            ),
        ),
        variant=variant,
    )


def FAQCard(question: str, answer: str, *, variant: str = "tinted") -> Div:
    """Card for a single FAQ question/answer pair (tinted by default)."""
    return InfoCard(
        question,
        P(answer, cls="text-muted-foreground leading-relaxed text-sm"),
        variant=variant,
    )


# ---------------------------------------------------------------------------
# Standalone editorial accents
# ---------------------------------------------------------------------------


def PullQuote(text: str, *, attribution: Optional[str] = None) -> Div:
    """Large pull-quote — use sparingly for emphasis."""
    children = [
        P(
            f"\u201C{text}\u201D",
            cls=("text-2xl md:text-3xl font-light leading-snug tracking-tight " "text-foreground"),
        )
    ]
    if attribution:
        children.append(
            P(
                f"— {attribution}",
                cls="mt-4 text-sm font-mono uppercase tracking-wider text-blue-600",
            )
        )
    return Div(
        *children,
        cls="py-6 pl-6 border-l-4 border-blue-600 my-2",
    )


def Divider(*, label: Optional[str] = None) -> Div:
    """Hairline divider, optionally with a small centered mono label."""
    if not label:
        return Div(cls="h-px w-full bg-border my-2")
    return Div(
        Div(cls="h-px flex-1 bg-border"),
        Span(label, cls="text-xs font-mono uppercase tracking-[0.2em] text-muted-foreground"),
        Div(cls="h-px flex-1 bg-border"),
        cls="flex items-center gap-4 my-2",
    )


__all__ = [
    "StaticPage",
    "PageSection",
    "BulletSection",
    "InfoCard",
    "FeatureGrid",
    "LinkCard",
    "FAQCard",
    "PullQuote",
    "Divider",
]
