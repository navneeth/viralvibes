"""
Growth Blueprint page — render-only, zero business logic.

Route: GET /creator/{creator_id}/blueprint

Layout
------
  ┌─ Channel diagnostic strip (subscriber count, VPV, viral coeff, peer rank) ─┐
  │                                                                              │
  │  ┌─ Top action card (free tier: 1 card) ───────────────────────────────┐   │
  │  │  Score gauge  │  Action name + mechanism  │  Studio link button      │   │
  │  └──────────────────────────────────────────────────────────────────────┘   │
  │                                                                              │
  │  [Pro gate: remaining actions blurred]                                       │
  └──────────────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

from fasthtml.common import *
from monsterui.all import *

from utils import format_number, safe_get_value
from utils.blueprint import ActionResult, CreatorSignals

import logging

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Score gauge
# ─────────────────────────────────────────────────────────────────────────────


def _score_colour(score: float) -> str:
    """Tailwind ring + fill colour based on confidence band."""
    if score >= 80:
        return "text-red-500"
    if score >= 55:
        return "text-amber-500"
    if score >= 30:
        return "text-blue-500"
    return "text-muted-foreground"


def _score_label(score: float) -> str:
    if score >= 80:
        return "High impact"
    if score >= 55:
        return "Medium impact"
    if score >= 30:
        return "Low impact"
    return "Not applicable"


def render_score_gauge(score: float) -> Div:
    """Circular-ish score badge — plain CSS, no SVG dependency."""
    colour = _score_colour(score)
    label = _score_label(score)
    return Div(
        Div(
            Span(f"{int(score)}", cls=f"text-4xl font-black tabular-nums {colour}"),
            Span("/100", cls="text-sm text-muted-foreground ml-0.5 self-end pb-1"),
            cls="flex items-end justify-center gap-0",
        ),
        P(label, cls=f"text-xs font-medium text-center mt-1 {colour}"),
        cls=(
            "flex flex-col items-center justify-center "
            "w-24 h-24 rounded-full border-4 "
            "border-current shrink-0 " + colour
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Action card
# ─────────────────────────────────────────────────────────────────────────────

_EFFORT_LABELS: dict[int, str] = {
    1: "5 min",
    2: "30 min",
    3: "1 hr",
    4: "1 hr",
    5: "2 hr",
    6: "30 min",
    7: "Half day",
    8: "Strategy",
}


def render_action_card(action: ActionResult, is_top: bool = False) -> Div:
    """
    One recommendation card.

    Args:
        action:  The scored ActionResult to display.
        is_top:  When True applies a highlighted ring; used for the #1 pick.
    """
    ring = "ring-2 ring-primary/60 shadow-lg shadow-primary/10" if is_top else "ring-1 ring-border"
    effort_label = _EFFORT_LABELS.get(action.effort, "—")

    return Div(
        # Left: score gauge
        render_score_gauge(action.score),
        # Centre: action info
        Div(
            Div(
                H3(action.name, cls="text-base font-semibold text-foreground leading-tight"),
                Span(
                    f"Effort: {effort_label}",
                    cls=(
                        "text-xs px-2 py-0.5 rounded-full bg-muted "
                        "text-muted-foreground font-medium"
                    ),
                ),
                cls="flex items-center gap-3 flex-wrap",
            ),
            P(action.mechanism, cls="text-sm text-muted-foreground mt-1.5 leading-snug"),
            cls="flex-1 min-w-0",
        ),
        # Right: Studio button
        A(
            UkIcon("external-link", cls="w-4 h-4 mr-1.5 shrink-0"),
            "How to do this",
            href=action.studio_url,
            target="_blank",
            rel="noopener noreferrer",
            cls=(
                "inline-flex items-center shrink-0 text-sm font-medium "
                "px-3 py-2 rounded-lg bg-primary/10 text-primary "
                "hover:bg-primary/20 transition-colors"
            ),
        ),
        cls=f"flex items-center gap-5 p-5 rounded-xl bg-card {ring}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Diagnostic strip
# ─────────────────────────────────────────────────────────────────────────────


def _stat_chip(label: str, value: str, highlight: bool = False) -> Div:
    val_cls = "text-foreground font-semibold" if not highlight else "text-primary font-bold"
    return Div(
        P(value, cls=f"text-lg {val_cls} tabular-nums"),
        P(label, cls="text-xs text-muted-foreground mt-0.5"),
        cls="flex flex-col items-center text-center px-4 py-3",
    )


def render_diagnostic_strip(signals: CreatorSignals) -> Div:
    """4-chip summary bar showing the key signals that drove the recommendation."""
    vpv = signals.views_per_video
    vpv_str = (
        f"{vpv / 1_000_000:.1f}M"
        if vpv >= 1_000_000
        else f"{vpv / 1_000:.0f}K" if vpv >= 1_000 else str(int(vpv))
    )
    peer_vpv = signals.category_peer_vpv
    peer_str = (
        f"{peer_vpv / 1_000_000:.1f}M"
        if peer_vpv >= 1_000_000
        else f"{peer_vpv / 1_000:.0f}K" if peer_vpv >= 1_000 else "—"
    )
    viral_str = f"{signals.viral_coeff:.2f}×"
    growth_str = f"{signals.sub_growth_pct:.2f}%"

    return Div(
        _stat_chip("Avg views / video", vpv_str),
        Div(cls="w-px h-10 bg-border self-center"),
        _stat_chip("Category p75 VPV", peer_str),
        Div(cls="w-px h-10 bg-border self-center"),
        _stat_chip("Viral coeff (30d)", viral_str, highlight=signals.viral_coeff > 2),
        Div(cls="w-px h-10 bg-border self-center"),
        _stat_chip("Sub growth (30d)", growth_str),
        cls=(
            "flex items-center rounded-xl bg-muted/40 border border-border "
            "divide-x divide-border overflow-x-auto"
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Empty state
# ─────────────────────────────────────────────────────────────────────────────


def render_no_actions() -> Div:
    """Shown when no action scores above zero — channel is well-optimised."""
    return Div(
        UkIcon("check-circle", cls="w-10 h-10 text-emerald-500 mx-auto mb-3"),
        H3(
            "Channel looks well-optimised",
            cls="text-lg font-semibold text-foreground text-center",
        ),
        P(
            "No high-confidence recommendations right now. " "Check back after the next data sync.",
            cls="text-sm text-muted-foreground text-center mt-1 max-w-sm mx-auto",
        ),
        cls="py-12 flex flex-col items-center",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Full page
# ─────────────────────────────────────────────────────────────────────────────


def render_blueprint_page(
    creator: dict,
    signals: CreatorSignals,
    actions: list[ActionResult],
    back_url: str = "/creators",
) -> Div:
    """
    Full Growth Blueprint page for one creator.

    Args:
        creator:   Raw DB dict (for name, thumbnail, profile link).
        signals:   Pre-built CreatorSignals from utils/blueprint.py.
        actions:   Output of score_all_actions(signals) — already sorted.
        back_url:  Href for the ← back link.
    """
    channel_name = safe_get_value(creator, "channel_name", "Creator")
    thumbnail = safe_get_value(creator, "channel_thumbnail_url") or "/static/favicon.jpeg"
    creator_id = safe_get_value(creator, "id", "")
    profile_url = f"/creator/{creator_id}"

    # Split actions: first scored action = free card; rest will be pro-gated.
    scored = [a for a in actions if a.score > 0]
    top_action = scored[0] if scored else None
    remaining = scored[1:]

    # Header
    header = Div(
        A(
            UkIcon("chevron-left", cls="w-4 h-4 mr-1"),
            "Back",
            href=back_url,
            cls="inline-flex items-center text-sm text-muted-foreground hover:text-foreground transition-colors mb-6",
        ),
        Div(
            Img(
                src=thumbnail,
                alt=channel_name,
                cls="w-14 h-14 rounded-full object-cover ring-2 ring-border shrink-0",
            ),
            Div(
                Div(
                    A(
                        channel_name,
                        href=profile_url,
                        cls="text-2xl font-bold text-foreground hover:underline",
                    ),
                    Span(
                        "Growth Blueprint",
                        cls=(
                            "text-xs font-semibold px-2 py-0.5 rounded-full "
                            "bg-primary/15 text-primary ml-3 align-middle"
                        ),
                    ),
                    cls="flex items-baseline flex-wrap gap-1",
                ),
                P(
                    "Studio-grounded actions ranked by confidence. "
                    "Each link opens the exact YouTube Studio help page.",
                    cls="text-sm text-muted-foreground mt-1",
                ),
                cls="flex-1 min-w-0",
            ),
            cls="flex items-start gap-4",
        ),
        cls="mb-8",
    )

    # Diagnostic strip
    diag_section = Div(
        H4(
            "Channel signals",
            cls="text-sm font-semibold text-muted-foreground uppercase tracking-wide mb-3",
        ),
        render_diagnostic_strip(signals),
        cls="mb-8",
    )

    # Actions section
    if not top_action:
        actions_section = render_no_actions()
    else:
        # Free tier: top action card
        free_card = Div(
            H4(
                "Top recommendation",
                cls="text-sm font-semibold text-muted-foreground uppercase tracking-wide mb-3",
            ),
            render_action_card(top_action, is_top=True),
            cls="mb-6",
        )

        # Pro gate: remaining actions blurred/locked
        if remaining:
            locked_cards = Div(
                *[render_action_card(a) for a in remaining],
                cls="flex flex-col gap-3",
            )
            pro_gate = Div(
                Div(locked_cards, cls="opacity-30 blur-sm pointer-events-none select-none"),
                Div(
                    UkIcon("lock", cls="w-5 h-5 text-primary mb-2"),
                    P(
                        f"{len(remaining)} more action{'s' if len(remaining) != 1 else ''} — Pro only",
                        cls="text-sm font-semibold text-foreground",
                    ),
                    P(
                        "Upgrade to see the full ranked list with confidence scores.",
                        cls="text-xs text-muted-foreground mt-0.5",
                    ),
                    A(
                        "Upgrade to Pro",
                        href="/pricing",
                        cls=(
                            "mt-3 inline-flex items-center px-4 py-2 rounded-lg "
                            "bg-primary text-primary-foreground text-sm font-medium "
                            "hover:bg-primary/90 transition-colors"
                        ),
                    ),
                    cls="absolute inset-0 flex flex-col items-center justify-center",
                ),
                cls="relative",
            )
            actions_section = Div(free_card, pro_gate)
        else:
            actions_section = free_card

    return Div(
        header,
        diag_section,
        actions_section,
        cls="max-w-3xl mx-auto px-4 py-10",
    )
