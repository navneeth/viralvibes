"""
Growth Blueprint page — render-only, zero business logic.

Route: GET /creator/{creator_id}/blueprint

Layout
------
  ┌─ Channel diagnostic strip (subscriber count, VPV, viral coeff, peer rank) ─┐
  │                                                                              │
  │  ┌─ Ranked recommendation cards (all shown, top card highlighted) ───────┐   │
  │  │  Score gauge  │  Action name + mechanism  │  Studio link button      │   │
  │  └──────────────────────────────────────────────────────┘   │
  └──────────────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

from fasthtml.common import *
from monsterui.all import *

from components.buttons import EstimatedBadge
from utils import format_number, safe_get_value
from utils.blueprint import ActionResult, CreatorSignals
from views.creators import creator_profile_url

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
    """Circular score badge — score only; /100 removed to prevent mobile overflow."""
    colour = _score_colour(score)
    label = _score_label(score)
    return Div(
        Span(f"{int(score)}", cls=f"text-3xl font-black tabular-nums leading-none {colour}"),
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

_FUNNEL_COLOURS: dict[str, str] = {
    "Reach": "bg-blue-500/10 text-blue-600",
    "Engagement": "bg-amber-500/10 text-amber-600",
    "Conversion": "bg-emerald-500/10 text-emerald-600",
    "Revenue": "bg-violet-500/10 text-violet-600",
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
    funnel_cls = _FUNNEL_COLOURS.get(action.funnel_stage, "bg-muted text-muted-foreground")

    # Build the badge row: action name + funnel stage chip + effort chip
    badges = [
        H3(action.name, cls="text-base font-semibold text-foreground leading-tight"),
    ]
    if action.funnel_stage:
        badges.append(
            Span(
                action.funnel_stage,
                cls=f"text-xs px-2 py-0.5 rounded-full font-medium {funnel_cls}",
            )
        )
    badges.append(
        Span(
            f"Effort: {effort_label}",
            cls=("text-xs px-2 py-0.5 rounded-full bg-muted " "text-muted-foreground font-medium"),
        )
    )

    return Div(
        # Gauge + text — always a row on both mobile and desktop
        Div(
            render_score_gauge(action.score),
            Div(
                Div(*badges, cls="flex items-center gap-2 flex-wrap"),
                P(action.mechanism, cls="text-sm text-muted-foreground mt-1.5 leading-snug"),
                cls="flex-1 min-w-0",
            ),
            cls="flex items-start gap-4 flex-1 min-w-0",
        ),
        # Button: full-width on mobile, auto-width on sm+
        A(
            UkIcon("external-link", cls="w-4 h-4 mr-1.5 shrink-0"),
            "How to do this",
            href=action.studio_url,
            target="_blank",
            rel="noopener noreferrer",
            cls=(
                "inline-flex items-center justify-center sm:justify-start "
                "w-full sm:w-auto shrink-0 text-sm font-medium "
                "px-3 py-2 rounded-lg bg-primary/10 text-primary "
                "hover:bg-primary/20 transition-colors"
            ),
        ),
        cls=f"flex flex-col sm:flex-row sm:items-center gap-4 p-4 sm:p-5 rounded-xl bg-card {ring}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Diagnostic strip
# ─────────────────────────────────────────────────────────────────────────────


def _stat_chip(label: str, value: str, highlight: bool = False, label_badge=None) -> Div:
    val_cls = "text-foreground font-semibold" if not highlight else "text-primary font-bold"
    label_el = (
        Div(
            P(label, cls="text-xs text-muted-foreground mt-0.5"),
            label_badge,
            cls="flex items-center gap-1 justify-center",
        )
        if label_badge is not None
        else P(label, cls="text-xs text-muted-foreground mt-0.5")
    )
    return Div(
        P(value, cls=f"text-lg {val_cls} tabular-nums"),
        label_el,
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
        else (
            f"{peer_vpv / 1_000:.0f}K"
            if peer_vpv >= 1_000
            else str(int(peer_vpv)) if peer_vpv > 0 else "—"
        )
    )
    viral_coeff = signals.viral_coeff
    if viral_coeff < 0:
        viral_str = "↓ declining"
        viral_highlight = False
    else:
        viral_str = f"{viral_coeff:.2f}×"
        viral_highlight = viral_coeff > 2

    # Cap sub growth display — first-sync anomaly sets subs_change == current_subs
    # (produces exactly 100.0%); use >= to catch that exact value too.
    sub_growth = signals.sub_growth_pct
    if abs(sub_growth) >= 100:
        growth_str = "—"
    else:
        growth_str = f"{sub_growth:.2f}%"

    return Div(
        _stat_chip("Avg views / video", vpv_str),
        Div(cls="w-px h-10 bg-border self-center"),
        _stat_chip(
            "Category p75 VPV",
            peer_str,
            label_badge=EstimatedBadge(
                detail=(
                    "75th-percentile views per video across channels in the same category. "
                    "Computed by ViralVibes from aggregate channel data — not a YouTube metric."
                )
            ),
        ),
        Div(cls="w-px h-10 bg-border self-center"),
        _stat_chip(
            "Reach multiplier (30d)",
            viral_str,
            highlight=viral_highlight,
            label_badge=EstimatedBadge(
                detail=(
                    "30-day views ÷ total subscribers. "
                    "Above 1× means content reached more people than the channel's subscriber count. "
                    "Computed by ViralVibes — not provided by YouTube's API."
                )
            ),
        ),
        Div(cls="w-px h-10 bg-border self-center"),
        _stat_chip(
            "Sub growth (30d)",
            growth_str,
            label_badge=EstimatedBadge(
                detail=(
                    "30-day subscriber change ÷ current subscribers, expressed as a percentage. "
                    "Computed by ViralVibes from YouTube channel data."
                )
            ),
        ),
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
# Sign-in CTA (unauthenticated visitors only)
# ─────────────────────────────────────────────────────────────────────────────


def _render_signin_cta(return_url: str) -> Div:
    """Soft sign-in invitation shown below blueprint content for logged-out visitors."""
    from urllib.parse import urlencode
    from components.auth_components import GoogleGLogo  # lazy — avoids circular import

    login_href = f"/login?{urlencode({'return_url': return_url})}"
    return Div(
        Div(
            UkIcon("bookmark", cls="w-5 h-5 text-primary shrink-0 mt-0.5"),
            Div(
                H4(
                    "Track this creator — it's free",
                    cls="text-base font-semibold text-foreground",
                ),
                P(
                    "Sign in to add this channel to your watchlist and stay updated when their blueprint changes.",
                    cls="text-sm text-muted-foreground mt-0.5",
                ),
                cls="flex-1 min-w-0",
            ),
            A(
                GoogleGLogo(18),
                Span("Continue with Google", cls="ml-2 text-sm font-semibold"),
                href=login_href,
                cls=(
                    "inline-flex items-center shrink-0 px-4 py-2.5 "
                    "bg-white border border-gray-200 hover:border-gray-300 "
                    "hover:shadow-sm text-gray-800 rounded-lg transition-all no-underline"
                ),
            ),
            cls="flex items-start gap-4 flex-wrap sm:flex-nowrap",
        ),
        cls="mt-10 p-5 rounded-xl border border-primary/20 bg-primary/5",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Full page
# ─────────────────────────────────────────────────────────────────────────────


def render_blueprint_page(
    creator: dict,
    signals: CreatorSignals,
    actions: list[ActionResult],
    back_url: str = "/creators",
    auth: bool = False,
    return_url: str = "/creators",
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
    profile_url = creator_profile_url(creator)

    # All actions already scored >= MIN_ACTIONABLE_SCORE (filtered in score_all_actions).

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
                    "ViralVibes-computed growth recommendations ranked by confidence. "
                    "Each action links to the official YouTube Studio help page.",
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
    if not actions:
        actions_section = render_no_actions()
    else:
        actions_section = Div(
            Div(
                H4(
                    "Recommendations",
                    cls="text-sm font-semibold text-muted-foreground uppercase tracking-wide",
                ),
                P(
                    "Scored by ViralVibes \u2014 not sourced from YouTube.",
                    cls="text-xs text-muted-foreground mt-0.5 mb-3",
                ),
            ),
            Div(
                render_action_card(actions[0], is_top=True),
                *[render_action_card(a) for a in actions[1:]],
                cls="flex flex-col gap-3",
            ),
        )

    body_parts = [header, diag_section, actions_section]
    if not auth:
        body_parts.append(_render_signin_cta(return_url))
    return Div(*body_parts, cls="max-w-3xl mx-auto px-4 py-10")
