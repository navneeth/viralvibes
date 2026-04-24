"""
Growth Blueprint — pure scoring infrastructure.

This module is intentionally I/O-free: zero DB calls, zero HTTP requests.
All data is passed in; all results are returned as plain dataclass instances.
That makes the scoring functions trivially testable and safe to run in threads.

Architecture
------------
Layer 0  DB row  ──► signals_from_row()  ──► CreatorSignals
Layer 1  CreatorSignals ──► score_all_actions() ──► list[ActionResult]
Layer 2  Route handler calls both layers, passes results to the view
Layer 3  views/blueprint.py renders — no logic

Adding a new action
-------------------
1. Write a ``score_<name>(s: CreatorSignals) -> float`` function (returns 0–100).
2. Add its ``ActionMeta`` entry to ``_ACTION_REGISTRY``.
Done. No branching, no existing code touched.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CreatorSignals:
    """
    Normalised signal vector computed from a single DB row + peer benchmarks.

    All arithmetic is null-safe: raw nullable DB values are coerced to 0 by
    ``signals_from_row()`` before being stored here.

    Phase-2 fields (shorts_ratio, caption_coverage, avg_duration_sec) are
    set to sentinel values until the worker computes them:
      shorts_ratio      = -1.0  (unknown)
      caption_coverage  = -1.0  (unknown)
      avg_duration_sec  = 0     (unknown)
    """

    # ── identity ──────────────────────────────────────────────────────────────
    creator_id: str
    channel_name: str
    primary_category: str
    country_code: str

    # ── raw DB fields ─────────────────────────────────────────────────────────
    subscribers: int
    video_count: int
    total_views: int
    views_change_30d: int  # 0 when DB value is NULL
    subs_change_30d: int  # 0 when DB value is NULL

    # ── derived (computed from raw DB, never NULL) ────────────────────────────
    viral_coeff: float  # views_change_30d / max(subscribers, 1)
    views_per_video: float  # total_views / max(video_count, 1)
    sub_growth_pct: float  # subs_change_30d / max(subscribers, 1) * 100

    # ── peer benchmarks (category cohort p75) ────────────────────────────────
    category_peer_vpv: float  # p75 views/video across same-category channels
    category_peer_vc: float  # p75 viral_coeff across same-category channels

    # ── Phase-2 stubs (worker enriches these; default = sentinel = unknown) ──
    shorts_ratio: float = -1.0
    caption_coverage: float = -1.0
    avg_duration_sec: int = 0


@dataclass(frozen=True)
class ActionResult:
    """A single scored and ranked growth action for a creator."""

    name: str  # Display name, e.g. "Add End Screens"
    score: float  # 0–100 confidence score
    mechanism: str  # One-line plain-English reason
    effort: int  # 1 (5 min) → 8 (strategy shift); used for tiebreaking
    studio_url: str  # Official YouTube support link


@dataclass(frozen=True)
class ActionMeta:
    """Registry entry — ties a scoring function to its display metadata."""

    score_fn: Callable[[CreatorSignals], float]
    mechanism: str
    effort: int
    studio_url: str


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# Official YouTube Studio support docs, one per action.
ACTION_STUDIO_LINKS: dict[str, str] = {
    "Rewrite Titles": "https://support.google.com/youtube/answer/57404?hl=en",
    "Thumbnail Audit": "https://support.google.com/youtube/answer/72431?hl=en",
    "Captions + Auto-Dub": "https://support.google.com/youtube/answer/6373554?hl=en",
    "Add End Screens": "https://support.google.com/youtube/answer/6388789?hl=en",
    "Add Chapter Timestamps": "https://support.google.com/youtube/answer/9884579?hl=en",
    "Change Category": "https://support.google.com/youtube/answer/2797468?hl=en",
    "Unlist Old Videos": "https://support.google.com/youtube/answer/1571778?hl=en",
    "Shift to Long-form": "https://support.google.com/youtube/answer/10059070?hl=en",
}

# Effort tier for tiebreaking: lower = faster win.
ACTION_EFFORT: dict[str, int] = {
    "Rewrite Titles": 1,
    "Thumbnail Audit": 2,
    "Captions + Auto-Dub": 3,
    "Add End Screens": 4,
    "Add Chapter Timestamps": 5,
    "Change Category": 6,
    "Unlist Old Videos": 7,
    "Shift to Long-form": 8,
}


# ─────────────────────────────────────────────────────────────────────────────
# Action registry  (populated by each action module below)
# ─────────────────────────────────────────────────────────────────────────────

_ACTION_REGISTRY: dict[str, ActionMeta] = {}


# ─────────────────────────────────────────────────────────────────────────────
# Action: Add End Screens
# ─────────────────────────────────────────────────────────────────────────────
# Diagnostic: high views-per-video but low subscriber conversion rate.
# Signal interpretation: viewers are watching but not being asked to subscribe.
# Studio path: YouTube Studio → Content → select video → Editor → End screen
# ─────────────────────────────────────────────────────────────────────────────


# Minimum subscriber count below which viral_coeff is too noisy to trust.
# A channel with 500 subs and 1 000 new views looks like viral_coeff = 2.0,
# but that's statistical noise, not a real leaky-bucket signal.
_END_SCREENS_MIN_SUBS = 50_000


def _score_end_screens(s: CreatorSignals) -> float:
    """
    Score the "Add End Screens" recommendation.

    High confidence when:
      - Channel is active (has recent views — not dormant)
      - Channel is large enough that viral_coeff is statistically meaningful
      - Views/video is above the category floor (content is watched)
      - Subscriber growth is low relative to viewership (viewers aren't converting)

    Scales with how far views outpace subscriber growth (the "leaky bucket" gap).

    Returns 0 when the channel is dormant or below the minimum subscriber
    threshold — end screens help only when new uploads exist to link from, and
    when the subscriber base is large enough that conversion rates are reliable.
    """
    # Guard: dormant channel — no new views or subs in 30 days
    if s.views_change_30d == 0 and s.subs_change_30d == 0:
        return 0.0

    # Guard: too small — viral_coeff is statistically unreliable below this threshold
    if s.subscribers < _END_SCREENS_MIN_SUBS:
        return 0.0

    score = 0.0

    # Core signal: views are flowing in but subscribers aren't converting
    if s.viral_coeff > 1.5 and s.sub_growth_pct < 0.5:
        score += 40.0

    # Reinforcing: large library means more surfaces for end-screen CTAs
    if s.video_count > 200:
        score += 20.0

    # Strong VPV signals engaged audience — high-value end-screen target
    if s.views_per_video > 5_000_000:
        score += 20.0

    # VPV at or above category floor → content quality isn't the bottleneck
    if s.category_peer_vpv > 0 and s.views_per_video >= s.category_peer_vpv * 0.5:
        score += 20.0

    return min(score, 100.0)


_ACTION_REGISTRY["Add End Screens"] = ActionMeta(
    score_fn=_score_end_screens,
    mechanism="Viewers watch but don't subscribe — end-screen CTAs close the conversion gap.",
    effort=ACTION_EFFORT["Add End Screens"],
    studio_url=ACTION_STUDIO_LINKS["Add End Screens"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Factory — build CreatorSignals from a raw DB row
# ─────────────────────────────────────────────────────────────────────────────


def signals_from_row(
    row: dict,
    peer_vpv_p75: float,
    peer_vc_p75: float,
) -> CreatorSignals:
    """
    Build a ``CreatorSignals`` from a creator DB row and pre-fetched peer
    benchmarks.  All nullable DB values are coerced to safe defaults so
    scoring functions never need to guard against None.

    Args:
        row:           Dict returned by ``db.get_creator_stats()``.
        peer_vpv_p75:  75th-percentile views/video in the same category.
                       Pass 0.0 when no peers exist (all comparison clauses
                       will be inactive, producing lower scores).
        peer_vc_p75:   75th-percentile viral_coeff in the same category.
    """
    subs = int(row.get("current_subscribers") or 0)
    videos = int(row.get("current_video_count") or 0)
    views = int(row.get("current_view_count") or 0)
    views_ch = int(row.get("views_change_30d") or 0)
    subs_ch = int(row.get("subscribers_change_30d") or 0)

    vpv = views / max(videos, 1)
    viral = round(views_ch / max(subs, 1), 6)
    sub_pct = round(subs_ch / max(subs, 1) * 100, 6)

    # Phase-2 fields: use cached worker value if present, else sentinel -1
    bp = row.get("blueprint_signals") or {}
    shorts_ratio = float(bp.get("shorts_ratio", -1.0))
    caption_cov = float(bp.get("caption_coverage", -1.0))
    avg_duration = int(bp.get("avg_duration_sec", 0))

    return CreatorSignals(
        creator_id=str(row.get("id") or ""),
        channel_name=str(row.get("channel_name") or ""),
        primary_category=str(row.get("primary_category") or ""),
        country_code=str(row.get("country_code") or ""),
        subscribers=subs,
        video_count=videos,
        total_views=views,
        views_change_30d=views_ch,
        subs_change_30d=subs_ch,
        viral_coeff=viral,
        views_per_video=vpv,
        sub_growth_pct=sub_pct,
        category_peer_vpv=float(peer_vpv_p75),
        category_peer_vc=float(peer_vc_p75),
        shorts_ratio=shorts_ratio,
        caption_coverage=caption_cov,
        avg_duration_sec=avg_duration,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Scorer
# ─────────────────────────────────────────────────────────────────────────────


def score_all_actions(signals: CreatorSignals) -> list[ActionResult]:
    """
    Run every registered scoring function against ``signals``.

    Returns a list of ``ActionResult`` sorted by:
      1. score descending  (highest-confidence action first)
      2. effort ascending  (faster win wins ties)

    Actions that score 0 are included in the list so callers can show
    "not applicable" states in a full breakdown view, but the route
    handler should filter them for the free-tier single-card display.
    """
    results: list[ActionResult] = []
    for name, meta in _ACTION_REGISTRY.items():
        score = min(100.0, max(0.0, meta.score_fn(signals)))
        results.append(
            ActionResult(
                name=name,
                score=score,
                mechanism=meta.mechanism,
                effort=meta.effort,
                studio_url=meta.studio_url,
            )
        )

    return sorted(results, key=lambda r: (-r.score, r.effort))
