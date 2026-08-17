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
1. Write a ``score_<name>(s: CreatorSignals) -> tuple[float, str]`` function.
   Return (score 0–100, specific reason referencing actual metric values).
   Return (0.0, "") when the action does not apply.
2. Add its ``ActionMeta`` entry to ``_ACTION_REGISTRY``.
Done. No branching, no existing code touched.

Design constraint
-----------------
Every action in the registry must satisfy three requirements before shipping:
  1. Trigger condition  — a specific metric comparison that surfaces the action.
  2. Official basis     — a verified YouTube Help Center URL.
  3. Effort             — already present.
The mechanism string is *generated* from the actual signal values at scoring
time so users see why their channel triggered the recommendation, not boilerplate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


# ─────────────────────────────────────────────────────────────────────────────
# Formatting helper (keeps this module I/O-free — no views imports)
# ─────────────────────────────────────────────────────────────────────────────


def _fmt(n: float) -> str:
    """Compact number formatter for mechanism text (1.2M, 45K, 800).

    Truncates (not rounds) to avoid overstating metrics — 1 500 → "1K", not "2K".
    """
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{int(n / 1_000)}K"
    return str(int(n))


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
    views_change_30d: int  # may be negative when views declined
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

    # ── activity signal (from DB row, coerced to 0.0 when NULL) ──────────────
    monthly_uploads: float = 0.0


@dataclass(frozen=True)
class ActionResult:
    """A single scored and ranked growth action for a creator."""

    name: str  # Display name, e.g. "Add End Screens"
    score: float  # 0–100 confidence score
    mechanism: str  # Personalised reason generated from actual signal values
    effort: int  # 1 (5 min) → 8 (strategy shift); used for tiebreaking
    studio_url: str  # Official YouTube support link
    funnel_stage: str = ""  # "Reach" | "Engagement" | "Conversion" | "Revenue"


@dataclass(frozen=True)
class ActionMeta:
    """Registry entry — ties a scoring function to its display metadata.

    score_fn returns (score, mechanism) where mechanism is a personalised
    string generated from the creator's actual signal values.  Returning
    (0.0, "") means the action does not apply to this creator.
    """

    score_fn: Callable[[CreatorSignals], tuple[float, str]]
    effort: int
    studio_url: str
    funnel_stage: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# Official YouTube Studio support docs, one per action.
ACTION_STUDIO_LINKS: dict[str, str] = {
    "Rewrite Titles": "https://support.google.com/youtube/answer/57404?hl=en",
    "Thumbnail Audit": "https://support.google.com/youtube/answer/72431?hl=en",
    "Captions + Auto-Dub": "https://support.google.com/youtube/answer/15569972?hl=en",
    "Add End Screens": "https://support.google.com/youtube/answer/6388789?hl=en",
    "Add Chapter Timestamps": "https://support.google.com/youtube/answer/9884579?hl=en",
    "Change Category": "https://support.google.com/youtube/answer/2797468?hl=en",
    "Unlist Old Videos": "https://support.google.com/youtube/answer/1571778?hl=en",
    "Shift to Long-form": "https://support.google.com/youtube/answer/10059070?hl=en",
    "Monetization Risk": "https://support.google.com/youtube/answer/72851?hl=en",
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
    "Monetization Risk": 2,  # fast action: post to Community or upload anything
}

# Funnel stage each action addresses.
ACTION_FUNNEL: dict[str, str] = {
    "Rewrite Titles": "Reach",
    "Thumbnail Audit": "Reach",
    "Captions + Auto-Dub": "Reach",
    "Add End Screens": "Conversion",
    "Add Chapter Timestamps": "Engagement",
    "Change Category": "Revenue",
    "Unlist Old Videos": "Reach",
    "Shift to Long-form": "Revenue",
    "Monetization Risk": "Revenue",
}


# Minimum score for an action to appear in output.
# Actions below this threshold have no reliable signal — they are suppressed
# rather than shown as "Not applicable" in the blurred pro-gate.
MIN_ACTIONABLE_SCORE: float = 30.0


# ─────────────────────────────────────────────────────────────────────────────
# Action registry  (populated by each action module below)
# ─────────────────────────────────────────────────────────────────────────────

_ACTION_REGISTRY: dict[str, ActionMeta] = {}


# ─────────────────────────────────────────────────────────────────────────────
# Action: Add End Screens
# ─────────────────────────────────────────────────────────────────────────────
# Official source: support.google.com/youtube/answer/6388789
# Funnel stage: Conversion (view → subscribe)
# Trigger: high views but low subscriber conversion rate.
# ─────────────────────────────────────────────────────────────────────────────


# Minimum subscriber count below which viral_coeff is too noisy to trust.
_END_SCREENS_MIN_SUBS = 50_000


def _score_end_screens(s: CreatorSignals) -> tuple[float, str]:
    """
    Score the "Add End Screens" recommendation.

    Fires when viewership is strong but subscriber conversion is weak — the
    classic leaky-bucket pattern that end-screen CTAs address directly.
    Also fires (lower confidence) when a large library + high VPV suggests
    many surfaces that would benefit from end-screen placement.
    """
    if s.views_change_30d == 0 and s.subs_change_30d == 0:
        return 0.0, ""
    if s.subscribers < _END_SCREENS_MIN_SUBS:
        return 0.0, ""

    score = 0.0
    primary_reason = ""

    # Core signal: conversion gap — views flowing but subscribers not converting
    conversion_gap_fires = s.viral_coeff > 1.5 and s.sub_growth_pct < 0.5
    if conversion_gap_fires:
        score += 40.0
        primary_reason = (
            f"{s.viral_coeff:.1f}× viral coefficient but only {s.sub_growth_pct:.2f}% "
            f"subscriber growth — viewers are watching but not converting."
        )

    # Large library: many surfaces for end-screen CTAs
    if s.video_count > 200:
        score += 20.0
        if not primary_reason:
            primary_reason = (
                f"{s.video_count:,} videos averaging {_fmt(s.views_per_video)} views — "
                f"adding end-screen CTAs across the library compounds the conversion rate."
            )

    # Strong VPV: engaged audience is a high-value target for CTAs
    if s.views_per_video > 5_000_000:
        score += 20.0

    # VPV at/above category floor: content quality isn't the bottleneck
    if s.category_peer_vpv > 0 and s.views_per_video >= s.category_peer_vpv * 0.5:
        score += 20.0
        if not primary_reason:
            primary_reason = (
                f"{_fmt(s.views_per_video)} avg views/video (category p75: {_fmt(s.category_peer_vpv)}) "
                f"— strong reach with {s.sub_growth_pct:.2f}% sub growth suggests end screens are missing."
            )

    score = min(score, 100.0)
    if score == 0.0:
        return 0.0, ""

    mechanism = primary_reason or (
        f"{_fmt(s.views_per_video)} avg views/video with {s.sub_growth_pct:.2f}% sub growth "
        f"— end-screen CTAs convert casual viewers into subscribers."
    )
    return score, mechanism


_ACTION_REGISTRY["Add End Screens"] = ActionMeta(
    score_fn=_score_end_screens,
    effort=ACTION_EFFORT["Add End Screens"],
    studio_url=ACTION_STUDIO_LINKS["Add End Screens"],
    funnel_stage=ACTION_FUNNEL["Add End Screens"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Thumbnail Audit
# ─────────────────────────────────────────────────────────────────────────────
# Official source: support.google.com/youtube/answer/72431
#   answer/12340300 — 90% of best-performing videos use custom thumbnails
# Funnel stage: Reach (impressions CTR)
# Trigger: stalled subscriber growth or VPV well below category peers.
# ─────────────────────────────────────────────────────────────────────────────


def _score_thumbnail_audit(s: CreatorSignals) -> tuple[float, str]:
    """
    Score the "Thumbnail Audit" recommendation.

    Thumbnail CTR affects impressions-to-view conversion (Reach stage).
    Two valid CTR proxies we can compute:
      - VPV well below category peers (browse impressions not clicking through)
      - Subscriber growth stalled on an active, large channel
    """
    if s.views_change_30d == 0 and s.subs_change_30d == 0:
        return 0.0, ""
    if s.subscribers < _END_SCREENS_MIN_SUBS:
        return 0.0, ""

    score = 0.0
    primary_reason = ""

    # VPV below peers: most direct proxy for low impressions CTR
    vpv_below_peers = s.category_peer_vpv > 0 and s.views_per_video < s.category_peer_vpv * 0.4
    if vpv_below_peers:
        ratio_pct = int(s.views_per_video / s.category_peer_vpv * 100)
        score += 25.0
        primary_reason = (
            f"Avg {_fmt(s.views_per_video)} views/video is {ratio_pct}% of "
            f"category p75 ({_fmt(s.category_peer_vpv)}) — browse impressions "
            f"aren't clicking through. Thumbnail A/B test is the fastest lever."
        )

    # Growth fully stalled while uploads continue
    if s.sub_growth_pct < 0.1 and s.views_change_30d > 0:
        score += 35.0
        if not primary_reason:
            primary_reason = (
                f"{s.sub_growth_pct:.2f}% subscriber growth despite "
                f"{_fmt(s.views_change_30d)} views gained last 30 days — "
                f"thumbnail CTR is the fastest lever to pull."
            )

    # Deeper stall
    if s.sub_growth_pct < 0.05:
        score += 25.0

    # Large channel: small CTR gain has large absolute impact
    if s.subscribers > 5_000_000:
        score += 15.0
        if not primary_reason:
            primary_reason = (
                f"{_fmt(s.subscribers)} subscribers but {s.sub_growth_pct:.2f}% monthly growth — "
                f"a 1-point CTR improvement on your impressions has outsized absolute impact."
            )

    score = min(score, 100.0)
    if score == 0.0:
        return 0.0, ""

    mechanism = primary_reason or (
        f"Active channel with {s.sub_growth_pct:.2f}% sub growth — thumbnail audit recommended."
    )
    return score, mechanism


_ACTION_REGISTRY["Thumbnail Audit"] = ActionMeta(
    score_fn=_score_thumbnail_audit,
    effort=ACTION_EFFORT["Thumbnail Audit"],
    studio_url=ACTION_STUDIO_LINKS["Thumbnail Audit"],
    funnel_stage=ACTION_FUNNEL["Thumbnail Audit"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Rewrite Titles
# ─────────────────────────────────────────────────────────────────────────────
# Official source: support.google.com/youtube/answer/57404
# Funnel stage: Reach (search + Browse impressions)
# Trigger: low viral coefficient — content not spreading beyond existing subs.
# ─────────────────────────────────────────────────────────────────────────────


def _score_rewrite_titles(s: CreatorSignals) -> tuple[float, str]:
    """
    Score the "Rewrite Titles" recommendation.

    Fires when the channel is active but content isn't reaching beyond the
    existing subscriber base — the packaging gap.  Viral coefficient is the
    primary signal: low coeff means views are mostly from subscribers, not
    algorithm-driven reach to new viewers.
    """
    if s.views_change_30d == 0 and s.subs_change_30d == 0:
        return 0.0, ""
    if s.subscribers < _END_SCREENS_MIN_SUBS:
        return 0.0, ""

    score = 0.0
    primary_reason = ""

    # Core: low viral spread despite active uploads
    if s.viral_coeff < 1.0 and s.views_change_30d > 0:
        score += 40.0
        primary_reason = (
            f"Only {s.viral_coeff:.2f}× viral coefficient — content isn't reaching beyond "
            f"your {_fmt(s.subscribers)} subscribers. Title reframing improves "
            f"Browse and Search surface."
        )

    # High VPV but low virality: existing audience likes it, algorithm doesn't surface it
    if s.views_per_video > 5_000_000 and s.viral_coeff < 2.0:
        score += 25.0
        if not primary_reason:
            primary_reason = (
                f"{_fmt(s.views_per_video)} avg views/video but only {s.viral_coeff:.2f}× "
                f"viral spread — packaging (title, thumbnail) is the bottleneck, not content quality."
            )

    # Weak sub growth confirms reach problem
    if s.sub_growth_pct < 0.3 and s.views_change_30d > 0:
        score += 20.0

    # Well below peer viral coeff
    if s.category_peer_vc > 0 and s.viral_coeff < s.category_peer_vc * 0.5:
        score += 15.0
        if not primary_reason:
            primary_reason = (
                f"{s.viral_coeff:.2f}× viral coeff vs {s.category_peer_vc:.2f}× category p75 — "
                f"peers achieve 2× more reach per subscriber."
            )

    score = min(score, 100.0)
    if score == 0.0:
        return 0.0, ""

    mechanism = primary_reason or (
        f"Low viral spread ({s.viral_coeff:.2f}×) — title reframing improves algorithm reach."
    )
    return score, mechanism


_ACTION_REGISTRY["Rewrite Titles"] = ActionMeta(
    score_fn=_score_rewrite_titles,
    effort=ACTION_EFFORT["Rewrite Titles"],
    studio_url=ACTION_STUDIO_LINKS["Rewrite Titles"],
    funnel_stage=ACTION_FUNNEL["Rewrite Titles"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Add Chapter Timestamps
# ─────────────────────────────────────────────────────────────────────────────
# Official source: support.google.com/youtube/answer/9884579
# Funnel stage: Engagement (average view duration / retention)
# Trigger: below-peer VPV on a meaningful catalogue — AVD likely suppressing
#          algorithmic ranking.  Chapters break long videos into navigable
#          sections, reducing early drop-off.
# ─────────────────────────────────────────────────────────────────────────────

_CHAPTERS_MIN_VIDEOS = 20
_CHAPTERS_VPV_PEER_THRESHOLD = 0.7


def _score_chapters(s: CreatorSignals) -> tuple[float, str]:
    """
    Score the "Add Chapter Timestamps" recommendation.

    Only fires for channels below category peers on VPV and with enough
    videos that the fix compounds.  Phase-2 ``avg_duration_sec`` reinforces
    the score when confirmed long-form content is present.
    """
    if (
        s.category_peer_vpv > 0
        and s.views_per_video >= s.category_peer_vpv * _CHAPTERS_VPV_PEER_THRESHOLD
    ):
        return 0.0, ""

    if s.video_count < _CHAPTERS_MIN_VIDEOS:
        return 0.0, ""

    score = 0.0

    if s.category_peer_vpv > 0 and s.views_per_video < s.category_peer_vpv * 0.5:
        score += 35.0

    if s.category_peer_vpv > 0 and s.views_per_video < s.category_peer_vpv * 0.3:
        score += 20.0

    if s.video_count > 100:
        score += 20.0

    if s.avg_duration_sec > 480:
        score += 25.0

    score = min(score, 100.0)
    if score == 0.0:
        return 0.0, ""

    peer_str = _fmt(s.category_peer_vpv) if s.category_peer_vpv > 0 else "category average"
    if s.avg_duration_sec > 480:
        mins = s.avg_duration_sec // 60
        mechanism = (
            f"~{mins}-minute average video with {_fmt(s.views_per_video)} avg views "
            f"vs {peer_str} category p75 — chapter timestamps reduce drop-off on "
            f"long-form content and improve average view duration."
        )
    else:
        mechanism = (
            f"Avg {_fmt(s.views_per_video)} views/video vs {peer_str} category p75 — "
            f"chapters break long videos into navigable sections, reducing early drop-off "
            f"and lifting average view duration."
        )
    return score, mechanism


_ACTION_REGISTRY["Add Chapter Timestamps"] = ActionMeta(
    score_fn=_score_chapters,
    effort=ACTION_EFFORT["Add Chapter Timestamps"],
    studio_url=ACTION_STUDIO_LINKS["Add Chapter Timestamps"],
    funnel_stage=ACTION_FUNNEL["Add Chapter Timestamps"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Captions + Auto-Dub
# ─────────────────────────────────────────────────────────────────────────────
# Official source: support.google.com/youtube/answer/2734796
# Funnel stage: Reach (non-domestic audience)
# Trigger: channel is in a market where auto-dub unlocks a second-language
#          audience with a single click in YouTube Studio.
# ─────────────────────────────────────────────────────────────────────────────

_AUTODUB_COUNTRIES = frozenset(
    {
        "IN",
        "BR",
        "AR",
        "MX",
        "ID",
        "AE",
        "KR",
        "JP",
        "RU",
        "CL",
        "PR",
        "ES",
        "PT",
        "PH",
        "CO",
        "PK",
        "NG",
        "EG",
        "TR",
        "BD",
    }
)

_COUNTRY_LANG: dict[str, str] = {
    "IN": "Hindi/regional",
    "BR": "Portuguese",
    "AR": "Spanish",
    "MX": "Spanish",
    "ID": "Indonesian",
    "KR": "Korean",
    "JP": "Japanese",
    "RU": "Russian",
    "ES": "Spanish",
    "PT": "Portuguese",
    "TR": "Turkish",
    "EG": "Arabic",
}


def _score_captions_dub(s: CreatorSignals) -> tuple[float, str]:
    """
    Score the "Captions + Auto-Dub" recommendation.

    Only fires for channels in markets where a second-language audience
    exists or the creator's language has large global diaspora reach.
    """
    if s.country_code.upper() not in _AUTODUB_COUNTRIES:
        return 0.0, ""

    score = 0.0
    score += 30.0  # country confirmed

    if s.subscribers > 1_000_000:
        score += 20.0
    elif s.subscribers > 100_000:
        score += 10.0

    if s.views_change_30d > 0:
        score += 20.0

    if s.caption_coverage >= 0 and s.caption_coverage < 0.3:
        score += 30.0
    elif s.caption_coverage == -1.0:
        score += 15.0

    score = min(score, 100.0)
    if score == 0.0:
        return 0.0, ""

    lang = _COUNTRY_LANG.get(s.country_code.upper(), "")
    lang_note = f" — {lang}-language content" if lang else ""
    mechanism = (
        f"Channel based in {s.country_code.upper()}{lang_note} with "
        f"{_fmt(s.subscribers)} subscribers. YouTube's auto-dub unlocks a "
        f"second-language audience — enable it in Studio → Settings → Channel → Advanced settings."
    )
    return score, mechanism


_ACTION_REGISTRY["Captions + Auto-Dub"] = ActionMeta(
    score_fn=_score_captions_dub,
    effort=ACTION_EFFORT["Captions + Auto-Dub"],
    studio_url=ACTION_STUDIO_LINKS["Captions + Auto-Dub"],
    funnel_stage=ACTION_FUNNEL["Captions + Auto-Dub"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Change Category
# ─────────────────────────────────────────────────────────────────────────────
# Official source: support.google.com/youtube/answer/2797468
# Funnel stage: Revenue (advertiser auction pool)
# Trigger: channel outperforms peers in a low-CPM category.
# Honesty note: No verified official source supports a specific RPM multiplier.
#   Mechanism states the mechanism only — different advertiser pool — without
#   a claimed numeric lift.
# ─────────────────────────────────────────────────────────────────────────────

_LOW_CPM_CATEGORIES = frozenset(
    {
        "entertainment",
        "gaming",
        "comedy",
        "sports",
        "lifestyle",
        "kids",
        "religion",
        "travel",
        "food",
        "beauty",
    }
)


def _score_change_category(s: CreatorSignals) -> tuple[float, str]:
    """
    Score the "Change Category" recommendation.

    Only fires when two conditions coincide:
      1. Current category is low-CPM (different advertiser auction pool).
      2. Channel outperforms its category peers (content quality is there —
         the issue is the label, not the content).

    A channel that under-performs in a low-CPM category should fix content
    first; changing the label won't help if viewership is weak.
    """
    if s.primary_category.lower() not in _LOW_CPM_CATEGORIES:
        return 0.0, ""

    # Hard gate: channel must outperform peers — changing the label won't help
    # a channel that under-performs in its own low-CPM category.
    if not (s.category_peer_vpv > 0 and s.views_per_video > s.category_peer_vpv):
        return 0.0, ""

    score = 0.0
    score += 30.0  # low-CPM category + peer outperformance confirmed
    score += 25.0  # VPV above peers (already validated above)

    if s.viral_coeff > 2.0:
        score += 25.0

    if s.sub_growth_pct > 0.5:
        score += 20.0

    score = min(score, 100.0)

    mechanism = (
        f"Strong reach ({s.viral_coeff:.1f}× viral coefficient, "
        f"{_fmt(s.views_per_video)} avg views/video) in {s.primary_category} — "
        f"a low-CPM category. Reclassifying accesses a different advertiser "
        f"auction pool that may carry higher CPM rates."
    )
    return score, mechanism


_ACTION_REGISTRY["Change Category"] = ActionMeta(
    score_fn=_score_change_category,
    effort=ACTION_EFFORT["Change Category"],
    studio_url=ACTION_STUDIO_LINKS["Change Category"],
    funnel_stage=ACTION_FUNNEL["Change Category"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Unlist Old Videos
# ─────────────────────────────────────────────────────────────────────────────
# Official source: support.google.com/youtube/answer/1571778
# Funnel stage: Reach (channel-average signal quality)
# Trigger: large catalogue with far-below-peer VPV — low-view tail dilutes
#          channel-average metrics that affect algorithmic surfacing.
# ─────────────────────────────────────────────────────────────────────────────

_UNLIST_MIN_VIDEOS = 500


def _score_unlist_catalogue(s: CreatorSignals) -> tuple[float, str]:
    """
    Score the "Unlist Old Videos" recommendation.

    Fires when a very large catalogue drags down channel-average VPV well
    below category peers.  Does NOT fire when VPV is at or above peers —
    a well-performing large catalogue should not be culled.
    """
    if s.video_count < _UNLIST_MIN_VIDEOS:
        return 0.0, ""

    # Requires peer benchmark — without it we can't confirm the low-view tail problem
    if s.category_peer_vpv == 0:
        return 0.0, ""

    # Hard exit: if VPV is at or above peers, there is no low-view tail problem
    if s.views_per_video >= s.category_peer_vpv * 0.5:
        return 0.0, ""

    score = 0.0
    score += 35.0  # large catalogue confirmed

    if s.category_peer_vpv > 0 and s.views_per_video < s.category_peer_vpv * 0.3:
        score += 30.0

    if s.video_count > 5_000:
        score += 20.0

    if s.sub_growth_pct < 0.2:
        score += 15.0

    score = min(score, 100.0)
    if score == 0.0:
        return 0.0, ""

    peer_str = f" vs {_fmt(s.category_peer_vpv)} category p75" if s.category_peer_vpv > 0 else ""
    mechanism = (
        f"{s.video_count:,} videos averaging {_fmt(s.views_per_video)} views/video{peer_str}. "
        f"A low-view tail dilutes channel-average signals — unlisting it raises "
        f"the average YouTube uses when deciding how to surface your channel."
    )
    return score, mechanism


_ACTION_REGISTRY["Unlist Old Videos"] = ActionMeta(
    score_fn=_score_unlist_catalogue,
    effort=ACTION_EFFORT["Unlist Old Videos"],
    studio_url=ACTION_STUDIO_LINKS["Unlist Old Videos"],
    funnel_stage=ACTION_FUNNEL["Unlist Old Videos"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Shift to Long-form  (Phase-2 activation)
# ─────────────────────────────────────────────────────────────────────────────
# Official source: support.google.com/youtube/answer/10059070
# Funnel stage: Revenue (RPM — long-form vs Shorts ad auction)
# Trigger: Shorts-dominant catalogue. Phase-2 worker must enrich shorts_ratio.
# ─────────────────────────────────────────────────────────────────────────────

_LONGFORM_MIN_SUBSCRIBERS = 1_000
_LONGFORM_MIN_VIEWS_PER_VIDEO = 500


def _score_shorts_to_longform(s: CreatorSignals) -> tuple[float, str]:
    """
    Score the "Shift to Long-form" recommendation.

    Returns (0, "") until Phase-2 worker computes ``shorts_ratio``.
    """
    if s.shorts_ratio < 0:
        return 0.0, ""
    if s.shorts_ratio < 0.7:
        return 0.0, ""
    if (
        s.subscribers < _LONGFORM_MIN_SUBSCRIBERS
        and s.views_per_video < _LONGFORM_MIN_VIEWS_PER_VIDEO
    ):
        return 0.0, ""

    score = 50.0

    if s.views_per_video > 10_000_000:
        score += 25.0

    if s.viral_coeff > 3.0:
        score += 25.0

    score = min(score, 100.0)
    shorts_pct = int(s.shorts_ratio * 100)
    mechanism = (
        f"{shorts_pct}% of uploads are Shorts with {_fmt(s.views_per_video)} avg views — "
        f"Shorts RPM is significantly lower than long-form. One long-form video per week "
        f"creates a high-CPM surface without abandoning the Shorts audience."
    )
    return score, mechanism


_ACTION_REGISTRY["Shift to Long-form"] = ActionMeta(
    score_fn=_score_shorts_to_longform,
    effort=ACTION_EFFORT["Shift to Long-form"],
    studio_url=ACTION_STUDIO_LINKS["Shift to Long-form"],
    funnel_stage=ACTION_FUNNEL["Shift to Long-form"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Monetization Risk
# ─────────────────────────────────────────────────────────────────────────────
# Official source: support.google.com/youtube/answer/72851
# Key detail: channels inactive for 6+ months (no uploads or Community posts)
#   can have YPP monetization removed regardless of subscriber count or watch
#   hours.  This is the highest-stakes, officially-grounded action in the
#   catalog — verified directly from the Help Center.
# Funnel stage: Revenue (YPP eligibility)
# Trigger: dormant channel with monetizable subscriber count.
# ─────────────────────────────────────────────────────────────────────────────

_MONETIZATION_MIN_SUBS = 1_000  # YPP threshold

# YouTube removes YPP monetization after 6+ months without uploads or Community
# posts (answer/72851). monthly_uploads < this threshold is consistent with
# ~6 months of inactivity (< 0.17 uploads/month over 6 months ≈ 1 upload).
_MONETIZATION_INACTIVITY_THRESHOLD: float = 0.2


def _score_monetization_risk(s: CreatorSignals) -> tuple[float, str]:
    """
    Score the "Monetization Risk" recommendation.

    Fires when a channel is inactive (no recent views, low upload rate) AND
    has enough subscribers that YPP monetization could be at risk per
    YouTube's 6-month inactivity policy.
    """
    if s.subscribers < _MONETIZATION_MIN_SUBS:
        return 0.0, ""

    is_view_dormant = s.views_change_30d == 0
    is_upload_dormant = s.monthly_uploads < _MONETIZATION_INACTIVITY_THRESHOLD

    if not (is_view_dormant or is_upload_dormant):
        return 0.0, ""

    score = 0.0

    if is_view_dormant and is_upload_dormant:
        score += 70.0
    elif is_view_dormant:
        score += 50.0
    elif is_upload_dormant:
        score += 40.0

    if s.subscribers > 100_000:
        score += 20.0
    elif s.subscribers > 10_000:
        score += 10.0

    score = min(score, 100.0)
    if score == 0.0:
        return 0.0, ""

    activity = "no views recorded" if is_view_dormant else "fewer than 1 upload every 5 months"
    mechanism = (
        f"{_fmt(s.subscribers)} subscribers but {activity} recently. "
        f"YouTube's YPP policy can remove monetization from channels inactive "
        f"for 6+ months — any upload or Community post resets the clock."
    )
    return score, mechanism


_ACTION_REGISTRY["Monetization Risk"] = ActionMeta(
    score_fn=_score_monetization_risk,
    effort=ACTION_EFFORT["Monetization Risk"],
    studio_url=ACTION_STUDIO_LINKS["Monetization Risk"],
    funnel_stage=ACTION_FUNNEL["Monetization Risk"],
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

    monthly_uploads = float(row.get("monthly_uploads") or 0.0)

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
        monthly_uploads=monthly_uploads,
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

    Only actions scoring >= _MIN_ACTIONABLE_SCORE are returned.  Sub-threshold
    scores have no reliable signal and are silently excluded — they should not
    appear in the blurred pro-gate as "Not applicable" recommendations.
    """
    results: list[ActionResult] = []
    for name, meta in _ACTION_REGISTRY.items():
        raw_score, mechanism = meta.score_fn(signals)
        score = min(100.0, max(0.0, raw_score))
        if score < MIN_ACTIONABLE_SCORE:
            continue
        results.append(
            ActionResult(
                name=name,
                score=score,
                mechanism=mechanism,
                effort=meta.effort,
                studio_url=meta.studio_url,
                funnel_stage=meta.funnel_stage,
            )
        )

    return sorted(results, key=lambda r: (-r.score, r.effort))
