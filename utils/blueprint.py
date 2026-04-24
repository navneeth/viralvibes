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
# Action: Thumbnail Audit
# ─────────────────────────────────────────────────────────────────────────────
# Diagnostic: large active channel, growth has stalled or reversed.
# Hypothesis: thumbnail click-through rate has degraded (trend, fatigue, or
# A/B never run). Fixing the thumbnail is the fastest lever with no content
# change required.
# Studio path: YouTube Studio → Content → select video → Details → Thumbnail
# ─────────────────────────────────────────────────────────────────────────────


def _score_thumbnail_audit(s: CreatorSignals) -> float:
    """
    Score the "Thumbnail Audit" recommendation.

    High confidence when subscriber growth has stalled on a channel large
    enough that the signal is statistically meaningful.  Reinforced when
    views-per-video is well below the category p75 — content exists and is
    being uploaded, but browse impressions aren't converting to clicks.

    Dormant channels (no activity in 30 days) score 0 — there is nothing to
    A/B test if no new videos are being published.
    """
    if s.views_change_30d == 0 and s.subs_change_30d == 0:
        return 0.0

    if s.subscribers < _END_SCREENS_MIN_SUBS:
        return 0.0

    score = 0.0

    # Core: growth fully stalled on an otherwise active channel
    if s.sub_growth_pct < 0.1 and s.views_change_30d > 0:
        score += 35.0

    # Deeper stall — subs actually shrinking or static for a long time
    if s.sub_growth_pct < 0.05:
        score += 25.0

    # VPV well below category peers — browse surface underperforming
    if s.category_peer_vpv > 0 and s.views_per_video < s.category_peer_vpv * 0.4:
        score += 25.0

    # Reinforcing: large channel means a small CTR gain has huge absolute impact
    if s.subscribers > 5_000_000:
        score += 15.0

    return min(score, 100.0)


_ACTION_REGISTRY["Thumbnail Audit"] = ActionMeta(
    score_fn=_score_thumbnail_audit,
    mechanism="Growth stalled on an active channel — thumbnail CTR is the fastest lever to pull.",
    effort=ACTION_EFFORT["Thumbnail Audit"],
    studio_url=ACTION_STUDIO_LINKS["Thumbnail Audit"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Rewrite Titles
# ─────────────────────────────────────────────────────────────────────────────
# Diagnostic: channel is uploading and views are flowing, but the viral
# coefficient (views_change_30d / subs) is low — content isn't spreading
# beyond the existing audience.  Title reframing (curiosity gap, specificity,
# search intent) is the primary packaging lever for reach beyond subscribers.
# Studio path: YouTube Studio → Content → select video → Details → Title
# ─────────────────────────────────────────────────────────────────────────────


def _score_rewrite_titles(s: CreatorSignals) -> float:
    """
    Score the "Rewrite Titles" recommendation.

    Fires when the channel is active but content isn't reaching beyond its
    existing subscriber base — the classic packaging gap.  Suppressed for
    dormant channels (no uploads/views) because there is nothing to rewrite.
    """
    # Guard: channel must be active
    if s.views_change_30d == 0 and s.subs_change_30d == 0:
        return 0.0

    if s.subscribers < _END_SCREENS_MIN_SUBS:
        return 0.0

    score = 0.0

    # Core signal: views are coming in but viral spread is low
    if s.viral_coeff < 1.0 and s.views_change_30d > 0:
        score += 40.0

    # Good VPV but low virality → existing audience enjoys content but
    # algorithm isn't surfacing it to new viewers (title/packaging problem)
    if s.views_per_video > 5_000_000 and s.viral_coeff < 2.0:
        score += 25.0

    # Subscriber growth also weak — confirms reach problem, not just views
    if s.sub_growth_pct < 0.3 and s.views_change_30d > 0:
        score += 20.0

    # Well below peer viral coeff — category peers achieve more spread per sub
    if s.category_peer_vc > 0 and s.viral_coeff < s.category_peer_vc * 0.5:
        score += 15.0

    return min(score, 100.0)


_ACTION_REGISTRY["Rewrite Titles"] = ActionMeta(
    score_fn=_score_rewrite_titles,
    mechanism="Content isn't spreading beyond subscribers — title reframing improves algorithm reach.",
    effort=ACTION_EFFORT["Rewrite Titles"],
    studio_url=ACTION_STUDIO_LINKS["Rewrite Titles"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Add Chapter Timestamps
# ─────────────────────────────────────────────────────────────────────────────
# Diagnostic: channel is below its category p75 VPV, and the content is long
# enough that chapter navigation would meaningfully improve Average View
# Duration (AVD).  YouTube's algorithm weights AVD heavily; chapters reduce
# drop-off at natural skip-points.
# Studio path: YouTube Studio → Content → select video → Details → Description
#              (add HH:MM:SS timecodes; YouTube auto-detects)
# ─────────────────────────────────────────────────────────────────────────────

_CHAPTERS_MIN_VIDEOS = 20  # below this, adding chapters is low-leverage
_CHAPTERS_VPV_PEER_THRESHOLD = 0.7  # must be below 70 % of peer p75 to fire


def _score_chapters(s: CreatorSignals) -> float:
    """
    Score the "Add Chapter Timestamps" recommendation.

    Only fires for channels with a meaningful back-catalogue that are
    performing below their category peers on views-per-video.  Above-average
    channels are unlikely to gain from chapters (their AVD is already strong).

    Phase-2 enrichment: ``avg_duration_sec`` from the worker raises the score
    further when the channel's average video length confirms long-form content.
    """
    # Guard: above-par performer — chapters won't move the needle
    if (
        s.category_peer_vpv > 0
        and s.views_per_video >= s.category_peer_vpv * _CHAPTERS_VPV_PEER_THRESHOLD
    ):
        return 0.0

    if s.video_count < _CHAPTERS_MIN_VIDEOS:
        return 0.0

    score = 0.0

    # Core: below-peer VPV on a long-form-sized catalogue
    if s.category_peer_vpv > 0 and s.views_per_video < s.category_peer_vpv * 0.5:
        score += 35.0

    # Severely below peers — larger opportunity
    if s.category_peer_vpv > 0 and s.views_per_video < s.category_peer_vpv * 0.3:
        score += 20.0

    # Large catalogue means retroactive chapter edits compound across many videos
    if s.video_count > 100:
        score += 20.0

    # Phase-2: confirmed long-form content (worker enriched)
    if s.avg_duration_sec > 480:
        score += 25.0
    # avg_duration_sec == 0 means Phase-2 worker hasn't run yet — no bonus;
    # the action already scores via the catalogue-size and below-peer-VPV
    # clauses, so we don't inflate further without format evidence.

    return min(score, 100.0)


_ACTION_REGISTRY["Add Chapter Timestamps"] = ActionMeta(
    score_fn=_score_chapters,
    mechanism="Below-peer VPV on a long-form catalogue — chapters reduce drop-off and lift AVD.",
    effort=ACTION_EFFORT["Add Chapter Timestamps"],
    studio_url=ACTION_STUDIO_LINKS["Add Chapter Timestamps"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Captions + Auto-Dub
# ─────────────────────────────────────────────────────────────────────────────
# Diagnostic: channel is in a market where YouTube's auto-dubbing can unlock
# a second (or third) language audience with a single click.  Particularly
# high-value for channels whose home country has a large diaspora online.
# Studio path: YouTube Studio → Subtitles → select video → Add language
#              YouTube Studio → Subtitles → Dubbing tab (for auto-dub)
# ─────────────────────────────────────────────────────────────────────────────

# Countries where YouTube's auto-dub is most impactful:
# either because the creator content is already in a globally-spoken language
# (EN, ES, PT, KO, JA) and can reach non-native speakers, or because the
# creator is in a non-English market with a large global diaspore (IN, PH, ID).
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


def _score_captions_dub(s: CreatorSignals) -> float:
    """
    Score the "Captions + Auto-Dub" recommendation.

    High confidence when the channel is in a non-English-primary market (or
    a globally-spoken language market) AND the channel is large enough that
    the incremental reach from a second language is meaningful in absolute
    subscriber terms.

    Phase-2 enrichment: ``caption_coverage`` from the worker raises or
    lowers the score based on whether captions are already present.
    """
    # Hard gate: auto-dub is only meaningful in markets where a second-language
    # audience exists or where the creator's language has global diaspora reach.
    # English-primary markets (US, CA, GB, AU …) gain nothing from this feature.
    if s.country_code.upper() not in _AUTODUB_COUNTRIES:
        return 0.0

    score = 0.0

    # Core: country confirmed — auto-dub reach multiplier is highest here
    score += 30.0

    # Channel has enough reach that a new language audience adds up
    if s.subscribers > 1_000_000:
        score += 20.0
    elif s.subscribers > 100_000:
        score += 10.0

    # Active channel — auto-dub only works on videos that are being watched
    if s.views_change_30d > 0:
        score += 20.0

    # Phase-2: low caption coverage confirmed — definite gap
    if s.caption_coverage >= 0 and s.caption_coverage < 0.3:
        score += 30.0
    elif s.caption_coverage == -1.0:
        # Stub: assume partial gap (conservative)
        score += 15.0

    return min(score, 100.0)


_ACTION_REGISTRY["Captions + Auto-Dub"] = ActionMeta(
    score_fn=_score_captions_dub,
    mechanism="Global market with uncaptioned content — auto-dub unlocks a second language audience in one click.",
    effort=ACTION_EFFORT["Captions + Auto-Dub"],
    studio_url=ACTION_STUDIO_LINKS["Captions + Auto-Dub"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Change Category
# ─────────────────────────────────────────────────────────────────────────────
# Diagnostic: channel is performing above its category peers on views and
# growth but is in a low-CPM category.  The category field determines which
# advertiser auction pool bids on the content.  Switching to a higher-CPM
# category (e.g. Science/Tech from Gaming) can increase RPM 2–5× with no
# content change.
# Studio path: YouTube Studio → Content → select video → Details → Category
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


def _score_change_category(s: CreatorSignals) -> float:
    """
    Score the "Change Category" recommendation.

    Only fires when two conditions coincide:
      1. The current category has inherently low CPM (advertiser pool issue)
      2. The channel out-performs its own category peers on reach/growth
         (proving the content quality is there — just mislabelled)

    A channel that under-performs in a low-CPM category should fix content
    first; changing the label won't help if the underlying content is weak.
    """
    if s.primary_category.lower() not in _LOW_CPM_CATEGORIES:
        return 0.0

    score = 0.0

    # Core: low-CPM category confirmed
    score += 30.0

    # Channel over-performs peers on views-per-video — content quality is there
    if s.category_peer_vpv > 0 and s.views_per_video > s.category_peer_vpv:
        score += 25.0

    # Strong viral coefficient — algorithm already surfaces this content broadly
    if s.viral_coeff > 2.0:
        score += 25.0

    # Growing subscriber base — engaged audience validates the CPM upgrade case
    if s.sub_growth_pct > 0.5:
        score += 20.0

    return min(score, 100.0)


_ACTION_REGISTRY["Change Category"] = ActionMeta(
    score_fn=_score_change_category,
    mechanism="High-performing content in a low-CPM category — reclassifying can lift RPM 2–5× with no edits.",
    effort=ACTION_EFFORT["Change Category"],
    studio_url=ACTION_STUDIO_LINKS["Change Category"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Unlist Old Videos
# ─────────────────────────────────────────────────────────────────────────────
# Diagnostic: massive catalogue dragging down channel average VPV.  YouTube's
# browse and search algorithms use channel-average metrics when deciding how
# aggressively to surface a channel.  A tail of zero-view videos dilutes the
# average and suppresses the whole channel.
# Studio path: YouTube Studio → Content → (bulk select) → Edit → Visibility
# ─────────────────────────────────────────────────────────────────────────────

_UNLIST_MIN_VIDEOS = 500  # below this, culling is unlikely to move metrics


def _score_unlist_catalogue(s: CreatorSignals) -> float:
    """
    Score the "Unlist Old Videos" recommendation.

    High confidence when the channel has a very large video count but
    views-per-video is far below category peers — a strong indicator that a
    long tail of low-performing uploads is diluting channel-average signals.
    """
    if s.video_count < _UNLIST_MIN_VIDEOS:
        return 0.0

    score = 0.0

    # Core: massive catalogue confirmed
    score += 35.0

    # Views-per-video well below category floor
    if s.category_peer_vpv > 0 and s.views_per_video < s.category_peer_vpv * 0.3:
        score += 30.0

    # Extreme volume compounds the dilution effect
    if s.video_count > 5_000:
        score += 20.0

    # No recent growth — algorithm already deprioritising the channel
    if s.sub_growth_pct < 0.2:
        score += 15.0

    return min(score, 100.0)


_ACTION_REGISTRY["Unlist Old Videos"] = ActionMeta(
    score_fn=_score_unlist_catalogue,
    mechanism="Massive low-view catalogue dilutes channel-average signals — unlisting the tail raises algorithmic priority.",
    effort=ACTION_EFFORT["Unlist Old Videos"],
    studio_url=ACTION_STUDIO_LINKS["Unlist Old Videos"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Action: Shift to Long-form  (Phase-2 activation)
# ─────────────────────────────────────────────────────────────────────────────
# Diagnostic: channel has built a large Shorts audience (high reach, high
# views) but subscriber-to-revenue conversion is poor because Shorts RPM is
# ~10–40× lower than long-form.  Introducing one long-form video per week
# creates an RPM-multiplier surface without abandoning the Shorts funnel.
# Studio path: YouTube Studio → Upload → (long-form video)
# NOTE: shorts_ratio requires Phase-2 worker enrichment.  Until the worker
# runs, the sentinel value (-1.0) causes this action to score 0 — it
# automatically activates once blueprint_signals is populated.
# ─────────────────────────────────────────────────────────────────────────────

# Minimum reach thresholds: only recommend a shift to long-form once the
# Shorts channel has built enough audience to seed a long-form baseline.
# Treat missing metrics as 0 to keep the guard conservative.
_LONGFORM_MIN_SUBSCRIBERS = 1_000
_LONGFORM_MIN_VIEWS_PER_VIDEO = 500


def _score_shorts_to_longform(s: CreatorSignals) -> float:
    """
    Score the "Shift to Long-form" recommendation.

    Returns 0 until Phase-2 worker computes ``shorts_ratio`` (sentinel = -1).
    Once enriched, fires strongly when Shorts dominate the catalogue AND the
    channel has enough reach to seed a long-form audience immediately.
    """
    # Phase-2 guard: sentinel means worker hasn't run yet — don't score
    if s.shorts_ratio < 0:
        return 0.0

    if s.shorts_ratio < 0.7:
        return 0.0

    # Minimum scale guard: only recommend a shift to long-form once there is
    # enough audience to seed an initial long-form baseline.
    # Require at least one of the reach metrics to clear its threshold.
    if (
        s.subscribers < _LONGFORM_MIN_SUBSCRIBERS
        and s.views_per_video < _LONGFORM_MIN_VIEWS_PER_VIDEO
    ):
        return 0.0

    score = 0.0

    # Core: Shorts-dominant catalogue confirmed
    score += 50.0

    # Large reach means long-form premiere will get immediate views
    if s.views_per_video > 10_000_000:
        score += 25.0

    # Strong viral coeff confirms algorithm is already promoting the channel
    if s.viral_coeff > 3.0:
        score += 25.0

    return min(score, 100.0)


_ACTION_REGISTRY["Shift to Long-form"] = ActionMeta(
    score_fn=_score_shorts_to_longform,
    mechanism="Shorts-dominant reach with low RPM — one long-form video per week unlocks the high-CPM ad auction.",
    effort=ACTION_EFFORT["Shift to Long-form"],
    studio_url=ACTION_STUDIO_LINKS["Shift to Long-form"],
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
