"""
Creator-surface metric provenance — single source of truth for compare & blueprint.

Mirrors the playlist analyzer contract (views/table.py _HEADER_PROVENANCE):
  yt   — YouTube Data API field
  fx   — arithmetic on API-sourced values (incl. 30d deltas from sync snapshots)
  est. — ViralVibes model or cohort benchmark
"""

from __future__ import annotations

from fasthtml.common import Div, P

from components.buttons import EstimatedBadge, FxBadge, YtSourceBadge

_FX_VPV_DETAIL = (
    "Lifetime total views ÷ video count. Calculated by ViralVibes from YouTube "
    "API statistics — not a field YouTube returns directly."
)
_FX_VIRAL_COEFF_DETAIL = (
    "Net 30-day view change ÷ current subscribers. Above 1× means views grew by "
    "more than the channel's subscriber count in the past month. Calculated by "
    "ViralVibes from synced YouTube API data."
)
_FX_SUB_GROWTH_DETAIL = (
    "30-day subscriber change ÷ current subscribers, expressed as a percentage. "
    "Calculated by ViralVibes from synced YouTube API data."
)
_FX_NET_CHANGE_30D_DETAIL = (
    "Net subscriber change over 30 days, computed by ViralVibes by comparing "
    "successive YouTube API sync snapshots."
)
_FX_GROWTH_RATE_DETAIL = (
    "30-day subscriber change ÷ current subscribers × 100. Calculated by "
    "ViralVibes — YouTube does not return a growth-rate percentage."
)
_FX_VPS_DETAIL = (
    "Lifetime views ÷ subscriber count. Calculated by ViralVibes from YouTube " "API statistics."
)
_FX_UPLOAD_RATE_DETAIL = (
    "Average uploads per month, derived by ViralVibes from the channel's "
    "YouTube API sync history."
)
_FX_CHANNEL_AGE_DETAIL = (
    "Time since the channel published date (snippet.publishedAt) from the "
    "YouTube API, formatted for display."
)
_EST_CATEGORY_P75_VPV_DETAIL = (
    "75th-percentile views per video across channels in the same category. "
    "Computed by ViralVibes from aggregate channel data — not a YouTube metric."
)
_EST_ENGAGEMENT_DETAIL = (
    "0–10 engagement score derived from recent video likes and comments relative "
    "to views. ViralVibes model — not provided by YouTube's API."
)

# Display label → provenance tier (None = no badge)
CREATOR_LABEL_PROVENANCE: dict[str, str | None] = {
    # Audience scale (compare)
    "Subscribers": "yt",
    "Total Views": "yt",
    "Videos Published": "yt",
    # Growth (compare + blueprint)
    "Net change (30d)": "fx",
    "Growth rate (30d)": "fx",
    "Sub growth (30d)": "fx",
    "Reach multiplier (30d)": "fx",
    # Quality / performance
    "Avg views / video": "fx",
    "Views / Subscriber": "fx",
    "Engagement": "est",
    # Output
    "Upload rate": "fx",
    "Channel age": "fx",
    # Blueprint benchmarks
    "Category p75 VPV": "est",
}

CREATOR_LABEL_DETAIL: dict[str, str] = {
    "Avg views / video": _FX_VPV_DETAIL,
    "Reach multiplier (30d)": _FX_VIRAL_COEFF_DETAIL,
    "Sub growth (30d)": _FX_SUB_GROWTH_DETAIL,
    "Net change (30d)": _FX_NET_CHANGE_30D_DETAIL,
    "Growth rate (30d)": _FX_GROWTH_RATE_DETAIL,
    "Views / Subscriber": _FX_VPS_DETAIL,
    "Upload rate": _FX_UPLOAD_RATE_DETAIL,
    "Channel age": _FX_CHANNEL_AGE_DETAIL,
    "Category p75 VPV": _EST_CATEGORY_P75_VPV_DETAIL,
    "Engagement": _EST_ENGAGEMENT_DETAIL,
}


def provenance_badge_for_label(label: str):
    """Return yt / fx / est. badge for a creator metric label, or None."""
    kind = CREATOR_LABEL_PROVENANCE.get(label)
    if kind is None:
        return None
    detail = CREATOR_LABEL_DETAIL.get(label, "")
    if kind == "yt":
        return YtSourceBadge()
    if kind == "fx":
        return FxBadge(detail=detail)
    if kind == "est":
        return EstimatedBadge(detail=detail)
    return None


def CreatorProvenanceFooter() -> Div:
    """Policy-compliant disclaimer for creator compare / blueprint surfaces."""
    return Div(
        Div(
            YtSourceBadge(),
            FxBadge(),
            EstimatedBadge(),
            cls="flex items-center gap-1.5 shrink-0",
        ),
        P(
            "Subscriber, view and video counts come directly from the YouTube API. "
            "Growth deltas, averages and ratios are ViralVibes-calculated from those "
            "values. Engagement scores and category benchmarks are ViralVibes estimates.",
            cls="text-[11px] text-muted-foreground leading-relaxed",
        ),
        cls=(
            "flex items-start gap-2 px-3 py-2 mb-4 rounded-md " "bg-muted/40 border border-border"
        ),
    )
