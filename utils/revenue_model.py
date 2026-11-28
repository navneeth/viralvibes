"""Monthly ad revenue: two understandable estimates, blended in log space.

    monthly_views = lifetime_views / channel_age_months, nudged by measured growth
    views_estimate = monthly_views / 1000 * effective_rpm
    log10(subscriber_estimate) = intercept + slope * log10(subscribers) + adjustments
    log(estimate) = views_weight * log(views_estimate)
                    + (1 - views_weight) * log(subscriber_estimate)

The geometric blend treats disagreement multiplicatively: equally weighting
$100 and $10,000 gives $1,000, not the arithmetic mean of $5,050.

FIT means reported fitted in the supplied proposal, not independently validated
here. PRIOR means an assumption. Peer-tool agreement is not verified earnings.
See docs/revenue-model.md for proposal differences and the retired model.
"""

import json
from dataclasses import dataclass, replace
from math import exp, isfinite, log, log10
from typing import Mapping

from .core import normalize_category_name


def _number(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if isfinite(number) and number >= 0 else None


# PRIOR: owner-set channel country is not measured viewer geography.
COUNTRY_TIERS = {
    "1": "US GB CA AU DE CH NO SE NL DK IE NZ AT LU IS FI".split(),
    "2": "JP KR FR IT ES AE SA IL SG TW BE HK QA KW PT CZ".split(),
    "3": "BR MX RU PH AR CL CO TR PL TH MY ZA PE UA RO HU GR CN".split(),
    "4": "IN PK ID NG BD EG VN KE ET LK NP GH TZ UG MA DZ IQ".split(),
}
COUNTRY_TIER = {country: tier for tier, countries in COUNTRY_TIERS.items() for country in countries}
DEFAULT_TIER = "2"

# FIT: supplied ridge fit on 103 peer-labelled creators, primarily 1M-500M subs.
# Underlying rows, collection dates and reproduced validation are not available.
SUBSCRIBER_TIER_ADJUSTMENT = {"1": 0.0, "2": -0.516, "3": -0.554, "4": -0.458}

# PRIOR: supplied country/niche RPM ratios, not fitted earnings guarantees.
TIER_RPM_MULTIPLIER = {"1": 1.0, "2": 0.55, "3": 0.25, "4": 0.12}
NICHE_RPM_MULTIPLIER = {
    "finance": 4.0,
    "business": 3.0,
    "tech": 2.0,
    "education": 1.5,
    "knowledge": 1.3,
    "health": 1.5,
    "fitness": 1.3,
    "lifestyle": 1.0,
    "other": 1.0,
    "gaming": 0.8,
    "entertainment": 0.8,
    "comedy": 0.8,
    "sports": 0.6,
    "music": 0.6,
    "film": 0.6,
    "tv": 0.5,
    "news": 0.5,
    "kids": 0.5,
}

# First matching niche wins, following the proposal's broad-category precedence.
# Extra aliases cover existing YouTube/category labels, not inferred bio topics.
NICHE_KEYWORDS = {
    "kids": ("kids",),
    "news": ("news", "politic"),
    "tv": ("television",),
    "music": ("music",),
    "film": ("film",),
    "sports": ("sport", "football", "wrestling"),
    "gaming": ("video game", "game", "gaming"),
    "knowledge": ("knowledge",),
    "comedy": ("humour", "comedy"),
    "entertainment": ("entertainment",),
    "lifestyle": ("lifestyle",),
    "finance": ("finance", "investing"),
    "business": ("business",),
    "tech": ("technology", "tech"),
    "education": ("education",),
    "health": ("health",),
    "fitness": ("fitness",),
}


@dataclass(frozen=True)
class RevenueParameters:
    """Edit defaults/version together; pass a replace() copy to compare a candidate."""

    version: str = "ads-2026-11-17.3"
    # FIT: log10(monthly USD) subscriber curve and Shorts-share adjustment.
    subscriber_intercept: float = -1.969
    subscriber_exponent: float = 0.906
    subscriber_shorts_adjustment: float = -0.587
    # FIT: dollars per 1,000 *all* US long-form views, not advertiser CPM.
    base_rpm: float = 1.70
    # PRIOR: Shorts RPM relative to long-form; share means share of views.
    shorts_rpm_ratio: float = 0.12
    # FIT/guard: corrupted deltas must not replace the lifetime-average anchor.
    # Match the proposal's code: clip low positive ratios, do not reject them.
    delta_reject_above: float = 25.0
    delta_clip: tuple[float, float] = (0.25, 4.0)
    delta_exponent: float = 0.5
    # FIT: supplied 17-mega-channel experiment preferred a 0.15 views weight.
    views_weight_large: float = 0.15
    # PRIOR: 0.60 below 500K is unvalidated extrapolation, not a second fit.
    views_weight_small: float = 0.60
    small_subscribers: float = 500_000
    # SCENARIO: proposal reports ~60% peer-label coverage at /3..*3, NOT 90%.
    band_fold: float = 3.0
    # GUARDS: calculation assumptions, not empirically established hard limits.
    max_effective_rpm: float = 12.0
    min_age_months: float = 3.0
    disagreement_fold: float = 6.0

    def __post_init__(self):
        def finite_number(value):
            return (
                isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)
            )

        if not isinstance(self.version, str) or not self.version:
            raise ValueError("Missing revenue model version")
        for name in (
            "base_rpm",
            "subscriber_exponent",
            "delta_reject_above",
            "delta_exponent",
            "small_subscribers",
            "max_effective_rpm",
            "min_age_months",
            "disagreement_fold",
        ):
            value = getattr(self, name)
            if not finite_number(value) or value <= 0:
                raise ValueError(f"Invalid revenue parameter: {name}")
        for name in ("views_weight_large", "views_weight_small", "shorts_rpm_ratio"):
            value = getattr(self, name)
            if not finite_number(value) or not 0 <= value <= 1:
                raise ValueError(f"Invalid revenue parameter: {name}")
        for value in (self.subscriber_intercept, self.subscriber_shorts_adjustment):
            if not finite_number(value):
                raise ValueError("Invalid subscriber coefficient")
        if (
            not isinstance(self.delta_clip, (tuple, list))
            or len(self.delta_clip) != 2
            or not all(finite_number(value) for value in self.delta_clip)
        ):
            raise ValueError("Invalid delta clipping range")
        lower, upper = self.delta_clip
        if not 0 < lower <= upper:
            raise ValueError("Invalid delta clipping range")
        if not finite_number(self.band_fold) or self.band_fold < 1:
            raise ValueError("Invalid scenario band")


DEFAULT_PARAMETERS = RevenueParameters()


@dataclass(frozen=True)
class RevenueProfile:
    subscribers: float | None = None
    total_views: float | None = None
    video_count: float | None = None
    channel_age_years: float | None = None
    views_30d: float | None = None
    country: str | None = None
    categories: tuple[str, ...] = ()
    is_kids: bool = False
    shorts_share: float | None = None
    niche_hint: str | None = None


@dataclass(frozen=True)
class RevenueEstimate:
    monthly_usd: float | None
    low_usd: float | None
    high_usd: float | None
    method: str
    confidence: str
    flags: tuple[str, ...]
    model_version: str
    # Unrounded components make each estimate explainable before presentation.
    monthly_views: float | None
    effective_rpm: float | None
    views_estimate_usd: float | None
    subscriber_estimate_usd: float | None
    views_weight: float | None
    country_tier: str
    niche: str
    shorts_share: float | None


def infer_niche(profile: RevenueProfile) -> str:
    hint = (profile.niche_hint or "").strip().lower()
    if hint in NICHE_RPM_MULTIPLIER:
        return hint
    if profile.is_kids:
        return "kids"
    categories = " ".join(profile.categories).lower()
    return next(
        (
            niche
            for niche, keywords in NICHE_KEYWORDS.items()
            if any(word in categories for word in keywords)
        ),
        "other",
    )


def estimate_monthly_views(
    profile: RevenueProfile,
    parameters: RevenueParameters = DEFAULT_PARAMETERS,
) -> tuple[float | None, tuple[str, ...]]:
    """Lifetime-average anchor with a bounded square-root growth adjustment.

    A 4x observed/base ratio produces a 2x adjustment, not 4x. This intentionally
    damps growth and decline; it does not measure current monthly views directly.
    """
    views = _number(profile.total_views)
    age = _number(profile.channel_age_years)
    if views is None or not views or age is None or not age:
        return None, ("no_lifetime_views_or_age",)
    monthly_views = views / max(age * 12, parameters.min_age_months)
    flags = ["lifetime_monthly_average_proxy"]
    delta = _number(profile.views_30d)
    if delta is not None and delta > 0 and monthly_views > 0:
        ratio = delta / monthly_views
        if ratio > parameters.delta_reject_above:
            flags.append("view_delta_rejected")
        else:
            lower, upper = parameters.delta_clip
            monthly_views *= min(max(ratio, lower), upper) ** parameters.delta_exponent
    elif profile.views_30d is not None:
        flags.append("nonpositive_or_invalid_delta_ignored")
    return monthly_views, tuple(flags)


def estimate_subscriber_revenue(
    subscribers: float,
    tier: str,
    shorts_share: float,
    parameters: RevenueParameters = DEFAULT_PARAMETERS,
) -> float:
    """The fitted power law expressed explicitly in base-10 log space."""
    log_monthly_usd = (
        parameters.subscriber_intercept
        + parameters.subscriber_exponent * log10(subscribers)
        + SUBSCRIBER_TIER_ADJUSTMENT[tier]
        + parameters.subscriber_shorts_adjustment * shorts_share
    )
    return 10**log_monthly_usd


def blend_revenue(views_estimate: float, subscriber_estimate: float, views_weight: float) -> float:
    """Weighted geometric mean: the blend stays between positive component estimates."""
    return exp(
        views_weight * log(max(views_estimate, 1e-300))
        + (1 - views_weight) * log(max(subscriber_estimate, 1e-300))
    )


def estimate_monthly_ad_revenue(
    profile: RevenueProfile,
    parameters: RevenueParameters = DEFAULT_PARAMETERS,
) -> RevenueEstimate:
    """Combine view/run-rate and subscriber estimates; retain evidence and caveats."""
    flags = []
    subscribers = _number(profile.subscribers)
    known_shorts_share = _number(profile.shorts_share)
    if profile.shorts_share is not None and (known_shorts_share is None or known_shorts_share > 1):
        known_shorts_share = None
        flags.append("invalid_shorts_share")
    shorts = known_shorts_share if known_shorts_share is not None else 0.0
    if known_shorts_share is None:
        flags.append("shorts_mix_unknown")
    country = (profile.country or "").strip().upper()
    tier = COUNTRY_TIER.get(country, DEFAULT_TIER)
    flags.append("channel_country_is_audience_proxy" if country else "country_unknown")
    niche = infer_niche(profile)
    monthly_views = effective_rpm = structural = statistical = weight = None

    def result(amount, method, confidence):
        if "no_public_videos" in flags or "invalid_shorts_share" in flags:
            confidence = "very_low"
        band = parameters.band_fold
        if amount is not None and (not isfinite(amount) or not isfinite(amount * band)):
            amount, method, confidence = None, "unavailable", "very_low"
            flags.append("invalid_numeric_result")
        return RevenueEstimate(
            monthly_usd=round(amount, 2) if amount is not None else None,
            low_usd=round(amount / band, 2) if amount is not None else None,
            high_usd=round(amount * band, 2) if amount is not None else None,
            method=method,
            confidence=confidence,
            flags=tuple(flags),
            model_version=parameters.version,
            monthly_views=monthly_views,
            effective_rpm=effective_rpm,
            views_estimate_usd=structural,
            subscriber_estimate_usd=statistical,
            views_weight=weight,
            country_tier=tier,
            niche=niche,
            shorts_share=shorts if known_shorts_share is not None else None,
        )

    if _number(profile.total_views) == 0:
        return result(0.0, "zero", "low")
    if _number(profile.video_count) == 0:
        flags.append("no_public_videos")

    monthly_views, view_flags = estimate_monthly_views(profile, parameters)
    flags.extend(view_flags)
    if monthly_views is not None:
        effective_rpm = min(
            parameters.base_rpm
            * TIER_RPM_MULTIPLIER[tier]
            * NICHE_RPM_MULTIPLIER[niche]
            * ((1 - shorts) + shorts * parameters.shorts_rpm_ratio),
            parameters.max_effective_rpm,
        )
        structural = monthly_views / 1000 * effective_rpm

    if subscribers is not None and subscribers > 0:
        try:
            statistical = estimate_subscriber_revenue(subscribers, tier, shorts, parameters)
        except OverflowError:
            flags.append("invalid_subscriber_result")
    if structural is None and statistical is None:
        return result(None, "unavailable", "very_low")
    if structural is None:
        flags.append("subscriber_only_fallback")
        return result(statistical, "subs_only", "very_low")
    if statistical is None:
        return result(structural, "views_only", "very_low")
    small_channel = subscribers < parameters.small_subscribers
    weight = parameters.views_weight_small if small_channel else parameters.views_weight_large
    if small_channel:
        flags.append("small_channel_extrapolation")
    if (
        max(structural, statistical) / max(min(structural, statistical), 1e-9)
        > parameters.disagreement_fold
    ):
        flags.append("estimators_disagree")
    return result(
        blend_revenue(structural, statistical, weight),
        "blend",
        "very_low" if small_channel else "low",
    )


def estimate_creator_ad_revenue(
    creator: Mapping,
    parameters: RevenueParameters = DEFAULT_PARAMETERS,
) -> RevenueEstimate:
    """Adapt a database row so cards and profiles use identical input semantics."""
    categories = creator.get("topic_categories") or []
    if isinstance(categories, str):
        try:
            categories = json.loads(categories)
        except (ValueError, TypeError):
            categories = categories.split(",")
    if not isinstance(categories, (list, tuple)):
        categories = []
    categories = [normalize_category_name(value) for value in categories if isinstance(value, str)]
    primary = creator.get("primary_category")
    if isinstance(primary, str):
        categories.append(primary)
    age_days = _number(creator.get("channel_age_days"))
    delta = _number(creator.get("views_change_30d"))
    window = _number(creator.get("growth_window_days"))
    monthly_delta = delta * 30 / window if delta is not None and window and window >= 7 else None
    partial_sync = creator.get("sync_status") == "synced_partial"
    if partial_sync:
        monthly_delta = None
    estimate = estimate_monthly_ad_revenue(
        RevenueProfile(
            subscribers=(
                None
                if creator.get("hidden_subscriber_count")
                else creator.get("current_subscribers")
            ),
            total_views=creator.get("current_view_count"),
            video_count=creator.get("current_video_count"),
            channel_age_years=age_days / 365.25 if age_days else None,
            views_30d=monthly_delta,
            country=creator.get("country_code"),
            categories=tuple(categories),
            is_kids=creator.get("is_made_for_kids") is True,
        ),
        parameters,
    )
    if partial_sync and delta is not None:
        return replace(estimate, flags=estimate.flags + ("growth_ignored_partial_sync",))
    return estimate
