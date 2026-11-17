import math
from dataclasses import replace

import pytest

from utils.revenue_model import (
    DEFAULT_PARAMETERS,
    RevenueParameters,
    RevenueProfile,
    blend_revenue,
    estimate_creator_ad_revenue,
    estimate_monthly_ad_revenue,
    estimate_monthly_views,
)


def test_supplied_mrbeast_reference():
    estimate = estimate_monthly_ad_revenue(
        RevenueProfile(
            subscribers=524e6,
            total_views=146.7e9,
            views_30d=1.7e9,
            video_count=1012,
            channel_age_years=14,
            country="US",
            categories=("Entertainment", "Lifestyle (sociology)"),
        )
    )
    assert estimate.monthly_usd == pytest.approx(941901.72)
    assert estimate.method == "blend"
    assert estimate.low_usd < estimate.monthly_usd < estimate.high_usd


@pytest.mark.parametrize("value", [None, -1, float("nan"), float("inf"), "bad", True])
def test_invalid_or_missing_data_is_unavailable(value):
    result = estimate_monthly_ad_revenue(RevenueProfile(subscribers=value, total_views=value))
    assert result.monthly_usd is None


def test_hidden_subscribers_allow_views_only():
    result = estimate_creator_ad_revenue(
        {
            "hidden_subscriber_count": True,
            "current_subscribers": 100000,
            "current_view_count": 12000000,
            "channel_age_days": 3652.5,
            "current_video_count": 100,
            "country_code": "US",
        }
    )
    assert result.method == "views_only"
    assert result.monthly_usd > 0


def test_delta_normalized_to_measured_window():
    creator = {"current_subscribers": 1e6, "current_view_count": 120e6, "channel_age_days": 3652.5}
    first = estimate_creator_ad_revenue(
        creator | {"views_change_30d": 1e6, "growth_window_days": 10}
    )
    second = estimate_creator_ad_revenue(
        creator | {"views_change_30d": 3e6, "growth_window_days": 30}
    )
    assert first == second


def test_corrupt_delta_does_not_inflate_estimate():
    profile = RevenueProfile(
        subscribers=202e6, total_views=227.3e9, channel_age_years=20, country="US", is_kids=True
    )
    baseline = estimate_monthly_ad_revenue(profile)
    corrupted = estimate_monthly_ad_revenue(replace(profile, views_30d=184.1e9))
    assert corrupted.monthly_usd == baseline.monthly_usd
    assert "view_delta_rejected" in corrupted.flags


def test_zero_is_distinct_from_unknown():
    assert estimate_monthly_ad_revenue(RevenueProfile(total_views=0)).monthly_usd == 0
    assert estimate_monthly_ad_revenue(RevenueProfile()).monthly_usd is None


def test_category_storage_variants_agree():
    creator = {"current_subscribers": 1e6, "current_view_count": 120e6, "channel_age_days": 3652.5}
    assert estimate_creator_ad_revenue(
        creator | {"topic_categories": '["Video game culture"]'}
    ) == estimate_creator_ad_revenue(
        creator | {"topic_categories": ["https://en.wikipedia.org/wiki/Video_game_culture"]}
    )


def test_unknown_country_not_treated_as_us_and_small_channel_low_confidence():
    profile = RevenueProfile(subscribers=100000, total_views=12e6, channel_age_years=5)
    estimate = estimate_monthly_ad_revenue(profile)
    assert (
        estimate.monthly_usd
        < estimate_monthly_ad_revenue(replace(profile, country="US")).monthly_usd
    )
    assert estimate.confidence == "very_low"
    assert math.isfinite(estimate.monthly_usd)


@pytest.mark.parametrize(
    "key,value",
    [
        ("base_rpm", 0),
        ("views_weight_small", 2),
        ("band_fold", float("nan")),
        ("delta_clip", [4, 0.25]),
        ("delta_clip", None),
        ("delta_clip", [1]),
        ("delta_clip", [0.25, "4"]),
        ("subscriber_intercept", "bad"),
        ("subscriber_shorts_adjustment", None),
        ("base_rpm", "1.7"),
        ("views_weight_small", "0.6"),
        ("band_fold", "3"),
    ],
)
def test_invalid_parameter_updates_are_rejected(key, value):
    with pytest.raises(ValueError):
        replace(DEFAULT_PARAMETERS, **{key: value})


def test_log_space_blend_is_geometric_not_arithmetic():
    assert blend_revenue(100, 10000, 0.5) == pytest.approx(1000)
    assert blend_revenue(100, 10000, 0) == pytest.approx(10000)
    assert blend_revenue(100, 10000, 1) == pytest.approx(100)


def test_components_explain_the_final_estimate():
    profile = RevenueProfile(subscribers=1e6, total_views=120e6, channel_age_years=10, country="US")
    estimate = estimate_monthly_ad_revenue(profile)
    assert estimate.views_estimate_usd == pytest.approx(
        estimate.monthly_views / 1000 * estimate.effective_rpm
    )
    assert estimate.subscriber_estimate_usd == pytest.approx(
        10 ** (-1.969 + 0.906 * math.log10(1e6))
    )
    assert estimate.monthly_usd == round(
        blend_revenue(
            estimate.views_estimate_usd, estimate.subscriber_estimate_usd, estimate.views_weight
        ),
        2,
    )
    assert estimate.shorts_share is None


def test_candidate_parameters_are_explicit_and_do_not_mutate_baseline():
    profile = RevenueProfile(subscribers=1e6, total_views=120e6, channel_age_years=10, country="US")
    baseline = estimate_monthly_ad_revenue(profile)
    candidate = replace(DEFAULT_PARAMETERS, version="candidate", base_rpm=3.4)
    changed = estimate_monthly_ad_revenue(profile, candidate)
    assert changed.views_estimate_usd == pytest.approx(baseline.views_estimate_usd * 2)
    assert changed.subscriber_estimate_usd == baseline.subscriber_estimate_usd
    assert changed.model_version == "candidate"
    assert DEFAULT_PARAMETERS == RevenueParameters()


def test_observed_growth_is_damped_not_substituted_for_monthly_views():
    profile = RevenueProfile(total_views=120e6, channel_age_years=10)
    monthly_views, _ = estimate_monthly_views(profile)
    adjusted, _ = estimate_monthly_views(replace(profile, views_30d=4e6))
    assert monthly_views == pytest.approx(1e6)
    assert adjusted == pytest.approx(2e6)


def test_known_shorts_share_affects_both_components_and_remains_inspectable():
    profile = RevenueProfile(subscribers=1e6, total_views=120e6, channel_age_years=10, country="US")
    long_form = estimate_monthly_ad_revenue(replace(profile, shorts_share=0))
    shorts = estimate_monthly_ad_revenue(replace(profile, shorts_share=1))
    assert shorts.shorts_share == 1
    assert "shorts_mix_unknown" not in shorts.flags
    assert shorts.views_estimate_usd == pytest.approx(long_form.views_estimate_usd * 0.12)
    assert shorts.subscriber_estimate_usd == pytest.approx(
        long_form.subscriber_estimate_usd * 10**-0.587
    )


def test_explicit_known_niche_overrides_broad_categories():
    profile = RevenueProfile(
        subscribers=1e6,
        total_views=120e6,
        channel_age_years=10,
        country="US",
        categories=("Knowledge",),
        niche_hint=" Finance ",
    )
    estimate = estimate_monthly_ad_revenue(profile)
    assert estimate.niche == "finance"
    assert estimate.effective_rpm == pytest.approx(1.7 * 4)
    assert estimate_monthly_ad_revenue(replace(profile, niche_hint="unknown")).niche == "knowledge"


@pytest.mark.parametrize(
    "subscribers,views,age,country,categories,kids,expected",
    [
        (526e6, 146.7e9, 14, "US", ("Entertainment", "Lifestyle (sociology)"), False, 898628.28),
        (316e6, 363.5e9, 20, "IN", ("Film", "Music"), False, 187443.42),
        (202e6, 227.3e9, 20, "US", ("Entertainment", "Film", "Music"), True, 405669.31),
        (5.2e6, 1.4e9, 9, "US", ("Entertainment", "Knowledge"), False, 14686.17),
        (6.6e6, 536e6, 18, "GB", ("Knowledge",), False, 13770.39),
        (2.3e6, 332.1e6, 6, "US", ("Knowledge", "Lifestyle"), False, 6710.67),
    ],
)
def test_transparency_refactor_preserves_sample_estimates(
    subscribers, views, age, country, categories, kids, expected
):
    estimate = estimate_monthly_ad_revenue(
        RevenueProfile(
            subscribers=subscribers,
            total_views=views,
            channel_age_years=age,
            country=country,
            categories=categories,
            is_kids=kids,
        )
    )
    assert estimate.monthly_usd == pytest.approx(expected)


def test_missing_observation_window_ignores_delta():
    creator = {"current_subscribers": 1e6, "current_view_count": 120e6, "channel_age_days": 3652.5}
    assert estimate_creator_ad_revenue(
        creator | {"views_change_30d": 1e9}
    ) == estimate_creator_ad_revenue(creator)


def test_extreme_finite_inputs_do_not_raise():
    result = estimate_monthly_ad_revenue(
        RevenueProfile(
            subscribers=1e308,
            total_views=1e308,
            channel_age_years=1e308,
            views_30d=1e308,
        )
    )
    assert result.monthly_usd is None or math.isfinite(result.monthly_usd)


@pytest.mark.parametrize("share", [50, -0.5, float("nan"), "bad", True])
def test_invalid_shorts_share_is_unknown_not_all_shorts(share):
    profile = RevenueProfile(subscribers=1e6, total_views=120e6, channel_age_years=10)
    baseline = estimate_monthly_ad_revenue(profile)
    result = estimate_monthly_ad_revenue(replace(profile, shorts_share=share))
    assert result.monthly_usd == baseline.monthly_usd
    assert result.shorts_share is None
    assert "invalid_shorts_share" in result.flags
    assert result.confidence == "very_low"


def test_zero_public_videos_does_not_erase_historical_view_evidence():
    result = estimate_monthly_ad_revenue(
        RevenueProfile(
            subscribers=1e6,
            total_views=120e6,
            channel_age_years=10,
            video_count=0,
        )
    )
    assert result.monthly_usd > 0
    assert result.method == "blend"
    assert "no_public_videos" in result.flags
    assert result.confidence == "very_low"
    assert estimate_monthly_ad_revenue(RevenueProfile(video_count=0)).monthly_usd is None


def test_partial_sync_does_not_apply_old_growth_to_fresh_counts():
    creator = {
        "current_subscribers": 1e6,
        "current_view_count": 120e6,
        "channel_age_days": 3652.5,
        "sync_status": "synced_partial",
    }
    baseline = estimate_creator_ad_revenue(creator)
    stale = estimate_creator_ad_revenue(
        creator | {"views_change_30d": 4e6, "growth_window_days": 30}
    )
    assert stale.monthly_usd == baseline.monthly_usd
    assert "growth_ignored_partial_sync" in stale.flags
