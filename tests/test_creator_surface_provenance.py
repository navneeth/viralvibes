"""Provenance labels on blueprint diagnostic strip and compare page."""

from __future__ import annotations

import pytest
from fasthtml.common import to_xml

from utils.blueprint import CreatorSignals
from utils.metric_provenance import CREATOR_LABEL_PROVENANCE, provenance_badge_for_label
from views.blueprint import render_diagnostic_strip
from views.compare import render_compare_page


def _signals(**overrides) -> CreatorSignals:
    defaults = dict(
        creator_id="id",
        channel_name="Ch",
        primary_category="Gaming",
        country_code="US",
        subscribers=100_000,
        video_count=50,
        total_views=5_000_000,
        views_change_30d=50_000,
        subs_change_30d=500,
        viral_coeff=0.5,
        views_per_video=100_000.0,
        sub_growth_pct=0.5,
        category_peer_vpv=80_000.0,
        category_peer_vc=1.0,
        monthly_uploads=2.0,
    )
    defaults.update(overrides)
    return CreatorSignals(**defaults)


def _creator(**overrides) -> dict:
    base = {
        "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "channel_id": "UCxxxxxxxx",
        "channel_name": "Creator One",
        "channel_url": "https://www.youtube.com/channel/UCxxxxxxxx",
        "channel_thumbnail_url": "",
        "current_subscribers": 100_000,
        "current_view_count": 5_000_000,
        "current_video_count": 50,
        "subscribers_change_30d": 500,
        "views_change_30d": 50_000,
        "engagement_score": 6.5,
        "monthly_uploads": 2.0,
        "channel_age_days": 800,
        "country_code": "US",
        "default_language": "en",
        "primary_category": "Gaming",
        "hidden_subscriber_count": False,
    }
    base.update(overrides)
    return base


@pytest.mark.parametrize(
    "label,expected_kind",
    [
        ("Subscribers", "yt"),
        ("Total Views", "yt"),
        ("Videos Published", "yt"),
        ("Net change (30d)", "fx"),
        ("Growth rate (30d)", "fx"),
        ("Avg views / video", "fx"),
        ("Views / Subscriber", "fx"),
        ("Engagement", "est"),
        ("Category p75 VPV", "est"),
        ("Reach multiplier (30d)", "fx"),
        ("Sub growth (30d)", "fx"),
    ],
)
def test_creator_label_provenance_mapping(label: str, expected_kind: str):
    assert CREATOR_LABEL_PROVENANCE[label] == expected_kind
    badge = provenance_badge_for_label(label)
    assert badge is not None
    assert f">{expected_kind}<" in str(badge) or (expected_kind == "est" and ">est.<" in str(badge))


def test_blueprint_diagnostic_strip_uses_fx_and_est_badges():
    html = to_xml(render_diagnostic_strip(_signals()))
    assert ">fx<" in html
    assert ">est.<" in html
    assert ">yt<" not in html


def test_compare_page_carries_yt_fx_and_est_badges():
    a = _creator(id="aaaaaaaa-bbbb-cccc-dddd-111111111111", channel_name="Alpha")
    b = _creator(
        id="aaaaaaaa-bbbb-cccc-dddd-222222222222",
        channel_name="Beta",
        current_subscribers=50_000,
    )
    html = to_xml(render_compare_page(a, b))
    assert ">yt<" in html
    assert ">fx<" in html
    assert ">est.<" in html
    assert "YouTube API" in html
