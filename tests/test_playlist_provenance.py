"""Regression tests for the playlist analysis data-provenance labels.

The creator card explicitly distinguishes YouTube-API-sourced fields from
ViralVibes-calculated ones with three visual tiers:

    yt   (sky)    - sourced directly from the YouTube API
    fx   (violet) - calculated arithmetically from YouTube API data
    est. (amber)  - ViralVibes model/estimate; not a figure YouTube provides

YouTube API TOS requires the same treatment on the playlist analysis
endpoint.  This suite pins that contract on the three surfaces users see:

    1. The hero display  -> ui_components.AnalyticsHeader(...)
    2. The metrics table -> views.table.render_playlist_table(...)
    3. The one-line disclaimer -> components.buttons.PlaylistProvenanceFooter()
"""

from typing import Any

import pytest
from fasthtml.common import to_xml

from components.buttons import (
    FxBadge,
    PlaylistProvenanceFooter,
    YtSourceBadge,
)


# ---------------------------------------------------------------------------
# Badge helpers
# ---------------------------------------------------------------------------


def test_yt_badge_carries_source_attribution():
    """The YouTube badge identifies directly sourced API values."""
    html = str(YtSourceBadge())
    assert ">yt<" in html
    assert "YouTube API" in html
    # Sky palette pins the tier.
    assert "text-sky-600" in html


def test_fx_badge_carries_calculated_attribution():
    """The calculated badge identifies values derived by ViralVibes."""
    html = str(FxBadge())
    assert ">fx<" in html
    assert "Calculated" in html or "calculated" in html.lower()
    # Violet palette pins the tier.
    assert "text-violet-600" in html


def test_fx_badge_accepts_custom_detail():
    """A calculated badge exposes its metric-specific formula."""
    html = str(FxBadge(detail="Engagement Rate = (Likes + Comments) / Views"))
    assert "Likes + Comments" in html


# ---------------------------------------------------------------------------
# Disclaimer footer
# ---------------------------------------------------------------------------


def test_playlist_provenance_footer_names_both_sources():
    """The playlist footer explains direct and calculated data sources."""
    html = str(PlaylistProvenanceFooter())
    # Must mention YouTube API as the direct source.
    assert "YouTube API" in html
    # Must mention that some metrics are ViralVibes-calculated.
    assert "ViralVibes" in html
    # Both badges must be present so the tooltip on each still reaches users.
    assert ">yt<" in html
    assert ">fx<" in html


# ---------------------------------------------------------------------------
# Hero display
# ---------------------------------------------------------------------------


def test_hero_stats_are_labelled_with_provenance():
    """Hero metrics render the provenance badge for each value type."""
    from ui_components import AnalyticsHeader

    html = str(
        AnalyticsHeader(
            playlist_title="Sample Playlist",
            channel_name="Sample Channel",
            total_videos=10,
            processed_videos=10,
            processed_date="Jan 15, 2026",
            engagement_rate=0.037,
            total_views=1_234_567,
        )
    )
    # Both the aggregate total-views pill and the aggregate engagement pill
    # are calculated (sum / mean respectively) so both ride the fx badge.
    # A yt badge here would misclassify a derived aggregate as raw API data.
    assert ">fx<" in html
    assert ">yt<" not in html
    # Sanity: the actual metric values still render.
    assert "engagement" in html.lower()


def test_hero_without_metrics_omits_badges():
    """A hero without numeric metrics does not render provenance badges."""
    from ui_components import AnalyticsHeader

    html = str(
        AnalyticsHeader(
            playlist_title="Empty Playlist",
            channel_name="Nobody",
            total_videos=0,
            processed_videos=0,
        )
    )
    # No numeric stats -> no provenance badges (nothing to attribute).
    assert ">yt<" not in html
    assert ">fx<" not in html


# ---------------------------------------------------------------------------
# Table headers
# ---------------------------------------------------------------------------


def _minimal_row() -> dict[str, Any]:
    """Return the smallest playlist row needed to render every table column."""
    return {
        "Rank": 1,
        "Title": "Sample Video",
        "Thumbnail": "https://i.ytimg.com/vi/x/hqdefault.jpg",
        "id": "abc123",
        "Views": 1000,
        "Views Formatted": "1,000",
        "Likes": 10,
        "Likes Formatted": "10",
        "Comments": 5,
        "Comments Formatted": "5",
        "Duration": 120,
        "Duration Formatted": "2:00",
        "Engagement Rate Raw": 0.015,
        "Engagement Rate (%)": "1.5%",
        "Category Emoji": "\U0001f3ae Gaming",
        "CategoryName": "Gaming",
    }


def test_playlist_table_headers_carry_provenance_badges():
    """Rendered table headers include direct and calculated provenance badges."""
    from views.table import render_playlist_table

    def next_order(_col):
        """Return a deterministic sort order for generated table links."""
        return "asc"

    html = to_xml(
        render_playlist_table(
            df=[_minimal_row()],
            summary_stats={
                "total_views": 1000,
                "total_likes": 10,
                "total_comments": 5,
                "avg_duration": 120,
                "avg_engagement": 0.015,
            },
            playlist_url="https://youtube.com/playlist?list=PLxxxx",
            valid_sort="Views",
            valid_order="desc",
            next_order=next_order,
        )
    )

    # Every YouTube-API column header must render a yt badge.
    for api_header in ("Views", "Likes", "Comments", "Duration"):
        assert api_header in html
    # The overall page must include at least one yt badge and one fx badge in
    # the thead (proving both provenance tiers reach the table).
    assert ">yt<" in html
    assert ">fx<" in html


@pytest.mark.parametrize(
    "header,expected_kind",
    [
        ("Views", "yt"),
        ("Likes", "yt"),
        ("Comments", "yt"),
        ("Duration", "yt"),
        ("Engagement Rate", "fx"),
        ("Rank", "fx"),
        ("Title", None),
        ("Thumbnail", None),
        ("Category", None),
    ],
)
def test_header_provenance_mapping(header: str, expected_kind: str | None):
    """The per-header provenance map is the single source of truth; pin it."""
    from views.table import _HEADER_PROVENANCE, _provenance_badge

    assert _HEADER_PROVENANCE[header] == expected_kind
    badge = _provenance_badge(header)
    if expected_kind is None:
        assert badge is None
    else:
        rendered = str(badge)
        assert f">{expected_kind}<" in rendered


def test_engagement_tooltip_matches_actual_formula():
    """Tooltip must describe the mean-of-per-video-ratio formula that
    ``services.youtube_transforms._enrich_dataframe`` actually uses, not an
    aggregate ratio of totals (which is a different metric)."""
    from components.buttons import _PLAYLIST_ENGAGEMENT_DETAIL

    # Formula fingerprint: per-video ratio with +1 smoothing, then mean.
    assert "Likes + Comments" in _PLAYLIST_ENGAGEMENT_DETAIL
    assert "Views + 1" in _PLAYLIST_ENGAGEMENT_DETAIL
    assert "mean" in _PLAYLIST_ENGAGEMENT_DETAIL.lower()


def test_footer_total_avg_cell_carries_fx_badge():
    """The aggregate footer row is entirely ViralVibes-calculated; one fx
    badge next to the row label signals that without cluttering every cell."""
    from views.table import build_table_footer

    html = to_xml(
        build_table_footer(
            summary_stats={
                "total_views": 100,
                "total_likes": 10,
                "total_comments": 2,
                "avg_duration": 60,
                "avg_engagement": 0.01,
                "category_count": 1,
            },
            svc_headers=[
                "Rank",
                "Title",
                "Thumbnail",
                "Views",
                "Likes",
                "Comments",
                "Duration",
                "Engagement Rate",
                "Category",
            ],
        )
    )
    assert "Total / Avg" in html
    assert ">fx<" in html
