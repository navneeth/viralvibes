"""Focused tests for calculated-metric and dashboard action UI."""

from fasthtml.common import Div, to_xml

from components.cards import MetricCard
from components.processing_tips import PROCESSING_TIPS, get_tip_for_progress
from ui_components import PlaylistMetricsOverview
from views import dashboard as dashboard_view


class TestMetricCardFormulaBadge:
    def test_formula_renders_fx_badge_and_tooltip(self):
        html = to_xml(
            MetricCard(
                title="Engagement Rate",
                value="3.5%",
                subtitle="Likes + comments ÷ views",
                icon="heart",
                formula="(Likes + Comments) ÷ Views × 100",
            )
        )

        assert ">fx</span>" in html
        assert 'title="(Likes + Comments) ÷ Views × 100"' in html
        assert "bg-violet-50 text-violet-400" in html

    def test_omitted_formula_does_not_render_badge(self):
        html = to_xml(
            MetricCard(
                title="Top Performer",
                value="1.2M",
                subtitle="Most-viewed video",
                icon="trending-up",
            )
        )

        assert ">fx</span>" not in html
        assert "bg-violet-50 text-violet-400" not in html

    def test_empty_formula_is_treated_as_absent(self):
        html = to_xml(
            MetricCard(
                title="Top Performer",
                value="1.2M",
                subtitle="Most-viewed video",
                icon="trending-up",
                formula="",
            )
        )

        assert ">fx</span>" not in html
        assert 'title=""' not in html


class TestPlaylistMetricsFormulaAnnotations:
    def test_calculated_cards_include_expected_formulas_and_legend(self):
        html = to_xml(
            PlaylistMetricsOverview(
                [
                    {"Views": 800, "Likes": 40, "Comments": 8},
                    {"Views": 200, "Likes": 10, "Comments": 2},
                ],
                {"actual_playlist_count": 2, "total_views": 1_000},
            )
        )

        assert 'title="Sum of individual video views from YouTube"' in html
        assert 'title="(Likes + Comments) ÷ Views × 100"' in html
        assert 'title="Total views ÷ number of videos"' in html
        assert "Calculated by ViralVibes from YouTube Analytics data." in html
        assert "Hover any badge for the formula." in html
        # Three calculated cards plus the legend badge.
        assert html.count(">fx</span>") == 4

    def test_source_metrics_remain_unbadged_when_optional_cards_render(self):
        html = to_xml(
            PlaylistMetricsOverview(
                [
                    {
                        "Views": 100,
                        "Likes": 5,
                        "Comments": 1,
                        "Definition": "hd",
                        "Caption": True,
                    }
                ],
                {"actual_playlist_count": 1, "total_views": 100},
            )
        )

        assert "Top Performer" in html
        assert "HD Content" in html
        assert "Captioned" in html
        # Adding direct-source cards must not accidentally annotate them as calculated.
        assert html.count('title="') == 3
        assert html.count(">fx</span>") == 4


def test_processing_tip_promotes_permanent_dashboard_sharing():
    sharing_tip = get_tip_for_progress(2 / len(PROCESSING_TIPS))

    assert sharing_tip == {
        "icon": "share-2",
        "title": "Share Your Results",
        "content": (
            "Once complete, share your dashboard with a permanent link — "
            "no sign-in required for viewers."
        ),
    }
    assert all(tip["title"] != "Export Your Data" for tip in PROCESSING_TIPS)


def _stub_dashboard_dependencies(monkeypatch):
    """Replace unrelated dashboard sections with small deterministic nodes."""
    monkeypatch.setattr(dashboard_view, "StepProgress", lambda *_args, **_kwargs: Div("steps"))
    monkeypatch.setattr(dashboard_view, "AnalyticsHeader", lambda **_kwargs: Div("header"))
    monkeypatch.setattr(
        dashboard_view,
        "render_playlist_table",
        lambda **_kwargs: Div("table"),
    )
    monkeypatch.setattr(
        dashboard_view,
        "VideoExtremesSection",
        lambda *_args, **_kwargs: Div("extremes"),
    )
    monkeypatch.setattr(
        dashboard_view,
        "AnalyticsDashboardSection",
        lambda *_args, **_kwargs: Div("analytics"),
    )


def _render_dashboard(monkeypatch, dashboard_id):
    _stub_dashboard_dependencies(monkeypatch)
    return to_xml(
        dashboard_view.render_full_dashboard(
            df=[],
            summary_stats={},
            playlist_name="Test playlist",
            channel_name="Test channel",
            channel_thumbnail=None,
            playlist_url="https://www.youtube.com/playlist?list=test",
            valid_sort="Views",
            valid_order="desc",
            next_order="asc",
            dashboard_id=dashboard_id,
        )
    )


class TestDashboardActions:
    def test_dashboard_with_id_offers_share_but_not_export(self, monkeypatch):
        html = _render_dashboard(monkeypatch, "dashboard-123")

        assert ">Share</button>" in html
        assert 'hx-get="/modal/share/dashboard-123"' in html
        assert "Export" not in html
        assert "/modal/export/" not in html

    def test_dashboard_without_id_omits_action_controls(self, monkeypatch):
        html = _render_dashboard(monkeypatch, None)

        assert ">Share</button>" not in html
        assert "/modal/share/" not in html
        assert "Export" not in html
