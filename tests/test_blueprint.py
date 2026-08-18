"""
Unit tests for utils/blueprint.py — pure scoring engine.

No mocks, no I/O, no fixtures.  Every function under test is a plain callable
that takes data and returns data, so each test is a direct assertion.

Coverage targets
----------------
_fmt                    — truncation, K, M, sub-1000
signals_from_row        — null coercions, arithmetic, Phase-2 JSONB
score_all_actions       — MIN_ACTIONABLE_SCORE boundary, sort order, clamp, no-crash
Individual scorers      — trigger (fires), guard (does not fire), mechanism text
"""

from __future__ import annotations

import pytest

from utils.blueprint import (
    MIN_ACTIONABLE_SCORE,
    CreatorSignals,
    ActionResult,
    _fmt,
    _score_captions_dub,
    _score_change_category,
    _score_chapters,
    _score_community_posts,
    _score_end_screens,
    _score_monetization_risk,
    _score_playlists,
    _score_rewrite_titles,
    _score_shorts_to_longform,
    _score_thumbnail_audit,
    _score_unlist_catalogue,
    score_all_actions,
    signals_from_row,
)


# ─────────────────────────────────────────────────────────────────────────────
# Factory helper
# ─────────────────────────────────────────────────────────────────────────────


def _make(**overrides) -> CreatorSignals:
    """
    Build a CreatorSignals with sensible active-channel defaults.

    Defaults are chosen so that no scorer fires without deliberate overrides,
    making each test's intent clear.

    Defaults:
      subscribers=200_000     above all min-subs gates
      video_count=100         above chapter/playlist minimums
      total_views=20_000_000
      views_change_30d=100_000  channel is active
      subs_change_30d=1_000
      viral_coeff=2.0          high enough to skip rewrite-titles trigger
      views_per_video=200_000  equals category peer (not above, not below thresholds)
      sub_growth_pct=0.5
      category_peer_vpv=200_000
      category_peer_vc=2.0
      country_code="US"        outside autodub country set
      primary_category="Education"  outside low-CPM set
      monthly_uploads=1.0      above monetization inactivity threshold
    """
    defaults = dict(
        creator_id="test-id",
        channel_name="Test Channel",
        primary_category="Education",
        country_code="US",
        subscribers=200_000,
        video_count=100,
        total_views=20_000_000,
        views_change_30d=100_000,
        subs_change_30d=1_000,
        viral_coeff=2.0,
        views_per_video=200_000.0,
        sub_growth_pct=0.5,
        category_peer_vpv=200_000.0,
        category_peer_vc=2.0,
        monthly_uploads=1.0,
    )
    defaults.update(overrides)
    return CreatorSignals(**defaults)


# ─────────────────────────────────────────────────────────────────────────────
# _fmt
# ─────────────────────────────────────────────────────────────────────────────


class TestFmt:
    def test_sub_thousand(self):
        assert _fmt(0) == "0"
        assert _fmt(999) == "999"

    def test_thousands_truncates(self):
        # 1 500 → "1K"  (truncates, not rounds to "2K")
        assert _fmt(1_000) == "1K"
        assert _fmt(1_500) == "1K"
        assert _fmt(9_999) == "9K"

    def test_millions(self):
        assert _fmt(1_000_000) == "1.0M"
        assert _fmt(1_200_000) == "1.2M"

    def test_float_input(self):
        # Floats are accepted and formatted the same way
        assert _fmt(500.0) == "500"
        assert _fmt(2_500.0) == "2K"


# ─────────────────────────────────────────────────────────────────────────────
# signals_from_row
# ─────────────────────────────────────────────────────────────────────────────


class TestSignalsFromRow:
    def test_null_coercions(self):
        """None DB values coerce to 0/0.0 without raising."""
        sig = signals_from_row(
            {
                "current_subscribers": None,
                "current_video_count": None,
                "current_view_count": None,
                "views_change_30d": None,
                "subscribers_change_30d": None,
                "monthly_uploads": None,
            },
            peer_vpv_p75=0.0,
            peer_vc_p75=0.0,
        )
        assert sig.subscribers == 0
        assert sig.video_count == 0
        assert sig.total_views == 0
        assert sig.views_change_30d == 0
        assert sig.subs_change_30d == 0
        assert sig.monthly_uploads == 0.0

    def test_arithmetic(self):
        """views_per_video, viral_coeff and sub_growth_pct are derived correctly."""
        sig = signals_from_row(
            {
                "current_subscribers": 1_000_000,
                "current_video_count": 100,
                "current_view_count": 50_000_000,
                "views_change_30d": 500_000,
                "subscribers_change_30d": 10_000,
            },
            peer_vpv_p75=100_000.0,
            peer_vc_p75=0.5,
        )
        assert sig.views_per_video == 500_000.0  # 50M / 100
        assert sig.viral_coeff == pytest.approx(0.5)  # 500K / 1M
        assert sig.sub_growth_pct == pytest.approx(1.0)  # 10K / 1M * 100

    def test_phase2_stubs_when_missing(self):
        """No blueprint_signals key → Phase-2 sentinels."""
        sig = signals_from_row({}, peer_vpv_p75=0.0, peer_vc_p75=0.0)
        assert sig.shorts_ratio == -1.0
        assert sig.caption_coverage == -1.0
        assert sig.avg_duration_sec == 0

    def test_phase2_from_jsonb(self):
        """blueprint_signals dict is unpacked into Phase-2 fields."""
        row = {
            "blueprint_signals": {
                "shorts_ratio": 0.8,
                "caption_coverage": 0.6,
                "avg_duration_sec": 720,
            }
        }
        sig = signals_from_row(row, peer_vpv_p75=0.0, peer_vc_p75=0.0)
        assert sig.shorts_ratio == pytest.approx(0.8)
        assert sig.caption_coverage == pytest.approx(0.6)
        assert sig.avg_duration_sec == 720

    def test_zero_videos_no_division_error(self):
        """video_count=0 uses max(0,1)=1 denominator — no ZeroDivisionError."""
        sig = signals_from_row(
            {"current_video_count": 0, "current_view_count": 50_000},
            peer_vpv_p75=0.0,
            peer_vc_p75=0.0,
        )
        assert sig.views_per_video == 50_000.0


# ─────────────────────────────────────────────────────────────────────────────
# score_all_actions — integration
# ─────────────────────────────────────────────────────────────────────────────


class TestScoreAllActions:
    def test_all_zeros_returns_no_actions(self):
        """Completely empty/zero signals should produce no actionable recommendations."""
        sig = CreatorSignals(
            creator_id="",
            channel_name="",
            primary_category="",
            country_code="",
            subscribers=0,
            video_count=0,
            total_views=0,
            views_change_30d=0,
            subs_change_30d=0,
            viral_coeff=0.0,
            views_per_video=0.0,
            sub_growth_pct=0.0,
            category_peer_vpv=0.0,
            category_peer_vc=0.0,
        )
        assert score_all_actions(sig) == []

    def test_min_actionable_score_boundary(self):
        """Only actions >= MIN_ACTIONABLE_SCORE=30 appear in output."""
        # Monetization risk fires at 40 (upload-dormant) + 20 (100K+ subs) = 60
        sig = _make(views_change_30d=0, subs_change_30d=0, monthly_uploads=0.0)
        results = score_all_actions(sig)
        assert all(r.score >= MIN_ACTIONABLE_SCORE for r in results)
        assert all(r.score <= 100.0 for r in results)

    def test_sorted_by_score_desc_then_effort_asc(self):
        """Results are ordered: score DESC, effort ASC on ties."""
        sig = _make(views_change_30d=0, subs_change_30d=0, monthly_uploads=0.0)
        results = score_all_actions(sig)
        if len(results) >= 2:
            for a, b in zip(results, results[1:]):
                # Either higher score, or equal score with lower/equal effort
                assert (a.score, -a.effort) >= (b.score, -b.effort)

    def test_negative_viral_coeff_does_not_crash(self):
        """Negative views_change_30d produces negative viral_coeff — must not raise."""
        sig = _make(views_change_30d=-500_000, viral_coeff=-2.5)
        # Just verifying no exception; result may be empty
        results = score_all_actions(sig)
        assert isinstance(results, list)

    def test_score_clamped_at_100(self):
        """No action result may exceed 100."""
        # Dormant large channel — monetization risk accumulates many bonus points
        sig = _make(
            subscribers=10_000_000,
            views_change_30d=0,
            subs_change_30d=0,
            monthly_uploads=0.0,
        )
        results = score_all_actions(sig)
        assert all(r.score <= 100.0 for r in results)

    def test_result_fields_populated(self):
        """Each ActionResult has all required non-empty fields."""
        sig = _make(views_change_30d=0, subs_change_30d=0, monthly_uploads=0.0)
        results = score_all_actions(sig)
        assert results, "expected at least one action for dormant channel"
        for r in results:
            assert r.name
            assert r.mechanism
            assert r.studio_url.startswith("https://")
            assert r.effort >= 1
            assert r.funnel_stage in {"Reach", "Engagement", "Conversion", "Revenue"}


# ─────────────────────────────────────────────────────────────────────────────
# Add End Screens
# ─────────────────────────────────────────────────────────────────────────────


class TestEndScreens:
    def test_inactive_channel_no_fire(self):
        s = _make(views_change_30d=0, subs_change_30d=0)
        score, _ = _score_end_screens(s)
        assert score == 0.0

    def test_small_channel_no_fire(self):
        s = _make(subscribers=10_000)
        score, _ = _score_end_screens(s)
        assert score == 0.0

    def test_conversion_gap_fires(self):
        """High viral coeff + low sub growth = conversion gap."""
        s = _make(viral_coeff=3.0, sub_growth_pct=0.2)
        score, mech = _score_end_screens(s)
        assert score >= MIN_ACTIONABLE_SCORE
        assert "viral coefficient" in mech or "viral coeff" in mech.lower() or "3.0×" in mech

    def test_large_library_fires(self):
        s = _make(video_count=250, viral_coeff=0.5, sub_growth_pct=0.6)
        score, _ = _score_end_screens(s)
        assert score >= MIN_ACTIONABLE_SCORE


# ─────────────────────────────────────────────────────────────────────────────
# Thumbnail Audit
# ─────────────────────────────────────────────────────────────────────────────


class TestThumbnailAudit:
    def test_inactive_no_fire(self):
        s = _make(views_change_30d=0, subs_change_30d=0)
        score, _ = _score_thumbnail_audit(s)
        assert score == 0.0

    def test_small_channel_no_fire(self):
        s = _make(subscribers=1_000)
        score, _ = _score_thumbnail_audit(s)
        assert score == 0.0

    def test_vpv_below_peers_fires(self):
        """VPV below 40% of peer p75 AND stalled growth → fires.

        VPV-below-peers alone only adds 25 points (below the 30 threshold).
        The scorer intentionally requires a second weak signal to surface
        the recommendation — stalled growth is the natural co-occurrence.
        """
        s = _make(
            views_per_video=30_000.0,
            category_peer_vpv=200_000.0,
            sub_growth_pct=0.04,
            views_change_30d=50_000,
        )
        score, mech = _score_thumbnail_audit(s)
        assert score >= MIN_ACTIONABLE_SCORE
        assert "category" in mech.lower() or "CTR" in mech or "thumbnail" in mech.lower()

    def test_stalled_growth_fires(self):
        s = _make(sub_growth_pct=0.05, views_change_30d=50_000)
        score, _ = _score_thumbnail_audit(s)
        assert score >= MIN_ACTIONABLE_SCORE


# ─────────────────────────────────────────────────────────────────────────────
# Rewrite Titles
# ─────────────────────────────────────────────────────────────────────────────


class TestRewriteTitles:
    def test_inactive_no_fire(self):
        s = _make(views_change_30d=0, subs_change_30d=0)
        score, _ = _score_rewrite_titles(s)
        assert score == 0.0

    def test_small_channel_no_fire(self):
        s = _make(subscribers=5_000)
        score, _ = _score_rewrite_titles(s)
        assert score == 0.0

    def test_low_viral_fires(self):
        """viral_coeff < 1 on an active channel → packaging gap."""
        s = _make(viral_coeff=0.4, views_change_30d=50_000)
        score, mech = _score_rewrite_titles(s)
        assert score >= MIN_ACTIONABLE_SCORE
        assert "viral" in mech.lower()

    def test_high_viral_no_fire(self):
        """High viral_coeff = content is already spreading — no action needed."""
        s = _make(viral_coeff=5.0, views_per_video=100_000.0, sub_growth_pct=1.0)
        score, _ = _score_rewrite_titles(s)
        assert score == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Add Chapter Timestamps
# ─────────────────────────────────────────────────────────────────────────────


class TestChapterTimestamps:
    def test_too_few_videos_no_fire(self):
        s = _make(video_count=10, views_per_video=30_000.0)
        score, _ = _score_chapters(s)
        assert score == 0.0

    def test_vpv_above_threshold_no_fire(self):
        """VPV >= 70% of peer p75 — no chapters needed."""
        s = _make(views_per_video=160_000.0, category_peer_vpv=200_000.0, video_count=50)
        score, _ = _score_chapters(s)
        assert score == 0.0

    def test_vpv_below_peer_fires(self):
        """VPV < 50% of peer p75 → strong chapters signal."""
        s = _make(views_per_video=80_000.0, category_peer_vpv=200_000.0, video_count=50)
        score, mech = _score_chapters(s)
        assert score >= MIN_ACTIONABLE_SCORE
        assert "chapter" in mech.lower() or "drop-off" in mech.lower()

    def test_long_form_confirmation_boosts_score(self):
        """avg_duration_sec > 480 adds 25 points and changes mechanism text."""
        s = _make(
            views_per_video=80_000.0,
            category_peer_vpv=200_000.0,
            video_count=50,
            avg_duration_sec=600,
        )
        score_long, mech = _score_chapters(s)
        s_short = _make(
            views_per_video=80_000.0,
            category_peer_vpv=200_000.0,
            video_count=50,
            avg_duration_sec=0,
        )
        score_short, _ = _score_chapters(s_short)
        assert score_long > score_short
        assert "minute" in mech


# ─────────────────────────────────────────────────────────────────────────────
# Captions + Auto-Dub
# ─────────────────────────────────────────────────────────────────────────────


class TestCaptionsDub:
    def test_non_autodub_country_no_fire(self):
        s = _make(country_code="US", subscribers=5_000_000)
        score, _ = _score_captions_dub(s)
        assert score == 0.0

    def test_case_insensitive_country(self):
        """Lower-case country code is uppercased before lookup."""
        s = _make(country_code="in", subscribers=500_000, views_change_30d=100_000)
        score, _ = _score_captions_dub(s)
        assert score >= MIN_ACTIONABLE_SCORE

    def test_autodub_country_fires(self):
        s = _make(country_code="BR", subscribers=2_000_000, views_change_30d=100_000)
        score, mech = _score_captions_dub(s)
        assert score >= MIN_ACTIONABLE_SCORE
        assert "BR" in mech

    def test_low_caption_coverage_boosts_score(self):
        s_low = _make(
            country_code="IN", subscribers=500_000, views_change_30d=50_000, caption_coverage=0.1
        )
        s_high = _make(
            country_code="IN", subscribers=500_000, views_change_30d=50_000, caption_coverage=-1.0
        )
        score_low, _ = _score_captions_dub(s_low)
        score_high, _ = _score_captions_dub(s_high)
        assert score_low > score_high


# ─────────────────────────────────────────────────────────────────────────────
# Change Category
# ─────────────────────────────────────────────────────────────────────────────


class TestChangeCategory:
    def test_non_low_cpm_category_no_fire(self):
        s = _make(
            primary_category="Finance", views_per_video=500_000.0, category_peer_vpv=200_000.0
        )
        score, _ = _score_change_category(s)
        assert score == 0.0

    def test_hard_gate_underperforming_no_fire(self):
        """Low-CPM category but VPV ≤ peer → hard gate prevents fire."""
        s = _make(primary_category="gaming", views_per_video=100_000.0, category_peer_vpv=200_000.0)
        score, _ = _score_change_category(s)
        assert score == 0.0

    def test_peer_vpv_zero_no_fire(self):
        s = _make(primary_category="gaming", views_per_video=500_000.0, category_peer_vpv=0.0)
        score, _ = _score_change_category(s)
        assert score == 0.0

    def test_outperforming_low_cpm_fires(self):
        """Low-CPM category AND VPV above peers → fire."""
        s = _make(
            primary_category="gaming",
            views_per_video=300_000.0,
            category_peer_vpv=200_000.0,
            viral_coeff=3.0,
        )
        score, mech = _score_change_category(s)
        assert score >= MIN_ACTIONABLE_SCORE
        assert "gaming" in mech.lower() or "category" in mech.lower()

    def test_category_match_is_case_insensitive(self):
        """Category lookup uses .lower() so "Gaming" matches "gaming"."""
        s = _make(
            primary_category="Gaming",
            views_per_video=300_000.0,
            category_peer_vpv=200_000.0,
        )
        score, _ = _score_change_category(s)
        assert score >= MIN_ACTIONABLE_SCORE


# ─────────────────────────────────────────────────────────────────────────────
# Unlist Old Videos
# ─────────────────────────────────────────────────────────────────────────────


class TestUnlistOldVideos:
    def test_too_few_videos_no_fire(self):
        s = _make(video_count=100, category_peer_vpv=200_000.0, views_per_video=20_000.0)
        score, _ = _score_unlist_catalogue(s)
        assert score == 0.0

    def test_no_peer_benchmark_no_fire(self):
        s = _make(video_count=600, category_peer_vpv=0.0, views_per_video=20_000.0)
        score, _ = _score_unlist_catalogue(s)
        assert score == 0.0

    def test_vpv_above_half_peer_no_fire(self):
        """VPV ≥ 50% of peer → performing well enough, don't unlist."""
        s = _make(video_count=600, category_peer_vpv=200_000.0, views_per_video=110_000.0)
        score, _ = _score_unlist_catalogue(s)
        assert score == 0.0  # 110K >= 200K * 0.5 = 100K

    def test_large_low_vpv_catalogue_fires(self):
        s = _make(video_count=600, category_peer_vpv=200_000.0, views_per_video=30_000.0)
        score, mech = _score_unlist_catalogue(s)
        assert score >= MIN_ACTIONABLE_SCORE
        assert "600" in mech

    def test_very_large_catalogue_extra_score(self):
        s_large = _make(video_count=6_000, category_peer_vpv=200_000.0, views_per_video=30_000.0)
        s_small = _make(video_count=600, category_peer_vpv=200_000.0, views_per_video=30_000.0)
        score_large, _ = _score_unlist_catalogue(s_large)
        score_small, _ = _score_unlist_catalogue(s_small)
        assert score_large > score_small


# ─────────────────────────────────────────────────────────────────────────────
# Shift to Long-form
# ─────────────────────────────────────────────────────────────────────────────


class TestShiftToLongform:
    def test_phase2_stub_no_fire(self):
        """shorts_ratio = -1 means Phase-2 not computed yet → silent."""
        s = _make(shorts_ratio=-1.0)
        score, _ = _score_shorts_to_longform(s)
        assert score == 0.0

    def test_below_threshold_no_fire(self):
        s = _make(shorts_ratio=0.5, subscribers=10_000, views_per_video=1_000.0)
        score, _ = _score_shorts_to_longform(s)
        assert score == 0.0

    def test_shorts_dominant_fires(self):
        s = _make(shorts_ratio=0.8, subscribers=50_000, views_per_video=5_000.0)
        score, mech = _score_shorts_to_longform(s)
        assert score >= MIN_ACTIONABLE_SCORE
        assert "80%" in mech

    def test_mechanism_pct_matches_ratio(self):
        s = _make(shorts_ratio=0.9, subscribers=50_000, views_per_video=5_000.0)
        _, mech = _score_shorts_to_longform(s)
        assert "90%" in mech


# ─────────────────────────────────────────────────────────────────────────────
# Monetization Risk
# ─────────────────────────────────────────────────────────────────────────────


class TestMonetizationRisk:
    def test_small_channel_no_fire(self):
        s = _make(subscribers=500, views_change_30d=0, monthly_uploads=0.0)
        score, _ = _score_monetization_risk(s)
        assert score == 0.0

    def test_active_channel_no_fire(self):
        """Active views AND adequate upload rate → no risk."""
        s = _make(views_change_30d=50_000, monthly_uploads=1.0)
        score, _ = _score_monetization_risk(s)
        assert score == 0.0

    def test_view_dormant_fires(self):
        s = _make(views_change_30d=0, subs_change_30d=0, monthly_uploads=1.0)
        score, mech = _score_monetization_risk(s)
        assert score >= MIN_ACTIONABLE_SCORE
        assert "YPP" in mech or "monetization" in mech.lower()

    def test_upload_dormant_fires(self):
        s = _make(views_change_30d=50_000, monthly_uploads=0.1)
        score, _ = _score_monetization_risk(s)
        assert score >= MIN_ACTIONABLE_SCORE

    def test_both_dormant_higher_score(self):
        """Both view-dormant AND upload-dormant scores higher than either alone."""
        s_both = _make(views_change_30d=0, subs_change_30d=0, monthly_uploads=0.0)
        s_view = _make(views_change_30d=0, subs_change_30d=0, monthly_uploads=1.0)
        s_upload = _make(views_change_30d=50_000, monthly_uploads=0.0)
        score_both, _ = _score_monetization_risk(s_both)
        score_view, _ = _score_monetization_risk(s_view)
        score_upload, _ = _score_monetization_risk(s_upload)
        assert score_both > score_view
        assert score_both > score_upload

    def test_large_channel_bonus(self):
        s_large = _make(
            subscribers=500_000, views_change_30d=0, subs_change_30d=0, monthly_uploads=0.0
        )
        s_small = _make(
            subscribers=5_000, views_change_30d=0, subs_change_30d=0, monthly_uploads=0.0
        )
        score_large, _ = _score_monetization_risk(s_large)
        score_small, _ = _score_monetization_risk(s_small)
        assert score_large > score_small


# ─────────────────────────────────────────────────────────────────────────────
# Community Posts
# ─────────────────────────────────────────────────────────────────────────────


class TestCommunityPosts:
    def test_small_channel_no_fire(self):
        s = _make(subscribers=5_000, views_per_video=1_000.0, views_change_30d=500)
        score, _ = _score_community_posts(s)
        assert score == 0.0

    def test_inactive_channel_no_fire(self):
        s = _make(views_change_30d=0, subs_change_30d=0)
        score, _ = _score_community_posts(s)
        assert score == 0.0

    def test_high_watch_rate_no_fire(self):
        """watch_rate >= 0.5 means engagement is already strong."""
        # views_per_video / subscribers = 150K / 200K = 0.75 >= 0.5
        s = _make(subscribers=200_000, views_per_video=150_000.0, views_change_30d=50_000)
        score, _ = _score_community_posts(s)
        assert score == 0.0

    def test_cold_subscriber_base_fires(self):
        # watch_rate = 2K / 200K = 0.01 — very cold
        s = _make(subscribers=200_000, views_per_video=2_000.0, views_change_30d=10_000)
        score, mech = _score_community_posts(s)
        assert score >= MIN_ACTIONABLE_SCORE
        assert "%" in mech  # watch percentage in mechanism text

    def test_mechanism_includes_watch_pct(self):
        s = _make(subscribers=100_000, views_per_video=1_000.0, views_change_30d=5_000)
        _, mech = _score_community_posts(s)
        # watch_rate = 1K / 100K = 1% — "1.0%"
        assert "1.0%" in mech


# ─────────────────────────────────────────────────────────────────────────────
# Playlists
# ─────────────────────────────────────────────────────────────────────────────


class TestPlaylists:
    def test_too_few_videos_no_fire(self):
        s = _make(video_count=5)
        score, _ = _score_playlists(s)
        assert score == 0.0

    def test_inactive_no_fire(self):
        s = _make(video_count=20, views_change_30d=0, subs_change_30d=0)
        score, _ = _score_playlists(s)
        assert score == 0.0

    def test_stalled_growth_fires(self):
        s = _make(
            video_count=20,
            views_change_30d=5_000,
            subs_change_30d=10,
            sub_growth_pct=0.1,
            views_per_video=50_000.0,
        )
        score, mech = _score_playlists(s)
        assert score >= MIN_ACTIONABLE_SCORE
        assert "playlist" in mech.lower()

    def test_below_peer_vpv_boosts_score(self):
        s_below = _make(
            video_count=30,
            views_change_30d=5_000,
            sub_growth_pct=0.1,
            views_per_video=100_000.0,
            category_peer_vpv=200_000.0,  # 100K < 140K = 0.7 * 200K
        )
        s_at_peer = _make(
            video_count=30,
            views_change_30d=5_000,
            sub_growth_pct=0.1,
            views_per_video=180_000.0,
            category_peer_vpv=200_000.0,  # 180K >= 140K — no bonus
        )
        score_below, _ = _score_playlists(s_below)
        score_at, _ = _score_playlists(s_at_peer)
        assert score_below > score_at
