from datetime import datetime, timedelta, timezone
import sys
import types

import pytest

googleapiclient_stub = types.ModuleType("googleapiclient")
googleapiclient_discovery_stub = types.ModuleType("googleapiclient.discovery")
googleapiclient_errors_stub = types.ModuleType("googleapiclient.errors")
googleapiclient_discovery_stub.build = lambda *args, **kwargs: None
googleapiclient_errors_stub.HttpError = type("HttpError", (Exception,), {})
googleapiclient_stub.discovery = googleapiclient_discovery_stub
googleapiclient_stub.errors = googleapiclient_errors_stub
sys.modules.setdefault("googleapiclient", googleapiclient_stub)
sys.modules.setdefault("googleapiclient.discovery", googleapiclient_discovery_stub)
sys.modules.setdefault("googleapiclient.errors", googleapiclient_errors_stub)

from worker.creator_worker import (
    _build_recent_video_intelligence,
    _empty_recent_video_intelligence,
    _preserve_cached_recent_upload_fields,
)


def _playlist_items(count: int) -> list[dict]:
    return [{"contentDetails": {"videoId": f"video-{idx:02d}"}} for idx in range(count)]


_AS_OF = datetime(2026, 2, 1, tzinfo=timezone.utc)


def test_empty_recent_video_sample_preserves_cached_cadence_and_latest_upload():
    cached_upload = {"published_at": "2026-10-20T12:00:00Z"}
    creator = {
        "recent_upload": cached_upload,
        "avg_days_between_uploads": 4.25,
    }

    result = _preserve_cached_recent_upload_fields(_empty_recent_video_intelligence(), creator)

    assert result["recent_upload"] == cached_upload
    assert result["avg_days_between_uploads"] == 4.25
    assert result["recent_video_sample_size"] == 0


def test_recent_video_sample_replaces_cached_cadence_and_latest_upload():
    fresh_upload = {"published_at": "2026-10-29T12:00:00Z"}
    video_intel = {
        "recent_video_sample_size": 2,
        "recent_upload": fresh_upload,
        "avg_days_between_uploads": 1.5,
    }

    result = _preserve_cached_recent_upload_fields(
        video_intel,
        {
            "recent_upload": {"published_at": "2026-10-20T12:00:00Z"},
            "avg_days_between_uploads": 4.25,
        },
    )

    assert result["recent_upload"] == fresh_upload
    assert result["avg_days_between_uploads"] == 1.5


def test_partial_recent_video_sample_preserves_cached_cadence():
    fresh_upload = {"published_at": "2026-10-29T12:00:00Z"}
    creator = {
        "recent_upload": {"published_at": "2026-10-20T12:00:00Z"},
        "avg_days_between_uploads": 4.25,
    }

    result = _preserve_cached_recent_upload_fields(
        {
            "recent_video_sample_size": 1,
            "recent_upload": fresh_upload,
            "avg_days_between_uploads": None,
        },
        creator,
    )

    assert result["recent_upload"] == fresh_upload
    assert result["avg_days_between_uploads"] == 4.25


def test_recent_sample_without_published_at_preserves_cached_upload():
    cached_upload = {"published_at": "2026-10-20T12:00:00Z"}
    creator = {"recent_upload": cached_upload, "avg_days_between_uploads": 4.25}

    result = _preserve_cached_recent_upload_fields(
        {
            "recent_video_sample_size": 1,
            "recent_upload": {"published_at": ""},
            "avg_days_between_uploads": None,
        },
        creator,
    )

    assert result["recent_upload"] == cached_upload
    assert result["avg_days_between_uploads"] == 4.25


def _video_item(
    idx: int,
    views: int,
    likes: int = 10,
    category_id: str = "24",
    duration_sec: int = 192,
    age_days: int | None = None,
) -> dict:
    age_days = 10 + idx if age_days is None else age_days
    published = _AS_OF - timedelta(days=age_days)
    return {
        "id": f"video-{idx:02d}",
        "snippet": {
            "title": f"Video {idx:02d}",
            "publishedAt": published.isoformat().replace("+00:00", "Z"),
            "categoryId": category_id,
            "thumbnails": {"medium": {"url": f"https://example.com/{idx}.jpg"}},
        },
        "statistics": {
            "viewCount": str(views),
            "likeCount": str(likes),
            "commentCount": "2",
        },
        "contentDetails": {"duration": f"PT{duration_sec}S"},
    }


def test_recent_video_intelligence_keeps_raw_view_median_for_context():
    videos = [_video_item(idx, 900 if idx < 25 else 1100) for idx in range(50)]

    result = _build_recent_video_intelligence(_playlist_items(50), videos, as_of=_AS_OF)

    assert result["recent_views_median"] == 1000
    assert result["recent_video_sample_size"] == 50


def test_breakout_score_excludes_candidate_from_its_baseline():
    videos = [
        _video_item(0, 3_000, age_days=10),
        _video_item(1, 2_000, age_days=20),
    ]

    result = _build_recent_video_intelligence(_playlist_items(2), videos, as_of=_AS_OF)

    standout = result["outlier_videos"][0]
    assert result["outlier_count"] == 1
    assert standout["video_id"] == "video-00"
    assert standout["outlier_percentile"] == 100.0
    assert standout["view_multiplier"] == 3.0
    assert standout["expected_views_per_day"] == 100
    assert standout["observed_views_per_day"] == 300
    assert standout["comparison_sample_size"] == 1


def test_breakout_score_is_age_normalized_without_a_fixed_multiplier_cutoff():
    videos = [
        _video_item(0, 1_500, age_days=10),
        _video_item(1, 2_000, age_days=20),
        _video_item(2, 3_000, age_days=30),
        _video_item(3, 4_000, age_days=40),
    ]

    result = _build_recent_video_intelligence(_playlist_items(4), videos, as_of=_AS_OF)

    assert result["outlier_count"] == 1
    assert result["outlier_videos"][0]["video_id"] == "video-00"
    assert result["outlier_videos"][0]["view_multiplier"] == 1.5


def test_breakout_baseline_never_uses_later_uploads():
    videos = [
        _video_item(0, 100_000, age_days=10),
        _video_item(1, 3_000, age_days=20),
        _video_item(2, 3_000, age_days=30),
    ]

    result = _build_recent_video_intelligence(_playlist_items(3), videos, as_of=_AS_OF)

    candidate = next(row for row in result["outlier_videos"] if row["video_id"] == "video-01")
    assert candidate["expected_views_per_day"] == 100
    assert candidate["view_multiplier"] == 1.5


def test_baseline_weights_similar_video_durations_without_hard_buckets():
    videos = [
        _video_item(0, 14_400, duration_sec=60, age_days=10),
        _video_item(1, 24_000, duration_sec=60, age_days=20),
        _video_item(2, 200_000, duration_sec=1_200, age_days=20),
    ]

    result = _build_recent_video_intelligence(_playlist_items(3), videos, as_of=_AS_OF)

    short_video = next(row for row in result["outlier_videos"] if row["video_id"] == "video-00")
    assert short_video["outlier_percentile"] == 80.0
    assert short_video["expected_views_per_day"] == 1_200
    assert short_video["view_multiplier"] == 1.2


def test_equal_views_per_day_across_video_ages_are_not_standouts():
    videos = [
        _video_item(0, 100, age_days=1),
        _video_item(1, 1_000, age_days=10),
        _video_item(2, 2_000, age_days=20),
    ]

    result = _build_recent_video_intelligence(_playlist_items(3), videos, as_of=_AS_OF)

    assert result["outlier_count"] == 0
    assert result["outlier_videos"] == []


def test_uploads_younger_than_one_day_are_not_scored():
    videos = [
        _video_item(0, 50_000, age_days=0),
        _video_item(1, 2_000, age_days=2),
        _video_item(2, 3_000, age_days=3),
        _video_item(3, 4_000, age_days=4),
    ]

    result = _build_recent_video_intelligence(_playlist_items(4), videos, as_of=_AS_OF)

    assert result["outlier_scored_video_count"] == 2
    assert all(row["video_id"] != "video-00" for row in result["outlier_videos"])


def test_recent_video_intelligence_returns_no_outliers_for_zero_median():
    videos = [_video_item(idx, 0) for idx in range(6)]

    result = _build_recent_video_intelligence(_playlist_items(6), videos)

    assert result["recent_views_median"] is None
    assert result["outlier_count"] == 0
    assert result["outlier_videos"] == []


def test_recent_video_intelligence_caps_stored_outliers_to_top_five():
    daily_rates = [400, 800, 300, 700, 200, 600] + [100] * 14
    views = [rate * (10 + idx) for idx, rate in enumerate(daily_rates)]
    videos = [_video_item(idx, view_count) for idx, view_count in enumerate(views)]

    result = _build_recent_video_intelligence(_playlist_items(20), videos, as_of=_AS_OF)

    assert result["outlier_count"] == 6
    assert len(result["outlier_videos"]) == 5
    assert [row["video_id"] for row in result["outlier_videos"]] == [
        "video-01",
        "video-03",
        "video-05",
        "video-04",
        "video-02",
    ]


def test_recent_video_intelligence_defaults_missing_likes_to_zero():
    videos = [_video_item(0, 4_000)] + [_video_item(idx, 50 * (10 + idx)) for idx in range(1, 5)]
    videos[0]["statistics"].pop("likeCount")

    result = _build_recent_video_intelligence(_playlist_items(5), videos, as_of=_AS_OF)

    assert result["outlier_videos"][0]["like_count"] == 0


def test_recent_video_intelligence_processes_videos_with_empty_playlist_items():
    """Videos are still processed even if playlist_items is empty (only affects ordering)."""
    videos = [_video_item(idx, 100 * (10 + idx)) for idx in range(3)]
    result = _build_recent_video_intelligence([], videos, as_of=_AS_OF)

    assert result["recent_views_median"] == 1100
    assert result["recent_video_sample_size"] == 3
    assert result["outlier_count"] == 0


def test_recent_video_intelligence_returns_empty_payload_for_empty_video_items():
    """Empty video_items should return empty payload."""
    result = _build_recent_video_intelligence(_playlist_items(3), [])

    assert result["recent_views_median"] is None
    assert result["outlier_count"] == 0
    assert result["outlier_videos"] == []


def test_recent_video_intelligence_processes_videos_with_missing_video_id_in_playlist_items():
    """Videos are still processed if playlist_items lack videoId (doesn't affect content processing)."""
    playlist_items = [{"contentDetails": {}} for _ in range(3)]
    videos = [_video_item(idx, 100 * (10 + idx)) for idx in range(3)]
    result = _build_recent_video_intelligence(playlist_items, videos, as_of=_AS_OF)

    assert result["recent_views_median"] == 1100
    assert result["recent_video_sample_size"] == 3
    assert result["outlier_count"] == 0


def test_recent_video_intelligence_skips_video_items_without_ids():
    """Video items without IDs are skipped, but others are processed."""
    playlist_items = _playlist_items(3)
    videos = [
        {
            "snippet": _video_item(0, 100)["snippet"],
            "statistics": _video_item(0, 100)["statistics"],
        },
        _video_item(1, 1100),
        _video_item(2, 1200),
    ]
    result = _build_recent_video_intelligence(playlist_items, videos, as_of=_AS_OF)

    assert result["recent_views_median"] == 1150
    assert result["recent_video_sample_size"] == 2
    assert result["outlier_count"] == 0
