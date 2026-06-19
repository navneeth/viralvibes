from datetime import datetime, timedelta, timezone

from worker.creator_worker import _build_recent_video_intelligence


def _playlist_items(count: int) -> list[dict]:
    return [{"contentDetails": {"videoId": f"video-{idx:02d}"}} for idx in range(count)]


def _video_item(idx: int, views: int, likes: int = 10, category_id: str = "24") -> dict:
    published = datetime(2026, 1, 31, tzinfo=timezone.utc) - timedelta(days=idx)
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
        "contentDetails": {"duration": "PT3M12S"},
    }


def test_recent_video_intelligence_uses_even_sample_median():
    videos = [_video_item(idx, 900 if idx < 25 else 1100) for idx in range(50)]

    result = _build_recent_video_intelligence(_playlist_items(50), videos)

    assert result["recent_views_median"] == 1000
    assert result["recent_video_sample_size"] == 50
    assert result["outlier_count"] == 0


def test_recent_video_intelligence_flags_only_greater_than_3x_median():
    views = [100, 100, 100, 300, 301]
    videos = [_video_item(idx, view_count) for idx, view_count in enumerate(views)]

    result = _build_recent_video_intelligence(_playlist_items(5), videos)

    assert result["recent_views_median"] == 100
    assert result["outlier_count"] == 1
    assert result["outlier_videos"][0]["video_id"] == "video-04"
    assert result["outlier_videos"][0]["view_multiplier"] == 3.01


def test_recent_video_intelligence_returns_no_outliers_for_zero_median():
    videos = [_video_item(idx, 0) for idx in range(6)]

    result = _build_recent_video_intelligence(_playlist_items(6), videos)

    assert result["recent_views_median"] is None
    assert result["outlier_count"] == 0
    assert result["outlier_videos"] == []


def test_recent_video_intelligence_caps_stored_outliers_to_top_five():
    views = [100] * 14 + [301, 450, 600, 750, 900, 1050]
    videos = [_video_item(idx, view_count) for idx, view_count in enumerate(views)]

    result = _build_recent_video_intelligence(_playlist_items(20), videos)

    assert result["outlier_count"] == 6
    assert len(result["outlier_videos"]) == 5
    assert result["outlier_videos"][0]["video_id"] == "video-19"


def test_recent_video_intelligence_defaults_missing_likes_to_zero():
    videos = [_video_item(idx, 100) for idx in range(5)]
    videos[-1]["statistics"].pop("likeCount")
    videos[-1]["statistics"]["viewCount"] = "400"

    result = _build_recent_video_intelligence(_playlist_items(5), videos)

    assert result["outlier_videos"][0]["like_count"] == 0
