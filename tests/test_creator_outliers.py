from datetime import datetime, timedelta, timezone
import asyncio
import sys
import types
from unittest.mock import AsyncMock, MagicMock
from types import SimpleNamespace

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
    _build_recent_video_snapshot,
    _empty_recent_video_intelligence,
    _empty_recent_video_intelligence_success,
    _preserve_cached_recent_upload_fields,
)


def _playlist_items(count: int) -> list[dict]:
    return [{"contentDetails": {"videoId": f"video-{idx:02d}"}} for idx in range(count)]


_AS_OF = datetime(2026, 2, 1, tzinfo=timezone.utc)
_SNAPSHOT_CREATOR_ID = "6488b4d0-96e9-4cb3-b7da-36cc086f8e75"


def test_snapshot_health_is_bounded_allowlisted_and_read_only(monkeypatch):
    import worker.creator_worker as creator_worker

    ids = [f"{index:08x}-0000-0000-0000-000000000000" for index in range(30)]
    monkeypatch.setenv("CREATOR_RECENT_VIDEO_ALLOWLIST", ",".join(reversed(ids)))
    monkeypatch.setattr(
        creator_worker.schema_detector,
        "available_columns",
        {
            "recent_videos",
            "recent_videos_fetched_at",
        },
    )
    client = MagicMock()
    query = client.table.return_value
    query.select.return_value = query
    query.in_.return_value = query
    query.limit.return_value = query
    query.execute.return_value = SimpleNamespace(
        data=[
            {"id": ids[0], "recent_videos": None, "recent_videos_fetched_at": None},
            {"id": ids[1], "recent_videos": [], "recent_videos_fetched_at": _AS_OF.isoformat()},
            {
                "id": ids[2],
                "recent_videos": [{"video_id": "abcdefghijk"}],
                "recent_videos_fetched_at": (_AS_OF - timedelta(days=7)).isoformat(),
            },
            {"id": ids[3], "recent_videos": "bad", "recent_videos_fetched_at": _AS_OF.isoformat()},
            {"id": ids[4], "recent_videos": [], "recent_videos_fetched_at": None},
            {"id": ids[29], "recent_videos": [], "recent_videos_fetched_at": _AS_OF.isoformat()},
        ]
    )
    monkeypatch.setattr(creator_worker, "supabase_client", client)

    assert creator_worker.get_recent_video_snapshot_health(as_of=_AS_OF) == {
        "configured": 30,
        "inspected": 25,
        "not_inspected": 5,
        "missing_or_inaccessible": 20,
        "uncollected": 1,
        "empty": 1,
        "populated": 1,
        "stale": 1,
        "invalid": 2,
        "cached_videos": 1,
    }
    query.in_.assert_called_once_with("id", sorted(ids)[:25])
    query.limit.assert_called_once_with(25)
    query.update.assert_not_called()
    query.insert.assert_not_called()
    client.rpc.assert_not_called()


@pytest.mark.parametrize("reason", ["disabled", "missing_columns", "read_failed"])
def test_snapshot_health_reports_unavailable_without_writes(monkeypatch, reason):
    import worker.creator_worker as creator_worker

    monkeypatch.setenv("CREATOR_RECENT_VIDEO_ALLOWLIST", _SNAPSHOT_CREATOR_ID)
    monkeypatch.setattr(
        creator_worker.schema_detector,
        "available_columns",
        {
            "recent_videos",
            "recent_videos_fetched_at",
        },
    )
    client = MagicMock()
    monkeypatch.setattr(creator_worker, "supabase_client", client)
    if reason == "disabled":
        monkeypatch.delenv("CREATOR_RECENT_VIDEO_ALLOWLIST")
    elif reason == "missing_columns":
        monkeypatch.setattr(creator_worker.schema_detector, "available_columns", None)
    else:
        client.table.side_effect = RuntimeError("read failed")

    assert creator_worker.get_recent_video_snapshot_health(as_of=_AS_OF) is None
    if reason != "read_failed":
        client.table.assert_not_called()
    client.rpc.assert_not_called()


@pytest.mark.parametrize(
    "snapshot, fetched_at",
    [
        ([], "bad"),
        ([], "2027-01-01T00:00:00Z"),
        ([{"video_id": "../bad"}], "2026-02-01T00:00:00Z"),
        ([{"video_id": "abcdefghijk"}] * 13, "2026-02-01T00:00:00Z"),
    ],
)
def test_snapshot_health_flags_invalid_data(monkeypatch, snapshot, fetched_at):
    import worker.creator_worker as creator_worker

    monkeypatch.setenv("CREATOR_RECENT_VIDEO_ALLOWLIST", _SNAPSHOT_CREATOR_ID)
    monkeypatch.setattr(
        creator_worker.schema_detector,
        "available_columns",
        {
            "recent_videos",
            "recent_videos_fetched_at",
        },
    )
    client = MagicMock()
    query = client.table.return_value
    query.select.return_value = query
    query.in_.return_value = query
    query.limit.return_value = query
    query.execute.return_value = SimpleNamespace(
        data=[
            {
                "id": _SNAPSHOT_CREATOR_ID,
                "recent_videos": snapshot,
                "recent_videos_fetched_at": fetched_at,
            }
        ]
    )
    monkeypatch.setattr(creator_worker, "supabase_client", client)

    health = creator_worker.get_recent_video_snapshot_health(as_of=_AS_OF)

    assert health["invalid"] == 1
    assert health["populated"] == health["empty"] == health["cached_videos"] == 0
    query.update.assert_not_called()


@pytest.mark.parametrize(
    "configured, enabled",
    [
        ("", False),
        ("*", False),
        ("invalid", False),
        (_SNAPSHOT_CREATOR_ID, True),
        (f"invalid, {_SNAPSHOT_CREATOR_ID.upper()},", True),
        ("14076ba5-f130-4f86-9b53-ecf3b93ed077", False),
    ],
)
def test_snapshot_allowlist_requires_explicit_matching_uuid(monkeypatch, configured, enabled):
    import worker.creator_worker as creator_worker

    monkeypatch.setenv("CREATOR_RECENT_VIDEO_ALLOWLIST", configured)
    assert creator_worker._recent_video_snapshot_enabled(_SNAPSHOT_CREATOR_ID) is enabled
    assert creator_worker._recent_video_snapshot_enabled("not-a-uuid") is False


@pytest.mark.parametrize("snapshot", [[], [{"video_id": "abcdefghijk"}]])
def test_snapshot_write_replaces_data_with_its_own_timestamp_guard(monkeypatch, snapshot):
    import worker.creator_worker as creator_worker

    monkeypatch.setenv("CREATOR_RECENT_VIDEO_ALLOWLIST", _SNAPSHOT_CREATOR_ID)
    monkeypatch.setattr(creator_worker.metrics, "video_snapshots_saved", 0)
    monkeypatch.setattr(
        creator_worker.schema_detector,
        "available_columns",
        {
            "recent_videos",
            "recent_videos_fetched_at",
        },
    )
    client = MagicMock()
    query = client.table.return_value
    query.update.return_value = query
    query.eq.return_value = query
    query.or_.return_value = query
    query.execute.return_value = SimpleNamespace(data=[{"id": _SNAPSHOT_CREATOR_ID}])
    monkeypatch.setattr(creator_worker, "supabase_client", client)

    assert (
        creator_worker._persist_recent_video_snapshot(
            _SNAPSHOT_CREATOR_ID,
            {"recent_video_fetch_succeeded": True, "recent_videos": snapshot},
            _AS_OF,
        )
        is True
    )
    query.update.assert_called_once_with(
        {
            "recent_videos": snapshot,
            "recent_videos_fetched_at": _AS_OF.isoformat(),
        }
    )
    query.eq.assert_called_once_with("id", _SNAPSHOT_CREATOR_ID)
    assert creator_worker.metrics.video_snapshots_saved == 1
    query.or_.assert_called_once_with(
        "recent_videos_fetched_at.is.null," f"recent_videos_fetched_at.lt.{_AS_OF.isoformat()}"
    )


@pytest.mark.parametrize("reason", ["disabled", "fetch_failed", "no_snapshot", "missing_columns"])
def test_snapshot_write_skips_without_touching_cached_data(monkeypatch, reason):
    import worker.creator_worker as creator_worker

    monkeypatch.setenv("CREATOR_RECENT_VIDEO_ALLOWLIST", _SNAPSHOT_CREATOR_ID)
    monkeypatch.setattr(creator_worker.metrics, "video_snapshot_schema_skips", 0)
    monkeypatch.setattr(
        creator_worker.schema_detector,
        "available_columns",
        {
            "recent_videos",
            "recent_videos_fetched_at",
        },
    )
    client = MagicMock()
    monkeypatch.setattr(creator_worker, "supabase_client", client)
    payload = {"recent_video_fetch_succeeded": True, "recent_videos": []}
    if reason == "disabled":
        monkeypatch.delenv("CREATOR_RECENT_VIDEO_ALLOWLIST")
    elif reason == "fetch_failed":
        payload["recent_video_fetch_succeeded"] = False
    elif reason == "no_snapshot":
        del payload["recent_videos"]
    else:
        monkeypatch.setattr(creator_worker.schema_detector, "available_columns", {"recent_videos"})

    assert (
        creator_worker._persist_recent_video_snapshot(
            _SNAPSHOT_CREATOR_ID,
            payload,
            _AS_OF,
        )
        is False
    )
    client.table.assert_not_called()
    assert creator_worker.metrics.video_snapshot_schema_skips == int(reason == "missing_columns")


@pytest.mark.parametrize(
    "error", [RuntimeError("write failed"), RuntimeError("PGRST204 schema cache lag")]
)
def test_snapshot_database_errors_are_nonfatal(monkeypatch, error, caplog):
    import worker.creator_worker as creator_worker

    monkeypatch.setenv("CREATOR_RECENT_VIDEO_ALLOWLIST", _SNAPSHOT_CREATOR_ID)
    monkeypatch.setattr(creator_worker.metrics, "video_snapshot_write_errors", 0)
    monkeypatch.setattr(
        creator_worker.schema_detector,
        "available_columns",
        {
            "recent_videos",
            "recent_videos_fetched_at",
        },
    )
    client = MagicMock()
    client.table.side_effect = error
    monkeypatch.setattr(creator_worker, "supabase_client", client)

    assert (
        creator_worker._persist_recent_video_snapshot(
            _SNAPSHOT_CREATOR_ID,
            {"recent_video_fetch_succeeded": True, "recent_videos": []},
            _AS_OF,
        )
        is False
    )
    assert "snapshot write failed" in caplog.text
    assert creator_worker.metrics.video_snapshot_write_errors == 1


@pytest.mark.parametrize("include_snapshot", [False, True])
@pytest.mark.parametrize(
    "scenario",
    [
        "normal",
        "empty_playlist",
        "empty_videos",
        "fetch_error",
        "invalid_metadata",
        "missing_video_items",
        "normalization_error",
    ],
)
def test_snapshot_fetch_reuses_existing_requests(monkeypatch, include_snapshot, scenario):
    import worker.creator_worker as creator_worker

    youtube = MagicMock()
    resolver = MagicMock()
    resolver._get_youtube_client.return_value = youtube
    video = {
        "id": "abcdefghijk",
        "snippet": {"title": "Example", "publishedAt": "2026-01-01T00:00:00Z"},
        "statistics": {"viewCount": "100"},
        "contentDetails": {"duration": "PT1M"},
    }
    playlist = {"items": [{"contentDetails": {"videoId": video["id"]}}]}
    response = {"items": [video]}
    if scenario == "empty_playlist":
        playlist = {"items": []}
    elif scenario == "empty_videos":
        response = {"items": []}
    elif scenario == "invalid_metadata":
        response = {"items": [{**video, "snippet": {"publishedAt": "bad"}}]}
    elif scenario == "missing_video_items":
        response = {}
    elif scenario == "normalization_error":
        monkeypatch.setattr(
            creator_worker,
            "_build_recent_video_snapshot",
            MagicMock(side_effect=RuntimeError("normalization failed")),
        )
    responses = [playlist, RuntimeError("API failed") if scenario == "fetch_error" else response]
    resolver._execute_async = AsyncMock(side_effect=responses)
    monkeypatch.setattr(creator_worker, "youtube_resolver", resolver)
    monkeypatch.setattr(creator_worker.metrics, "youtube_credits_used", 0)

    result = asyncio.run(
        creator_worker._fetch_recent_video_intelligence(
            "UCexample",
            uploads_playlist_id="UUexample",
            include_snapshot=include_snapshot,
        )
    )

    expected_requests = 1 if scenario == "empty_playlist" else 2
    assert resolver._execute_async.await_count == expected_requests
    assert creator_worker.metrics.youtube_credits_used == (
        1 if scenario == "fetch_error" else expected_requests
    )
    if include_snapshot and scenario in ("normal", "empty_playlist", "empty_videos"):
        assert result["recent_video_fetch_succeeded"] is True
        assert result["recent_videos"] == (
            _build_recent_video_snapshot([video]) if scenario == "normal" else []
        )
    else:
        assert "recent_videos" not in result
    if scenario == "normalization_error":
        assert result["recent_video_fetch_succeeded"] is True
        assert result["recent_video_sample_size"] == 1


def test_recent_video_snapshot_is_bounded_newest_first_and_metadata_only():
    items = [
        {
            "id": f"{index:011d}",
            "snippet": {
                "title": "Example",
                "publishedAt": f"2026-01-{index + 1:02d}T00:00:00Z",
                "thumbnails": {"high": {"url": "https://i.ytimg.com/example.jpg"}},
            },
            "statistics": {"viewCount": "100", "likeCount": "0"},
            "contentDetails": {"duration": "PT1M30S"},
        }
        for index in range(20)
    ]
    snapshot = _build_recent_video_snapshot(items)

    assert len(snapshot) == 12
    assert snapshot[0]["video_id"] == "00000000019"
    assert snapshot[-1]["video_id"] == "00000000008"
    assert snapshot[0]["duration_sec"] == 90
    assert snapshot[0]["view_count"] == 100
    assert snapshot[0]["like_count"] == 0
    assert snapshot[0]["comment_count"] is None
    assert set(snapshot[0]) == {
        "video_id",
        "title",
        "published_at",
        "duration_sec",
        "view_count",
        "like_count",
        "comment_count",
    }


def test_recent_video_snapshot_validates_ids_dates_counts_and_deduplicates():
    valid = {
        "id": "abcdefghijk",
        "snippet": {"title": "x" * 1000, "publishedAt": "2026-01-01T01:00:00+01:00"},
        "statistics": {"viewCount": "bad", "likeCount": "-1", "commentCount": True},
    }
    snapshot = _build_recent_video_snapshot(
        [
            valid,
            valid,
            {**valid, "id": "invalid/id"},
            {**valid, "id": "ABCDEFGHIJK", "snippet": {"publishedAt": "bad"}},
        ]
    )

    assert len(snapshot) == 1
    assert len(snapshot[0]["title"]) == 500
    assert snapshot[0]["published_at"] == "2026-01-01T00:00:00.000000+00:00"
    assert snapshot[0]["duration_sec"] is None
    assert all(snapshot[0][key] is None for key in ("view_count", "like_count", "comment_count"))
    assert _build_recent_video_snapshot([]) == []


@pytest.mark.parametrize("duration", [None, "bad", 123])
def test_recent_video_snapshot_preserves_unknown_duration(duration):
    snapshot = _build_recent_video_snapshot(
        [
            {
                "id": "abcdefghijk",
                "snippet": {"publishedAt": "2026-01-01T00:00:00Z"},
                "contentDetails": {"duration": duration},
            }
        ]
    )

    assert snapshot[0]["duration_sec"] is None


def test_recent_video_snapshot_uses_video_id_to_break_publication_ties():
    items = [
        {"id": video_id, "snippet": {"publishedAt": "2026-01-01T00:00:00Z"}}
        for video_id in ("aaaaaaaaaaa", "bbbbbbbbbbb")
    ]

    assert [row["video_id"] for row in _build_recent_video_snapshot(items)] == [
        "bbbbbbbbbbb",
        "aaaaaaaaaaa",
    ]


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
            "recent_video_fetch_succeeded": True,
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
            "recent_video_fetch_succeeded": True,
            "recent_video_sample_size": 1,
            "recent_upload": {"published_at": ""},
            "avg_days_between_uploads": None,
        },
        creator,
    )

    assert result["recent_upload"] == cached_upload
    assert result["avg_days_between_uploads"] == 4.25


def test_successful_empty_recent_sample_clears_cached_upload_and_cadence():
    result = _preserve_cached_recent_upload_fields(
        _empty_recent_video_intelligence_success(),
        {
            "recent_upload": {"published_at": "2026-10-20T12:00:00Z"},
            "avg_days_between_uploads": 4.25,
        },
    )

    assert result["recent_video_fetch_succeeded"] is True
    assert result["recent_upload"] is None
    assert result["avg_days_between_uploads"] is None


def test_successful_empty_playlist_is_distinguished_from_fetch_failure(monkeypatch):
    import asyncio
    import worker.creator_worker as creator_worker

    class _Request:
        def list(self, **_kwargs):
            return self

    class _YouTube:
        def playlistItems(self):
            return _Request()

    class _Resolver:
        def _reset_client(self):
            pass

        def _get_youtube_client(self):
            return _YouTube()

        async def _execute_async(self, _request):
            return {"items": []}

    monkeypatch.setattr(creator_worker, "youtube_resolver", _Resolver())

    result = asyncio.run(
        creator_worker._fetch_recent_video_intelligence(
            "UCcreator", uploads_playlist_id="UUcreator"
        )
    )

    assert result["recent_video_fetch_succeeded"] is True
    assert result["recent_video_sample_size"] == 0


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
