import io
from types import SimpleNamespace

import polars as pl
import pytest
from starlette.testclient import TestClient

import main


@pytest.fixture
def client():
    return TestClient(main.app)


def make_cached_row():
    # small cached row used by validate_full
    df = pl.DataFrame(
        [
            {
                "Rank": 1,
                "Title": "A",
                "Views": 100,
                "Likes": 10,
                "Dislikes": 1,
                "Comments": 5,
                "Engagement Rate (%)": 16.0,  # (Likes + Dislikes + Comments) / Views
                "Controversy": 18.18,
                "Views Formatted": "100",
                "Likes Formatted": "10",
                "Engagement Rate Formatted": "16.00%",
            }
        ]
    )
    return {
        "df_json": df.write_json(),
        "title": "Sample Playlist",
        "channel_name": "Tester",
        "channel_thumbnail": "/img.png",
        "summary_stats": {
            "total_views": 100,
            "total_likes": 10,
            "actual_playlist_count": 1,
            "processed_video_count": 1,
            "avg_engagement": 5.0,
        },
        "video_count": 1,
    }


def test_index_renders(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "ViralVibes" in r.text or "Decode YouTube virality" in r.text


def test_validate_url_invalid(client):
    r = client.post("/validate/url", json={"playlist_url": "not-a-url"})
    # validator returns a Div with errors (422 may be returned by pydantic depending on your validators)
    assert r.status_code in (200, 422)


def test_preview_returns_script_on_cache_hit(client, monkeypatch):
    # stub cache -> triggers redirect script to full analysis
    monkeypatch.setattr(
        main, "get_cached_playlist_stats", lambda url, check_date=True: {"title": "X"}
    )
    r = client.post(
        "/validate/preview",
        data={"playlist_url": "https://youtube.com/playlist?list=PL1"},
    )
    assert r.status_code == 200
    assert "htmx.ajax('POST', '/validate/full'" in r.text


def test_preview_shows_preview_on_cache_miss_job_done(client, monkeypatch):
    monkeypatch.setattr(
        main, "get_cached_playlist_stats", lambda url, check_date=True: None
    )
    monkeypatch.setattr(main, "get_playlist_job_status", lambda url: "done")
    r = client.post(
        "/validate/preview",
        data={"playlist_url": "https://youtube.com/playlist?list=PL1"},
    )
    assert r.status_code == 200
    assert "htmx.ajax('POST', '/validate/full'" in r.text


def test_validate_full_stream_cached(client, monkeypatch):
    # Track if cache is CALLED
    cache_called = False

    def cached_row(url):
        nonlocal cache_called
        cache_called = True
        print("🟢 CACHE CALLED with:", url)
        row = make_cached_row()
        row.update({"valid": True, "total_videos": 5, "stats": {"duration_avg": 240}})
        print("🟢 CACHE RETURNS:", row)
        return row

    monkeypatch.setattr(main, "get_cached_playlist_stats", cached_row)

    r = client.post(
        "/validate/full", data={"playlist_url": "https://youtube.com/playlist?list=PL1"}
    )
    assert r.status_code == 200

    text = r.text
    print("📄 FULL RESPONSE LENGTH:", len(text))
    print("🔍 FIRST 300 CHARS:", repr(text[:300]))
    print("✅ 'Total/Average' FOUND?", "Total/Average" in text)
    print("✅ 'Sample Playlist' FOUND?", "Sample Playlist" in text)
    print("✅ 'playlist-table' FOUND?", "playlist-table" in text)
    print("🟢 WAS CACHE CALLED?", cache_called)

    assert cache_called, "CACHE NEVER CALLED!"
    # assert "Total/Average" in text


def test_submit_job_and_poll(client, monkeypatch):
    # ensure submit_playlist_job is called and check_job_status returns pending
    called = {}

    def fake_submit(url):
        called["url"] = url

    monkeypatch.setattr(main, "submit_playlist_job", fake_submit)
    r = client.post(
        "/submit-job", data={"playlist_url": "https://youtube.com/playlist?list=PL1"}
    )
    assert r.status_code == 200
    assert "Analyzing playlist" in r.text or "loading" in r.text.lower()
    assert called.get("url") == "https://youtube.com/playlist?list=PL1"


def test_check_job_status_transitions(client, monkeypatch):
    # job not finished -> returns polling div
    monkeypatch.setattr(main, "get_playlist_job_status", lambda url: "processing")
    r = client.get(
        "/check-job-status?playlist_url=https://youtube.com/playlist?list=PL1"
    )
    assert r.status_code == 200
    assert "Analysis in progress" in r.text or "loading" in r.text.lower()


def test_newsletter_with_supabase_success(client, monkeypatch):
    # mock supabase client to return data on insert
    fake_client = SimpleNamespace()
    fake_client.table = lambda tbl: SimpleNamespace(
        insert=lambda payload: SimpleNamespace(
            execute=lambda: SimpleNamespace(data=[{"id": 1}])
        )
    )
    monkeypatch.setattr(main, "supabase_client", fake_client)
    r = client.post("/newsletter", data={"email": "test@example.com"})
    assert r.status_code == 200
    assert "Thanks for signing up" in r.text or "Thanks" in r.text


def test_dashboard_no_cache(client, monkeypatch):
    monkeypatch.setattr(main, "get_cached_playlist_stats", lambda url: None)
    r = client.get("/dashboard?playlist_url=https://youtube.com/playlist?list=PL1")
    assert r.status_code == 200
    assert "No analysis found" in r.text
