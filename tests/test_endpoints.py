import pytest
from starlette.testclient import TestClient
from types import SimpleNamespace
import io
import polars as pl

import main

@pytest.fixture
def client():
    return TestClient(main.app)

def make_cached_row():
    # small cached row used by validate_full
    df = pl.DataFrame([{"Rank": 1, "Title": "A", "View Count": 100, "Like Count": 10}])
    return {
        "df_json": df.write_json(),
        "title": "Sample Playlist",
        "channel_name": "Tester",
        "channel_thumbnail": "/img.png",
        "summary_stats": {"total_views": 100, "total_likes": 10, "actual_playlist_count": 1, "processed_video_count": 1, "avg_engagement": 5.0},
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
    monkeypatch.setattr(main, "get_cached_playlist_stats", lambda url, check_date=True: {"title": "X"})
    r = client.post("/validate/preview", data={"playlist_url": "https://youtube.com/playlist?list=PL1"})
    assert r.status_code == 200
    assert "htmx.ajax('POST', '/validate/full'" in r.text

def test_preview_shows_preview_on_cache_miss_job_done(client, monkeypatch):
    monkeypatch.setattr(main, "get_cached_playlist_stats", lambda url, check_date=True: None)
    monkeypatch.setattr(main, "get_playlist_job_status", lambda url: "done")
    r = client.post("/validate/preview", data={"playlist_url": "https://youtube.com/playlist?list=PL1"})
    assert r.status_code == 200
    assert "htmx.ajax('POST', '/validate/full'" in r.text

def test_validate_full_stream_cached(client, monkeypatch):
    # stub cache to return a cached row -> streaming should include table/footer markers
    monkeypatch.setattr(main, "get_cached_playlist_stats", lambda url: make_cached_row())
    r = client.post("/validate/full", data={"playlist_url": "https://youtube.com/playlist?list=PL1"})
    assert r.status_code == 200
    text = r.text
    assert "playlist-table" in text or "Total/Average" in text or "Sample Playlist" in text

def test_submit_job_and_poll(client, monkeypatch):
    # ensure submit_playlist_job is called and check_job_status returns pending
    called = {}
    def fake_submit(url):
        called["url"] = url
    monkeypatch.setattr(main, "submit_playlist_job", fake_submit)
    r = client.post("/submit-job", data={"playlist_url": "https://youtube.com/playlist?list=PL1"})
    assert r.status_code == 200
    assert "Analyzing playlist" in r.text or "loading" in r.text.lower()
    assert called.get("url") == "https://youtube.com/playlist?list=PL1"

def test_check_job_status_transitions(client, monkeypatch):
    # job not finished -> returns polling div
    monkeypatch.setattr(main, "get_playlist_job_status", lambda url: "processing")
    r = client.get("/check-job-status?playlist_url=https://youtube.com/playlist?list=PL1")
    assert r.status_code == 200
    assert "Analysis in progress" in r.text or "loading" in r.text.lower()

def test_newsletter_with_supabase_success(client, monkeypatch):
    # mock supabase client to return data on insert
    fake_client = SimpleNamespace()
    fake_client.table = lambda tbl: SimpleNamespace(insert=lambda payload: SimpleNamespace(execute=lambda: SimpleNamespace(data=[{"id":1}])))
    monkeypatch.setattr(main, "supabase_client", fake_client)
    r = client.post("/newsletter", data={"email": "test@example.com"})
    assert r.status_code == 200
    assert "Thanks for signing up" in r.text or "Thanks" in r.text

def test_dashboard_no_cache(client, monkeypatch):
    monkeypatch.setattr(main, "get_cached_playlist_stats", lambda url: None)
    r = client.get("/dashboard?playlist_url=https://youtube.com/playlist?list=PL1")
    assert r.status_code == 200
    assert "No analysis found" in r.text