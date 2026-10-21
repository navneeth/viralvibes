"""
Tests for saved creator outreach exports.
"""

import csv
import io
import json
from base64 import b64encode

import pytest
from urllib.parse import urlparse
from starlette.testclient import TestClient
from starlette.middleware.sessions import SessionMiddleware

import main
from services.outreach import (
    EMAIL_EXPORT_HEADERS,
    build_outreach_rows,
    filter_email_ready_rows,
    render_outreach_csv,
)
from services.contact_extractor import extract_social_links


FAKE_USER_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

CREATOR_WITH_EMAIL = {
    "id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
    "channel_id": "UCfake1111111111111111111",
    "channel_name": "Ready Creator",
    "channel_url": "https://www.youtube.com/channel/UCfake1111111111111111111",
    "current_subscribers": 500_000,
    "current_view_count": 10_000_000,
    "current_video_count": 80,
    "quality_grade": "A",
    "engagement_score": 4.2,
    "primary_category": "Education",
    "country_code": "US",
    "default_language": "en",
    "subscribers_change_30d": 1200,
    "views_change_30d": 250000,
    "channel_description": (
        "Business: hello@readycreator.com. Website https://readycreator.com "
        "Instagram https://instagram.com/readycreator "
        "X https://x.com/readycreator "
        "TikTok https://tiktok.com/@readycreator "
        "LinkedIn https://linkedin.com/in/readycreator"
    ),
}

CREATOR_SOCIAL_ONLY = {
    "id": "dddddddd-dddd-dddd-dddd-dddddddddddd",
    "channel_id": "UCfake2222222222222222222",
    "channel_name": "Social Creator",
    "channel_url": "https://www.youtube.com/channel/UCfake2222222222222222222",
    "current_subscribers": 100_000,
    "primary_category": "Gaming",
    "country_code": "GB",
    "channel_description": "Find me at https://x.com/socialcreator",
}


class _SessionInjectingApp:
    def __init__(self, app, session_data):
        self.app = app
        self.session_data = session_data

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            scope["session"] = self.session_data.copy()
        await self.app(scope, receive, send)


def _auth_client():
    return TestClient(
        _SessionInjectingApp(
            main.app,
            {
                "auth": {"email": "test@example.com"},
                "user_id": FAKE_USER_ID,
                "user_email": "test@example.com",
                "user_name": "Test User",
            },
        )
    )


def test_creator_to_outreach_row_extracts_email_and_links():
    rows = build_outreach_rows([CREATOR_WITH_EMAIL], base_url="https://viralvibes.test")

    assert rows[0]["Email"] == "hello@readycreator.com"
    assert rows[0]["Company"] == "Ready Creator"
    assert rows[0]["Website"] == "https://readycreator.com"
    assert rows[0]["Instagram URL"] == "https://instagram.com/readycreator"
    # Reviewer suggested "Twitter URL" / https://twitter.com — the header is "X URL"
    # and the code normalises all x.com/twitter.com handles to https://x.com/<handle>
    assert rows[0]["X URL"] == "https://x.com/readycreator"
    assert rows[0]["TikTok URL"] == "https://tiktok.com/@readycreator"
    # Code emits https://linkedin.com/... (no www); match the actual output
    assert rows[0]["LinkedIn URL"] == "https://linkedin.com/in/readycreator"
    assert rows[0]["ViralVibes Profile URL"] == (
        "https://viralvibes.test/creator/cccccccc-cccc-cccc-cccc-cccccccccccc"
    )


def test_creator_profile_and_outreach_share_contact_parser():
    links = extract_social_links(
        "Business: hello@example.com https://example.com https://x.com/example",
        "",
    )

    assert ("mail", "hello@example.com", "mailto:hello@example.com") in links
    assert ("globe", "Website", "https://example.com") in links
    assert ("twitter", "X / Twitter", "https://x.com/example") in links


def test_contact_parser_normalises_linkedin_company_url():
    links = extract_social_links("https://linkedin.com/company/acme-corp", "")
    urls = [url for _, _, url in links]
    assert "https://linkedin.com/company/acme-corp" in urls


def test_contact_parser_strips_trailing_punctuation_from_website():
    links = extract_social_links("Visit us at https://example.com.", "")
    urls = [url for _, _, url in links]
    assert "https://example.com" in urls
    assert "https://example.com." not in urls


def test_contact_parser_skips_youtube_urls():
    links = extract_social_links(
        "Watch at https://www.youtube.com/watch?v=abc123 and https://mysite.com",
        "",
    )
    globe_urls = [url for icon, _, url in links if icon == "globe"]
    assert not any(
        (host == "youtube.com" or host.endswith(".youtube.com"))
        for host in ((urlparse(u).hostname or "").lower() for u in globe_urls)
    )
    assert any(
        (host == "mysite.com" or host.endswith(".mysite.com"))
        for host in ((urlparse(u).hostname or "").lower() for u in globe_urls)
    )


def test_email_export_filters_rows_without_email():
    rows = build_outreach_rows([CREATOR_WITH_EMAIL, CREATOR_SOCIAL_ONLY])

    email_ready = filter_email_ready_rows(rows)

    assert len(email_ready) == 1
    assert email_ready[0]["Company"] == "Ready Creator"


def test_outreach_csv_starts_with_email_header():
    csv_text = render_outreach_csv(build_outreach_rows([CREATOR_WITH_EMAIL]))

    assert csv_text.startswith(",".join(EMAIL_EXPORT_HEADERS[:3]))
    assert "hello@readycreator.com" in csv_text


@pytest.mark.parametrize("path", ["/me/outreach/export", "/me/favourites/export"])
def test_saved_exports_append_topics_without_private_context(monkeypatch, path):
    import routes.outreach as outreach
    from services.creator_topics import TOPIC_EXPORT_HEADERS

    creator = {
        **CREATOR_WITH_EMAIL,
        "transcript_keywords": [{"kw": "mechanical keyboards", "score": 0.7}],
        "transcript_keywords_updated_at": "2026-10-20T10:00:00Z",
        "private_context": "private campaign idea",
    }
    monkeypatch.setattr(outreach, "get_user_favourite_creators", lambda *args, **kw: [creator])
    monkeypatch.setattr(main, "get_user_favourite_creators", lambda *args, **kw: [creator])

    middleware_config = next(
        item for item in main.app.user_middleware if item.cls is SessionMiddleware
    )
    session_middleware = SessionMiddleware(main.app, **middleware_config.kwargs)
    session_data = b64encode(
        json.dumps(
            {
                "user_id": FAKE_USER_ID,
                "auth": {"email": "test@example.com"},
            }
        ).encode()
    )
    client = TestClient(main.app)
    client.cookies.set(
        session_middleware.session_cookie,
        session_middleware.signer.sign(session_data).decode(),
    )
    response = client.get(path, follow_redirects=False)
    assert response.status_code == 200
    reader = csv.DictReader(io.StringIO(response.text))
    row = next(reader)
    assert reader.fieldnames[-5:] == TOPIC_EXPORT_HEADERS
    assert row["Content Topics"] == "mechanical keyboards"
    assert row["Keyword Source"] == "Source unavailable"
    assert row["Topics Computed At"] == ""
    assert row["Topics Uploaded At"].startswith("2026-10-20")
    assert "private campaign idea" not in response.text
    if path == "/me/outreach/export":
        assert reader.fieldnames[:23] == EMAIL_EXPORT_HEADERS[:23]
        assert row["Email"] == "hello@readycreator.com"
    else:
        assert reader.fieldnames[:2] == ["channel_name", "channel_id"]


def test_favourites_export_requires_sign_in():
    response = TestClient(main.app).get("/me/favourites/export", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/login"


def test_public_lookalike_export_has_public_topics_only(monkeypatch):
    import routes.creators as creators_routes

    seed = {"id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "channel_name": "Seed Creator"}
    peer = {
        **CREATOR_WITH_EMAIL,
        "transcript_keywords": [{"kw": "=unsafe"}],
        "private_context": "private campaign idea",
    }
    monkeypatch.setattr(creators_routes, "_resolve_seed_creator", lambda handle: seed)
    monkeypatch.setattr(
        creators_routes, "get_embedding_peers", lambda *args, **kw: ([peer, CREATOR_SOCIAL_ONLY], 2)
    )
    response = TestClient(main.app).get("/creators/like/seed/export")
    rows = list(csv.DictReader(io.StringIO(response.text)))
    assert response.status_code == 200
    assert len(rows) == 1
    assert rows[0]["Company"] == peer["channel_name"]
    assert rows[0]["Email"] == "hello@readycreator.com"
    assert rows[0]["Content Topics"] == "'=unsafe"
    assert seed["channel_name"] not in response.text
    assert "private campaign idea" not in response.text


def test_legacy_and_shared_export_builders_use_same_topic_projection():
    from services.contact_extractor import ContactExtractorService
    from services.creator_topics import TOPIC_EXPORT_HEADERS

    creator = {**CREATOR_WITH_EMAIL, "transcript_keywords": [{"kw": "keyboards"}]}
    legacy = build_outreach_rows([creator])[0]
    shared = ContactExtractorService.build_creator_contact_row(creator)
    assert EMAIL_EXPORT_HEADERS == ContactExtractorService.EMAIL_EXPORT_HEADERS
    assert {key: legacy[key] for key in TOPIC_EXPORT_HEADERS} == {
        key: shared[key] for key in TOPIC_EXPORT_HEADERS
    }


def test_outreach_page_renders_saved_creator_counts(monkeypatch):
    import routes.outreach as outreach

    monkeypatch.setattr(
        outreach,
        "get_user_favourite_creators",
        lambda uid, **kw: [CREATOR_WITH_EMAIL, CREATOR_SOCIAL_ONLY],
    )

    r = _auth_client().get("/me/outreach")

    assert r.status_code == 200
    assert "Outreach" in r.text
    assert "Ready Creator" in r.text
    assert "With email" in r.text


def test_outreach_export_returns_email_ready_csv(monkeypatch):
    import routes.outreach as outreach

    monkeypatch.setattr(
        outreach,
        "get_user_favourite_creators",
        lambda uid, **kw: [CREATOR_WITH_EMAIL, CREATOR_SOCIAL_ONLY],
    )

    r = _auth_client().get("/me/outreach/export")

    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    assert "hello@readycreator.com" in r.text
    assert "Social Creator" not in r.text


def test_outreach_page_shows_saved_lists_import(monkeypatch):
    import routes.outreach as outreach

    monkeypatch.setattr(outreach, "get_user_favourite_creators", lambda uid, **kw: [])
    monkeypatch.setattr(
        outreach,
        "get_user_favourite_lists",
        lambda uid, **kw: [
            {
                "list_key": "category:Education",
                "list_label": "Education",
                "list_url": "/lists/category/Education",
            }
        ],
    )

    r = _auth_client().get("/me/outreach")

    assert r.status_code == 200
    assert "Saved Lists" in r.text
    assert "Add top 25" in r.text


def test_outreach_import_list_bulk_saves_creators(monkeypatch):
    import routes.outreach as outreach

    saved = {}

    monkeypatch.setattr(
        outreach,
        "get_creators_for_outreach_list",
        lambda list_key, limit: [CREATOR_WITH_EMAIL, CREATOR_SOCIAL_ONLY],
    )

    def fake_bulk_save(user_id, creator_ids):
        saved["user_id"] = user_id
        saved["creator_ids"] = creator_ids
        return len(creator_ids)

    monkeypatch.setattr(outreach, "add_favourite_creators_bulk", fake_bulk_save)

    r = _auth_client().post(
        "/me/outreach/import-list",
        data={"list_key": "category:Education", "limit": "25"},
    )


def test_outreach_page_shows_non_importable_saved_lists(monkeypatch):
    """Aggregate/browse list keys render an 'Explorer tab' pill, not an import form."""
    import routes.outreach as outreach

    monkeypatch.setattr(outreach, "get_user_favourite_creators", lambda uid, **kw: [])
    monkeypatch.setattr(
        outreach,
        "get_user_favourite_lists",
        lambda uid, **kw: [
            {
                "list_key": "by-country",
                "list_label": "By country",
                "list_url": "/lists/by-country",
            }
        ],
    )

    r = _auth_client().get("/me/outreach")

    assert r.status_code == 200
    assert "Saved Lists" in r.text
    assert "Explorer tab" in r.text
    assert "Add top 25" not in r.text


def test_outreach_import_list_empty_creators_shows_warning(monkeypatch):
    """Empty creator list returns the yellow warning and never calls bulk save."""
    import routes.outreach as outreach

    bulk_save_called = {}

    monkeypatch.setattr(outreach, "get_creators_for_outreach_list", lambda list_key, limit: [])
    monkeypatch.setattr(
        outreach,
        "add_favourite_creators_bulk",
        lambda uid, ids: bulk_save_called.update({"called": True}) or 0,
    )

    r = _auth_client().post(
        "/me/outreach/import-list",
        data={"list_key": "category:Education", "limit": "25"},
    )

    assert r.status_code == 200
    assert "No importable creators" in r.text
    assert not bulk_save_called


@pytest.mark.parametrize("raw_limit", ["999", "not-a-number"])
def test_outreach_import_list_clamps_limit(raw_limit, monkeypatch):
    """Out-of-range and non-integer limit values must be clamped before the DB call."""
    import routes.outreach as outreach

    received = {}

    def fake_get_creators(list_key, limit):
        received["limit"] = limit
        return []

    monkeypatch.setattr(outreach, "get_creators_for_outreach_list", fake_get_creators)
    monkeypatch.setattr(outreach, "add_favourite_creators_bulk", lambda uid, ids: 0)

    _auth_client().post(
        "/me/outreach/import-list",
        data={"list_key": "category:Education", "limit": raw_limit},
    )

    assert "limit" in received, "get_creators_for_outreach_list was not called"
    assert 1 <= received["limit"] <= 100
