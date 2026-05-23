"""
Tests for saved creator outreach exports.
"""

from starlette.testclient import TestClient

import main
from services.outreach import (
    EMAIL_EXPORT_HEADERS,
    build_outreach_rows,
    filter_email_ready_rows,
    render_outreach_csv,
)


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


def test_email_export_filters_rows_without_email():
    rows = build_outreach_rows([CREATOR_WITH_EMAIL, CREATOR_SOCIAL_ONLY])

    email_ready = filter_email_ready_rows(rows)

    assert len(email_ready) == 1
    assert email_ready[0]["Company"] == "Ready Creator"


def test_outreach_csv_starts_with_email_header():
    csv_text = render_outreach_csv(build_outreach_rows([CREATOR_WITH_EMAIL]))

    assert csv_text.startswith(",".join(EMAIL_EXPORT_HEADERS[:3]))
    assert "hello@readycreator.com" in csv_text


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
