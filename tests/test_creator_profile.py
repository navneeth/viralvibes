"""
Integration tests for creator browse and profile endpoints.

Coverage:
  1. GET /creators           — public browse page renders with mocked DB
  2. GET /creator/{id}       — known creator renders full profile
  3. GET /creator/{id}       — unknown ID returns 404-style component (not a 500)
  4. GET /creators?search=   — search query flows through without crashing
  5. GET /creators?page=999  — out-of-range page redirects to last valid page
"""

import pytest
from starlette.testclient import TestClient

import main


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

FAKE_CREATOR_UUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

FAKE_CREATOR = {
    "id": FAKE_CREATOR_UUID,
    "channel_id": "UCfakeChannelId0123456789",
    "channel_name": "Test Channel",
    "channel_url": "https://www.youtube.com/channel/UCfakeChannelId0123456789",
    "channel_thumbnail_url": "https://example.com/thumb.jpg",
    "current_subscribers": 1_000_000,
    "current_view_count": 50_000_000,
    "current_video_count": 200,
    "quality_grade": "A",
    "engagement_score": 3.5,
    "sync_status": "synced",
    # Empty geo/language fields — avoids thread-pool rank queries in _get_context_ranks
    "country_code": "",
    "default_language": "",
    "primary_category": "",
    "custom_url": "testchannel",
    "last_updated_at": "2026-01-01T00:00:00+00:00",
    "last_synced_at": "2026-01-01T00:00:00+00:00",
    "published_at": "2020-01-01T00:00:00+00:00",
    "monthly_uploads": 8,
    "channel_age_days": 2200,
    "subscribers_change_30d": 5000,
    "views_change_30d": 100_000,
    "keywords": "",
    "description": "",
    "topic_categories": "",
}


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------


def make_creators_result(creators=None):
    """Return a CreatorsResult NamedTuple populated with test data."""
    from db import CreatorsResult

    items = creators if creators is not None else [FAKE_CREATOR]
    return CreatorsResult(creators=items, total_count=len(items))


def make_empty_hero_stats():
    """Stub for get_creator_hero_stats() that returns no data."""
    return {}


def make_stub_page_stats(creators, include_all=False):
    """Stub for calculate_creator_stats() that returns a minimal stats dict."""
    return {
        "total_creators": len(creators),
        "grade_counts": {},
        "top_countries": [],
        "top_languages": [],
        "top_categories": [],
        "growing_creators": 0,
        "premium_creators": 0,
        "total_countries": 0,
        "total_languages": 0,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def client():
    """Unauthenticated test client."""
    return TestClient(main.app)


# ===========================================================================
# 1. GET /creators — public browse page
# ===========================================================================


class TestCreatorsPage:
    """Integration tests for GET /creators."""

    def _patch_creators_db(self, monkeypatch, creators=None):
        """Patch all DB calls made by creators_route."""
        import routes.creators as rc

        monkeypatch.setattr(rc, "get_creators", lambda **kw: make_creators_result(creators))
        monkeypatch.setattr(rc, "calculate_creator_stats", make_stub_page_stats)
        monkeypatch.setattr(rc, "get_creator_hero_stats", make_empty_hero_stats)
        monkeypatch.setattr(rc, "get_top_countries_with_counts", lambda limit=8: [])
        monkeypatch.setattr(rc, "get_top_languages_with_counts", lambda limit=5: [])
        monkeypatch.setattr(rc, "get_top_categories_with_counts", lambda limit=4: [])
        monkeypatch.setattr(rc, "get_lists_meta", lambda: {"total_categories": 0})

    def test_browse_page_returns_200(self, client, monkeypatch):
        self._patch_creators_db(monkeypatch)
        r = client.get("/creators")
        assert r.status_code == 200

    def test_browse_page_contains_page_title(self, client, monkeypatch):
        """Page title should reference Creators."""
        self._patch_creators_db(monkeypatch)
        r = client.get("/creators")
        assert r.status_code == 200
        assert "Creator" in r.text

    def test_browse_page_shows_creator_card(self, client, monkeypatch):
        """Creator cards should show the mocked channel name."""
        self._patch_creators_db(monkeypatch)
        r = client.get("/creators")
        assert r.status_code == 200
        assert "Test Channel" in r.text

    def test_browse_page_with_search_query(self, client, monkeypatch):
        """Search query param must not crash the route."""
        self._patch_creators_db(monkeypatch)
        r = client.get("/creators?search=MrBeast")
        assert r.status_code == 200

    def test_browse_page_with_multiple_filters(self, client, monkeypatch):
        """Multiple filter params combined must render without errors."""
        self._patch_creators_db(monkeypatch)
        r = client.get("/creators?sort=engagement&grade=A&language=en&activity=active")
        assert r.status_code == 200

    def test_browse_page_empty_results(self, client, monkeypatch):
        """Zero creators must not raise — should render an empty state."""
        self._patch_creators_db(monkeypatch, creators=[])
        r = client.get("/creators")
        assert r.status_code == 200

    def test_browse_page_out_of_range_page_redirects(self, monkeypatch):
        """page > total_pages must redirect to the last valid page, not return a 500."""
        # Build a client that does NOT follow redirects so we can inspect the 303.
        no_redirect_client = TestClient(main.app, follow_redirects=False)
        # 1 creator → 1 total page; requesting page=999 triggers the redirect
        self._patch_creators_db(monkeypatch)
        r = no_redirect_client.get("/creators?page=999")
        assert r.status_code in (302, 303)
        assert "page=" in r.headers.get("location", "")


# ===========================================================================
# 2–3. GET /creator/{id} — known and unknown creators
# ===========================================================================


class TestCreatorProfile:
    """Integration tests for GET /creator/{creator_id}."""

    def _patch_profile_db(self, monkeypatch, creator=FAKE_CREATOR):
        """Patch all DB calls made by creator_profile_route."""
        import routes.creators as rc

        monkeypatch.setattr(rc, "get_creator_stats", lambda creator_id: creator)
        monkeypatch.setattr(
            rc,
            "_get_context_ranks",
            lambda c: {"country_rank": None, "language_rank": None, "category_rank": None},
        )
        monkeypatch.setattr(rc, "get_cached_category_box_stats", lambda cat: None)

    def test_known_creator_returns_200(self, client, monkeypatch):
        self._patch_profile_db(monkeypatch)
        r = client.get(f"/creator/{FAKE_CREATOR_UUID}")
        assert r.status_code == 200

    def test_known_creator_shows_channel_name(self, client, monkeypatch):
        """Profile page must include the creator's channel name."""
        self._patch_profile_db(monkeypatch)
        r = client.get(f"/creator/{FAKE_CREATOR_UUID}")
        assert r.status_code == 200
        assert "Test Channel" in r.text

    def test_unknown_creator_returns_200_not_500(self, client, monkeypatch):
        """Unknown UUIDs must return a friendly component, not raise a server error."""
        self._patch_profile_db(monkeypatch, creator=None)
        r = client.get("/creator/00000000-0000-0000-0000-000000000000")
        assert r.status_code == 200

    def test_unknown_creator_shows_not_found_message(self, client, monkeypatch):
        """404 component must render H2('Creator not found')."""
        self._patch_profile_db(monkeypatch, creator=None)
        r = client.get("/creator/00000000-0000-0000-0000-000000000000")
        assert "not found" in r.text.lower()

    def test_unknown_creator_has_back_link(self, client, monkeypatch):
        """404 component must include a link back to /creators."""
        self._patch_profile_db(monkeypatch, creator=None)
        r = client.get("/creator/00000000-0000-0000-0000-000000000000")
        assert "/creators" in r.text

    def test_profile_from_query_param_does_not_crash(self, client, monkeypatch):
        """?from= back-nav query param must pass through without errors."""
        self._patch_profile_db(monkeypatch)
        r = client.get(f"/creator/{FAKE_CREATOR_UUID}?from=/creators%3Fsort%3Dengagement")
        assert r.status_code == 200
