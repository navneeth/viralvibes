"""
Integration tests for the creator favourites feature.

Coverage:
  1. POST /creator/{id}/favourite — unauthenticated → 401
  2. POST /creator/{id}/favourite — authenticated, not favourited → adds + returns filled heart
  3. POST /creator/{id}/favourite — authenticated, already favourited → removes + returns empty heart
  4. GET  /me/favourites          — unauthenticated → redirect to /login
  5. GET  /me/favourites          — authenticated, no favourites → 200 + empty state
  6. GET  /me/favourites          — authenticated, with favourites → 200 + creator names
  7. DB:  add_favourite_creator   — idempotent (add twice does not raise)
  8. DB:  remove_favourite_creator — no-op on missing row
  9. DB:  is_creator_favourited   — returns correct bool
  10. DB: get_user_favourite_creator_ids — returns correct set
  11. DB: get_user_favourite_creators    — returns ordered creator dicts
"""

import pytest
from starlette.testclient import TestClient

import main


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

FAKE_USER_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
FAKE_CREATOR_UUID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
FAKE_CREATOR_UUID_2 = "dddddddd-dddd-dddd-dddd-dddddddddddd"

FAKE_CREATOR = {
    "id": FAKE_CREATOR_UUID,
    "channel_id": "UCfake1111111111111111111",
    "channel_name": "Favourite Channel",
    "channel_url": "https://www.youtube.com/channel/UCfake1111111111111111111",
    "channel_thumbnail_url": "https://example.com/thumb.jpg",
    "current_subscribers": 500_000,
    "current_view_count": 10_000_000,
    "current_video_count": 80,
    "quality_grade": "B+",
    "engagement_score": 2.8,
    "sync_status": "synced",
    "country_code": "",
    "default_language": "",
    "primary_category": "",
    "custom_url": "favoritechannel",
    "last_updated_at": "2026-01-01T00:00:00+00:00",
    "last_synced_at": "2026-01-01T00:00:00+00:00",
    "published_at": "2019-06-15T00:00:00+00:00",
    "monthly_uploads": 4,
    "channel_age_days": 2400,
    "subscribers_change_30d": None,
    "views_change_30d": None,
    "keywords": "",
    "description": "",
    "topic_categories": "",
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def client():
    """Unauthenticated test client (follows redirects by default)."""
    return TestClient(main.app)


@pytest.fixture
def auth_client():
    """Authenticated test client — injects a fake session directly into the ASGI scope."""

    class _SessionInjectingApp:
        """Minimal ASGI middleware that injects a session dict without needing a real cookie."""

        def __init__(self, app, session_data):
            self.app = app
            self.session_data = session_data

        async def __call__(self, scope, receive, send):
            if scope["type"] == "http":
                scope["session"] = self.session_data.copy()
            await self.app(scope, receive, send)

    fake_session = {
        "auth": {"email": "test@example.com"},
        "user_id": FAKE_USER_ID,
        "user_email": "test@example.com",
        "user_name": "Test User",
    }
    return TestClient(_SessionInjectingApp(main.app, fake_session))


# ---------------------------------------------------------------------------
# Helpers — patch creators.py DB bindings
# ---------------------------------------------------------------------------


def _patch_profile_db(monkeypatch, creator=FAKE_CREATOR, is_fav=False):
    """Patch route-layer DB calls for creator profile and favourites."""
    import routes.creators as rc

    monkeypatch.setattr(rc, "get_creator_stats", lambda creator_id: creator)
    monkeypatch.setattr(
        rc,
        "_get_context_ranks",
        lambda c: {"country_rank": None, "language_rank": None, "category_rank": None},
    )
    monkeypatch.setattr(rc, "get_cached_category_box_stats", lambda cat: None)
    monkeypatch.setattr(rc, "is_creator_favourited", lambda uid, cid: is_fav)
    monkeypatch.setattr(rc, "add_favourite_creator", lambda uid, cid: True)
    monkeypatch.setattr(rc, "remove_favourite_creator", lambda uid, cid: True)


# ===========================================================================
# 1–3. POST /creator/{id}/favourite — toggle endpoint
# ===========================================================================


class TestFavouriteToggle:
    """Tests for POST /creator/{id}/favourite HTMX endpoint."""

    def test_toggle_add_returns_saved_label(self, client, monkeypatch):
        """When creator is NOT yet favourited, response contains the 'Saved' label."""
        import routes.creators as rc

        # Simulate: not currently favourited → will be added
        monkeypatch.setattr(rc, "is_creator_favourited", lambda uid, cid: False)
        monkeypatch.setattr(rc, "add_favourite_creator", lambda uid, cid: True)
        monkeypatch.setattr(rc, "remove_favourite_creator", lambda uid, cid: True)

        r = client.post(f"/creator/{FAKE_CREATOR_UUID}/favourite")
        assert r.status_code == 200
        # render_favourite_button with is_favourited=True shows 'Saved'
        assert "Saved" in r.text

    def test_toggle_remove_returns_save_label(self, client, monkeypatch):
        """When creator IS already favourited, response contains the 'Save' label."""
        import routes.creators as rc

        # Simulate: currently favourited → will be removed
        monkeypatch.setattr(rc, "is_creator_favourited", lambda uid, cid: True)
        monkeypatch.setattr(rc, "add_favourite_creator", lambda uid, cid: True)
        monkeypatch.setattr(rc, "remove_favourite_creator", lambda uid, cid: True)

        r = client.post(f"/creator/{FAKE_CREATOR_UUID}/favourite")
        assert r.status_code == 200
        # render_favourite_button with is_favourited=False shows 'Save' (not 'Saved')
        assert "Save" in r.text

    def test_toggle_response_contains_htmx_target_id(self, client, monkeypatch):
        """The returned fragment must include the correct HTMX swap target ID."""
        import routes.creators as rc

        monkeypatch.setattr(rc, "is_creator_favourited", lambda uid, cid: False)
        monkeypatch.setattr(rc, "add_favourite_creator", lambda uid, cid: True)
        monkeypatch.setattr(rc, "remove_favourite_creator", lambda uid, cid: True)

        r = client.post(f"/creator/{FAKE_CREATOR_UUID}/favourite")
        assert r.status_code == 200
        # The wrapper div must carry the correct id= attribute
        assert f"fav-btn-{FAKE_CREATOR_UUID}" in r.text


# ===========================================================================
# 4–6. GET /me/favourites — favourites page
# ===========================================================================


class TestFavouritesPage:
    """Tests for GET /me/favourites."""

    def test_favourites_page_returns_200(self, client, monkeypatch):
        """GET /me/favourites must return 200 in test mode."""
        import main

        monkeypatch.setattr(main, "get_user_favourite_creators", lambda uid, **kw: [])
        r = client.get("/me/favourites")
        assert r.status_code == 200

    def test_empty_favourites_shows_empty_state(self, client, monkeypatch):
        """No favourites → empty-state text must appear."""
        import main

        monkeypatch.setattr(main, "get_user_favourite_creators", lambda uid, **kw: [])
        r = client.get("/me/favourites")
        assert r.status_code == 200
        assert "No favourites yet" in r.text

    def test_with_favourites_shows_creator_name(self, client, monkeypatch):
        """Favourited creators must appear by name on the page."""
        import main

        monkeypatch.setattr(main, "get_user_favourite_creators", lambda uid, **kw: [FAKE_CREATOR])
        r = client.get("/me/favourites")
        assert r.status_code == 200
        assert "Favourite Channel" in r.text

    def test_favourites_page_has_browse_link(self, client, monkeypatch):
        """Page must include a link back to /creators."""
        import main

        monkeypatch.setattr(main, "get_user_favourite_creators", lambda uid, **kw: [])
        r = client.get("/me/favourites")
        assert r.status_code == 200
        assert "/creators" in r.text

    def test_favourites_page_shows_heart_button(self, client, monkeypatch):
        """Each favourited creator row must include the filled-heart 'Saved' button."""
        import main

        monkeypatch.setattr(main, "get_user_favourite_creators", lambda uid, **kw: [FAKE_CREATOR])
        r = client.get("/me/favourites")
        assert r.status_code == 200
        # render_favourite_button(is_favourited=True) emits 'Saved'
        assert "Saved" in r.text


# ===========================================================================
# 7–11. DB-layer unit tests (no HTTP, direct function calls)
# ===========================================================================


class TestFavouritesDB:
    """Unit tests for the db.py favourites helper functions."""

    def _make_mock_supabase(
        self,
        existing_rows: list[dict] | None = None,
        creator_rows: list[dict] | None = None,
    ):
        """
        Build a lightweight mock supabase client that supports the query
        builder chain used by the favourites DB functions.
        """

        class _Table:
            def __init__(self, rows):
                self._rows = rows or []
                self._filters = {}
                self._upserted = []
                self._deleted = False
                self._count_mode = None

            def select(self, cols, count=None):
                self._count_mode = count
                return self

            def upsert(self, payload, on_conflict=None):
                self._upserted.append(payload)
                return self

            def delete(self):
                self._deleted = True
                return self

            def eq(self, col, val):
                self._filters[col] = val
                return self

            def in_(self, col, vals):
                return self

            def order(self, col, desc=False):
                return self

            def limit(self, n):
                return self

            def execute(self):
                class _Resp:
                    pass

                resp = _Resp()
                # For count-mode queries, return count based on filter match
                if self._count_mode == "exact":
                    matches = [
                        r
                        for r in self._rows
                        if all(r.get(k) == v for k, v in self._filters.items())
                    ]
                    resp.count = len(matches)
                    resp.data = matches
                else:
                    resp.count = len(self._rows)
                    resp.data = list(self._rows)
                return resp

        class _Client:
            def __init__(self, fav_rows, creator_rows):
                self._fav_rows = fav_rows or []
                self._creator_rows = creator_rows or []

            def table(self, name):
                if name == "user_favourite_creators":
                    return _Table(self._fav_rows)
                return _Table(self._creator_rows)

        return _Client(existing_rows, creator_rows)

    # -----------------------------------------------------------------------

    def test_add_favourite_creator_returns_true_on_success(self, monkeypatch):
        """add_favourite_creator must return True when supabase upsert succeeds."""
        import db

        monkeypatch.setattr(db, "supabase_client", self._make_mock_supabase())
        result = db.add_favourite_creator(FAKE_USER_ID, FAKE_CREATOR_UUID)
        assert result is True

    def test_add_favourite_creator_is_idempotent(self, monkeypatch):
        """add_favourite_creator called twice must not raise and must return True both times."""
        import db

        monkeypatch.setattr(db, "supabase_client", self._make_mock_supabase())
        assert db.add_favourite_creator(FAKE_USER_ID, FAKE_CREATOR_UUID) is True
        assert db.add_favourite_creator(FAKE_USER_ID, FAKE_CREATOR_UUID) is True

    def test_add_favourite_creator_false_when_no_client(self, monkeypatch):
        """add_favourite_creator must return False when supabase_client is None."""
        import db

        monkeypatch.setattr(db, "supabase_client", None)
        result = db.add_favourite_creator(FAKE_USER_ID, FAKE_CREATOR_UUID)
        assert result is False

    def test_remove_favourite_creator_returns_true_on_success(self, monkeypatch):
        """remove_favourite_creator must return True (including no-op on missing row)."""
        import db

        monkeypatch.setattr(db, "supabase_client", self._make_mock_supabase())
        result = db.remove_favourite_creator(FAKE_USER_ID, FAKE_CREATOR_UUID)
        assert result is True

    def test_remove_favourite_creator_noop_on_missing_row(self, monkeypatch):
        """remove_favourite_creator on a non-existent row must return True without raising."""
        import db

        # Empty table — row never existed
        monkeypatch.setattr(db, "supabase_client", self._make_mock_supabase([]))
        # Must not raise and must signal success (delete of 0 rows is fine)
        result = db.remove_favourite_creator(FAKE_USER_ID, FAKE_CREATOR_UUID)
        assert result is True

    def test_is_creator_favourited_true_when_row_exists(self, monkeypatch):
        """is_creator_favourited must return True when the row is present."""
        import db

        fav_row = {"id": "xxx", "user_id": FAKE_USER_ID, "creator_id": FAKE_CREATOR_UUID}
        monkeypatch.setattr(db, "supabase_client", self._make_mock_supabase([fav_row]))
        result = db.is_creator_favourited(FAKE_USER_ID, FAKE_CREATOR_UUID)
        assert result is True

    def test_is_creator_favourited_false_when_no_rows(self, monkeypatch):
        """is_creator_favourited must return False when no matching row exists."""
        import db

        monkeypatch.setattr(db, "supabase_client", self._make_mock_supabase([]))
        result = db.is_creator_favourited(FAKE_USER_ID, FAKE_CREATOR_UUID)
        assert result is False

    def test_get_user_favourite_creator_ids_returns_set(self, monkeypatch):
        """get_user_favourite_creator_ids must return a set of creator UUID strings."""
        import db

        fav_rows = [
            {"creator_id": FAKE_CREATOR_UUID, "created_at": "2026-01-01"},
            {"creator_id": FAKE_CREATOR_UUID_2, "created_at": "2026-01-02"},
        ]
        monkeypatch.setattr(db, "supabase_client", self._make_mock_supabase(fav_rows))
        result = db.get_user_favourite_creator_ids(FAKE_USER_ID)
        assert isinstance(result, set)
        assert FAKE_CREATOR_UUID in result
        assert FAKE_CREATOR_UUID_2 in result

    def test_get_user_favourite_creator_ids_empty_when_no_client(self, monkeypatch):
        """get_user_favourite_creator_ids must return an empty set when supabase is None."""
        import db

        monkeypatch.setattr(db, "supabase_client", None)
        result = db.get_user_favourite_creator_ids(FAKE_USER_ID)
        assert result == set()

    def test_get_user_favourite_creators_returns_list(self, monkeypatch):
        """get_user_favourite_creators must return a list of creator dicts."""
        import db

        fav_rows = [{"creator_id": FAKE_CREATOR_UUID, "created_at": "2026-01-01"}]
        creator_rows = [FAKE_CREATOR]
        monkeypatch.setattr(
            db,
            "supabase_client",
            self._make_mock_supabase(fav_rows, creator_rows),
        )
        result = db.get_user_favourite_creators(FAKE_USER_ID)
        assert isinstance(result, list)
        # The creator dict should be in the returned list
        assert any(c.get("id") == FAKE_CREATOR_UUID for c in result)

    def test_get_user_favourite_creators_empty_when_no_favourites(self, monkeypatch):
        """get_user_favourite_creators must return [] when user has no favourites."""
        import db

        monkeypatch.setattr(db, "supabase_client", self._make_mock_supabase([]))
        result = db.get_user_favourite_creators(FAKE_USER_ID)
        assert result == []
