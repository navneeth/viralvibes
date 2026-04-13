"""
Tests for the "add creator by @handle or channel ID" feature.

Coverage:
  1. db._validate_creator_input          — format validation (unit)
  2. db.queue_creator_add_request        — all guard clauses + success (unit)
  3. db.get_creator_add_request_status   — completed / failed / processing (unit)
  4. handle_resolve_and_add_job          — worker flow paths (async unit)
  5. POST /creators/request              — HTMX endpoint (integration)
  6. GET  /creators/add-status           — polling endpoint (integration)
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.testclient import TestClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_supabase_resp(data=None, count=None):
    """Return a minimal Supabase-like response object."""
    resp = SimpleNamespace()
    resp.data = data or []
    resp.count = count
    return resp


def _chainable(final_resp):
    """
    Return a mock that supports arbitrary chained attribute accesses ending in
    .execute() that returns *final_resp*.
    """
    mock = MagicMock()
    mock.execute.return_value = final_resp
    # Explicitly return self from every filter/builder method so the chain
    # always terminates at the same mock whose .execute() is configured.
    for _m in (
        "select",
        "insert",
        "update",
        "upsert",
        "eq",
        "neq",
        "ilike",
        "like",
        "gte",
        "lte",
        "gt",
        "lt",
        "is_",
        "in_",
        "not_in",
        "limit",
        "order",
        "offset",
    ):
        getattr(mock, _m).return_value = mock
    mock.not_ = mock
    return mock


# ===========================================================================
# 1. _validate_creator_input
# ===========================================================================


class TestValidateCreatorInput:
    """Unit tests for db._validate_creator_input."""

    def setup_method(self):
        from db import _validate_creator_input

        self.validate = _validate_creator_input

    def test_valid_uc_id(self):
        ok, norm = self.validate("UCX6OQ3DkcsbYNE6H8uQQuVA")
        assert ok is True
        assert norm == "UCX6OQ3DkcsbYNE6H8uQQuVA"

    def test_uc_id_mixed_case_preserved(self):
        """Channel IDs must be kept as-is (case-sensitive)."""
        ok, norm = self.validate("UCaBcDeFgHiJkLmNoPqRsTuV")
        assert ok is True
        assert norm == "UCaBcDeFgHiJkLmNoPqRsTuV"

    def test_uc_id_too_short(self):
        ok, _ = self.validate("UCshort")
        assert ok is False

    def test_uc_id_too_long(self):
        ok, _ = self.validate("UC" + "a" * 23)
        assert ok is False

    def test_handle_with_at(self):
        ok, norm = self.validate("@MrBeast")
        assert ok is True
        assert norm == "@mrbeast"

    def test_handle_without_at_gets_prefix(self):
        """Bare handle without @ should be accepted and prefixed."""
        ok, norm = self.validate("MrBeast")
        assert ok is True
        assert norm == "@mrbeast"

    def test_handle_lowercased(self):
        ok, norm = self.validate("@CAPS_CHANNEL")
        assert ok is True
        assert norm == "@caps_channel"

    def test_handle_with_dots_and_dashes(self):
        ok, norm = self.validate("@some.channel-name")
        assert ok is True
        assert norm == "@some.channel-name"

    def test_handle_too_long(self):
        ok, _ = self.validate("@" + "a" * 101)
        assert ok is False

    def test_empty_string_invalid(self):
        ok, _ = self.validate("")
        assert ok is False

    def test_full_youtube_url_invalid(self):
        ok, _ = self.validate("https://youtube.com/@MrBeast")
        assert ok is False

    def test_strips_whitespace(self):
        ok, norm = self.validate("  @MrBeast  ")
        assert ok is True
        assert norm == "@mrbeast"


# ===========================================================================
# 2. queue_creator_add_request
# ===========================================================================


class TestQueueCreatorAddRequest:
    """Unit tests for db.queue_creator_add_request — all guard clauses."""

    def _patch_supabase(self, monkeypatch, table_responses: dict):
        """
        Patch supabase_client so that .table(name).xxx...execute()
        returns the dict entry for that table name.

        *table_responses* maps table name → response object.
        """
        import db as db_module

        def make_table(name):
            return _chainable(table_responses.get(name, _make_supabase_resp()))

        fake_client = SimpleNamespace()
        fake_client.table = make_table
        monkeypatch.setattr(db_module, "supabase_client", fake_client)

    def test_invalid_format_returns_error(self, monkeypatch):
        import db as db_module

        monkeypatch.setattr(db_module, "supabase_client", object())  # won't be called

        ok, msg, creator_id = db_module.queue_creator_add_request(
            "https://youtube.com/@channel", "user-1"
        )
        assert ok is False
        assert "invalid" in msg.lower()
        assert creator_id is None

    def test_creator_already_in_db(self, monkeypatch):
        import db as db_module

        self._patch_supabase(
            monkeypatch,
            {
                db_module.CREATOR_TABLE: _make_supabase_resp(data=[{"id": "existing-uuid"}]),
            },
        )

        ok, msg, creator_id = db_module.queue_creator_add_request("@MrBeast", "user-1")
        assert ok is False
        assert "already in the database" in msg.lower()
        assert creator_id == "existing-uuid"

    def test_duplicate_pending_job(self, monkeypatch):
        import db as db_module

        # creators table = empty (not in DB), jobs table = has a pending row
        creator_resp = _make_supabase_resp(data=[])

        def make_table(name):
            if name == db_module.CREATOR_TABLE:
                return _chainable(creator_resp)
            # CREATOR_SYNC_JOBS_TABLE — first call is the duplicate check
            return _chainable(_make_supabase_resp(data=[{"id": 99}]))

        fake_client = SimpleNamespace(table=make_table)
        monkeypatch.setattr(db_module, "supabase_client", fake_client)

        ok, msg, creator_id = db_module.queue_creator_add_request("@channel", "user-1")
        assert ok is False
        assert "already pending" in msg.lower()
        assert creator_id is None

    def test_rate_limit_exceeded(self, monkeypatch):
        import db as db_module

        call_count = {"n": 0}

        def make_table(name):
            if name == db_module.CREATOR_TABLE:
                return _chainable(_make_supabase_resp(data=[]))
            # First jobs call = no duplicate, second = rate limit count
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _chainable(_make_supabase_resp(data=[]))  # no dup
            # Rate-limit query returns count=10 (> limit of 5)
            return _chainable(_make_supabase_resp(data=[], count=10))

        fake_client = SimpleNamespace(table=make_table)
        monkeypatch.setattr(db_module, "supabase_client", fake_client)
        monkeypatch.setattr(db_module, "_ADD_REQUEST_LIMIT", 5)

        ok, msg, creator_id = db_module.queue_creator_add_request("@channel", "user-1")
        assert ok is False
        assert "submit" in msg.lower() or "requests" in msg.lower()
        assert creator_id is None

    def test_success_queues_job(self, monkeypatch):
        import db as db_module

        inserted_job = [{"id": 42}]
        call_count = {"n": 0}

        def make_table(name):
            if name == db_module.CREATOR_TABLE:
                return _chainable(_make_supabase_resp(data=[]))  # not in DB
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _chainable(_make_supabase_resp(data=[]))  # no dup
            if call_count["n"] == 2:
                return _chainable(_make_supabase_resp(data=[], count=0))  # under limit
            return _chainable(_make_supabase_resp(data=inserted_job))  # insert

        fake_client = SimpleNamespace(table=make_table)
        monkeypatch.setattr(db_module, "supabase_client", fake_client)
        monkeypatch.setattr(db_module, "_ADD_REQUEST_LIMIT", 5)

        ok, msg, creator_id = db_module.queue_creator_add_request(
            "UCX6OQ3DkcsbYNE6H8uQQuVA", "user-1"
        )
        assert ok is True
        assert msg == "queued"
        assert creator_id is None

    def test_no_supabase_client(self, monkeypatch):
        import db as db_module

        monkeypatch.setattr(db_module, "supabase_client", None)

        ok, msg, creator_id = db_module.queue_creator_add_request("@MrBeast", "user-1")
        assert ok is False
        assert "unavailable" in msg.lower()
        assert creator_id is None


# ===========================================================================
# 3. get_creator_add_request_status
# ===========================================================================


class TestGetCreatorAddRequestStatus:
    """Unit tests for db.get_creator_add_request_status."""

    def _patch_jobs_table(self, monkeypatch, row):
        import db as db_module

        fake_client = SimpleNamespace(
            table=lambda _: _chainable(_make_supabase_resp(data=[row] if row else []))
        )
        monkeypatch.setattr(db_module, "supabase_client", fake_client)

    def test_completed_when_creator_id_set(self, monkeypatch):
        import db as db_module

        self._patch_jobs_table(monkeypatch, {"status": "completed", "creator_id": "uuid-123"})

        result = db_module.get_creator_add_request_status("@mrbeast")
        assert result is not None
        assert result["status"] == "completed"
        assert result["creator_id"] == "uuid-123"

    def test_failed_when_status_is_failed(self, monkeypatch):
        import db as db_module

        self._patch_jobs_table(monkeypatch, {"status": "failed", "creator_id": None})

        result = db_module.get_creator_add_request_status("@mrbeast")
        assert result is not None
        assert result["status"] == "failed"
        assert result["creator_id"] is None

    def test_processing_when_pending(self, monkeypatch):
        import db as db_module

        self._patch_jobs_table(monkeypatch, {"status": "pending", "creator_id": None})

        result = db_module.get_creator_add_request_status("@mrbeast")
        assert result is not None
        assert result["status"] == "processing"

    def test_processing_when_processing(self, monkeypatch):
        import db as db_module

        self._patch_jobs_table(monkeypatch, {"status": "processing", "creator_id": None})

        result = db_module.get_creator_add_request_status("@mrbeast")
        assert result["status"] == "processing"

    def test_no_job_row_returns_processing(self, monkeypatch):
        """Empty result set = timing race right after submission — should keep polling."""
        import db as db_module

        self._patch_jobs_table(monkeypatch, None)

        result = db_module.get_creator_add_request_status("@mrbeast")
        assert result is not None
        assert result["status"] == "processing"

    def test_returns_none_for_invalid_input(self, monkeypatch):
        import db as db_module

        monkeypatch.setattr(db_module, "supabase_client", object())  # should not be reached

        result = db_module.get_creator_add_request_status("https://youtube.com")
        assert result is None

    def test_returns_none_when_no_client(self, monkeypatch):
        import db as db_module

        monkeypatch.setattr(db_module, "supabase_client", None)

        # No client — None means the query could not be attempted at all
        result = db_module.get_creator_add_request_status("@mrbeast")
        assert result is None


# ===========================================================================
# 4. handle_resolve_and_add_job  (worker)
# ===========================================================================


class TestHandleResolveAndAddJob:
    """Async unit tests for worker.creator_worker.handle_resolve_and_add_job."""

    def _mock_supabase(self, creators_data=None, insert_data=None):
        """Build a minimal fake supabase_client for the worker."""
        call_counts = {"select": 0, "insert": 0, "update": 0}

        def make_table(name):
            chain = MagicMock()

            def select_chain(*a, **kw):
                inner = MagicMock()
                inner.eq.return_value = inner
                inner.limit.return_value = inner
                inner.execute.return_value = _make_supabase_resp(data=creators_data or [])
                return inner

            def insert_chain(payload):
                inner = MagicMock()
                inner.execute.return_value = _make_supabase_resp(
                    data=insert_data or [{"id": "new-uuid"}]
                )
                return inner

            def update_chain(payload):
                inner = MagicMock()
                inner.eq.return_value = inner
                inner.execute.return_value = _make_supabase_resp(data=[{}])
                return inner

            chain.select.side_effect = select_chain
            chain.insert.side_effect = insert_chain
            chain.update.side_effect = update_chain
            return chain

        fake = SimpleNamespace(table=make_table)
        return fake

    @pytest.mark.asyncio
    async def test_uc_id_skips_resolution_and_inserts_stub(self, monkeypatch):
        import worker.creator_worker as cw

        fake_supabase = self._mock_supabase(creators_data=[], insert_data=[{"id": "stub-id"}])
        monkeypatch.setattr(cw, "supabase_client", fake_supabase)
        monkeypatch.setattr(cw, "mark_creator_sync_processing", lambda jid: None)
        monkeypatch.setattr(cw, "mark_creator_sync_failed", lambda jid, error=None: None)

        result = await cw.handle_resolve_and_add_job(
            job_id=1,
            input_query="UCX6OQ3DkcsbYNE6H8uQQuVA",
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_handle_resolved_and_stub_inserted(self, monkeypatch):
        import worker.creator_worker as cw

        fake_supabase = self._mock_supabase(creators_data=[], insert_data=[{"id": "stub-id"}])
        monkeypatch.setattr(cw, "supabase_client", fake_supabase)
        monkeypatch.setattr(cw, "mark_creator_sync_processing", lambda jid: None)
        monkeypatch.setattr(cw, "mark_creator_sync_failed", lambda jid, error=None: None)

        # Mock youtube_resolver (module-level variable used by handle_resolve_and_add_job)
        mock_resolver = AsyncMock()
        mock_resolver.resolve_handle_to_channel_id = AsyncMock(
            return_value="UCX6OQ3DkcsbYNE6H8uQQuVA"
        )
        monkeypatch.setattr(cw, "youtube_resolver", mock_resolver)

        result = await cw.handle_resolve_and_add_job(
            job_id=1,
            input_query="@MrBeast",
        )
        assert result is True
        mock_resolver.resolve_handle_to_channel_id.assert_awaited_once_with("MrBeast")

    @pytest.mark.asyncio
    async def test_creator_already_in_db_is_idempotent(self, monkeypatch):
        """If creator already exists, job should still be converted to sync_stats."""
        import worker.creator_worker as cw

        # creators table has a row → should skip insert and still convert job
        fake_supabase = self._mock_supabase(
            creators_data=[{"id": "existing-id"}],
        )
        updated_calls = []

        original_table = fake_supabase.table

        def table_with_spy(name):
            chain = original_table(name)
            original_update = chain.update

            def update_spy(payload):
                updated_calls.append(payload)
                return original_update(payload)

            chain.update = update_spy
            return chain

        fake_supabase.table = table_with_spy
        monkeypatch.setattr(cw, "supabase_client", fake_supabase)
        monkeypatch.setattr(cw, "mark_creator_sync_processing", lambda jid: None)
        monkeypatch.setattr(cw, "mark_creator_sync_failed", lambda jid, error=None: None)

        result = await cw.handle_resolve_and_add_job(
            job_id=1,
            input_query="UCX6OQ3DkcsbYNE6H8uQQuVA",
        )
        assert result is True
        # The update to sync_stats should have been called
        assert any(p.get("job_type") == "sync_stats" for p in updated_calls)

    @pytest.mark.asyncio
    async def test_channel_not_found_marks_failed(self, monkeypatch):
        import worker.creator_worker as cw

        fake_supabase = self._mock_supabase(creators_data=[])
        monkeypatch.setattr(cw, "supabase_client", fake_supabase)
        monkeypatch.setattr(cw, "mark_creator_sync_processing", lambda jid: None)

        failed_jobs = []
        monkeypatch.setattr(
            cw, "mark_creator_sync_failed", lambda jid, error=None: failed_jobs.append(jid)
        )

        mock_resolver = AsyncMock()
        mock_resolver.resolve_handle_to_channel_id = AsyncMock(return_value=None)
        monkeypatch.setattr(cw, "youtube_resolver", mock_resolver)

        result = await cw.handle_resolve_and_add_job(job_id=5, input_query="@ghost_channel")
        assert result is False
        assert 5 in failed_jobs

    @pytest.mark.asyncio
    async def test_quota_exception_is_reraised(self, monkeypatch):
        import worker.creator_worker as cw

        fake_supabase = self._mock_supabase(creators_data=[])
        monkeypatch.setattr(cw, "supabase_client", fake_supabase)
        monkeypatch.setattr(cw, "mark_creator_sync_processing", lambda jid: None)
        monkeypatch.setattr(cw, "mark_creator_sync_failed", lambda jid, error=None: None)

        mock_resolver = AsyncMock()
        mock_resolver.resolve_handle_to_channel_id = AsyncMock(
            side_effect=cw.QuotaExceededException("@MrBeast")
        )
        monkeypatch.setattr(cw, "youtube_resolver", mock_resolver)

        with pytest.raises(cw.QuotaExceededException):
            await cw.handle_resolve_and_add_job(job_id=7, input_query="@MrBeast")


# ===========================================================================
# 5. POST /creators/request  (endpoint)
# ===========================================================================


class TestCreatorsRequestEndpoint:
    """Integration tests for POST /creators/request."""

    @pytest.fixture
    def client(self):
        import main

        return TestClient(main.app)

    def test_unauthenticated_returns_error_partial(self, client):
        r = client.post("/creators/request", data={"q": "@MrBeast"})
        assert r.status_code == 200
        # Production: "You must be logged in".
        # TESTING=1: require_auth bypasses → route responds with an error
        # from the DB layer (supabase unavailable).  Either way the response
        # must NOT be a success card containing the add-status poll URL.
        assert "/creators/add-status" not in r.text

    def test_missing_q_returns_error_partial(self, authenticated_client):
        r = authenticated_client.post("/creators/request", data={"q": ""})
        assert r.status_code == 200
        assert "enter" in r.text.lower() or "handle" in r.text.lower()

    def test_invalid_format_returns_error_card(self, authenticated_client, monkeypatch):
        import routes.creators as rc

        monkeypatch.setattr(
            rc,
            "queue_creator_add_request",
            lambda q, uid: (False, "Invalid input — please enter a YouTube @handle.", None),
        )
        r = authenticated_client.post("/creators/request", data={"q": "https://youtube.com"})
        assert r.status_code == 200
        assert "invalid" in r.text.lower() or "error" in r.text.lower()

    def test_success_returns_queued_card_with_htmx_poll(self, authenticated_client, monkeypatch):
        import routes.creators as rc

        monkeypatch.setattr(
            rc,
            "queue_creator_add_request",
            lambda q, uid: (True, "queued", None),
        )
        r = authenticated_client.post("/creators/request", data={"q": "@MrBeast"})
        assert r.status_code == 200
        assert "queued" in r.text.lower()
        # Success card must embed the polling URL
        assert "/creators/add-status" in r.text

    def test_already_in_db_returns_profile_link(self, authenticated_client, monkeypatch):
        import routes.creators as rc

        monkeypatch.setattr(
            rc,
            "queue_creator_add_request",
            lambda q, uid: (False, "This creator is already in the database.", "existing-uuid"),
        )
        r = authenticated_client.post("/creators/request", data={"q": "@MrBeast"})
        assert r.status_code == 200
        assert "/creator/existing-uuid" in r.text


# ===========================================================================
# 6. GET /creators/add-status  (endpoint)
# ===========================================================================


class TestCreatorsAddStatusEndpoint:
    """Integration tests for GET /creators/add-status."""

    def test_unauthenticated_returns_failed_partial(self):
        import main

        client = TestClient(main.app)
        r = client.get("/creators/add-status?q=@MrBeast")
        assert r.status_code == 200
        # Unauthenticated → failed card (no profile link, no spinner)
        assert "creator/existing" not in r.text

    def test_missing_q_returns_failed_partial(self, authenticated_client):
        r = authenticated_client.get("/creators/add-status")
        assert r.status_code == 200
        assert "processing" not in r.text.lower()

    def test_processing_returns_spinner_with_poll(self, authenticated_client, monkeypatch):
        import routes.creators as creators_routes

        monkeypatch.setattr(
            creators_routes,
            "get_creator_add_request_status",
            lambda q: {"status": "processing", "creator_id": None},
        )
        r = authenticated_client.get("/creators/add-status?q=@MrBeast")
        assert r.status_code == 200
        assert "processing" in r.text.lower() or "every 3s" in r.text
        # Card must re-attach the poll directive
        assert "/creators/add-status" in r.text

    def test_completed_returns_profile_link_and_stops_polling(
        self, authenticated_client, monkeypatch
    ):
        import routes.creators as creators_routes

        monkeypatch.setattr(
            creators_routes,
            "get_creator_add_request_status",
            lambda q: {"status": "completed", "creator_id": "new-creator-uuid"},
        )
        r = authenticated_client.get("/creators/add-status?q=@MrBeast")
        assert r.status_code == 200
        assert "/creator/new-creator-uuid" in r.text
        # Completed card must NOT contain a poll trigger (stops polling)
        assert "every 3s" not in r.text

    def test_failed_returns_error_card_and_stops_polling(self, authenticated_client, monkeypatch):
        import routes.creators as creators_routes

        monkeypatch.setattr(
            creators_routes,
            "get_creator_add_request_status",
            lambda q: {"status": "failed", "creator_id": None},
        )
        r = authenticated_client.get("/creators/add-status?q=@MrBeast")
        assert r.status_code == 200
        # Should surface an error message
        assert (
            "couldn't find" in r.text.lower()
            or "error" in r.text.lower()
            or "failed" in r.text.lower()
        )
        assert "every 3s" not in r.text

    def test_none_result_renders_failed_terminal_card(self, authenticated_client, monkeypatch):
        """
        None from get_creator_add_request_status means invalid input or no DB
        client.  The route must render a terminal failed card so polling stops.
        """
        import routes.creators as creators_routes

        monkeypatch.setattr(
            creators_routes,
            "get_creator_add_request_status",
            lambda q: None,
        )
        r = authenticated_client.get("/creators/add-status?q=@MrBeast")
        assert r.status_code == 200
        # Terminal state — must NOT include a polling trigger
        assert "every 3s" not in r.text
