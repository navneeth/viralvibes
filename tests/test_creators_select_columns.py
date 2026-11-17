"""Regression tests for db._CREATORS_LIST_COLUMNS.

Locks in the explicit column list used by get_creators() so a caller that
starts reading a new field is caught at test time instead of silently
returning None (or worse, breaking the card render in production).

Rules:
- Every field name below MUST appear in _CREATORS_LIST_COLUMNS.
- The list MUST NOT contain "*" — that would defeat the payload reduction.
- The list MUST NOT contain duplicates.

When adding a caller that reads a NEW column, add both:
  1. the column to db._CREATORS_LIST_COLUMNS, and
  2. the column to REQUIRED_COLUMNS below.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from db import _CREATORS_LIST_COLUMNS
from db import _PEER_CREATOR_FIELDS_FOR_DISPLAY, _PEER_CREATOR_FIELDS_WITH_CONTACT

# Every column the view / route / util layers were observed reading from
# get_creators() output (via safe_get_value(creator, ...), creator.get(...),
# creator[...]) at PR-plan time.  Sourced from a systematic grep of
# views/**/*.py, routes/**/*.py, utils/**/*.py.
REQUIRED_COLUMNS = frozenset(
    {
        # Identity / routing
        "id",
        "channel_id",
        "custom_url",
        # Display
        "channel_name",
        "channel_url",
        "channel_thumbnail_url",
        "banner_image_url",
        "channel_description",
        "keywords",
        # Categorisation
        "primary_category",
        "topic_categories",
        "country_code",
        "default_language",
        # Stats
        "current_subscribers",
        "current_view_count",
        "current_video_count",
        "subscribers_change_30d",
        "views_change_30d",
        "videos_change_30d",
        "growth_window_days",
        # Engagement / quality
        "engagement_score",
        "quality_grade",
        "monthly_uploads",
        "avg_views_10",
        "avg_days_between_uploads",
        # Age / time
        "channel_age_days",
        "published_at",
        "last_updated_at",
        "last_synced_at",
        # Flags
        "is_made_for_kids",
        "has_long_upload_status",
        "hidden_subscriber_count",
        "official",
        # Bookkeeping
        "sync_status",
    }
)


def _split_columns(select_string: str) -> list[str]:
    return [c.strip() for c in select_string.split(",") if c.strip()]


def test_peer_export_projection_includes_topic_provenance():
    columns = set(_split_columns(_PEER_CREATOR_FIELDS_WITH_CONTACT))
    assert {
        "transcript_keywords",
        "transcript_keywords_metadata",
        "transcript_keywords_updated_at",
    } <= columns


@pytest.mark.parametrize(
    "projection", [_PEER_CREATOR_FIELDS_FOR_DISPLAY, _PEER_CREATOR_FIELDS_WITH_CONTACT]
)
def test_peer_projections_include_shared_revenue_inputs(projection):
    assert {
        "current_subscribers",
        "current_view_count",
        "current_video_count",
        "views_change_30d",
        "growth_window_days",
        "channel_age_days",
        "topic_categories",
        "primary_category",
        "is_made_for_kids",
        "hidden_subscriber_count",
        "sync_status",
        "country_code",
    } <= set(_split_columns(projection))


@pytest.mark.parametrize(
    "message",
    [
        "column creators.growth_window_days does not exist",
        "PGRST204 Could not find the 'growth_window_days' column in the schema cache",
    ],
)
def test_listing_retries_only_without_missing_growth_window(monkeypatch, message):
    import db

    calls = []

    class Query:
        @property
        def not_(self):
            return self

        def __getattr__(self, name):
            def chained(*args, **kwargs):
                calls[-1].setdefault("filters", []).append((name, args, kwargs))
                return self

            return chained

        def execute(self):
            if len(calls) == 1:
                raise Exception(message)
            return SimpleNamespace(data=[{"id": "creator-1", "current_subscribers": 10}], count=23)

    def select(columns, **kwargs):
        calls.append({"columns": columns, "select_options": kwargs})
        return Query()

    client = MagicMock()
    client.table.return_value.select.side_effect = select
    monkeypatch.setattr(db, "supabase_client", client)
    monkeypatch.setattr(
        db, "_db_execute_readonly_without_read_timeout", lambda operation: operation()
    )

    result = db.get_creators(
        country_filter="US", offset=20, limit=20, return_count=True, count_strategy="exact"
    )

    assert len(calls) == 2
    assert "growth_window_days" in _split_columns(calls[0]["columns"])
    assert "growth_window_days" not in _split_columns(calls[1]["columns"])
    assert set(_split_columns(calls[0]["columns"])) - {"growth_window_days"} == set(
        _split_columns(calls[1]["columns"])
    )
    assert calls[0]["select_options"] == calls[1]["select_options"] == {"count": "exact"}
    assert calls[0]["filters"] == calls[1]["filters"]
    assert result.total_count == 23
    assert result.creators[0]["id"] == "creator-1"


@pytest.mark.parametrize(
    "message",
    [
        "57014 statement timeout while selecting growth_window_days",
        "42703 column creators.other_field does not exist",
        "permission denied for growth_window_days",
    ],
)
def test_unrelated_errors_do_not_trigger_growth_window_retry(message):
    import db

    assert not db._is_missing_growth_window_error(Exception(message))


@pytest.mark.parametrize("include_contacts", [False, True])
def test_peer_hydration_retry_preserves_ranking_and_projection(monkeypatch, include_contacts):
    import db

    peer_query = MagicMock()
    for method in ("select", "eq", "limit"):
        getattr(peer_query, method).return_value = peer_query
    peer_query.execute.return_value = SimpleNamespace(data=[{"peer_list": ["first", "second"]}])
    projections = []
    hydration_queries = []

    def select(columns):
        projections.append(columns)
        query = MagicMock()
        query.in_.return_value = query
        if "growth_window_days" in _split_columns(columns):
            query.execute.side_effect = Exception("42703 column growth_window_days does not exist")
        else:
            query.execute.return_value = SimpleNamespace(data=[{"id": "second"}, {"id": "first"}])
        hydration_queries.append(query)
        return query

    creators_table = MagicMock()
    creators_table.select.side_effect = select
    client = MagicMock()
    client.table.side_effect = lambda name: (
        peer_query if name == "creator_peers" else creators_table
    )
    monkeypatch.setattr(db, "supabase_client", client)
    monkeypatch.setattr(db, "_db_execute", lambda operation: operation())

    creators, total = db.get_embedding_peers("seed", include_contacts=include_contacts)

    assert [creator["id"] for creator in creators] == ["first", "second"]
    assert total == 2
    assert len(projections) == 2
    assert set(_split_columns(projections[0])) - {"growth_window_days"} == set(
        _split_columns(projections[1])
    )
    assert ("extracted_email" in _split_columns(projections[1])) is include_contacts
    for query in hydration_queries:
        query.in_.assert_called_once_with("id", ["first", "second"])


def test_column_list_has_no_wildcard():
    assert "*" not in _CREATORS_LIST_COLUMNS


def test_column_list_has_no_whitespace_in_field_names():
    for col in _split_columns(_CREATORS_LIST_COLUMNS):
        assert " " not in col, f"field {col!r} contains whitespace"


def test_column_list_has_no_duplicates():
    cols = _split_columns(_CREATORS_LIST_COLUMNS)
    duplicates = {c for c in cols if cols.count(c) > 1}
    assert not duplicates, f"duplicate columns in _CREATORS_LIST_COLUMNS: {duplicates}"


def test_column_list_contains_every_required_field():
    cols = set(_split_columns(_CREATORS_LIST_COLUMNS))
    missing = REQUIRED_COLUMNS - cols
    assert not missing, (
        f"_CREATORS_LIST_COLUMNS is missing fields that the view code reads: "
        f"{sorted(missing)}. Add them to db._CREATORS_LIST_COLUMNS."
    )


def test_column_list_starts_with_id():
    """Convention: put identity fields first so the response payload's most
    important columns show up at the start of any JSON dump.
    """
    cols = _split_columns(_CREATORS_LIST_COLUMNS)
    assert cols[0] == "id"
