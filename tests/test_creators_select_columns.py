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

from db import _CREATORS_LIST_COLUMNS


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
