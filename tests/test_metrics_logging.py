"""Regression tests for _log_get_creators_metrics format.

The format is grep/parse-friendly:
    [Metrics] op=get_creators req_id=... status=... dur_ms=... columns=* sort=...
              limit=... offset=... return_count=1|0 rows=... [total=N] [degraded=1]
              [search="q"] [grade=A+] [lang=en] [activity=active] [age=new]
              [country=US] [category="Name"]

Only non-default filters are included so most lines stay short.
"""

import logging

import pytest

from db import _log_get_creators_metrics, _METRICS_PREFIX


@pytest.fixture
def metrics_line(caplog):
    """Return the last INFO record emitted by _log_get_creators_metrics."""

    def _run(**kwargs):
        caplog.clear()
        with caplog.at_level(logging.INFO, logger="vv_db"):
            _log_get_creators_metrics(**kwargs)
        info_lines = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
        assert info_lines, "expected exactly one INFO metrics line"
        assert len(info_lines) == 1
        return info_lines[0]

    return _run


_BASE = dict(
    req_id="abcd1234",
    status="ok",
    duration_ms=125,
    sort="views",
    limit=50,
    offset=0,
    return_count=True,
    search="",
    grade_filter="all",
    language_filter="all",
    activity_filter="all",
    age_filter="all",
    country_filter="all",
    category_filter="all",
    rows=50,
    total_count=1234,
)


def test_metrics_prefix_and_core_fields(metrics_line):
    line = metrics_line(**_BASE)
    assert line.startswith(_METRICS_PREFIX + " ")
    assert "op=get_creators" in line
    assert "req_id=abcd1234" in line
    assert "status=ok" in line
    assert "dur_ms=125" in line
    assert "columns=*" in line
    assert "sort=views" in line
    assert "limit=50" in line
    assert "offset=0" in line
    assert "return_count=1" in line
    assert "rows=50" in line
    assert "total=1234" in line


def test_metrics_omits_default_filters(metrics_line):
    """All 'all' filters and empty search MUST be absent from the line."""
    line = metrics_line(**_BASE)
    for skip in ("grade=", "lang=", "activity=", "age=", "country=", "category=", "search="):
        assert skip not in line, f"unexpected default field: {skip!r} in {line!r}"


def test_metrics_includes_non_default_filters(metrics_line):
    line = metrics_line(
        **{
            **_BASE,
            "grade_filter": "A+",
            "country_filter": "US",
            "category_filter": "Video game culture",
            "search": "mrbeast",
        }
    )
    assert "grade=A+" in line
    assert "country=US" in line
    assert 'category="Video game culture"' in line
    assert 'search="mrbeast"' in line


def test_metrics_degraded_flag(metrics_line):
    line = metrics_line(**{**_BASE, "status": "timeout_57014", "degraded": True})
    assert "status=timeout_57014" in line
    assert "degraded=1" in line


def test_metrics_return_count_false(metrics_line):
    line = metrics_line(**{**_BASE, "return_count": False, "total_count": None})
    assert "return_count=0" in line
    assert "total=" not in line


def test_metrics_truncates_long_search(metrics_line):
    long_query = "a" * 200
    line = metrics_line(**{**_BASE, "search": long_query})
    # 64-char cap plus quotes; line should not contain the 65th 'a'
    assert 'search="' + ("a" * 64) + '"' in line
    assert "a" * 65 not in line


def test_metrics_escapes_quotes_in_search(metrics_line):
    line = metrics_line(**{**_BASE, "search": 'evil"quote'})
    # The embedded quote must be escaped so the field stays parseable.
    assert 'search="evil\\"quote"' in line


def test_metrics_escapes_newlines_and_tabs_in_search(metrics_line):
    """Newlines in user input MUST NOT split the metric line into two log records."""
    line = metrics_line(**{**_BASE, "search": "foo\nbar\rbaz\tqux"})
    # No literal newline / CR / tab may appear anywhere in the emitted line.
    assert "\n" not in line
    assert "\r" not in line
    assert "\t" not in line
    assert 'search="foo\\nbar\\rbaz\\tqux"' in line


def test_metrics_escapes_backslash_before_quote(metrics_line):
    """Backslash MUST be escaped first, otherwise later quote escapes get corrupted."""
    line = metrics_line(**{**_BASE, "search": 'weird\\"stuff'})
    # After escaping: \\ becomes \\\\, then " becomes \" — final string is \\\\\"
    assert 'search="weird\\\\\\"stuff"' in line


def test_metrics_escapes_control_chars_in_category(metrics_line):
    """Category values also go through the escaper (same class of user-like input)."""
    line = metrics_line(**{**_BASE, "category_filter": "Music\nRock"})
    assert "\n" not in line
    assert 'category="Music\\nRock"' in line
