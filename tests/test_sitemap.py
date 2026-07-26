"""Unit tests for services.sitemap.build_sitemap_xml.

These tests exercise the XML-building logic in isolation — no Supabase
connection required. The A+ lookalike path was previously untested in CI
because generate_sitemap.py falls back to empty lists when DB creds are absent.
"""

import xml.etree.ElementTree as ET

from services.rankings import iter_ranking_sitemap_paths
from services.sitemap import STATIC_ROUTES, build_sitemap_xml, fetch_synced_creators

_NS = "http://www.sitemaps.org/schemas/sitemap/0.9"


def _parse_locs(xml: str) -> list[str]:
    root = ET.fromstring(xml)
    return [el.text for el in root.findall(f".//{{{_NS}}}url/{{{_NS}}}loc")]


class TestBuildSitemapXml:
    def test_static_routes_only_when_no_creators(self):
        xml = build_sitemap_xml([])
        locs = _parse_locs(xml)
        assert len(locs) == len(STATIC_ROUTES) + len(iter_ranking_sitemap_paths())

    def test_programmatic_ranking_urls_included(self):
        locs = _parse_locs(build_sitemap_xml([]))
        assert "https://www.viralvibes.fyi/rankings/gaming/united-states" in locs

    def test_creator_urls_included_fallback_to_uuid(self):
        """Creators without custom_url fall back to /creator/{id} entries."""
        creators = [
            {"id": "abc123", "last_updated_at": "2026-01-01T00:00:00+00:00"},
            {"id": "def456", "last_updated_at": "2026-06-15T12:00:00+00:00"},
        ]
        locs = _parse_locs(build_sitemap_xml(creators))
        assert any("/creator/abc123" in loc for loc in locs)
        assert any("/creator/def456" in loc for loc in locs)

    def test_creator_with_handle_uses_handle_url(self):
        """Creators with custom_url must emit /creators/@handle, not /creator/{id}."""
        creators = [
            {"id": "abc123", "custom_url": "mychannel", "last_updated_at": "2026-01-01"},
            {"id": "def456", "custom_url": "@another", "last_updated_at": "2026-01-01"},
        ]
        locs = _parse_locs(build_sitemap_xml(creators))
        assert any("/creators/@mychannel" in loc for loc in locs)
        assert any("/creators/@another" in loc for loc in locs)
        # UUID form must NOT appear — would confuse Google's canonical election
        assert not any("/creator/abc123" in loc for loc in locs)
        assert not any("/creator/def456" in loc for loc in locs)

    def test_creator_mixed_handle_and_no_handle(self):
        """Mixed batch: handle → @handle URL, no-handle → UUID fallback."""
        creators = [
            {"id": "uuid-1", "custom_url": "withhandle", "last_updated_at": "2026-01-01"},
            {"id": "uuid-2", "custom_url": None, "last_updated_at": "2026-01-01"},
        ]
        locs = _parse_locs(build_sitemap_xml(creators))
        assert any("/creators/@withhandle" in loc for loc in locs)
        assert any("/creator/uuid-2" in loc for loc in locs)
        assert not any("/creator/uuid-1" in loc for loc in locs)

    def test_aplus_lookalike_urls_included(self):
        """A+ creator handles produce /creators/like/{handle} entries."""
        aplus = [
            {"custom_url": "@mrbeast", "last_updated_at": "2026-01-01T00:00:00+00:00"},
            {"custom_url": "pewdiepie", "last_updated_at": "2026-01-01T00:00:00+00:00"},
        ]
        locs = _parse_locs(build_sitemap_xml([], aplus_creators=aplus))
        assert any("/creators/like/mrbeast" in loc for loc in locs)
        assert any("/creators/like/pewdiepie" in loc for loc in locs)

    def test_aplus_at_prefix_stripped(self):
        """Leading '@' in custom_url must be stripped from the URL slug."""
        aplus = [{"custom_url": "@MrBeast", "last_updated_at": None}]
        locs = _parse_locs(build_sitemap_xml([], aplus_creators=aplus))
        assert any("/creators/like/mrbeast" in loc for loc in locs)
        assert not any("@" in loc for loc in locs)

    def test_aplus_null_handle_skipped(self):
        """Entries with missing or empty custom_url must be silently skipped."""
        aplus = [
            {"custom_url": None, "last_updated_at": "2026-01-01T00:00:00+00:00"},
            {"custom_url": "", "last_updated_at": "2026-01-01T00:00:00+00:00"},
        ]
        xml = build_sitemap_xml([], aplus_creators=aplus)
        locs = _parse_locs(xml)
        assert len(locs) == len(STATIC_ROUTES) + len(iter_ranking_sitemap_paths())

    def test_creator_missing_id_skipped(self):
        """Creator rows without an 'id' key must not produce a URL entry."""
        creators = [{"last_updated_at": "2026-01-01T00:00:00+00:00"}]
        locs = _parse_locs(build_sitemap_xml(creators))
        assert len(locs) == len(STATIC_ROUTES) + len(iter_ranking_sitemap_paths())

    def test_total_url_count(self):
        creators = [{"id": f"id-{i}", "last_updated_at": "2026-01-01"} for i in range(5)]
        aplus = [{"custom_url": f"handle{i}", "last_updated_at": "2026-01-01"} for i in range(3)]
        locs = _parse_locs(build_sitemap_xml(creators, aplus_creators=aplus))
        assert len(locs) == len(STATIC_ROUTES) + len(iter_ranking_sitemap_paths()) + 5 + 3

    def test_output_is_valid_xml(self):
        """Mixed input must produce well-formed XML that ET can parse."""
        creators = [{"id": "test-id", "last_updated_at": None}]
        aplus = [{"custom_url": "@handle", "last_updated_at": "2026-06-01"}]
        xml = build_sitemap_xml(creators, aplus_creators=aplus)
        ET.fromstring(xml)  # raises if not well-formed


class TestFetchSyncedCreators:
    """Contract tests for fetch_synced_creators — no real Supabase connection needed."""

    def _make_client(self, rows):
        """Return a fake Supabase client that records the SELECT projection."""
        recorded = {}

        class _Exe:
            data = rows

        class _Query:
            def select(self_, cols):
                recorded["cols"] = cols
                return self_

            def eq(self_, *_):
                return self_

            def execute(self_):
                return _Exe()

        class _Client:
            def table(self_, *_):
                return _Query()

        return _Client(), recorded

    def test_select_projection_includes_custom_url(self):
        """The SELECT must include custom_url — without it all creators silently
        fall back to UUID URLs in the sitemap, defeating the handle migration."""
        client, recorded = self._make_client([])
        fetch_synced_creators(client)
        assert "custom_url" in recorded["cols"], (
            f"fetch_synced_creators projected {recorded['cols']!r}; "
            "expected 'custom_url' to be present"
        )

    def test_returned_rows_retain_custom_url(self):
        """Rows returned by the client must pass through with custom_url intact."""
        rows = [
            {"id": "u1", "custom_url": "mrbeast", "last_updated_at": "2026-01-01"},
            {"id": "u2", "custom_url": None, "last_updated_at": "2026-01-01"},
        ]
        client, _ = self._make_client(rows)
        result = fetch_synced_creators(client)
        assert result[0]["custom_url"] == "mrbeast"
        assert result[1]["custom_url"] is None

    def test_exception_returns_empty_list(self):
        """Any DB error must be swallowed and return []."""

        class _BadClient:
            def table(self_, *_):
                raise RuntimeError("connection refused")

        assert fetch_synced_creators(_BadClient()) == []
