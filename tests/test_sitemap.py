"""Unit tests for services.sitemap.build_sitemap_xml.

These tests exercise the XML-building logic in isolation — no Supabase
connection required. The A+ lookalike path was previously untested in CI
because generate_sitemap.py falls back to empty lists when DB creds are absent.
"""

import xml.etree.ElementTree as ET

from services.rankings import iter_ranking_sitemap_paths
from services.sitemap import STATIC_ROUTES, build_sitemap_xml

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

    def test_creator_urls_included(self):
        creators = [
            {"id": "abc123", "last_updated_at": "2026-01-01T00:00:00+00:00"},
            {"id": "def456", "last_updated_at": "2026-06-15T12:00:00+00:00"},
        ]
        locs = _parse_locs(build_sitemap_xml(creators))
        assert any("/creator/abc123" in loc for loc in locs)
        assert any("/creator/def456" in loc for loc in locs)

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
