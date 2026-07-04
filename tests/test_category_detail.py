"""Tests for /lists/category/{slug} slug resolution and topic category queries."""

import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

supabase_stub = types.ModuleType("supabase")
supabase_stub.Client = object
supabase_stub.create_client = lambda *args, **kwargs: None
sys.modules.setdefault("supabase", supabase_stub)

monsterui_pkg = types.ModuleType("monsterui")
monsterui_all = types.ModuleType("monsterui.all")
monsterui_pkg.all = monsterui_all
sys.modules.setdefault("monsterui", monsterui_pkg)
sys.modules.setdefault("monsterui.all", monsterui_all)

db_stub = types.ModuleType("db")
db_stub.get_creators = lambda *args, **kwargs: None
db_stub.get_user_favourite_list_keys = lambda *args, **kwargs: frozenset()
sys.modules.setdefault("db", db_stub)

views_lists_stub = types.ModuleType("views.lists")
for _name in [
    "render_lists_page",
    "render_more_categories",
    "render_more_countries",
    "render_more_languages",
    "render_categories_explorer_page",
    "render_countries_explorer_page",
    "render_languages_explorer_page",
    "render_country_detail_page",
    "render_country_creators_rows",
    "render_category_detail_page",
    "render_category_creators_rows",
    "render_language_detail_page",
    "render_language_creators_rows",
]:
    setattr(views_lists_stub, _name, lambda *args, **kwargs: None)
views_lists_stub._unslugify = lambda slug: slug.replace("-", " ").title()
views_lists_stub._list_heart_btn = lambda *args, **kwargs: None
sys.modules.setdefault("views.lists", views_lists_stub)

import db_lists
import routes.lists as lists_routes


class _FakeQuery:
    def __init__(self, client, call):
        self._client = client
        self._call = call

    @property
    def not_(self):
        return self

    def select(self, *args, **kwargs):
        self._call["select"] = {"args": args, "kwargs": kwargs}
        return self

    def filter(self, *args):
        self._call["filter"] = args
        return self

    def eq(self, *args):
        self._call.setdefault("eq", []).append(args)
        return self

    def is_(self, *args):
        self._call.setdefault("is", []).append(args)
        return self

    def gt(self, *args):
        self._call.setdefault("gt", []).append(args)
        return self

    def ilike(self, *args):
        self._call["ilike"] = args
        return self

    def order(self, *args, **kwargs):
        self._call["order"] = {"args": args, "kwargs": kwargs}
        return self

    def limit(self, limit):
        self._call["limit"] = limit
        return self

    def offset(self, offset):
        self._call["offset"] = offset
        return self

    def execute(self):
        result = self._client.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return SimpleNamespace(**result)


class _FakeSupabaseClient:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def table(self, name):
        call = {"table": name}
        self.calls.append(call)
        return _FakeQuery(self, call)


def test_topic_category_ilike_term_wikipedia_url():
    term = db_lists._topic_category_ilike_term(
        "https://en.wikipedia.org/wiki/Lifestyle (sociology)"
    )
    assert term == "Lifestyle_(sociology)"


def test_topic_category_ilike_term_handles_query_fragment_and_encoding():
    term = db_lists._topic_category_ilike_term(
        "https://en.wikipedia.org/wiki/Lifestyle_%28sociology%29?utm=foo#bar"
    )
    assert term == "Lifestyle_(sociology)"


def test_topic_category_ilike_term_empty_input_returns_empty_string():
    assert db_lists._topic_category_ilike_term("") == ""
    assert db_lists._topic_category_ilike_term(None) == ""


def test_topic_category_ilike_term_plain_name():
    assert db_lists._topic_category_ilike_term("Music") == "Music"


def test_topic_category_jsonb_value_builds_wikipedia_url():
    assert (
        db_lists._topic_category_jsonb_value("Lifestyle (sociology)")
        == "https://en.wikipedia.org/wiki/Lifestyle_(sociology)"
    )
    assert (
        db_lists._topic_category_jsonb_value("https://en.wikipedia.org/wiki/Video_game_culture")
        == "https://en.wikipedia.org/wiki/Video_game_culture"
    )


def test_topic_category_jsonb_value_non_catalog_label_returns_empty():
    assert db_lists._topic_category_jsonb_value("Totally New Niche") == ""


def test_resolve_category_slug_matches_slugify_catalogue(monkeypatch):
    monkeypatch.setattr(
        db_lists,
        "_get_category_slug_map",
        lambda: {
            "lifestyle-sociology": "Lifestyle (sociology)",
            "music": "Music",
        },
    )
    assert db_lists.resolve_category_slug("lifestyle-sociology") == "Lifestyle (sociology)"
    assert db_lists.resolve_category_slug("music") == "Music"
    assert db_lists.resolve_category_slug("unknown-niche") == "Unknown niche"


def test_resolve_category_slug_handles_wikipedia_url_input():
    assert (
        db_lists.resolve_category_slug("https://en.wikipedia.org/wiki/Video_game_culture")
        == "Video game culture"
    )


def test_get_category_slug_map_learns_observed_non_fixed_labels(monkeypatch):
    monkeypatch.setattr(
        db_lists,
        "_fetch_top_counts",
        lambda *args, **kwargs: [("Unexpected category", 7)],
    )
    monkeypatch.setattr(db_lists, "_category_slug_cache", {"expires_at": 0.0, "map": {}})

    slug_map = db_lists._get_category_slug_map()

    assert slug_map["unexpected-category"] == "Unexpected category"


def test_get_topic_category_creators_empty_category_returns_empty(monkeypatch):
    monkeypatch.setattr(
        db_lists,
        "_get_supabase_client",
        lambda: (_ for _ in ()).throw(AssertionError("client should not be used")),
    )

    result = db_lists.get_topic_category_creators("   ", return_count=True)

    assert result.creators == []
    assert result.total_count == 0


def test_get_topic_category_creators_always_uses_ilike_directly(monkeypatch):
    # cs. containment filter always fails on the text column; the function
    # now skips the exact attempt and goes straight to the ilike path.
    fake_client = _FakeSupabaseClient(
        {
            "data": [{"channel_name": "Fallback Channel", "current_subscribers": 123}],
            "count": 1,
        },
    )
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: fake_client)

    result = db_lists.get_topic_category_creators(
        "Lifestyle (sociology)",
        limit=10,
        return_count=True,
    )

    assert result.total_count == 1
    assert result.creators[0]["channel_name"] == "Fallback Channel"
    assert len(fake_client.calls) == 1
    assert "ilike" in fake_client.calls[0]
    assert "filter" not in fake_client.calls[0]


def test_get_topic_category_creators_ilike_returns_count_and_creators(monkeypatch):
    fake_client = _FakeSupabaseClient(
        {
            "data": [{"channel_name": "Legacy Match", "current_subscribers": 456}],
            "count": 1,
        },
    )
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: fake_client)

    result = db_lists.get_topic_category_creators("Lifestyle (sociology)", return_count=True)

    assert result.total_count == 1
    assert result.creators[0]["channel_name"] == "Legacy Match"
    assert len(fake_client.calls) == 1
    assert "ilike" in fake_client.calls[0]


def test_get_topic_category_creators_applies_offset_to_rank(monkeypatch):
    fake_client = _FakeSupabaseClient(
        {
            "data": [
                {"channel_name": "Ranked Channel 1", "current_subscribers": 100},
                {"channel_name": "Ranked Channel 2", "current_subscribers": 90},
            ],
            "count": 2,
        }
    )
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: fake_client)

    offset = 10
    result = db_lists.get_topic_category_creators(
        "Video game culture",
        limit=2,
        offset=offset,
        return_count=True,
    )

    assert result.total_count == 2
    assert result.creators[0]["_rank"] == 11
    assert result.creators[1]["_rank"] == 12


def test_fetch_category_page_uses_topic_categories_not_primary_category(monkeypatch):
    page_result = db_lists.TopicCategoryPageResult(
        creators=[{"channel_name": "Test Channel", "current_subscribers": 1000}],
        total_count=42,
    )

    monkeypatch.setattr(lists_routes, "resolve_category_slug", lambda slug: "Lifestyle (sociology)")
    mock_get = MagicMock(return_value=page_result)
    monkeypatch.setattr(lists_routes, "get_topic_category_creators", mock_get)

    creators, total_count, total_pages, category_name = lists_routes._fetch_category_page(
        "lifestyle-sociology", page=1
    )

    assert category_name == "Lifestyle (sociology)"
    assert total_count == 42
    assert total_pages == 3  # ceil(42 / 20)
    assert len(creators) == 1
    mock_get.assert_called_once_with(
        "Lifestyle (sociology)",
        limit=20,
        offset=0,
        return_count=True,
    )
