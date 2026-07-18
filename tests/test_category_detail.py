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
db_stub.get_latest_playlist_job = lambda *args, **kwargs: None
db_stub.get_or_create_creator_from_playlist = lambda *args, **kwargs: None
db_stub.init_supabase = lambda *args, **kwargs: None
db_stub.setup_logging = lambda *args, **kwargs: None
db_stub.supabase_client = None
db_stub.upsert_playlist_stats = lambda *args, **kwargs: None
db_stub.get_cached_playlist_stats = lambda *args, **kwargs: None
db_stub.get_dashboard_event_counts = lambda *args, **kwargs: {}
db_stub.record_dashboard_event = lambda *args, **kwargs: None
db_stub.PLAYLIST_STATS_TABLE = "playlist_stats"
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
    "render_ranking_detail_page",
    "render_ranking_creators_rows",
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
    monkeypatch.setattr(db_lists, "_category_creators_cache", {})

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
    monkeypatch.setattr(db_lists, "_category_creators_cache", {})

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
    monkeypatch.setattr(db_lists, "_category_creators_cache", {})

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


def test_get_topic_category_country_creators_filters_country_and_category(monkeypatch):
    fake_client = _FakeSupabaseClient(
        {
            "data": [{"channel_name": "US Gaming", "current_subscribers": 100}],
            "count": 1,
        }
    )
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: fake_client)
    monkeypatch.setattr(db_lists, "_category_country_creators_cache", {})

    result = db_lists.get_topic_category_country_creators(
        "Video game culture",
        "us",
        limit=10,
        return_count=True,
    )

    assert result.total_count == 1
    assert result.creators[0]["_rank"] == 1
    call = fake_client.calls[0]
    assert call["ilike"][0] == "topic_categories"
    assert ("country_code", "US") in call["eq"]
    assert call["order"]["args"] == ("current_subscribers",)


# ---------------------------------------------------------------------------
# Cache behaviour tests
# ---------------------------------------------------------------------------


def test_get_topic_category_creators_caches_result_on_second_call(monkeypatch):
    """A second call with identical params is served from cache (no extra DB hit)."""
    fake_client = _FakeSupabaseClient(
        {"data": [{"channel_name": "Cached Channel", "current_subscribers": 999}], "count": 1}
    )
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: fake_client)
    monkeypatch.setattr(db_lists, "_category_creators_cache", {})

    result1 = db_lists.get_topic_category_creators("Music", limit=5, return_count=True)
    assert result1.total_count == 1
    assert len(fake_client.calls) == 1

    # Second call — same params → cache hit; no new .table() call.
    result2 = db_lists.get_topic_category_creators("Music", limit=5, return_count=True)
    assert result2.total_count == 1
    assert len(fake_client.calls) == 1  # still 1


def test_get_topic_category_creators_serves_stale_cache_on_exception(monkeypatch):
    """When the DB raises (e.g. statement timeout), the last cached result is returned."""
    stale_result = db_lists.TopicCategoryPageResult(
        [{"channel_name": "Stale Channel", "current_subscribers": 100}], 1
    )
    label = db_lists._topic_category_label("Music")
    # Insert an expired cache entry (timestamp 0.0 is in the distant past).
    cache = {(label, 20, 0, True): (0.0, stale_result)}
    monkeypatch.setattr(db_lists, "_category_creators_cache", cache)

    error_client = _FakeSupabaseClient(Exception("canceling statement due to statement timeout"))
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: error_client)

    result = db_lists.get_topic_category_creators("Music", return_count=True)

    assert result.creators[0]["channel_name"] == "Stale Channel"
    assert result.total_count == 1


def test_get_topic_category_country_creators_serves_stale_cache_on_exception(monkeypatch):
    """Country-scoped variant also returns stale cache on DB exception."""
    stale_result = db_lists.TopicCategoryPageResult(
        [{"channel_name": "Stale US Channel", "current_subscribers": 50}], 1
    )
    label = db_lists._topic_category_label("Video game culture")
    cache = {(label, "US", 20, 0, True): (0.0, stale_result)}
    monkeypatch.setattr(db_lists, "_category_country_creators_cache", cache)

    error_client = _FakeSupabaseClient(Exception("canceling statement due to statement timeout"))
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: error_client)

    result = db_lists.get_topic_category_country_creators(
        "Video game culture", "US", return_count=True
    )

    assert result.creators[0]["channel_name"] == "Stale US Channel"
    assert result.total_count == 1


def test_clear_category_creators_cache_clears_both_dicts(monkeypatch):
    """clear_category_creators_cache() empties both category cache dicts."""
    monkeypatch.setattr(db_lists, "_category_creators_cache", {"key1": (0.0, [])})
    monkeypatch.setattr(db_lists, "_category_country_creators_cache", {"key2": (0.0, [])})

    db_lists.clear_category_creators_cache()

    assert db_lists._category_creators_cache == {}
    assert db_lists._category_country_creators_cache == {}


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


def test_fetch_ranking_page_uses_topic_category_country_query(monkeypatch):
    page_result = db_lists.TopicCategoryPageResult(
        creators=[{"channel_name": "Gaming Channel", "current_subscribers": 1000}],
        total_count=41,
    )

    monkeypatch.setattr(
        lists_routes,
        "resolve_ranking_category_slug",
        lambda slug: "Video game culture" if slug == "gaming" else None,
    )
    mock_get = MagicMock(return_value=page_result)
    monkeypatch.setattr(lists_routes, "get_topic_category_country_creators", mock_get)

    creators, total_count, total_pages, category_name, country_code = (
        lists_routes._fetch_ranking_page(
            "gaming",
            "united-states",
            page=2,
        )
    )

    assert category_name == "Video game culture"
    assert country_code == "US"
    assert total_count == 41
    assert total_pages == 3
    assert creators[0]["channel_name"] == "Gaming Channel"
    mock_get.assert_called_once_with(
        "Video game culture",
        "US",
        limit=20,
        offset=20,
        return_count=True,
    )
