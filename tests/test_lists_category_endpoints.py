"""Endpoint-level unit tests for lists category flows."""

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


def _req(*, query_params=None, session=None, category_slug=None):
    req = SimpleNamespace(
        query_params=query_params or {},
        session=session or {},
    )
    if category_slug is not None:
        req.category_slug = category_slug
    return req


def _ranking_req(*, query_params=None, session=None, category_slug=None, country_slug=None):
    req = _req(query_params=query_params, session=session, category_slug=category_slug)
    if country_slug is not None:
        req.country_slug = country_slug
    return req


def test_get_top_categories_with_counts_projects_onto_fixed_taxonomy(monkeypatch):
    monkeypatch.setattr(db_lists, "_top_categories_cache", None)  # bypass process-level cache
    monkeypatch.setattr(
        db_lists,
        "_try_top_counts_rpc",
        lambda *args, **kwargs: [
            ("Music", 100),
            ("Technology", 80),
            ("Lifestyle (sociology)", 50),
            ("Unexpected category", 999),
        ],
    )

    rows = db_lists.get_top_categories_with_counts(limit=db_lists.TOTAL_TOPIC_CATEGORIES)

    assert len(rows) == db_lists.TOTAL_TOPIC_CATEGORIES
    assert rows[0] == ("Music", 100)
    assert not any(name == "Unexpected category" for name, _ in rows)


def test_get_top_categories_with_counts_respects_smaller_limit_and_tie_order(monkeypatch):
    monkeypatch.setattr(db_lists, "_top_categories_cache", None)  # bypass process-level cache
    monkeypatch.setattr(
        db_lists,
        "_try_top_counts_rpc",
        lambda *args, **kwargs: [
            ("Technology", 80),
            ("Music", 80),
            ("Lifestyle (sociology)", 50),
        ],
    )

    rows = db_lists.get_top_categories_with_counts(limit=2)

    assert rows == [("Music", 80), ("Technology", 80)]


def test_get_top_categories_with_counts_zero_limit_returns_empty(monkeypatch):
    monkeypatch.setattr(db_lists, "_top_categories_cache", None)  # bypass process-level cache
    monkeypatch.setattr(
        db_lists,
        "_try_top_counts_rpc",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not fetch")),
    )

    assert db_lists.get_top_categories_with_counts(limit=0) == []


def test_get_lists_meta_forces_fixed_total_categories(monkeypatch):
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: MagicMock())
    monkeypatch.setattr(
        db_lists,
        "_rpc_with_retry",
        lambda *_args, **_kwargs: SimpleNamespace(
            data=[
                {
                    "total_creators": 10,
                    "total_countries": 3,
                    "total_categories": 999,
                    "total_languages": 4,
                }
            ]
        ),
    )
    monkeypatch.setattr(db_lists, "_lists_meta_cache", None)
    db_lists.clear_lists_meta_cache()

    meta = db_lists.get_lists_meta()

    assert meta["total_creators"] == 10
    assert meta["total_countries"] == 3
    assert meta["total_languages"] == 4
    assert meta["total_categories"] == db_lists.TOTAL_TOPIC_CATEGORIES


def test_get_lists_meta_omits_language_count_when_rpc_does_not_supply_it(monkeypatch):
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: MagicMock())
    monkeypatch.setattr(
        db_lists,
        "_rpc_with_retry",
        lambda *_args, **_kwargs: SimpleNamespace(
            data=[{"total_creators": 10, "total_countries": 3}]
        ),
    )
    monkeypatch.setattr(db_lists, "_lists_meta_cache", None)
    db_lists.clear_lists_meta_cache()

    meta = db_lists.get_lists_meta()

    assert "total_languages" not in meta


def test_count_distinct_languages_reads_cached_metadata(monkeypatch):
    query = MagicMock()
    query.select.return_value = query
    query.limit.return_value = query
    query.execute.return_value = SimpleNamespace(data=[{"total_languages": 12}])
    client = MagicMock()
    client.table.return_value = query
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: client)

    assert db_lists._count_distinct_languages() == 12
    client.table.assert_called_once_with("mv_lists_meta")
    query.select.assert_called_once_with("total_languages")
    query.limit.assert_called_once_with(1)


def test_count_distinct_languages_falls_back_when_cached_metadata_is_unavailable(monkeypatch):
    monkeypatch.setattr(db_lists, "_get_lists_meta_fallback", lambda: {"total_languages": 7})
    for data, error in (([], None), ([{}], None), (None, RuntimeError("missing view"))):
        query = MagicMock()
        query.select.return_value = query
        query.limit.return_value = query
        if error:
            query.execute.side_effect = error
        else:
            query.execute.return_value = SimpleNamespace(data=data)
        client = MagicMock()
        client.table.return_value = query
        monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: client)

        assert db_lists._count_distinct_languages() == 7


def test_get_top_creators_by_languages_uses_synced_population(monkeypatch):
    query = MagicMock()
    query.select.return_value = query
    query.in_.return_value = query
    query.eq.return_value = query
    query.not_.is_.return_value = query
    query.gt.return_value = query
    query.order.return_value = query
    query.limit.return_value = query
    query.execute.return_value = SimpleNamespace(data=[])
    client = MagicMock()
    client.table.return_value = query
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: client)

    assert db_lists.get_top_creators_by_languages(["en"]) == {"en": []}
    query.eq.assert_called_once_with("sync_status", "synced")


def test_lists_route_skips_language_count_fallback_when_meta_has_count(monkeypatch):
    monkeypatch.setattr(
        lists_routes,
        "get_lists_meta",
        lambda: {
            "total_countries": 1,
            "total_categories": 1,
            "total_languages": 0,
        },
    )
    for name in (
        "get_top_rated_creators",
        "get_most_active_creators",
        "get_country_groups",
        "get_category_groups",
        "get_rising_creators",
        "get_veteran_creators",
        "get_new_channels",
        "get_niche_heatmap_data",
        "get_language_groups",
    ):
        monkeypatch.setattr(lists_routes, name, lambda *args, **kwargs: [])
    language_count_fallback = MagicMock()
    monkeypatch.setattr(lists_routes, "_count_distinct_languages", language_count_fallback)
    captured = {}

    def _render(**kwargs):
        captured.update(kwargs)
        return captured

    monkeypatch.setattr(lists_routes, "render_lists_page", _render)

    result = lists_routes.lists_route(_req())

    assert result["tab_data"]["total_languages"] == 0
    language_count_fallback.assert_not_called()


def test_lists_route_uses_language_count_fallback_when_meta_omits_field(monkeypatch):
    monkeypatch.setattr(
        lists_routes,
        "get_lists_meta",
        lambda: {"total_countries": 1, "total_categories": 1},
    )
    for name in (
        "get_top_rated_creators",
        "get_most_active_creators",
        "get_country_groups",
        "get_category_groups",
        "get_rising_creators",
        "get_veteran_creators",
        "get_new_channels",
        "get_niche_heatmap_data",
        "get_language_groups",
    ):
        monkeypatch.setattr(lists_routes, name, lambda *args, **kwargs: [])
    monkeypatch.setattr(lists_routes, "_count_distinct_languages", lambda: 7)
    monkeypatch.setattr(lists_routes, "render_lists_page", lambda **kwargs: kwargs)

    result = lists_routes.lists_route(_req())

    assert result["tab_data"]["total_languages"] == 7


def test_lists_more_categories_route_uses_meta_and_renders(monkeypatch):
    monkeypatch.setattr(
        lists_routes,
        "get_lists_meta",
        lambda: {"total_categories": db_lists.TOTAL_TOPIC_CATEGORIES},
    )
    monkeypatch.setattr(
        lists_routes,
        "get_category_groups",
        lambda **kwargs: [{"category": "Music", "count": 100, "creators": []}],
    )

    captured = {}

    def _render(groups, next_offset, has_more, total, fav_keys, authenticated):
        captured.update(
            {
                "groups": groups,
                "next_offset": next_offset,
                "has_more": has_more,
                "total": total,
                "authenticated": authenticated,
            }
        )
        return captured

    monkeypatch.setattr(lists_routes, "render_more_categories", _render)

    out = lists_routes.lists_more_categories_route(_req(query_params={"offset": "8"}))

    assert out["groups"][0]["category"] == "Music"
    assert out["next_offset"] == 9
    assert out["total"] == db_lists.TOTAL_TOPIC_CATEGORIES
    assert out["has_more"] is True


def test_categories_explorer_route_calls_counts_endpoint(monkeypatch):
    mock_counts = MagicMock(return_value=[("Music", 100), ("Technology", 80)])
    monkeypatch.setattr(lists_routes, "get_top_categories_with_counts", mock_counts)
    monkeypatch.setattr(lists_routes, "render_categories_explorer_page", lambda data: data)

    out = lists_routes.categories_explorer_route()

    mock_counts.assert_called_once_with(limit=2000)
    assert out[0] == ("Music", 100)


def test_category_detail_route_renders_with_slug_resolution(monkeypatch):
    monkeypatch.setattr(
        lists_routes,
        "_fetch_category_page",
        lambda slug, page: ([{"channel_name": "A"}], 21, 2, "Lifestyle (sociology)"),
    )

    captured = {}

    def _render(**kwargs):
        captured.update(kwargs)
        return captured

    monkeypatch.setattr(lists_routes, "render_category_detail_page", _render)

    page_content, total_count = lists_routes.category_detail_route(
        _req(query_params={"page": "1"}), "lifestyle-sociology"
    )

    assert page_content["category_slug"] == "lifestyle-sociology"
    assert page_content["category_name"] == "Lifestyle (sociology)"
    assert page_content["total_count"] == 21
    assert page_content["total_pages"] == 2
    assert total_count == 21


def test_country_detail_route_returns_tuple(monkeypatch):
    """country_detail_route must return (page_content, total_count).

    Mirrors test_category_detail_route_renders_with_slug_resolution to
    confirm both detail routes share the same tuple-return contract.
    """
    monkeypatch.setattr(
        lists_routes,
        "_fetch_country_page",
        lambda code, page: ([{"channel_name": "A"}], 47, 2),
    )

    captured = {}

    def _render(**kwargs):
        captured.update(kwargs)
        return captured

    monkeypatch.setattr(lists_routes, "render_country_detail_page", _render)

    page_content, total_count = lists_routes.country_detail_route(
        _req(query_params={"page": "1"}), "LT"
    )

    assert page_content["country_code"] == "LT"
    assert page_content["total_count"] == 47
    assert page_content["total_pages"] == 2
    assert total_count == 47


def test_category_detail_more_route_requires_slug(monkeypatch):
    monkeypatch.setattr(lists_routes, "Div", lambda text, cls=None: {"text": text, "cls": cls})

    out = lists_routes.category_detail_more_route(_req())

    assert out["text"] == "Error: Invalid category"


def test_category_detail_more_route_happy_path(monkeypatch):
    monkeypatch.setattr(
        lists_routes,
        "_fetch_category_page",
        lambda slug, page: ([{"channel_name": "A"}], 21, 2, "Lifestyle (sociology)"),
    )

    captured = {}

    def _render(**kwargs):
        captured.update(kwargs)
        return captured

    monkeypatch.setattr(lists_routes, "render_category_creators_rows", _render)

    out = lists_routes.category_detail_more_route(
        _req(query_params={"page": "1"}, category_slug="lifestyle-sociology")
    )

    assert out["category_slug"] == "lifestyle-sociology"
    assert out["category_name"] == "Lifestyle (sociology)"
    assert out["page"] == 1
    assert out["total_pages"] == 2


def test_ranking_detail_route_renders_category_country_page(monkeypatch):
    monkeypatch.setattr(
        lists_routes,
        "_fetch_ranking_page",
        lambda category_slug, country_slug, page: (
            [{"channel_name": "A"}],
            21,
            2,
            "Video game culture",
            "US",
        ),
    )

    captured = {}

    def _render(**kwargs):
        captured.update(kwargs)
        return captured

    monkeypatch.setattr(lists_routes, "render_ranking_detail_page", _render)

    out = lists_routes.ranking_detail_route(
        _req(query_params={"page": "1"}),
        "gaming",
        "united-states",
    )

    assert out["category_slug"] == "gaming"
    assert out["country_slug"] == "united-states"
    assert out["category_name"] == "Video game culture"
    assert out["country_code"] == "US"
    assert out["total_count"] == 21
    assert out["total_pages"] == 2


def test_ranking_detail_more_route_requires_slugs(monkeypatch):
    monkeypatch.setattr(lists_routes, "Div", lambda text, cls=None: {"text": text, "cls": cls})

    out = lists_routes.ranking_detail_more_route(_req())

    assert out["text"] == "Error: Invalid ranking"


def test_ranking_detail_more_route_happy_path(monkeypatch):
    monkeypatch.setattr(
        lists_routes,
        "_fetch_ranking_page",
        lambda category_slug, country_slug, page: (
            [{"channel_name": "A"}],
            21,
            2,
            "Video game culture",
            "US",
        ),
    )

    captured = {}

    def _render(**kwargs):
        captured.update(kwargs)
        return captured

    monkeypatch.setattr(lists_routes, "render_ranking_creators_rows", _render)

    out = lists_routes.ranking_detail_more_route(
        _ranking_req(
            query_params={"page": "1"},
            category_slug="gaming",
            country_slug="united-states",
        )
    )

    assert out["category_slug"] == "gaming"
    assert out["country_slug"] == "united-states"
    assert out["category_name"] == "Video game culture"
    assert out["country_code"] == "US"
    assert out["page"] == 1
    assert out["total_pages"] == 2
