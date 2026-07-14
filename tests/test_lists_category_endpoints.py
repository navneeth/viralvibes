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


def _req(*, query_params=None, session=None, category_slug=None):
    req = SimpleNamespace(
        query_params=query_params or {},
        session=session or {},
    )
    if category_slug is not None:
        req.category_slug = category_slug
    return req


def test_get_top_categories_with_counts_projects_onto_fixed_taxonomy(monkeypatch):
    monkeypatch.setattr(
        db_lists,
        "_fetch_top_counts",
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
    monkeypatch.setattr(
        db_lists,
        "_fetch_top_counts",
        lambda *args, **kwargs: [
            ("Technology", 80),
            ("Music", 80),
            ("Lifestyle (sociology)", 50),
        ],
    )

    rows = db_lists.get_top_categories_with_counts(limit=2)

    assert rows == [("Music", 80), ("Technology", 80)]


def test_get_top_categories_with_counts_zero_limit_returns_empty(monkeypatch):
    monkeypatch.setattr(
        db_lists,
        "_fetch_top_counts",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not fetch")),
    )

    assert db_lists.get_top_categories_with_counts(limit=0) == []


def test_get_lists_meta_forces_fixed_total_categories(monkeypatch):
    class _RpcCall:
        def execute(self):
            return SimpleNamespace(
                data=[
                    {
                        "total_creators": 10,
                        "total_countries": 3,
                        "total_categories": 999,
                        "total_languages": 4,
                    }
                ]
            )

    class _Client:
        def rpc(self, *_args, **_kwargs):
            return _RpcCall()

    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: _Client())
    db_lists.clear_lists_meta_cache()

    meta = db_lists.get_lists_meta()

    assert meta["total_creators"] == 10
    assert meta["total_countries"] == 3
    assert meta["total_languages"] == 4
    assert meta["total_categories"] == db_lists.TOTAL_TOPIC_CATEGORIES


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
