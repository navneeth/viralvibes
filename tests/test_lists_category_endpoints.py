"""Endpoint-level unit tests for lists category flows."""

import sys
import threading
import types
from types import SimpleNamespace
from unittest.mock import MagicMock, call

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
db_stub._supabase_request_semaphore = threading.BoundedSemaphore(20)
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

    class APIError(Exception):
        code = "42P01"

    for data, error in (([], None), ([{}], None), (None, APIError("missing view"))):
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


def test_count_distinct_languages_does_not_scan_creators_after_transient_error(monkeypatch):
    fallback = MagicMock(return_value={"total_languages": 7})
    monkeypatch.setattr(db_lists, "_get_lists_meta_fallback", fallback)
    query = MagicMock()
    query.select.return_value = query
    query.limit.return_value = query
    query.execute.side_effect = TimeoutError("database read timed out")
    client = MagicMock()
    client.table.return_value = query
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: client)

    assert db_lists._count_distinct_languages() == 0
    fallback.assert_not_called()


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


def test_get_top_creators_by_countries_uses_one_grouped_rpc(monkeypatch):
    client = MagicMock()
    response = SimpleNamespace(
        data=[
            {"country_code": "US", "channel_name": "US creator"},
            {"country_code": "IN", "channel_name": "India creator"},
        ]
    )
    rpc = MagicMock(return_value=response)
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: client)
    monkeypatch.setattr(db_lists, "_rpc_with_retry", rpc)

    result = db_lists.get_top_creators_by_countries(["us", "IN", "US"], limit_per_country=3)

    rpc.assert_called_once_with(
        client,
        "get_country_preview_creators",
        {"p_country_codes": ["US", "IN"], "p_limit_per_country": 3},
    )
    assert result == {
        "US": [{"country_code": "US", "channel_name": "US creator"}],
        "IN": [{"country_code": "IN", "channel_name": "India creator"}],
    }


def test_get_most_active_creators_uses_recent_cadence_and_recency(monkeypatch):
    query = MagicMock()
    for method in ("select", "eq", "gt", "gte", "order", "limit"):
        getattr(query, method).return_value = query
    query.not_.is_.return_value = query
    expected = [{"channel_name": "Recent uploader", "avg_days_between_uploads": 0.02}]
    query.execute.return_value = SimpleNamespace(data=expected)
    client = MagicMock()
    client.table.return_value = query
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: client)

    result = db_lists.get_most_active_creators(limit=1)

    assert result == expected
    query.gt.assert_any_call("avg_days_between_uploads", 0.01)
    query.gte.assert_called_once()
    cutoff_field, cutoff_value = query.gte.call_args.args
    assert cutoff_field == "recent_upload->>published_at"
    assert cutoff_value.endswith("+00:00")
    query.order.assert_has_calls(
        [
            call("avg_days_between_uploads"),
            call("recent_upload->>published_at", desc=True),
        ]
    )


def test_get_top_creators_by_countries_falls_back_when_rpc_times_out(monkeypatch):
    client = MagicMock()
    rpc = MagicMock(side_effect=TimeoutError("RPC timed out"))
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: client)
    monkeypatch.setattr(db_lists, "_rpc_with_retry", rpc)
    fallback_calls = []

    def _get_country_creators(country_code, *, limit):
        fallback_calls.append((country_code, limit))
        return [{"country_code": country_code, "channel_name": f"{country_code} creator"}]

    monkeypatch.setattr(db_lists, "get_country_creators", _get_country_creators)

    result = db_lists.get_top_creators_by_countries(["us", "IN", "US"], limit_per_country=3)

    rpc.assert_called_once_with(
        client,
        "get_country_preview_creators",
        {"p_country_codes": ["US", "IN"], "p_limit_per_country": 3},
    )
    assert fallback_calls == [("US", 3), ("IN", 3)]
    assert result == {
        "US": [{"country_code": "US", "channel_name": "US creator"}],
        "IN": [{"country_code": "IN", "channel_name": "IN creator"}],
    }


def test_get_top_creators_by_countries_serves_cache_without_client(monkeypatch):
    creator = {"country_code": "US", "channel_name": "cached creator"}
    cache_key = ("US", 3, 0, False)
    monkeypatch.setattr(
        db_lists,
        "_country_creators_cache",
        {
            cache_key: (
                db_lists.time.monotonic(),
                db_lists.CountryPageResult([creator], 1),
            )
        },
    )
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: None)

    result = db_lists.get_top_creators_by_countries(["US"], limit_per_country=3)

    assert result == {"US": [creator]}


def test_country_creators_query_invalidated_midflight_does_not_repopulate_cache(monkeypatch):
    class _Query:
        @property
        def not_(self):
            return self

        def __getattr__(self, _name):
            return lambda *args, **kwargs: self

        def execute(self):
            db_lists.clear_country_creators_cache()
            return SimpleNamespace(data=[{"channel_name": "pre-refresh result"}], count=1)

    client = MagicMock()
    client.table.return_value = _Query()
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: client)
    db_lists.clear_country_creators_cache()

    result = db_lists.get_country_creators("US", limit=1)

    assert result == [{"channel_name": "pre-refresh result", "_rank": 1}]
    assert db_lists._country_creators_cache == {}


def test_older_country_query_cannot_overwrite_newer_cached_result(monkeypatch):
    first_query_started = threading.Event()
    release_first_query = threading.Event()
    execute_lock = threading.Lock()
    execute_count = 0

    class _Query:
        @property
        def not_(self):
            return self

        def __getattr__(self, _name):
            return lambda *args, **kwargs: self

        def execute(self):
            nonlocal execute_count
            with execute_lock:
                execute_count += 1
                query_number = execute_count
            if query_number == 1:
                first_query_started.set()
                assert release_first_query.wait(timeout=5)
                name = "older result"
            else:
                name = "newer result"
            return SimpleNamespace(data=[{"channel_name": name}], count=1)

    client = MagicMock()
    client.table.return_value = _Query()
    monotonic_values = iter((10.0, 20.0))
    monkeypatch.setattr(db_lists, "_get_supabase_client", lambda: client)
    monkeypatch.setattr(db_lists.time, "monotonic", lambda: next(monotonic_values))
    db_lists.clear_country_creators_cache()

    older_result = []
    thread = threading.Thread(
        target=lambda: older_result.extend(db_lists.get_country_creators("US", limit=1))
    )
    thread.start()
    assert first_query_started.wait(timeout=5)

    newer_result = db_lists.get_country_creators("US", limit=1)
    release_first_query.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert older_result[0]["channel_name"] == "older result"
    assert newer_result[0]["channel_name"] == "newer result"
    cache_key = ("US", 1, 0, False)
    assert (
        db_lists._country_creators_cache[cache_key][1].creators[0]["channel_name"] == "newer result"
    )


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
