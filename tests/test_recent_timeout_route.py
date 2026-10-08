from starlette.testclient import TestClient


def test_recent_sort_timeout_renders_degraded_state(monkeypatch):
    import main
    import routes.creators as route
    from db import CreatorListResult

    calls = []

    def fake_get_creators(**kwargs):
        calls.append(kwargs)
        return CreatorListResult([], degraded=True)

    monkeypatch.setattr(route, "get_creators", fake_get_creators)
    monkeypatch.setattr(route, "calculate_creator_stats", lambda creators: {})
    monkeypatch.setattr(route, "get_creator_hero_stats", lambda: {})
    monkeypatch.setattr(route, "get_top_countries_with_counts", lambda limit=8: [])
    monkeypatch.setattr(route, "get_top_languages_with_counts", lambda limit=7: [])
    monkeypatch.setattr(route, "get_top_categories_with_counts", lambda limit=9: [])
    monkeypatch.setattr(route, "get_aplus_category_counts", lambda: [])

    response = TestClient(main.app).get("/creators?sort=recent&page=2")

    assert response.status_code == 200
    assert "page=2" in str(response.url)
    assert len(calls) == 1
    assert calls[0]["sort"] == "recent"
    assert calls[0]["return_count"] is False
    assert "We couldn't finish the query in time" in response.text
    assert "No creators found" not in response.text
