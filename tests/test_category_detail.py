"""Tests for /lists/category/{slug} slug resolution and topic category queries."""

from unittest.mock import MagicMock, patch

import db_lists
import routes.lists as lists_routes


def test_topic_category_ilike_term_wikipedia_url():
    term = db_lists._topic_category_ilike_term(
        "https://en.wikipedia.org/wiki/Lifestyle (sociology)"
    )
    assert term == "Lifestyle_(sociology)"


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
    assert db_lists.resolve_category_slug("unknown-niche") is None


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
