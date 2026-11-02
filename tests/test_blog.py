from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
from starlette.testclient import TestClient

import main
from utils import blog


def test_published_posts_exclude_placeholders_and_empty_drafts(tmp_path, monkeypatch):
    monkeypatch.setattr(blog, "_POSTS_DIR", tmp_path)
    (tmp_path / "published.md").write_text(
        "---\ntitle: Published\n---\nArticle body", encoding="utf-8"
    )
    (tmp_path / "draft.md").write_text("", encoding="utf-8")
    (tmp_path / "placeholder.md").write_text(
        "---\nplaceholder: true\n---\nComing soon", encoding="utf-8"
    )

    assert [post.slug for post in blog.get_posts(published_only=True)] == ["published"]


def test_blog_and_published_article_are_public_and_indexable():
    client = TestClient(main.app)
    for path in ("/blog", "/blog/social-blade-alternative"):
        response = client.get(path, follow_redirects=False)

        assert response.status_code == 200
        assert "noindex" not in response.headers.get("x-robots-tag", "").lower()
        assert 'name="robots" content="noindex' not in response.text
        assert 'rel="canonical"' in response.text
        assert f'href="https://www.viralvibes.fyi{path}"' in response.text


def test_empty_article_is_not_published():
    client = TestClient(main.app)

    assert client.get("/blog/engagement-rate-vs-subscribers").status_code == 404
    assert "/blog/engagement-rate-vs-subscribers" not in client.get("/rss.xml").text


@pytest.mark.parametrize(
    "publication_date,is_published",
    [
        (None, True),
        ("2026-11-01", True),
        ("2026-11-02", True),
        ("2026-11-03", False),
    ],
)
def test_publication_date_controls_all_public_surfaces(
    tmp_path, monkeypatch, publication_date, is_published
):
    from services.sitemap import build_sitemap_xml

    clock = Mock()
    clock.now.return_value = datetime(2026, 11, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(blog, "datetime", clock)
    monkeypatch.setattr(blog, "_POSTS_DIR", tmp_path)
    date_field = f"date: {publication_date}\n" if publication_date else ""
    (tmp_path / "scheduled-article.md").write_text(
        f"---\ntitle: Scheduled Article\n{date_field}---\nArticle body",
        encoding="utf-8",
    )
    client = TestClient(main.app)

    assert bool(blog.get_posts(published_only=True)) is is_published
    assert ("Scheduled Article" in client.get("/blog").text) is is_published
    feed = main.rss_feed(None)
    assert feed.status_code == 200
    assert ("/blog/scheduled-article" in feed.body.decode()) is is_published
    assert ("/blog/scheduled-article" in build_sitemap_xml([])) is is_published
    assert client.get("/blog/scheduled-article").status_code == (200 if is_published else 404)
