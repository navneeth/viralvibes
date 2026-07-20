"""
tests/test_mentions.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Unit tests for services/mentions.py.
All HTTP calls are stubbed — no network required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

import pytest

from services.mentions import (
    MentionBundle,
    fetch_news_mentions,
    fetch_recent_videos,
    get_mentions,
    _strip_source,
    _parse_yt_date,
)

# ── Fixtures ───────────────────────────────────────────────────────────────

_YT_RSS_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:yt="http://www.youtube.com/xml/schemas/2015"
      xmlns:media="http://search.yahoo.com/mrss/">
  <entry>
    <yt:videoId>vid001</yt:videoId>
    <title>Amazing Video Title</title>
    <link rel="alternate" href="https://www.youtube.com/watch?v=vid001"/>
    <published>2025-12-01T10:00:00+00:00</published>
    <yt:statistics viewCount="4200000"/>
    <media:group>
      <media:thumbnail url="https://i.ytimg.com/vi/vid001/hqdefault.jpg"/>
    </media:group>
  </entry>
  <entry>
    <yt:videoId>vid002</yt:videoId>
    <title>Another Great Video</title>
    <link rel="alternate" href="https://www.youtube.com/watch?v=vid002"/>
    <published>2025-11-20T10:00:00+00:00</published>
    <yt:statistics viewCount="800000"/>
    <media:group>
      <media:thumbnail url="https://i.ytimg.com/vi/vid002/hqdefault.jpg"/>
    </media:group>
  </entry>
</feed>
"""

_GNEWS_RSS_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>MrBeast Breaks YouTube Record - BBC News</title>
      <link>https://bbc.com/news/article-1</link>
      <pubDate>Mon, 02 Dec 2025 09:00:00 GMT</pubDate>
      <description>The popular YouTuber has set a new record for fastest video to reach 100 million views.</description>
    </item>
    <item>
      <title>How MrBeast Funds His Videos - Forbes</title>
      <link>https://forbes.com/article-2</link>
      <pubDate>Sat, 30 Nov 2025 14:00:00 GMT</pubDate>
      <description>An inside look at the business model behind viral philanthropy content.</description>
    </item>
  </channel>
</rss>
"""


def _mock_http(xml_text: str):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = xml_text
    mock_resp.raise_for_status = MagicMock()
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.return_value = mock_resp
    return mock_client


# ── YouTube RSS ────────────────────────────────────────────────────────────


def test_fetch_recent_videos_returns_correct_fields():
    with patch("services.mentions.httpx.Client", return_value=_mock_http(_YT_RSS_XML)):
        videos = fetch_recent_videos("UCtest123", limit=10)

    assert len(videos) == 2
    v = videos[0]
    assert v.title == "Amazing Video Title"
    assert "watch?v=vid001" in v.url
    assert v.view_count == 4_200_000
    thumbnail_host = urlparse(v.thumbnail_url).hostname
    assert thumbnail_host and (
        thumbnail_host == "ytimg.com" or thumbnail_host.endswith(".ytimg.com")
    )
    assert "2025" in v.published


def test_fetch_recent_videos_respects_limit():
    with patch("services.mentions.httpx.Client", return_value=_mock_http(_YT_RSS_XML)):
        videos = fetch_recent_videos("UCtest123", limit=1)
    assert len(videos) == 1


def test_fetch_recent_videos_returns_empty_on_http_error():
    import httpx

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.side_effect = httpx.HTTPStatusError(
        "500", request=MagicMock(), response=MagicMock(status_code=500)
    )
    with patch("services.mentions.httpx.Client", return_value=mock_client):
        videos = fetch_recent_videos("UCbad", limit=6)
    assert videos == []


def test_fetch_recent_videos_404_returns_empty_and_logs_debug():
    """404 is expected for deleted/private channels — must not log at ERROR."""
    import httpx

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.side_effect = httpx.HTTPStatusError(
        "404", request=MagicMock(), response=MagicMock(status_code=404)
    )
    with patch("services.mentions.httpx.Client", return_value=mock_client):
        with patch("services.mentions.logger") as mock_logger:
            videos = fetch_recent_videos("UCM0QeKc-ozt38Fw-0hhhAFw", limit=6)
    assert videos == []
    mock_logger.error.assert_not_called()
    mock_logger.debug.assert_called_once()
    assert "404" in mock_logger.debug.call_args[0][0]


def test_fetch_recent_videos_timeout_returns_empty_and_logs_warning():
    import httpx

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.side_effect = httpx.TimeoutException("timed out")
    with patch("services.mentions.httpx.Client", return_value=mock_client):
        with patch("services.mentions.logger") as mock_logger:
            videos = fetch_recent_videos("UCtest", limit=6)
    assert videos == []
    mock_logger.error.assert_not_called()
    mock_logger.warning.assert_called_once()


def test_fetch_recent_videos_returns_empty_on_malformed_xml():
    with patch("services.mentions.httpx.Client", return_value=_mock_http("not xml at all <<<")):
        videos = fetch_recent_videos("UCtest", limit=6)
    assert videos == []


def test_fetch_news_mentions_returns_empty_on_malformed_xml():
    with patch("services.mentions.httpx.Client", return_value=_mock_http("not xml at all <<<")):
        mentions = fetch_news_mentions("MrBeast", limit=10)
    assert mentions == []


def test_fetch_news_mentions_returns_empty_when_channel_missing():
    xml = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
      <notchannel>
        <item>
    news_host = urlparse(m.url).hostname
    assert news_host and (news_host == "bbc.com" or news_host.endswith(".bbc.com"))
        </item>
      </notchannel>
    </rss>
    """
    with patch("services.mentions.httpx.Client", return_value=_mock_http(xml)):
        mentions = fetch_news_mentions("MrBeast", limit=10)
    assert mentions == []


# ── Google News RSS ────────────────────────────────────────────────────────


def test_fetch_news_mentions_returns_correct_fields():
    with patch("services.mentions.httpx.Client", return_value=_mock_http(_GNEWS_RSS_XML)):
        mentions = fetch_news_mentions("MrBeast", limit=10)

    assert len(mentions) == 2
    m = mentions[0]
    assert m.title == "MrBeast Breaks YouTube Record"
    assert m.source == "BBC News"
    assert "bbc.com" in m.url
    assert "2025" in m.published
    assert len(m.snippet) <= 200


def test_fetch_news_mentions_splits_source_from_title():
    headline, source = _strip_source("Some Big Story - The New York Times")
    assert headline == "Some Big Story"
    assert source == "The New York Times"


def test_fetch_news_mentions_handles_title_without_source():
    headline, source = _strip_source("Untitled Article")
    assert headline == "Untitled Article"
    assert source == ""


def test_fetch_news_mentions_returns_empty_on_http_error():
    import httpx

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.side_effect = httpx.TimeoutException("timeout")
    with patch("services.mentions.httpx.Client", return_value=mock_client):
        mentions = fetch_news_mentions("MrBeast", limit=6)
    assert mentions == []


# ── Date helpers ───────────────────────────────────────────────────────────


def test_parse_yt_date_formats_correctly():
    result = _parse_yt_date("2025-01-05T10:00:00+00:00")
    assert "Jan" in result
    assert "2025" in result


def test_parse_yt_date_handles_garbage_gracefully():
    result = _parse_yt_date("not-a-date")
    assert isinstance(result, str)  # does not raise


# ── Cache ──────────────────────────────────────────────────────────────────


def test_get_mentions_serves_from_cache_on_second_call():
    with (
        patch("services.mentions.fetch_recent_videos", return_value=[]) as mock_vid,
        patch("services.mentions.fetch_news_mentions", return_value=[]) as mock_news,
    ):
        get_mentions("UCtest", "Test Channel")
        get_mentions("UCtest", "Test Channel")

    # Both fetchers called exactly once — second call served from cache
    mock_vid.assert_called_once()
    mock_news.assert_called_once()


def test_get_mentions_bundle_has_any_false_when_both_empty():
    with (
        patch("services.mentions.fetch_recent_videos", return_value=[]),
        patch("services.mentions.fetch_news_mentions", return_value=[]),
    ):
        # Clear cache for clean test (use a unique channel_id)
        bundle = get_mentions("UCempty_test_xyz", "Empty Channel XYZ")
    assert bundle.has_any is False


# ── YouTube RSS with partial/missing fields ──────────────────────────────


_YT_RSS_MIXED_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:yt="http://www.youtube.com/xml/schemas/2015"
      xmlns:media="http://search.yahoo.com/mrss/">
  <entry>
    <title>Video With All Fields</title>
    <link rel="alternate" href="https://www.youtube.com/watch?v=full001"/>
    <media:group>
      <media:thumbnail url="https://i.ytimg.com/vi/full001/hqdefault.jpg"/>
    </media:group>
    <yt:statistics viewCount="1000"/>
    <published>2025-01-01T00:00:00+00:00</published>
  </entry>
  <entry>
    <title>No Stats Video</title>
    <link rel="alternate" href="https://www.youtube.com/watch?v=nostats"/>
    <media:group>
      <media:thumbnail url="https://i.ytimg.com/vi/nostats/hqdefault.jpg"/>
    </media:group>
    <published>2025-01-02T00:00:00+00:00</published>
  </entry>
  <entry>
    <title>No Thumbnail Video</title>
    <link rel="alternate" href="https://www.youtube.com/watch?v=nothumb"/>
    <yt:statistics viewCount="2000"/>
    <published>2025-01-03T00:00:00+00:00</published>
  </entry>
  <entry>
    <title>Missing Link Should Be Skipped</title>
    <published>2025-01-04T00:00:00+00:00</published>
  </entry>
  <entry>
    <link rel="alternate" href="https://www.youtube.com/watch?v=notitle"/>
    <published>2025-01-05T00:00:00+00:00</published>
  </entry>
</feed>
"""


def test_fetch_recent_videos_handles_partial_entries():
    """Verify partial/missing fields are handled gracefully."""
    with patch("services.mentions.httpx.Client", return_value=_mock_http(_YT_RSS_MIXED_XML)):
        videos = fetch_recent_videos("UCtest123", limit=10)

    # Two entries are missing either title or link and should be skipped.
    # The remaining three entries should be present with partially populated fields.
    assert len(videos) == 3

    videos_by_title = {video.title for video in videos}
    assert "Video With All Fields" in videos_by_title
    assert "No Stats Video" in videos_by_title
    assert "No Thumbnail Video" in videos_by_title

    videos_map = {video.title: video for video in videos}

    full = videos_map["Video With All Fields"]
    assert "watch?v=full001" in full.url
    assert full.view_count == 1000
    assert "full001" in full.thumbnail_url
    full_thumb_host = urlparse(full.thumbnail_url).hostname
    assert full_thumb_host is not None
    assert full_thumb_host == "ytimg.com" or full_thumb_host.endswith(".ytimg.com")

    no_stats = videos_map["No Stats Video"]
    assert "watch?v=nostats" in no_stats.url
    # When yt:statistics is missing, view_count should be None.
    assert no_stats.view_count is None
    assert "nostats" in no_stats.thumbnail_url
    no_stats_thumb_host = urlparse(no_stats.thumbnail_url).hostname
    assert no_stats_thumb_host is not None
    assert no_stats_thumb_host == "ytimg.com" or no_stats_thumb_host.endswith(".ytimg.com")

    no_thumb = videos_map["No Thumbnail Video"]
    assert "watch?v=nothumb" in no_thumb.url
    assert no_thumb.view_count == 2000
    # When media:thumbnail is missing, thumbnail_url should be empty.
    assert no_thumb.thumbnail_url == ""
