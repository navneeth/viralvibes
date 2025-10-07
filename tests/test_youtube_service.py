# tests/test_youtube_service.py
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest
from httpx import Response

from services.youtube_service import YoutubePlaylistService


# --- Fixtures for mock data ---
@pytest.fixture
def mock_playlist_info():
    """Mock a full yt-dlp playlist info dictionary."""
    return {
        "title": "Mock Playlist",
        "uploader": "Mock Channel",
        "thumbnails": [{"url": "http://example.com/thumb.jpg", "width": 1080}],
        "entries": [
            {"url": "http://youtube.com/watch?v=video1", "id": "video1"},
            {"url": "http://youtube.com/watch?v=video2", "id": "video2"},
        ],
    }


@pytest.fixture
def mock_video_info_1():
    return {
        "url": "http://youtube.com/watch?v=video1",
        "id": "video1",
        "view_count": 10000,
        "like_count": 500,
        "duration": 60,
        "comment_count": 25,
    }


@pytest.fixture
def mock_video_info_2():
    return {
        "url": "http://youtube.com/watch?v=video2",
        "id": "video2",
        "view_count": 20000,
        "like_count": 1000,
        "duration": 120,
        "comment_count": 50,
    }


@pytest.fixture
def mock_dislike_api_response():
    """Mock a successful response from the dislike API."""
    mock_response = MagicMock(spec=Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "dislikes": 10,
        "likes": 500,
        "rating": 4.5,
    }
    return mock_response


# --- Tests for the public API ---
@pytest.mark.asyncio
async def test_get_playlist_data_success(
    mock_playlist_info,
    mock_video_info_1,
    mock_video_info_2,
    mock_dislike_api_response,
):
    """Test that get_playlist_data correctly fetches and processes data."""
    service = YoutubePlaylistService()

    # Update mock_video_info values to match expected totals
    mock_video_info_1["like_count"] = 500
    mock_video_info_2["like_count"] = 1000
    total_expected_likes = (
        mock_video_info_1["like_count"] + mock_video_info_2["like_count"]
    )

    with (
        patch(
            "asyncio.to_thread",
            new=AsyncMock(
                side_effect=[
                    mock_playlist_info,
                    mock_video_info_1,
                    mock_video_info_2,
                ]
            ),
        ),
        patch(
            "httpx.AsyncClient.get",
            new=AsyncMock(return_value=mock_dislike_api_response),
        ),
    ):
        df, name, channel, thumb, stats = await service.get_playlist_data(
            "https://www.youtube.com/playlist?list=MOCK"
        )

        assert df.height == 2
        assert stats["total_views"] == 30000
        assert stats["total_likes"] == total_expected_likes  # Now matches mock data
