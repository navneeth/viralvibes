import os
from unittest.mock import patch

import polars as pl
import pytest
from dotenv import load_dotenv

from services.youtube_service import YoutubePlaylistService

# Load environment variables at module level
load_dotenv()

# Stable, public test playlist (replace with your own if needed)
TEST_PLAYLIST_URL = (
    "https://www.youtube.com/playlist?list=PLbRwAHeGGL1xG4gUqvkuo__yHEFSMxRqP"
)


@pytest.mark.asyncio
async def test_schema_contract_between_backends(mock_youtube_api):
    """Tests that both backends return the same schema."""
    with patch("youtube_service.build", return_value=mock_youtube_api):
        # yt-dlp backend
        yt_service = YoutubePlaylistService(backend="yt-dlp")
        (
            yt_df,
            yt_name,
            yt_channel,
            yt_thumb,
            yt_stats,
        ) = await yt_service.get_playlist_data(TEST_PLAYLIST_URL, max_expanded=5)

        # API backend
        api_service = YoutubePlaylistService(backend="youtubeapi")
        (
            api_df,
            api_name,
            api_channel,
            api_thumb,
            api_stats,
        ) = await api_service.get_playlist_data(TEST_PLAYLIST_URL, max_expanded=5)

        # Verify schemas match
        assert set(yt_df.columns) == set(api_df.columns)
        assert set(yt_stats.keys()) == set(api_stats.keys())


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["yt-dlp", "youtubeapi"])
async def test_playlist_service_runs(backend):
    if backend == "youtubeapi" and not os.getenv("YOUTUBE_API_KEY"):
        pytest.skip("Skipping YouTube API backend test: YOUTUBE_API_KEY not set")

    service = YoutubePlaylistService(backend=backend)
    (
        df,
        playlist_name,
        channel_name,
        channel_thumb,
        stats,
    ) = await service.get_playlist_data(TEST_PLAYLIST_URL, max_expanded=3)

    assert isinstance(df, pl.DataFrame)
    assert isinstance(playlist_name, str)
    assert isinstance(channel_name, str)
    assert isinstance(channel_thumb, str)
    assert isinstance(stats, dict)
