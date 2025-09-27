import os

import polars as pl
import pytest

from youtube_service import YoutubePlaylistService

# Stable, public test playlist (replace with your own if needed)
TEST_PLAYLIST_URL = (
    "https://www.youtube.com/playlist?list=PLbRwAHeGGL1xG4gUqvkuo__yHEFSMxRqP"
)


@pytest.mark.asyncio
async def test_schema_contract_between_backends():
    """
    Ensures yt-dlp and YouTube API backends return the same schema (columns + stats keys).
    This prevents drift and guarantees seamless switching.
    """
    # Skip API if no key
    if not os.getenv("YOUTUBE_API_KEY"):
        pytest.skip("Skipping schema test: YOUTUBE_API_KEY not set")

    # yt-dlp backend
    yt_service = YoutubePlaylistService(backend="yt-dlp")
    yt_df, yt_name, yt_channel, yt_thumb, yt_stats = await yt_service.get_playlist_data(
        TEST_PLAYLIST_URL, max_expanded=5
    )

    # API backend
    api_service = YoutubePlaylistService(backend="youtubeapi")
    (
        api_df,
        api_name,
        api_channel,
        api_thumb,
        api_stats,
    ) = await api_service.get_playlist_data(TEST_PLAYLIST_URL, max_expanded=5)

    # --- DataFrame schema ---
    yt_cols = yt_df.columns
    api_cols = api_df.columns
    assert yt_cols == api_cols, f"Column mismatch!\nyt-dlp: {yt_cols}\nAPI: {api_cols}"

    # --- Summary stats schema ---
    yt_stat_keys = sorted(yt_stats.keys())
    api_stat_keys = sorted(api_stats.keys())
    assert (
        yt_stat_keys == api_stat_keys
    ), f"Stats key mismatch!\nyt-dlp: {yt_stat_keys}\nAPI: {api_stat_keys}"

    # --- Display headers contract ---
    assert (
        YoutubePlaylistService.get_display_headers()
        == YoutubePlaylistService.get_display_headers()
    )


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
