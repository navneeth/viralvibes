"""
Test multi-creator seeding from playlist analysis.
"""

import polars as pl
import pytest

from worker.worker import extract_unique_creators_from_dataframe


class TestMultiCreatorSeeding:
    """Tests for extracting unique creators from playlist DataFrames."""

    def test_extract_unique_creators_single_creator(self):
        """Test playlist with videos from single creator."""
        df = pl.DataFrame(
            {
                "id": ["video1", "video2", "video3"],
                "Title": ["Video 1", "Video 2", "Video 3"],
                "Uploader": ["TestChannel", "TestChannel", "TestChannel"],
                "UploaderChannelId": ["UC123abc", "UC123abc", "UC123abc"],
            }
        )

        creators = extract_unique_creators_from_dataframe(df)

        assert len(creators) == 1
        assert creators[0]["channel_id"] == "UC123abc"
        assert creators[0]["channel_name"] == "TestChannel"
        assert creators[0]["channel_url"] == "https://www.youtube.com/channel/UC123abc"

    def test_extract_unique_creators_multiple_creators(self):
        """Test playlist with videos from multiple creators."""
        df = pl.DataFrame(
            {
                "id": ["video1", "video2", "video3", "video4"],
                "Title": ["Video 1", "Video 2", "Video 3", "Video 4"],
                "Uploader": ["Creator1", "Creator2", "Creator1", "Creator3"],
                "UploaderChannelId": ["UC111", "UC222", "UC111", "UC333"],
            }
        )

        creators = extract_unique_creators_from_dataframe(df)

        assert len(creators) == 3

        # Check all unique creators present
        channel_ids = {c["channel_id"] for c in creators}
        assert channel_ids == {"UC111", "UC222", "UC333"}

        # Check proper URL construction
        for creator in creators:
            assert creator["channel_url"].startswith("https://www.youtube.com/channel/")
            assert creator["channel_id"] in creator["channel_url"]

    def test_extract_unique_creators_filters_empty_ids(self):
        """Test that empty/null channel IDs are filtered out."""
        df = pl.DataFrame(
            {
                "id": ["video1", "video2", "video3", "video4"],
                "Title": ["Video 1", "Video 2", "Video 3", "Video 4"],
                "Uploader": ["Creator1", "Creator2", "Creator3", "Creator4"],
                "UploaderChannelId": ["UC111", "", None, "UC444"],
            }
        )

        creators = extract_unique_creators_from_dataframe(df)

        # Should only have 2 creators (empty and None filtered out)
        assert len(creators) == 2
        channel_ids = {c["channel_id"] for c in creators}
        assert channel_ids == {"UC111", "UC444"}

    def test_extract_unique_creators_empty_dataframe(self):
        """Test with empty DataFrame."""
        df = pl.DataFrame(
            {
                "id": [],
                "Title": [],
                "Uploader": [],
                "UploaderChannelId": [],
            }
        )

        creators = extract_unique_creators_from_dataframe(df)

        assert len(creators) == 0

    def test_extract_unique_creators_none_dataframe(self):
        """Test with None DataFrame."""
        creators = extract_unique_creators_from_dataframe(None)

        assert len(creators) == 0

    def test_extract_unique_creators_missing_columns(self):
        """Test DataFrame missing required columns."""
        df = pl.DataFrame(
            {
                "id": ["video1", "video2"],
                "Title": ["Video 1", "Video 2"],
                # Missing Uploader and UploaderChannelId
            }
        )

        creators = extract_unique_creators_from_dataframe(df)

        # Should return empty list gracefully
        assert len(creators) == 0

    def test_extract_unique_creators_deduplication(self):
        """Test that duplicate creators across many videos are deduplicated."""
        # Simulate a compilation with same creator appearing many times
        df = pl.DataFrame(
            {
                "id": [f"video{i}" for i in range(100)],
                "Title": [f"Video {i}" for i in range(100)],
                "Uploader": ["PopularCreator"] * 50 + ["AnotherCreator"] * 50,
                "UploaderChannelId": ["UC_POPULAR"] * 50 + ["UC_ANOTHER"] * 50,
            }
        )

        creators = extract_unique_creators_from_dataframe(df)

        # Should only have 2 unique creators despite 100 videos
        assert len(creators) == 2
        channel_ids = {c["channel_id"] for c in creators}
        assert channel_ids == {"UC_POPULAR", "UC_ANOTHER"}

    def test_extract_unique_creators_real_world_scenario(self):
        """Test realistic scenario: Top Songs playlist with various artists."""
        df = pl.DataFrame(
            {
                "id": ["v1", "v2", "v3", "v4", "v5"],
                "Title": [
                    "Song A - Artist 1",
                    "Song B - Artist 2",
                    "Song C - Artist 1",
                    "Song D - Artist 3",
                    "Song E - Artist 2",
                ],
                "Uploader": [
                    "Artist1VEVO",
                    "Artist2Official",
                    "Artist1VEVO",
                    "Artist3Music",
                    "Artist2Official",
                ],
                "UploaderChannelId": [
                    "UCaaa111",
                    "UCbbb222",
                    "UCaaa111",  # Duplicate
                    "UCccc333",
                    "UCbbb222",  # Duplicate
                ],
            }
        )

        creators = extract_unique_creators_from_dataframe(df)

        assert len(creators) == 3

        # Verify correct mapping
        creator_map = {c["channel_id"]: c["channel_name"] for c in creators}
        assert creator_map == {
            "UCaaa111": "Artist1VEVO",
            "UCbbb222": "Artist2Official",
            "UCccc333": "Artist3Music",
        }
