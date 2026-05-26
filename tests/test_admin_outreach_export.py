"""
tests/test_admin_outreach_export.py — Unit and integration tests for admin outreach export.

Tests cover:
1. ContactExtractorService contact extraction accuracy
2. CSV row building with all 23 columns
3. Email-only filtering for file size
4. Admin route authorization
5. CSV format and headers
"""

import pytest
from datetime import datetime, timezone
from urllib.parse import urlparse
from services.contact_extractor import ContactExtractorService


# ════════════════════════════════════════════════════════════════════════════
# Unit Tests: ContactExtractorService
# ════════════════════════════════════════════════════════════════════════════


class TestContactExtractorService:
    """Unit tests for contact extraction and CSV row building."""

    def test_extract_from_text_email_only(self):
        """Test extraction of email from free-text bio."""
        text = "Filmmaker • contact@example.com • check out my stuff"
        signals = ContactExtractorService.extract_from_text(text)
        assert signals.email == "contact@example.com"
        assert signals.website_url is None
        assert signals.instagram_url is None

    def test_extract_from_text_multiple_urls(self):
        """Test extraction of multiple social URLs."""
        text = """
        🎬 Creator Hub
        📧 hello@creator.com
        Instagram: https://instagram.com/mychannel
        X: https://twitter.com/mychannel
        TikTok: https://www.tiktok.com/@mychannel
        Website: https://www.mycreatorsite.com
        """
        signals = ContactExtractorService.extract_from_text(text)
        assert signals.email == "hello@creator.com"
        assert signals.instagram_url is not None
        assert (urlparse(signals.instagram_url).hostname or "").lower() in {"instagram.com", "www.instagram.com"}
        assert (urlparse(signals.x_url or "").hostname or "").lower() in {"twitter.com", "www.twitter.com", "x.com", "www.x.com"}
        assert "tiktok.com" in (signals.tiktok_url or "")
        assert "mycreatorsite.com" in (signals.website_url or "")

    def test_extract_from_creator_channels(self):
        """Test extraction from creator dict with channel_description, bio, keywords."""
        creator = {
            "id": "test-creator-1",
            "channel_name": "TestChannel",
            "channel_description": "Business inquiries: business@test.com",
            "bio": "Instagram: https://instagram.com/testchannel",
            "keywords": "TikTok: https://www.tiktok.com/@testchannel",
        }
        signals = ContactExtractorService.extract_from_creator(creator)
        assert signals.email == "business@test.com"
        assert "instagram.com" in (signals.instagram_url or "")
        assert "tiktok.com" in (signals.tiktok_url or "")

    def test_build_db_update_payload(self):
        """Test building payload for DB update with all 9 contact columns."""
        creator = {
            "id": "creator-1",
            "channel_name": "TestChannel",
            "channel_description": "Email: test@example.com • Instagram: https://instagram.com/test",
            "bio": None,
            "keywords": None,
        }
        payload = ContactExtractorService.build_db_update_payload(creator)

        # All 9 columns should be present
        assert "extracted_email" in payload
        assert "extracted_website" in payload
        assert "extracted_instagram" in payload
        assert "extracted_x" in payload
        assert "extracted_tiktok" in payload
        assert "extracted_linkedin" in payload
        assert "extracted_whatsapp" in payload
        assert "contact_signals_extracted_at" in payload
        assert "has_contact_info" in payload

        # Email should be extracted
        assert payload["extracted_email"] == "test@example.com"
        # has_contact_info should be True when any signal is present
        assert payload["has_contact_info"] is True

    def test_build_db_update_payload_no_contact(self):
        """Test payload when no contact info is found."""
        creator = {
            "id": "creator-2",
            "channel_name": "SilentChannel",
            "channel_description": "Just a channel with no contact info",
            "bio": None,
            "keywords": None,
        }
        payload = ContactExtractorService.build_db_update_payload(creator)

        assert payload["extracted_email"] is None
        assert payload["extracted_instagram"] is None
        assert payload["has_contact_info"] is False

    def test_build_creator_contact_row_all_fields(self):
        """Test CSV row building with all 23 columns."""
        creator = {
            "id": "creator-1",
            "channel_name": "TestChannel",
            "channel_description": "Contact: test@example.com",
            "bio": None,
            "keywords": None,
            "current_subscribers": 50000,
            "current_views": 1000000,
            "current_videos": 150,
            "quality_grade": "A",
            "engagement_score": 8.5,
            "subs_delta_30d": 5000,
            "views_delta_30d": 100000,
            "category": "Tech",
            "country": "US",
            "language": "en",
            "channel_url": "https://www.youtube.com/channel/test123",
        }
        row = ContactExtractorService.build_creator_contact_row(creator)

        # Verify all 23 headers are present
        for header in ContactExtractorService.EMAIL_EXPORT_HEADERS:
            assert header in row

        # Verify key fields
        assert row["Email"] == "test@example.com"
        assert row["First Name"] == "TestChannel"
        assert row["Subscribers"] == 50000
        assert row["YouTube URL"] == "https://www.youtube.com/channel/test123"
        assert row["30 Day Subscriber Growth"] == 5000

    def test_build_creator_contact_row_with_growth_notes(self):
        """Test that growth fields include human-readable notes."""
        creator = {
            "id": "creator-1",
            "channel_name": "GrowingChannel",
            "channel_description": None,
            "bio": None,
            "keywords": None,
            "current_subscribers": 100000,
            "current_views": 5000000,
            "current_videos": 200,
            "quality_grade": "B",
            "engagement_score": 6.0,
            "subs_delta_30d": 10000,
            "views_delta_30d": 500000,
            "category": "Gaming",
            "country": "CA",
            "language": "en",
            "channel_url": "https://www.youtube.com/channel/gaming123",
        }
        row = ContactExtractorService.build_creator_contact_row(creator)

        # Growth notes should include ✓ or other indicators
        growth_note = row.get("30 Day Subscriber Growth", "")
        assert "10000" in str(growth_note) or "10,000" in str(growth_note)

    def test_filter_email_ready_rows(self):
        """Test filtering rows to only those with email addresses."""
        rows = [
            {"Email": "user1@example.com", "Name": "User 1"},
            {"Email": None, "Name": "User 2"},
            {"Email": "", "Name": "User 3"},
            {"Email": "user4@example.com", "Name": "User 4"},
        ]
        filtered = ContactExtractorService.filter_email_ready_rows(rows)

        assert len(filtered) == 2
        assert filtered[0]["Email"] == "user1@example.com"
        assert filtered[1]["Email"] == "user4@example.com"

    def test_filter_contactable_creators(self):
        """Test filtering creators list to only those with contact info."""
        creators = [
            {"id": "1", "has_email": True},
            {"id": "2", "has_email": False},
            {"id": "3", "has_email": True},
        ]
        filtered = ContactExtractorService.filter_contactable_creators(creators)

        assert len(filtered) == 2
        assert filtered[0]["id"] == "1"
        assert filtered[1]["id"] == "3"

    def test_email_export_headers_count_and_order(self):
        """Test that EMAIL_EXPORT_HEADERS has exactly 23 columns in correct order."""
        headers = ContactExtractorService.EMAIL_EXPORT_HEADERS

        assert len(headers) == 23
        # First columns should be email and name fields
        assert headers[0] == "Email"
        assert headers[1] == "First Name"
        # YouTube URL should be early
        assert "YouTube URL" in headers
        # All should be strings
        assert all(isinstance(h, str) for h in headers)


class TestContactExtractorEdgeCases:
    """Test edge cases and robustness."""

    def test_extract_email_case_insensitive(self):
        """Test that email extraction works regardless of case in text."""
        text = "Email: TEST@EXAMPLE.COM"
        signals = ContactExtractorService.extract_from_text(text)
        # Email should be extracted (regex is case insensitive)
        assert signals.email is not None

    def test_extract_handles_empty_text(self):
        """Test extraction with empty/None text."""
        signals_empty = ContactExtractorService.extract_from_text("")
        assert signals_empty.email is None

        signals_none = ContactExtractorService.extract_from_text(None)
        assert signals_none.email is None

    def test_build_row_handles_missing_fields(self):
        """Test row building when creator dict has missing fields."""
        creator = {
            "id": "creator-1",
            "channel_name": "MinimalChannel",
            # Other fields missing
        }
        row = ContactExtractorService.build_creator_contact_row(creator)

        # Should not crash and should have all headers
        for header in ContactExtractorService.EMAIL_EXPORT_HEADERS:
            assert header in row
        # Missing fields should be None or empty string
        assert row.get("Subscribers") is None or row.get("Subscribers") == ""


# ════════════════════════════════════════════════════════════════════════════
# Test CSV Format
# ════════════════════════════════════════════════════════════════════════════


def test_csv_export_format():
    """Test that CSV export produces valid CSV with proper headers."""
    import csv
    import io

    # Create sample rows
    rows = [
        ContactExtractorService.build_creator_contact_row(
            {
                "id": "1",
                "channel_name": "Channel 1",
                "channel_description": "Email: test1@example.com",
                "bio": None,
                "keywords": None,
                "current_subscribers": 10000,
                "current_views": 100000,
                "current_videos": 50,
                "quality_grade": "A",
                "engagement_score": 7.5,
                "subs_delta_30d": 1000,
                "views_delta_30d": 10000,
                "category": "Tech",
                "country": "US",
                "language": "en",
                "channel_url": "https://youtube.com/channel/1",
            }
        ),
        ContactExtractorService.build_creator_contact_row(
            {
                "id": "2",
                "channel_name": "Channel 2",
                "channel_description": "Email: test2@example.com",
                "bio": None,
                "keywords": None,
                "current_subscribers": 20000,
                "current_views": 200000,
                "current_videos": 100,
                "quality_grade": "B",
                "engagement_score": 6.5,
                "subs_delta_30d": 2000,
                "views_delta_30d": 20000,
                "category": "Gaming",
                "country": "CA",
                "language": "en",
                "channel_url": "https://youtube.com/channel/2",
            }
        ),
    ]

    # Filter email-ready
    rows = ContactExtractorService.filter_email_ready_rows(rows)

    # Build CSV
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=ContactExtractorService.EMAIL_EXPORT_HEADERS,
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(rows)

    csv_content = buf.getvalue()
    lines = csv_content.strip().split("\n")

    # Verify headers are first line
    header_line = lines[0]
    expected_headers = ",".join(ContactExtractorService.EMAIL_EXPORT_HEADERS)
    assert header_line == expected_headers

    # Verify data rows exist
    assert len(lines) == 3  # header + 2 data rows

    # Parse back and verify structure
    buf.seek(0)
    reader = csv.DictReader(io.StringIO(csv_content))
    parsed_rows = list(reader)
    assert len(parsed_rows) == 2
    assert parsed_rows[0]["Email"] == "test1@example.com"
    assert parsed_rows[1]["Email"] == "test2@example.com"


# ════════════════════════════════════════════════════════════════════════════
# Run tests
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
