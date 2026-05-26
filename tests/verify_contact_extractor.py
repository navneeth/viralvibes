"""
Direct test script for ContactExtractorService (no pytest needed).
Run with: python tests/verify_contact_extractor.py
"""

import sys
import os
from urllib.parse import urlparse

# Set up path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.contact_extractor import ContactExtractorService


def test_extract_from_text():
    """Test basic email extraction."""
    print("🧪 Test: Extract email from text...")
    text = "Filmmaker • contact@example.com • check out my stuff"
    try:
        signals = ContactExtractorService.extract_from_text(text)
        print(f"   Signals: {signals}")
        assert (
            signals.email == "contact@example.com"
        ), f"Expected contact@example.com, got {signals.email}"
        assert signals.website_url == "", f"Expected empty website_url, got {signals.website_url}"
        print("   ✅ Email extraction works")
    except Exception as e:
        print(f"   Error: {e}")
        import traceback

        traceback.print_exc()
        raise


def test_extract_from_text_multiple():
    """Test extraction of multiple social URLs."""
    print("🧪 Test: Extract multiple social URLs...")
    text = """
    🎬 Creator Hub
    📧 hello@creator.com
    Instagram: https://instagram.com/mychannel
    X: https://twitter.com/mychannel
    TikTok: https://www.tiktok.com/@mychannel
    Website: https://www.mycreatorsite.com
    """
    signals = ContactExtractorService.extract_from_text(text)
    assert signals.email == "hello@creator.com", f"Expected hello@creator.com, got {signals.email}"
    assert signals.instagram_url, f"Instagram not found: {signals.instagram_url}"
    instagram_host = (urlparse(signals.instagram_url).hostname or "").lower()
    assert instagram_host == "instagram.com" or instagram_host.endswith(
        ".instagram.com"
    ), f"Instagram host mismatch: {signals.instagram_url}"
    # X/Twitter pattern converts twitter.com to x.com URLs
    x_host = (urlparse(signals.x_url or "").hostname or "").lower()
    assert (
        x_host == "x.com"
        or x_host.endswith(".x.com")
        or x_host == "twitter.com"
        or x_host.endswith(".twitter.com")
    ), f"X/Twitter host mismatch: {signals.x_url}"
    print("   ✅ Multiple URL extraction works")


def test_build_db_update_payload():
    """Test building DB update payload."""
    print("🧪 Test: Build DB update payload...")
    creator = {
        "id": "creator-1",
        "channel_name": "TestChannel",
        "channel_description": "Email: test@example.com • Instagram: https://instagram.com/test",
        "bio": None,
        "keywords": None,
    }
    payload = ContactExtractorService.build_db_update_payload(creator)

    # Check all 9 columns are present
    required_keys = [
        "extracted_email",
        "extracted_website",
        "extracted_instagram",
        "extracted_x",
        "extracted_tiktok",
        "extracted_linkedin",
        "extracted_whatsapp",
        "contact_signals_extracted_at",
        "has_contact_info",
    ]
    for key in required_keys:
        assert key in payload, f"Missing key: {key}"

    assert payload["extracted_email"] == "test@example.com"
    assert payload["has_contact_info"] is True
    print("   ✅ DB payload building works")


def test_build_creator_contact_row():
    """Test CSV row building with all 23 columns."""
    print("🧪 Test: Build CSV row with all columns...")
    creator = {
        "id": "creator-1",
        "channel_name": "TestChannel",
        "channel_description": "Contact: test@example.com",
        "bio": None,
        "keywords": None,
        "current_subscribers": 50000,
        "current_view_count": 1000000,
        "current_video_count": 150,
        "quality_grade": "A",
        "engagement_score": 8.5,
        "subscribers_change_30d": 5000,
        "views_change_30d": 100000,
        "category": "Tech",
        "country_code": "US",
        "language": "en",
        "channel_url": "https://www.youtube.com/channel/test123",
    }
    try:
        row = ContactExtractorService.build_creator_contact_row(creator)
        print(f"   Row keys: {list(row.keys())}")
        print(f"   Email: {row.get('Email')}")
        print(f"   Company: {row.get('Company')}")

        # Verify all 23 headers are present
        for header in ContactExtractorService.EMAIL_EXPORT_HEADERS:
            assert header in row, f"Missing header: {header}"

        # Verify key fields (Note: Company has the channel_name, not First Name)
        assert row.get("Email") == "test@example.com", f"Email mismatch: {row.get('Email')}"
        assert row.get("Company") == "TestChannel", f"Company mismatch: {row.get('Company')}"
        assert row.get("Subscribers") == "50000", f"Subscribers mismatch: {row.get('Subscribers')}"
        assert (
            row.get("YouTube URL") == "https://www.youtube.com/channel/test123"
        ), f"YouTube URL mismatch: {row.get('YouTube URL')}"
        print("   ✅ CSV row building works (all 23 columns present)")
    except Exception as e:
        print(f"   Error: {e}")
        import traceback

        traceback.print_exc()
        raise


def test_email_export_headers():
    """Test EMAIL_EXPORT_HEADERS constant."""
    print("🧪 Test: EMAIL_EXPORT_HEADERS constant...")
    headers = ContactExtractorService.EMAIL_EXPORT_HEADERS
    assert len(headers) == 23, f"Expected 23 headers, got {len(headers)}"
    assert headers[0] == "Email", f"First header should be Email, got {headers[0]}"
    assert headers[1] == "First Name", f"Second header should be First Name, got {headers[1]}"
    assert all(isinstance(h, str) for h in headers)
    print(f"   ✅ EMAIL_EXPORT_HEADERS has {len(headers)} columns in correct order")
    print(f"      Headers: {', '.join(headers[:5])}...")


def test_filter_email_ready_rows():
    """Test email-only filtering."""
    print("🧪 Test: Email-ready row filtering...")
    rows = [
        {"Email": "user1@example.com", "Name": "User 1"},
        {"Email": None, "Name": "User 2"},
        {"Email": "", "Name": "User 3"},
        {"Email": "user4@example.com", "Name": "User 4"},
    ]
    filtered = ContactExtractorService.filter_email_ready_rows(rows)

    assert len(filtered) == 2, f"Expected 2 filtered rows, got {len(filtered)}"
    assert filtered[0]["Email"] == "user1@example.com"
    assert filtered[1]["Email"] == "user4@example.com"
    print("   ✅ Email filtering works (2/4 rows retained)")


def test_csv_export_format():
    """Test CSV export format with proper headers."""
    print("🧪 Test: CSV export format...")
    import csv
    import io

    # Create sample rows
    creators = [
        {
            "id": "1",
            "channel_name": "Channel 1",
            "channel_description": "Email: test1@example.com",
            "bio": None,
            "keywords": None,
            "current_subscribers": 10000,
            "current_view_count": 100000,
            "current_video_count": 50,
            "quality_grade": "A",
            "engagement_score": 7.5,
            "subscribers_change_30d": 1000,
            "views_change_30d": 10000,
            "category": "Tech",
            "country_code": "US",
            "language": "en",
            "channel_url": "https://youtube.com/channel/1",
        },
        {
            "id": "2",
            "channel_name": "Channel 2",
            "channel_description": "Email: test2@example.com",
            "bio": None,
            "keywords": None,
            "current_subscribers": 20000,
            "current_view_count": 200000,
            "current_video_count": 100,
            "quality_grade": "B",
            "engagement_score": 6.5,
            "subscribers_change_30d": 2000,
            "views_change_30d": 20000,
            "category": "Gaming",
            "country_code": "CA",
            "language": "en",
            "channel_url": "https://youtube.com/channel/2",
        },
    ]

    # Build rows
    rows = [ContactExtractorService.build_creator_contact_row(c) for c in creators]
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

    # Verify data rows exist
    assert len(lines) == 3, f"Expected 3 lines (header + 2 data), got {len(lines)}"

    # Parse back CSV to verify format
    buf_read = io.StringIO(csv_content)
    reader = csv.DictReader(buf_read)
    parsed_rows = list(reader)

    # Verify parsed rows have correct data
    assert len(parsed_rows) == 2, f"Expected 2 data rows, got {len(parsed_rows)}"
    assert (
        parsed_rows[0]["Email"] == "test1@example.com"
    ), f"Row 1 email mismatch: {parsed_rows[0]['Email']}"
    assert (
        parsed_rows[1]["Email"] == "test2@example.com"
    ), f"Row 2 email mismatch: {parsed_rows[1]['Email']}"

    # Verify all expected headers are in parsed rows
    for header in ContactExtractorService.EMAIL_EXPORT_HEADERS:
        assert header in parsed_rows[0], f"Missing header in parsed row: {header}"

    print("   ✅ CSV export format is valid")


def main():
    """Run all tests."""
    print("\n" + "=" * 80)
    print("ContactExtractorService Verification Tests")
    print("=" * 80 + "\n")

    try:
        test_extract_from_text()
        test_extract_from_text_multiple()
        test_build_db_update_payload()
        test_build_creator_contact_row()
        test_email_export_headers()
        test_filter_email_ready_rows()
        test_csv_export_format()

        print("\n" + "=" * 80)
        print("✅ All tests passed!")
        print("=" * 80 + "\n")
        return 0

    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
