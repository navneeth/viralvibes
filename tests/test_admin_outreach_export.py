"""
tests/test_admin_outreach_export.py — unit tests for the contact extractor
service and the admin outreach CSV export.

These tests target ``services.contact_extractor.ContactExtractorService`` and
its CSV row builder. They are intentionally aligned with the **actual**
runtime behavior of the service:

* ``ContactSignals`` field default is ``""`` (an empty string), not ``None``.
* The CSV row builder writes the channel name to the ``Company`` column.
  ``First Name`` and ``Last Name`` are intentionally left blank — there is
  no reliable way to split a channel name into a person's first/last name.
* All numeric CSV cells are *strings* (``csv.DictWriter`` doesn't coerce
  types). Missing numeric fields are written as empty strings; real zeros
  are preserved as ``"0"``.
* DB / API field names follow the schema: ``current_view_count``,
  ``current_video_count``, ``subscribers_change_30d``, ``views_change_30d``,
  ``country_code``.
"""

import csv
import io
from urllib.parse import urlparse

import pytest

from services.contact_extractor import ContactExtractorService


# ════════════════════════════════════════════════════════════════════════════
# Unit Tests: ContactExtractorService extraction
# ════════════════════════════════════════════════════════════════════════════


class TestContactExtractorService:
    """Unit tests for contact extraction and CSV row building."""

    def test_extract_from_text_email_only(self):
        text = "Filmmaker • contact@example.com • check out my stuff"
        signals = ContactExtractorService.extract_from_text(text)
        assert signals.email == "contact@example.com"
        # Empty fields default to "" (dataclass default), not None.
        assert signals.website_url == ""
        assert signals.instagram_url == ""

    def test_extract_from_text_multiple_urls(self):
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
        assert (urlparse(signals.instagram_url).hostname or "").lower() in {
            "instagram.com",
            "www.instagram.com",
        }
        # The X/Twitter pattern produces an https://x.com/... URL.
        assert (urlparse(signals.x_url).hostname or "").lower() in {
            "x.com",
            "www.x.com",
            "twitter.com",
            "www.twitter.com",
        }
        assert (urlparse(signals.tiktok_url).hostname or "").lower() in {
            "tiktok.com",
            "www.tiktok.com",
        }
        assert (urlparse(signals.website_url).hostname or "").lower() in {
            "mycreatorsite.com",
            "www.mycreatorsite.com",
        }

    def test_extract_from_creator_channels(self):
        creator = {
            "id": "test-creator-1",
            "channel_name": "TestChannel",
            "channel_description": "Business inquiries: business@test.com",
            "bio": "Instagram: https://instagram.com/testchannel",
            "keywords": "TikTok: https://www.tiktok.com/@testchannel",
        }
        signals = ContactExtractorService.extract_from_creator(creator)
        assert signals.email == "business@test.com"
        assert (urlparse(signals.instagram_url).hostname or "").lower() in {
            "instagram.com",
            "www.instagram.com",
        }
        assert (urlparse(signals.tiktok_url).hostname or "").lower() in {
            "tiktok.com",
            "www.tiktok.com",
        }

    def test_build_db_update_payload(self):
        creator = {
            "id": "creator-1",
            "channel_name": "TestChannel",
            "channel_description": (
                "Email: test@example.com • Instagram: https://instagram.com/test"
            ),
            "bio": None,
            "keywords": None,
        }
        payload = ContactExtractorService.build_db_update_payload(creator)

        # All 9 persisted columns are present.
        for key in (
            "extracted_email",
            "extracted_website",
            "extracted_instagram",
            "extracted_x",
            "extracted_tiktok",
            "extracted_linkedin",
            "extracted_whatsapp",
            "contact_signals_extracted_at",
            "has_contact_info",
        ):
            assert key in payload

        assert payload["extracted_email"] == "test@example.com"
        assert payload["has_contact_info"] is True

    def test_build_db_update_payload_no_contact(self):
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
        # Use the real DB schema field names.
        creator = {
            "id": "creator-1",
            "channel_name": "TestChannel",
            "channel_description": "Contact: test@example.com",
            "bio": None,
            "keywords": None,
            "current_subscribers": 50000,
            "current_view_count": 1_000_000,
            "current_video_count": 150,
            "quality_grade": "A",
            "engagement_score": 8.5,
            "subscribers_change_30d": 5000,
            "views_change_30d": 100_000,
            "primary_category": "Tech",
            "country_code": "US",
            "default_language": "en",
            "channel_url": "https://www.youtube.com/channel/test123",
        }
        row = ContactExtractorService.build_creator_contact_row(creator)

        for header in ContactExtractorService.EMAIL_EXPORT_HEADERS:
            assert header in row

        # Channel name lands in Company; First/Last Name stay blank.
        assert row["Email"] == "test@example.com"
        assert row["Company"] == "TestChannel"
        assert row["First Name"] == ""
        assert row["Last Name"] == ""

        # All numeric cells are strings (csv.DictWriter doesn't coerce).
        assert row["Subscribers"] == "50000"
        assert row["Views"] == "1000000"
        assert row["Videos"] == "150"
        assert row["30 Day Subscriber Growth"] == "5000"
        assert row["30 Day View Growth"] == "100000"
        assert row["YouTube URL"] == "https://www.youtube.com/channel/test123"

    def test_build_creator_contact_row_growth_in_notes(self):
        creator = {
            "id": "creator-1",
            "channel_name": "GrowingChannel",
            "channel_description": None,
            "bio": None,
            "keywords": None,
            "current_subscribers": 100_000,
            "current_view_count": 5_000_000,
            "current_video_count": 200,
            "quality_grade": "B",
            "engagement_score": 6.0,
            "subscribers_change_30d": 10_000,
            "views_change_30d": 500_000,
            "primary_category": "Gaming",
            "country_code": "CA",
            "default_language": "en",
            "channel_url": "https://www.youtube.com/channel/gaming123",
        }
        row = ContactExtractorService.build_creator_contact_row(creator)

        # Growth lands in the human-readable Notes column, formatted with a
        # thousands separator.
        assert "10,000 subscribers" in row["Notes"]
        # And in the dedicated numeric column.
        assert row["30 Day Subscriber Growth"] == "10000"

    def test_filter_email_ready_rows(self):
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

    def test_filter_contactable_creators_uses_persisted_flag(self):
        """Prefers the denormalized has_contact_info column over re-extracting."""
        creators = [
            # Persisted flag wins even when bio text would yield nothing.
            {"id": "1", "has_contact_info": True, "channel_description": ""},
            {"id": "2", "has_contact_info": False, "channel_description": ""},
            # Worker has run and persisted an email.
            {
                "id": "3",
                "has_contact_info": True,
                "extracted_email": "a@b.com",
                "contact_signals_extracted_at": "2026-01-01T00:00:00Z",
            },
            # Worker has run and found no email.
            {
                "id": "4",
                "has_contact_info": False,
                "extracted_email": None,
                "contact_signals_extracted_at": "2026-01-01T00:00:00Z",
            },
        ]
        filtered = ContactExtractorService.filter_contactable_creators(creators)
        assert [c["id"] for c in filtered] == ["1", "3"]

    def test_filter_contactable_creators_legacy_fallback(self):
        """When persisted columns are absent (legacy row), fall back to extraction."""
        creators = [
            {"id": "legacy-1", "channel_description": "Email: hi@x.com"},
            {"id": "legacy-2", "channel_description": "no contact here"},
        ]
        filtered = ContactExtractorService.filter_contactable_creators(creators)
        assert [c["id"] for c in filtered] == ["legacy-1"]

    def test_email_export_headers_count_and_order(self):
        headers = ContactExtractorService.EMAIL_EXPORT_HEADERS
        assert len(headers) == 23
        assert headers[0] == "Email"
        assert headers[1] == "First Name"
        assert "YouTube URL" in headers
        assert all(isinstance(h, str) for h in headers)


# ════════════════════════════════════════════════════════════════════════════
# Edge cases
# ════════════════════════════════════════════════════════════════════════════


class TestContactExtractorEdgeCases:
    def test_extract_email_case_insensitive(self):
        text = "Email: TEST@EXAMPLE.COM"
        signals = ContactExtractorService.extract_from_text(text)
        # Real email is extracted (regex matches uppercase too).
        assert signals.email != ""
        assert signals.email.lower() == "test@example.com"

    def test_extract_from_text_empty_string(self):
        signals = ContactExtractorService.extract_from_text("")
        # Empty input → all fields are the dataclass default "".
        assert signals.email == ""
        assert signals.website_url == ""
        assert signals.has_any_contact is False

    def test_extract_from_text_normalizes_none(self):
        """``None`` is coerced to empty string at the service boundary.

        This is a defensive contract documented on the public API so callers
        don't have to guard their inputs.
        """
        signals = ContactExtractorService.extract_from_text(None)  # type: ignore[arg-type]
        assert signals.email == ""
        assert signals.has_any_contact is False

    def test_build_row_handles_missing_fields(self):
        """Missing numeric fields render as empty CSV cells (consistent)."""
        creator = {
            "id": "creator-1",
            "channel_name": "MinimalChannel",
            # Everything else missing.
        }
        row = ContactExtractorService.build_creator_contact_row(creator)

        for header in ContactExtractorService.EMAIL_EXPORT_HEADERS:
            assert header in row

        # All-numeric columns: empty string when the source value is missing.
        for col in (
            "Subscribers",
            "Views",
            "Videos",
            "Engagement Score",
            "30 Day Subscriber Growth",
            "30 Day View Growth",
        ):
            assert row[col] == "", f"{col!r} should be empty, got {row[col]!r}"

        # Channel name still propagates to Company.
        assert row["Company"] == "MinimalChannel"

    def test_build_row_preserves_real_zero(self):
        """Real zero values are kept (not coerced to empty)."""
        creator = {
            "id": "creator-zero",
            "channel_name": "ZeroChannel",
            "current_subscribers": 0,
            "current_view_count": 0,
            "current_video_count": 0,
        }
        row = ContactExtractorService.build_creator_contact_row(creator)
        assert row["Subscribers"] == "0"
        assert row["Views"] == "0"
        assert row["Videos"] == "0"


# ════════════════════════════════════════════════════════════════════════════
# CSV format end-to-end
# ════════════════════════════════════════════════════════════════════════════


def _sample_creator(creator_id: str, name: str, email_local: str) -> dict:
    return {
        "id": creator_id,
        "channel_name": name,
        "channel_description": f"Email: {email_local}@example.com",
        "bio": None,
        "keywords": None,
        "current_subscribers": 10_000,
        "current_view_count": 100_000,
        "current_video_count": 50,
        "quality_grade": "A",
        "engagement_score": 7.5,
        "subscribers_change_30d": 1_000,
        "views_change_30d": 10_000,
        "primary_category": "Tech",
        "country_code": "US",
        "default_language": "en",
        "channel_url": f"https://youtube.com/channel/{creator_id}",
    }


def test_csv_export_format():
    """End-to-end: builder + filter + csv.DictWriter produces valid CSV."""
    rows = [
        ContactExtractorService.build_creator_contact_row(
            _sample_creator("1", "Channel 1", "test1")
        ),
        ContactExtractorService.build_creator_contact_row(
            _sample_creator("2", "Channel 2", "test2")
        ),
    ]
    rows = ContactExtractorService.filter_email_ready_rows(rows)

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

    assert lines[0] == ",".join(ContactExtractorService.EMAIL_EXPORT_HEADERS)
    assert len(lines) == 3  # header + 2 data rows

    reader = csv.DictReader(io.StringIO(csv_content))
    parsed = list(reader)
    assert len(parsed) == 2
    assert parsed[0]["Email"] == "test1@example.com"
    assert parsed[1]["Email"] == "test2@example.com"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
