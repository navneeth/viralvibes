"""
Outreach helpers for saved creators.

The first version deliberately exports contact-ready rows instead of sending
email from ViralVibes. Most email tools and CRMs can import a CSV when the
email column comes first and core contact fields have plain names.
"""

from __future__ import annotations

import csv
import io
from typing import Any

from services.contact_extractor import extract_contact_signals_from_creator


EMAIL_EXPORT_HEADERS = [
    "Email",
    "First Name",
    "Last Name",
    "Company",
    "Website",
    "YouTube URL",
    "Instagram URL",
    "X URL",
    "TikTok URL",
    "LinkedIn URL",
    "Tags",
    "Notes",
    "Subscribers",
    "Views",
    "Videos",
    "Quality Grade",
    "Engagement Score",
    "30 Day Subscriber Growth",
    "30 Day View Growth",
    "Category",
    "Country",
    "Language",
    "ViralVibes Profile URL",
]


def _channel_url(creator: dict[str, Any]) -> str:
    channel_id = creator.get("channel_id") or ""
    return creator.get("channel_url") or (
        f"https://www.youtube.com/channel/{channel_id}" if channel_id else ""
    )


def outreach_angle(creator: dict[str, Any]) -> str:
    """Short note useful for manual personalization in an external tool."""
    name = creator.get("channel_name") or "This creator"
    category = creator.get("primary_category") or creator.get("category") or "their niche"
    grade = creator.get("quality_grade") or ""
    subs_delta = creator.get("subscribers_change_30d")

    growth = ""
    if isinstance(subs_delta, int) and subs_delta > 0:
        growth = f" and gained {subs_delta:,} subscribers in the last 30 days"

    grade_note = f" with a {grade} quality grade" if grade else ""
    return f"{name} is active in {category}{grade_note}{growth}."


def creator_to_outreach_row(
    creator: dict[str, Any], *, base_url: str = "https://www.viralvibes.fyi"
) -> dict[str, str]:
    """
    Convert a creator row into a CSV import row.

    The row intentionally uses broadly recognized import column names. Tools
    that require custom mapping should still auto-detect Email, Company,
    Website, Notes, and Tags cleanly.
    """
    contacts = extract_contact_signals_from_creator(creator)
    creator_id = str(creator.get("id") or "")
    category = creator.get("primary_category") or creator.get("category") or ""
    country = creator.get("country_code") or ""
    language = creator.get("default_language") or creator.get("language") or ""

    tags = [tag for tag in ("viralvibes", "saved-creator", category, country) if tag]

    return {
        "Email": contacts.email,
        "First Name": "",
        "Last Name": "",
        "Company": str(creator.get("channel_name") or ""),
        "Website": contacts.website_url,
        "YouTube URL": _channel_url(creator),
        "Instagram URL": contacts.instagram_url,
        "X URL": contacts.x_url,
        "TikTok URL": contacts.tiktok_url,
        "LinkedIn URL": contacts.linkedin_url,
        "Tags": ", ".join(tags),
        "Notes": outreach_angle(creator),
        "Subscribers": str(creator.get("current_subscribers") or 0),
        "Views": str(creator.get("current_view_count") or 0),
        "Videos": str(creator.get("current_video_count") or 0),
        "Quality Grade": str(creator.get("quality_grade") or ""),
        "Engagement Score": str(creator.get("engagement_score") or ""),
        "30 Day Subscriber Growth": str(creator.get("subscribers_change_30d") or ""),
        "30 Day View Growth": str(creator.get("views_change_30d") or ""),
        "Category": str(category),
        "Country": str(country),
        "Language": str(language),
        "ViralVibes Profile URL": (
            f"{base_url.rstrip('/')}/creator/{creator_id}" if creator_id else ""
        ),
    }


def build_outreach_rows(
    creators: list[dict[str, Any]], *, base_url: str = "https://www.viralvibes.fyi"
) -> list[dict[str, str]]:
    return [creator_to_outreach_row(c, base_url=base_url) for c in creators]


def filter_email_ready_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Rows suitable for email tools, which usually require a non-empty email."""
    return [row for row in rows if row.get("Email")]


def render_outreach_csv(rows: list[dict[str, str]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=EMAIL_EXPORT_HEADERS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()
