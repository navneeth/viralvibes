"""
Outreach helpers for saved creators.

The first version deliberately exports contact-ready rows instead of sending
email from ViralVibes. Most email tools and CRMs can import a CSV when the
email column comes first and core contact fields have plain names.
"""

from __future__ import annotations

import csv
import io
import re
from typing import Any


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

_EMAIL_RE = re.compile(r"\b([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})\b")
_URL_RE = re.compile(r"https?://[^\s<>)\"']+", re.I)

_SOCIAL_PATTERNS = {
    "instagram_url": re.compile(r"(?:https?://)?(?:www\.)?instagram\.com/([\w.]+)", re.I),
    "x_url": re.compile(r"(?:https?://)?(?:www\.)?(?:x|twitter)\.com/([\w]+)", re.I),
    "tiktok_url": re.compile(r"(?:https?://)?(?:www\.)?tiktok\.com/@([\w.]+)", re.I),
    "linkedin_url": re.compile(
        r"(?:https?://)?(?:www\.)?linkedin\.com/(?:in|company)/([\w-]+)", re.I
    ),
}

_SKIP_WEBSITE_DOMAINS = (
    "youtube.com",
    "youtu.be",
    "instagram.com",
    "twitter.com",
    "x.com",
    "tiktok.com",
    "facebook.com",
    "linkedin.com",
    "google.com",
    "wikipedia.org",
    "bit.ly",
    "goo.gl",
)


def _text_for_contact_scan(creator: dict[str, Any]) -> str:
    """Return the creator text fields likely to contain public contact info."""
    parts = [
        creator.get("channel_description"),
        creator.get("description"),
        creator.get("bio"),
        creator.get("keywords"),
    ]
    return " ".join(str(p) for p in parts if p).strip()


def _first_email(text: str) -> str:
    emails = _EMAIL_RE.findall(text or "")
    return emails[0] if emails else ""


def _first_social_url(text: str, key: str) -> str:
    pattern = _SOCIAL_PATTERNS[key]
    match = pattern.search(text or "")
    if not match:
        return ""
    handle = match.group(1).strip("/ .")
    if not handle:
        return ""
    if key == "instagram_url":
        return f"https://instagram.com/{handle}"
    if key == "x_url":
        return f"https://x.com/{handle}"
    if key == "tiktok_url":
        return f"https://tiktok.com/@{handle}"
    if key == "linkedin_url":
        raw = match.group(0)
        kind = "company" if "/company/" in raw.lower() else "in"
        return f"https://linkedin.com/{kind}/{handle}"
    return ""


def _is_skipped_website_domain(domain: str) -> bool:
    """Return True if *domain* is a known non-website platform domain.

    Matches the domain exactly or as a subdomain of a skip entry, but never
    as an arbitrary substring — so ``myyoutube.com`` is not skipped while
    ``www.youtube.com`` still is.
    """
    domain = domain.lower()
    for skip in _SKIP_WEBSITE_DOMAINS:
        if domain == skip or domain.endswith(f".{skip}"):
            return True
    return False


def _first_website(text: str) -> str:
    for match in _URL_RE.findall(text or ""):
        url = match.rstrip(".,;")
        domain = url.lower().split("//", 1)[-1].split("/", 1)[0].removeprefix("www.")
        if _is_skipped_website_domain(domain):
            continue
        return url
    return ""


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
    text = _text_for_contact_scan(creator)
    creator_id = str(creator.get("id") or "")
    category = creator.get("primary_category") or creator.get("category") or ""
    country = creator.get("country_code") or ""
    language = creator.get("default_language") or creator.get("language") or ""

    tags = [tag for tag in ("viralvibes", "saved-creator", category, country) if tag]

    return {
        "Email": _first_email(text),
        "First Name": "",
        "Last Name": "",
        "Company": str(creator.get("channel_name") or ""),
        "Website": _first_website(text),
        "YouTube URL": _channel_url(creator),
        "Instagram URL": _first_social_url(text, "instagram_url"),
        "X URL": _first_social_url(text, "x_url"),
        "TikTok URL": _first_social_url(text, "tiktok_url"),
        "LinkedIn URL": _first_social_url(text, "linkedin_url"),
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
