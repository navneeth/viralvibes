"""
Contact extraction helpers shared by creator profiles and outreach exports.

This intentionally stays lightweight: parse public emails, websites, and social
links from fields the worker already stores on the creator row.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ContactSignals:
    email: str = ""
    website_url: str = ""
    instagram_url: str = ""
    x_url: str = ""
    tiktok_url: str = ""
    linkedin_url: str = ""

    @property
    def has_email(self) -> bool:
        return bool(self.email)

    @property
    def has_any_contact(self) -> bool:
        return any(
            (
                self.email,
                self.website_url,
                self.instagram_url,
                self.x_url,
                self.tiktok_url,
                self.linkedin_url,
            )
        )


_EMAIL_RE = re.compile(r"\b([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})\b")

# Each entry: (compiled_regex, lucide_icon, display_label, url_template)
_SOCIAL_PATTERNS = [
    (
        re.compile(r"instagram\.com/(?!(?:p|reel|stories|explore)/)([\w.]+)", re.I),
        "instagram",
        "Instagram",
        "https://instagram.com/{}",
    ),
    (
        re.compile(r"(?:instagram|ig)[:\s]+@?([\w.]+)", re.I),
        "instagram",
        "Instagram",
        "https://instagram.com/{}",
    ),
    (re.compile(r"(?:twitter|x)\.com/([\w]+)", re.I), "twitter", "X / Twitter", "https://x.com/{}"),
    (re.compile(r"twitter[:\s]+@?([\w]+)", re.I), "twitter", "X / Twitter", "https://x.com/{}"),
    (
        re.compile(r"facebook\.com/([\w.]+)", re.I),
        "facebook",
        "Facebook",
        "https://facebook.com/{}",
    ),
    (
        re.compile(r"linkedin\.com/(in|company)/([\w-]+)", re.I),
        "linkedin",
        "LinkedIn",
        "https://linkedin.com/{}/{}",
    ),
    (re.compile(r"github\.com/([\w-]+)", re.I), "github", "GitHub", "https://github.com/{}"),
    (re.compile(r"tiktok\.com/@([\w.]+)", re.I), "music", "TikTok", "https://tiktok.com/@{}"),
    (
        re.compile(r"(?:tiktok|tt)[:\s]+@?([\w.]+)", re.I),
        "music",
        "TikTok",
        "https://tiktok.com/@{}",
    ),
    (re.compile(r"twitch\.tv/([\w]+)", re.I), "monitor-play", "Twitch", "https://twitch.tv/{}"),
    (
        re.compile(r"discord\.gg/([\w-]+)", re.I),
        "message-circle",
        "Discord",
        "https://discord.gg/{}",
    ),
    (re.compile(r"patreon\.com/([\w-]+)", re.I), "heart", "Patreon", "https://patreon.com/{}"),
    (re.compile(r"linktr\.ee/([\w-]+)", re.I), "link", "Linktree", "https://linktr.ee/{}"),
    (
        re.compile(r"(https?://(?:www\.)?([\w.-]+\.[a-z]{2,})(?:/[^\s<>)\"']*)?)", re.I),
        "globe",
        "Website",
        "{}",
    ),
]

_SKIP_WEBSITE_DOMAINS = frozenset(
    {
        "youtube.com",
        "youtu.be",
        "instagram.com",
        "twitter.com",
        "x.com",
        "tiktok.com",
        "facebook.com",
        "twitch.tv",
        "github.com",
        "patreon.com",
        "discord.gg",
        "linkedin.com",
        "linktr.ee",
        "t.co",
        "bit.ly",
        "google.com",
        "wikipedia.org",
        "goo.gl",
        "amzn.to",
    }
)


def creator_contact_text(creator: dict[str, Any]) -> str:
    """Return creator text fields likely to contain public contact info."""
    parts = [
        creator.get("channel_description"),
        creator.get("description"),
        creator.get("bio"),
        creator.get("keywords"),
    ]
    return " ".join(str(part) for part in parts if part).strip()


def _is_skipped_website_domain(domain: str) -> bool:
    domain = domain.lower().removeprefix("www.")
    return any(domain == skip or domain.endswith(f".{skip}") for skip in _SKIP_WEBSITE_DOMAINS)


def extract_social_links(bio: str = "", keywords: str = "") -> list[tuple[str, str, str]]:
    """
    Parse social links and emails from bio + keywords text.

    Returns a deduplicated list of (lucide_icon, label, href) tuples, capped at
    eight entries for compact profile display.
    """
    text = f"{bio or ''} {keywords or ''}".strip()
    if not text:
        return []

    found: list[tuple[str, str, str]] = []
    seen: set[str] = set()

    for email in _EMAIL_RE.findall(text):
        href = f"mailto:{email}"
        if href not in seen:
            found.append(("mail", email, href))
            seen.add(href)

    for pattern, icon, label, url_tpl in _SOCIAL_PATTERNS:
        for match in pattern.finditer(text):
            if icon == "globe":
                full_url = match.group(1).rstrip(".,;")
                domain = match.group(2)
                if _is_skipped_website_domain(domain):
                    continue
                href = full_url
            elif icon == "linkedin":
                kind = match.group(1).lower()
                handle = match.group(2).strip("/ .")
                if not handle or len(handle) < 2:
                    continue
                href = url_tpl.format(kind, handle)
            else:
                handle = match.group(1).strip("/ .")
                if not handle or len(handle) < 2:
                    continue
                href = url_tpl.format(handle)

            if href not in seen:
                found.append((icon, label, href))
                seen.add(href)

    return found[:8]


def extract_contact_signals(text: str) -> ContactSignals:
    """Return normalized first-contact signals from free text."""
    links = extract_social_links(text, "")
    values = {
        "email": "",
        "website_url": "",
        "instagram_url": "",
        "x_url": "",
        "tiktok_url": "",
        "linkedin_url": "",
    }

    for icon, label, href in links:
        if icon == "mail" and not values["email"]:
            values["email"] = href.replace("mailto:", "")
        elif icon == "globe" and not values["website_url"]:
            values["website_url"] = href
        elif icon == "instagram" and not values["instagram_url"]:
            values["instagram_url"] = href
        elif icon == "twitter" and not values["x_url"]:
            values["x_url"] = href
        elif icon == "music" and label == "TikTok" and not values["tiktok_url"]:
            values["tiktok_url"] = href
        elif icon == "linkedin" and not values["linkedin_url"]:
            values["linkedin_url"] = href

    return ContactSignals(**values)


def extract_contact_signals_from_creator(creator: dict[str, Any]) -> ContactSignals:
    return extract_contact_signals(creator_contact_text(creator))


# ═════════════════════════════════════════════════════════════════════════════
# CENTRALIZED SERVICE FOR CONTACT EXTRACTION AND ROW BUILDING
# ═════════════════════════════════════════════════════════════════════════════


class ContactExtractorService:
    """
    Centralized service for extracting and formatting contact signals.

    Consolidates extraction logic used by:
    - Creator profile display
    - User outreach export (/me/outreach/export.csv)
    - Admin bulk export (/admin/outreach/export.csv)

    Single source of truth for contact handling — easy to extend for
    new contact types (WhatsApp, email variants, etc.).
    """

    # CSV export headers shared by both user and admin exports
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

    @staticmethod
    def extract_from_text(text: str) -> ContactSignals:
        """Extract contact signals from free-text bio/keywords.

        Args:
            text: Combined bio + keywords text to parse

        Returns:
            ContactSignals with first-contact info (email, socials)
        """
        return extract_contact_signals(text)

    @staticmethod
    def extract_from_creator(creator: dict[str, Any]) -> ContactSignals:
        """Extract contact signals from a creator dict.

        Combines channel_description, bio, keywords text and runs extraction.

        Args:
            creator: Creator dict from database or API

        Returns:
            ContactSignals with first-contact info
        """
        return extract_contact_signals_from_creator(creator)

    @staticmethod
    def build_db_update_payload(creator: dict[str, Any]) -> dict[str, Any]:
        """Build database UPDATE payload for persisting contact signals.

        Called by worker during sync to persist extracted contacts to DB.

        Args:
            creator: Creator dict with channel_description, bio, keywords

        Returns:
            Dict with columns: extracted_email, extracted_website, extracted_instagram,
            extracted_x, extracted_tiktok, extracted_linkedin, extracted_whatsapp,
            contact_signals_extracted_at, has_contact_info
        """
        from datetime import datetime, timezone

        signals = ContactExtractorService.extract_from_creator(creator)

        # Determine if any contact field is non-null
        has_contact = any(
            (
                signals.email,
                signals.website_url,
                signals.instagram_url,
                signals.x_url,
                signals.tiktok_url,
                signals.linkedin_url,
            )
        )

        return {
            "extracted_email": signals.email or None,
            "extracted_website": signals.website_url or None,
            "extracted_instagram": signals.instagram_url or None,
            "extracted_x": signals.x_url or None,
            "extracted_tiktok": signals.tiktok_url or None,
            "extracted_linkedin": signals.linkedin_url or None,
            "extracted_whatsapp": None,  # Reserved for future use
            "contact_signals_extracted_at": datetime.now(timezone.utc).isoformat(),
            "has_contact_info": has_contact,
        }

    @staticmethod
    def build_creator_contact_row(
        creator: dict[str, Any], *, base_url: str = "https://www.viralvibes.fyi"
    ) -> dict[str, str]:
        """Build a CSV export row from a creator dict.

        Unified row builder used by both user outreach export and admin bulk export.
        Consolidates the old creator_to_outreach_row() logic from services/outreach.py.

        Args:
            creator: Creator dict from database
            base_url: Base URL for profile links (e.g., ViralVibes domain)

        Returns:
            Dict with keys matching EMAIL_EXPORT_HEADERS
        """
        # Extract contact signals
        signals = ContactExtractorService.extract_from_creator(creator)

        # Build channel URL
        channel_id = creator.get("channel_id") or ""
        channel_url = creator.get("channel_url") or (
            f"https://www.youtube.com/channel/{channel_id}" if channel_id else ""
        )

        # Prepare metadata
        creator_id = str(creator.get("id") or "")
        channel_name = str(creator.get("channel_name") or "")
        category = creator.get("primary_category") or creator.get("category") or ""
        country = creator.get("country_code") or ""
        language = creator.get("default_language") or creator.get("language") or ""

        # Build tags
        tags = [tag for tag in ("viralvibes", "saved-creator", category, country) if tag]

        # Calculate 30-day growth for notes
        subs_delta = creator.get("subscribers_change_30d")
        growth = ""
        if isinstance(subs_delta, int) and subs_delta > 0:
            growth = f" and gained {subs_delta:,} subscribers in the last 30 days"

        grade = creator.get("quality_grade") or ""
        grade_note = f" with a {grade} quality grade" if grade else ""
        notes = (
            f"{channel_name or 'This creator'} is active in {category or 'their niche'}"
            f"{grade_note}{growth}."
        )

        return {
            "Email": signals.email,
            "First Name": "",
            "Last Name": "",
            "Company": channel_name,
            "Website": signals.website_url,
            "YouTube URL": channel_url,
            "Instagram URL": signals.instagram_url,
            "X URL": signals.x_url,
            "TikTok URL": signals.tiktok_url,
            "LinkedIn URL": signals.linkedin_url,
            "Tags": ", ".join(tags),
            "Notes": notes,
            "Subscribers": str(creator.get("current_subscribers") or 0),
            "Views": str(creator.get("current_view_count") or 0),
            "Videos": str(creator.get("current_video_count") or 0),
            "Quality Grade": grade,
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

    @staticmethod
    def filter_email_ready_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
        """Filter export rows to only those with email addresses.

        Email-only filtering keeps file size down (v1). Future: extend to
        include Instagram or other contact channels.

        Args:
            rows: List of export row dicts

        Returns:
            Filtered list of rows with non-empty Email field
        """
        return [row for row in rows if row.get("Email")]

    @staticmethod
    def filter_contactable_creators(creators: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter creators to those with at least one contact method.

        Currently filters by has_email. Future: extend to include Instagram, etc.

        Args:
            creators: List of creator dicts

        Returns:
            Filtered list of creators with extractable contact info
        """
        return [c for c in creators if ContactExtractorService.extract_from_creator(c).has_email]
