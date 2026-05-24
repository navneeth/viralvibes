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
