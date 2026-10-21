"""Public creator topics shared by profile rendering and exports."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


TOPIC_EXPORT_HEADERS = [
    "Content Topics",
    "Keyword Source",
    "Topics Computed At",
    "Topics Uploaded At",
    "Extractor Version",
]

_SOURCE_LABELS = {
    "transcript": "Transcripts",
    "description": "Video descriptions",
    "mixed": "Mixed sources",
    "unknown": "Source unavailable",
}


def _decode(value: Any) -> Any:
    if isinstance(value, str):
        if len(value) > 65_536:
            return None
        try:
            return json.loads(value)
        except (ValueError, TypeError, RecursionError):
            return None
    return value


def _timestamp(value: Any) -> str:
    if not isinstance(value, str) or len(value) > 64:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return ""
        return parsed.astimezone(timezone.utc).isoformat()
    except (ValueError, OverflowError):
        return ""


@dataclass(frozen=True)
class CreatorTopics:
    topics: tuple[str, ...] = ()
    source: str = "unknown"
    computed_at: str = ""
    uploaded_at: str = ""
    extractor_version: str = ""

    @property
    def source_label(self) -> str:
        return _SOURCE_LABELS[self.source]


def parse_creator_topics(
    keywords: Any, metadata: Any = None, uploaded_at: Any = None
) -> CreatorTopics:
    """Read legacy and versioned payloads without inferring their provenance."""
    payload = _decode(keywords)
    topics: list[str] = []
    seen: set[str] = set()
    if isinstance(payload, list):
        for item in payload[:200]:
            if not isinstance(item, dict) or not isinstance(item.get("kw"), str):
                continue
            keyword = item["kw"]
            if len(keyword) > 120:
                continue
            phrase = " ".join(keyword.split())
            if not phrase or phrase.casefold() in seen:
                continue
            seen.add(phrase.casefold())
            topics.append(phrase)
            if len(topics) == 15:
                break

    details = _decode(metadata)
    if not isinstance(details, dict) or type(details.get("schema_version")) is not int:
        details = {}
    if details.get("schema_version") != 1:
        details = {}

    source = details.get("source")
    counts = [
        details.get(key)
        for key in ("sampled_video_count", "transcript_video_count", "description_video_count")
    ]
    valid_counts = all(type(count) is int and count >= 0 for count in counts)
    if valid_counts:
        sampled, transcripts, descriptions = counts
        valid_counts = sampled > 0 and transcripts + descriptions <= sampled
        valid_source = (
            (source == "transcript" and transcripts > 0 and descriptions == 0)
            or (source == "description" and descriptions > 0 and transcripts == 0)
            or (source == "mixed" and transcripts > 0 and descriptions > 0)
        )
    else:
        valid_source = False
    version = details.get("extractor_version")
    return CreatorTopics(
        topics=tuple(topics),
        source=source if valid_counts and valid_source else "unknown",
        computed_at=_timestamp(details.get("computed_at")),
        uploaded_at=_timestamp(uploaded_at),
        extractor_version=version[:100] if isinstance(version, str) else "",
    )


def _csv_text(value: str) -> str:
    return "'" + value if value.lstrip().startswith(("=", "+", "-", "@")) else value


def creator_topic_export_fields(creator: dict[str, Any]) -> dict[str, str]:
    profile = parse_creator_topics(
        creator.get("transcript_keywords"),
        creator.get("transcript_keywords_metadata"),
        creator.get("transcript_keywords_updated_at"),
    )
    return dict(
        zip(
            TOPIC_EXPORT_HEADERS,
            (
                _csv_text("; ".join(profile.topics)),
                profile.source_label if profile.topics else "",
                profile.computed_at if profile.topics else "",
                profile.uploaded_at if profile.topics else "",
                _csv_text(profile.extractor_version) if profile.topics else "",
            ),
        )
    )
