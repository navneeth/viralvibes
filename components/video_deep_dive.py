"""Read-only latest-video comparison backed by the worker's bounded snapshot."""

from __future__ import annotations

import re

from fasthtml.common import A, Button, Div, H2, Img, P, Script, Section, Span
from monsterui.all import UkIcon

from utils import format_number
from utils.dates import format_duration, parse_iso_utc


def _count(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        count = int(value)
        return count if count >= 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _video_row(video: dict) -> Div:
    video_id = video["video_id"]
    title = str(video.get("title") or "Untitled")[:500]
    published = video.get("published_at")
    published_date = parse_iso_utc(
        published.replace("Z", "+00:00") if isinstance(published, str) else None
    )
    date_label = published_date.strftime("%b %d, %Y") if published_date else "Date unavailable"
    duration = _count(video.get("duration_sec"))
    duration_label = format_duration(duration) if duration is not None else "Duration unavailable"
    views = _count(video.get("view_count"))
    likes = _count(video.get("like_count"))
    comments = _count(video.get("comment_count"))
    engagement = (
        f"{(likes + comments) / views * 100:.1f}%"
        if views and likes is not None and comments is not None
        else "Unavailable"
    )
    metrics = [("Views", views), ("Likes", likes), ("Comments", comments)]
    image_class = "w-full h-full object-contain"
    return Div(
        Div(
            Div(
                Div(
                    Img(
                        src=f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg",
                        alt=f"Official thumbnail for {title}",
                        loading="lazy",
                        decoding="async",
                        cls=image_class,
                        data_vv_official="true",
                        data_vv_video_id=video_id,
                    ),
                    Span(
                        "Thumbnail unavailable",
                        cls="hidden text-xs text-muted-foreground",
                        data_vv_official_missing="true",
                    ),
                    cls="aspect-video bg-muted overflow-hidden flex items-center justify-center rounded",
                ),
                P("Official", cls="text-xs text-muted-foreground mt-1"),
            ),
            Div(
                Div(
                    Img(
                        alt=f"Auto-generated frame for {title}",
                        loading="lazy",
                        decoding="async",
                        cls=f"hidden {image_class}",
                        data_vv_frame_image="true",
                    ),
                    Span(
                        "Frame not loaded",
                        cls="text-xs text-muted-foreground text-center px-2",
                        data_vv_frame_placeholder="true",
                    ),
                    cls="aspect-video bg-muted overflow-hidden flex items-center justify-center rounded",
                ),
                Div(
                    *[
                        Button(
                            label,
                            type="button",
                            data_vv_frame_choice=str(frame),
                            aria_pressed="true" if frame == 2 else "false",
                            cls="bg-transparent border-0 px-2 py-1 text-xs text-muted-foreground rounded "
                            "aria-pressed:bg-muted aria-pressed:text-foreground "
                            "aria-pressed:underline "
                            "focus-visible:outline focus-visible:outline-2",
                        )
                        for label, frame in (("Early", 1), ("Mid", 2), ("Late", 3))
                    ],
                    role="group",
                    aria_label="Auto-generated frame selection",
                    title="YouTube auto-generated stills; positions are approximate, not guaranteed timestamps.",
                    cls="flex items-center gap-1 mt-1 flex-wrap",
                ),
            ),
            cls="grid grid-cols-2 gap-3 w-full md:w-96 shrink-0 min-w-0",
        ),
        Div(
            A(
                Span(title, cls="min-w-0 line-clamp-2"),
                UkIcon("external-link", cls="size-3 shrink-0 mt-0.5"),
                href=f"https://www.youtube.com/watch?v={video_id}",
                target="_blank",
                rel="noopener noreferrer",
                title=title,
                cls="flex items-start gap-1 font-semibold text-sm text-foreground hover:underline break-words",
            ),
            P(f"{date_label} / {duration_label}", cls="text-xs text-muted-foreground mt-1"),
            Div(
                *[
                    Span(f"{label}: {format_number(value) if value is not None else 'Unavailable'}")
                    for label, value in metrics
                ],
                Span(f"Engagement: {engagement}"),
                cls="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground mt-3",
            ),
            Div(
                Span(
                    "Frame not loaded",
                    role="status",
                    aria_live="polite",
                    data_vv_frame_status="true",
                    cls="text-xs text-muted-foreground",
                ),
                Button(
                    UkIcon("rotate-cw", cls="size-3.5"),
                    type="button",
                    data_vv_frame_retry="true",
                    aria_label="Retry frame",
                    title="Retry frame",
                    cls="bg-transparent border-0 size-7 p-1 text-muted-foreground hover:text-foreground",
                ),
                cls="flex items-center gap-2 mt-2",
            ),
            cls="min-w-0 flex-1",
        ),
        cls="flex flex-col md:flex-row gap-4 py-4 border-b border-border last:border-0",
        data_vv_video=video_id,
        data_vv_selected_frame="2",
    )


def render_video_deep_dive(creator: dict) -> Section | None:
    snapshot = creator.get("recent_videos")
    if not isinstance(snapshot, list):
        return None
    videos = []
    seen = set()
    for video in snapshot:
        if not isinstance(video, dict):
            continue
        video_id = video.get("video_id")
        if not isinstance(video_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
            continue
        if video_id not in seen:
            seen.add(video_id)
            videos.append(video)
        if len(videos) == 12:
            break
    fetched = creator.get("recent_videos_fetched_at")
    fetched_date = parse_iso_utc(
        fetched.replace("Z", "+00:00") if isinstance(fetched, str) else None
    )
    freshness = f" / Updated {fetched_date.strftime('%b %d, %Y')}" if fetched_date else ""
    video_label = "video" if len(videos) == 1 else "videos"
    return Section(
        H2(
            "Video Deep Dive", id="video-deep-dive-title", cls="text-base font-bold text-foreground"
        ),
        P(
            f"Latest {len(videos)} cached {video_label}{freshness}",
            cls="text-xs text-muted-foreground mt-1",
        ),
        (
            Div(*[_video_row(video) for video in videos])
            if videos
            else P(
                (
                    "No accessible videos in the latest snapshot."
                    if not snapshot
                    else "Video metadata unavailable."
                ),
                cls="text-sm text-muted-foreground py-4",
            )
        ),
        Script(src="/js/video_deep_dive.js", defer=True) if videos else None,
        aria_labelledby="video-deep-dive-title",
        cls="mb-8 border-t border-border pt-5",
    )
