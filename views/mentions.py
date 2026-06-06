"""
views/mentions.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Renders the "In the News / Recent Videos" card for the creator profile.

render_mentions_placeholder()
  → Emits the HTMX trigger div that the profile page drops in.
    No network calls here — just a skeleton with hx-get pointing to the
    lazy-load endpoint.

render_mentions_card(bundle)
  → The actual card, returned by the lazy-load route once the service
    has fetched both feeds.
"""

from __future__ import annotations

from fasthtml.common import (
    A,
    Div,
    H2,
    H3,
    Li,
    P,
    Span,
    Ul,
)
from monsterui.all import Card, UkIcon

from services.mentions import MentionBundle, MentionItem, VideoItem
from utils.formatting import format_number


# ── Helpers ────────────────────────────────────────────────────────────────


def _view_badge(view_count: int) -> Span:
    return Span(
        UkIcon("eye", cls="w-3 h-3 mr-1"),
        format_number(view_count),
        cls="inline-flex items-center text-xs text-muted-foreground",
    )


def _video_row(video: VideoItem) -> Li:
    return Li(
        A(
            Div(
                Span(
                    video.title, cls="text-sm font-medium text-foreground line-clamp-2 leading-snug"
                ),
                Div(
                    Span(video.published, cls="text-xs text-muted-foreground"),
                    *([_view_badge(video.view_count)] if video.view_count else []),
                    cls="flex items-center gap-3 mt-1",
                ),
                cls="flex-1 min-w-0",
            ),
            UkIcon("external-link", cls="w-3.5 h-3.5 text-muted-foreground shrink-0 ml-2"),
            href=video.url,
            target="_blank",
            rel="noopener noreferrer",
            cls="flex items-start gap-2 py-2.5 hover:bg-accent/50 rounded-lg px-2 -mx-2 transition-colors",
        ),
        cls="border-b border-border/50 last:border-0",
    )


def _news_row(mention: MentionItem) -> Li:
    return Li(
        A(
            Div(
                Span(
                    mention.title,
                    cls="text-sm font-medium text-foreground line-clamp-2 leading-snug",
                ),
                Div(
                    *(
                        [
                            Span(
                                mention.source,
                                cls="text-xs font-medium text-blue-600 dark:text-blue-400",
                            )
                        ]
                        if mention.source
                        else []
                    ),
                    Span(mention.published, cls="text-xs text-muted-foreground"),
                    cls="flex items-center gap-2 mt-1",
                ),
                cls="flex-1 min-w-0",
            ),
            UkIcon("external-link", cls="w-3.5 h-3.5 text-muted-foreground shrink-0 ml-2"),
            href=mention.url,
            target="_blank",
            rel="noopener noreferrer",
            cls="flex items-start gap-2 py-2.5 hover:bg-accent/50 rounded-lg px-2 -mx-2 transition-colors",
        ),
        cls="border-b border-border/50 last:border-0",
    )


def _section(icon: str, title: str, items: list, row_fn) -> Div:
    return Div(
        Div(
            UkIcon(icon, cls="w-4 h-4 text-muted-foreground"),
            H3(title, cls="text-sm font-semibold text-foreground"),
            cls="flex items-center gap-2 mb-2",
        ),
        Ul(*[row_fn(item) for item in items], cls="divide-y divide-border/50"),
        cls="mb-5 last:mb-0",
    )


# ── Public renderers ───────────────────────────────────────────────────────


def render_mentions_placeholder(creator_id: str) -> Div:
    """
    Drop this into the profile page template.
    HTMX fetches the real card after the profile has rendered.
    hx-trigger="load" fires as soon as this element enters the DOM.
    """
    return Div(
        Div(
            Div(cls="h-3 w-24 bg-accent animate-pulse rounded"),
            Div(cls="h-3 w-full bg-accent animate-pulse rounded mt-2"),
            Div(cls="h-3 w-4/5 bg-accent animate-pulse rounded mt-2"),
            cls="p-5 space-y-2",
        ),
        id="mentions-card",
        hx_get=f"/creator/{creator_id}/mentions",
        hx_trigger="load",
        hx_swap="outerHTML",
        cls="rounded-xl border border-border bg-card",
    )


def render_mentions_card(bundle: MentionBundle) -> Div:
    """
    Full card rendered once both feeds have been fetched.
    Returned by the lazy-load route and swapped in by HTMX.
    """
    if not bundle.has_any:
        return Div(
            P(
                "No recent news or videos found for this creator.",
                cls="text-sm text-muted-foreground text-center py-6",
            ),
            id="mentions-card",
            cls="rounded-xl border border-border bg-card px-5",
        )

    sections = []
    if bundle.news_mentions:
        sections.append(_section("newspaper", "In the News", bundle.news_mentions, _news_row))
    if bundle.recent_videos:
        sections.append(_section("youtube", "Recent Videos", bundle.recent_videos, _video_row))

    return Div(
        Card(
            Div(
                H2("Mentions & Recent Content", cls="text-base font-bold text-foreground"),
                cls="mb-4",
            ),
            *sections,
            body_cls="p-5",
        ),
        id="mentions-card",
    )


def render_mentions_error() -> Div:
    """Shown when both feeds fail — silently degrades."""
    return Div(id="mentions-card")
