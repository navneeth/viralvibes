"""
Favourites page — user's bookmarked creators.
"""

import logging

from fasthtml.common import *
from monsterui.all import *

from utils import format_number

logger = logging.getLogger(__name__)


def _render_creator_row(creator: dict) -> Tr:
    """
    One table row for a favourited creator.

    Shows: thumbnail + name  |  subscribers  |  views  |  quality grade  |  actions
    """
    from views.creators import render_favourite_button  # local import to avoid circulars

    creator_id = creator.get("id", "")
    channel_name = creator.get("channel_name") or "Unknown"
    channel_id = creator.get("channel_id", "")
    thumbnail = (
        creator.get("channel_thumbnail_url")
        or creator.get("thumbnail_url")
        or "/static/favicon.jpeg"
    )
    channel_url = creator.get("channel_url") or f"https://www.youtube.com/channel/{channel_id}"
    current_subs = int(creator.get("current_subscribers") or 0)
    current_views = int(creator.get("current_view_count") or 0)
    quality_grade = creator.get("quality_grade") or "—"
    custom_url = creator.get("custom_url") or ""
    handle = f"@{custom_url.lstrip('@')}" if custom_url else ""

    _grade_colours = {
        "A+": "bg-green-100 text-green-800",
        "A": "bg-green-50 text-green-700",
        "B+": "bg-yellow-100 text-yellow-800",
        "B": "bg-yellow-50 text-yellow-700",
        "C": "bg-gray-100 text-gray-600",
    }
    grade_cls = _grade_colours.get(quality_grade, "bg-gray-100 text-gray-500")

    return Tr(
        # Creator identity
        Td(
            A(
                Div(
                    Img(
                        src=thumbnail,
                        alt=channel_name,
                        cls="w-10 h-10 rounded-lg object-cover shrink-0",
                        onerror="this.src='/static/favicon.jpeg'",
                    ),
                    Div(
                        Span(
                            channel_name,
                            cls="font-semibold text-sm text-foreground leading-tight block",
                        ),
                        (Span(handle, cls="text-xs text-muted-foreground") if handle else None),
                        cls="min-w-0",
                    ),
                    cls="flex items-center gap-3",
                ),
                href=f"/creator/{creator_id}",
                cls="no-underline hover:opacity-80 transition-opacity",
            ),
        ),
        # Subscribers
        Td(
            Span(format_number(current_subs), cls="text-sm font-medium tabular-nums"),
        ),
        # Views
        Td(
            Span(format_number(current_views), cls="text-sm tabular-nums text-muted-foreground"),
        ),
        # Quality grade
        Td(
            Span(quality_grade, cls=f"text-xs font-bold px-2 py-0.5 rounded-md {grade_cls}"),
        ),
        # Actions
        Td(
            Div(
                render_favourite_button(creator_id, is_favourited=True),
                A(
                    UkIcon("external-link", cls="w-4 h-4"),
                    href=channel_url,
                    target="_blank",
                    rel="noopener noreferrer",
                    cls="inline-flex items-center p-1.5 rounded-md hover:bg-accent text-muted-foreground hover:text-foreground transition-colors",
                    aria_label="Open YouTube channel",
                ),
                cls="flex items-center gap-2",
            ),
        ),
        cls="hover:bg-accent/30 transition-colors",
    )


def _render_empty_state() -> Div:
    """Shown when the user has no favourited creators yet."""
    return Div(
        UkIcon("heart", cls="w-16 h-16 text-muted-foreground/30 mx-auto mb-4"),
        H2("No favourites yet", cls="text-xl font-semibold text-foreground mb-2"),
        P(
            "Browse the creator directory and click the ",
            Span("♡ Save", cls="font-semibold text-red-500"),
            " button on any creator to bookmark them here.",
            cls="text-muted-foreground max-w-sm text-center",
        ),
        A(
            UkIcon("users", cls="w-4 h-4 mr-2"),
            "Browse Creators",
            href="/creators",
            cls="mt-6 inline-flex items-center px-5 py-2.5 bg-red-500 hover:bg-red-600 text-white font-semibold rounded-lg no-underline transition-colors shadow-sm",
        ),
        cls="flex flex-col items-center justify-center py-20 text-center",
    )


def render_favourites_page(creators: list[dict], user_name: str) -> Div:
    """
    Full favourites page — /me/favourites.

    Shows each favourited creator in a table row with a heart toggle so users
    can quickly unfavourite from this page too.

    Args:
        creators:   Ordered list of creator dicts (most-recently-favourited first).
                    Each dict is a full row from the ``creators`` table.
        user_name:  Display name for the greeting header.

    Returns:
        A Container component suitable for wrapping in Titled(... Container(nav, ...)).
    """
    count = len(creators)
    count_label = f"{count} creator{'s' if count != 1 else ''}"

    return Container(
        # Page header
        Div(
            Div(
                H1(
                    f"{user_name}'s Saved Creators" if user_name else "Saved Creators",
                    cls="text-3xl font-bold text-foreground mb-1",
                ),
                P(
                    f"{count_label} bookmarked" if count else "Discover creators to save",
                    cls="text-muted-foreground text-sm",
                ),
                cls="",
            ),
            Div(
                *(
                    [
                        A(
                            UkIcon("download", cls="w-4 h-4 mr-2"),
                            "Export CSV",
                            href="/me/favourites/export.csv",
                            cls="inline-flex items-center px-4 py-2 bg-green-600 hover:bg-green-700 text-white text-sm font-semibold rounded-lg no-underline transition-colors mr-2",
                        )
                    ]
                    if count
                    else []
                ),
                A(
                    UkIcon("search", cls="w-4 h-4 mr-2"),
                    "Browse Creators",
                    href="/creators",
                    cls="inline-flex items-center px-4 py-2 bg-accent hover:bg-accent/80 text-foreground text-sm font-semibold rounded-lg no-underline transition-colors",
                ),
                cls="flex items-center",
            ),
            cls="flex items-start justify-between mt-8 mb-6",
        ),
        # Breadcrumb-style back link to /me/dashboards
        Div(
            A(
                UkIcon("arrow-left", cls="w-3.5 h-3.5 mr-1"),
                "My Dashboard",
                href="/me/dashboards",
                cls="inline-flex items-center text-xs text-muted-foreground hover:text-foreground no-underline transition-colors",
            ),
            cls="mb-4",
        ),
        # Table or empty state
        (
            Div(
                Table(
                    Thead(
                        Tr(
                            Th(
                                "Creator",
                                cls="text-left text-xs font-semibold text-muted-foreground py-3 pl-2",
                            ),
                            Th(
                                "Subscribers",
                                cls="text-left text-xs font-semibold text-muted-foreground py-3",
                            ),
                            Th(
                                "Total Views",
                                cls="text-left text-xs font-semibold text-muted-foreground py-3",
                            ),
                            Th(
                                "Grade",
                                cls="text-left text-xs font-semibold text-muted-foreground py-3",
                            ),
                            Th("", cls="py-3"),
                        ),
                        cls="border-b border-border",
                    ),
                    Tbody(*[_render_creator_row(c) for c in creators]),
                    cls="w-full text-sm",
                ),
                cls="bg-background border border-border rounded-xl overflow-hidden shadow-sm",
            )
            if creators
            else _render_empty_state()
        ),
        cls=ContainerT.xl,
    )
