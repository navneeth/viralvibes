"""
Outreach page for saved creators.
"""

from __future__ import annotations

from fasthtml.common import *
from monsterui.all import *

from services.outreach import filter_email_ready_rows
from utils import format_number


def _stat_card(label: str, value: int, icon: str) -> Div:
    return Div(
        Div(
            UkIcon(icon, cls="w-4 h-4 text-muted-foreground"),
            Span(label, cls="text-xs font-semibold text-muted-foreground uppercase"),
            cls="flex items-center gap-2 mb-2",
        ),
        Div(str(value), cls="text-2xl font-bold text-foreground"),
        cls="p-4 border border-border rounded-lg bg-background",
    )


def _contact_status(row: dict[str, str]) -> Span:
    if row.get("Email"):
        return Span(
            "Email ready",
            cls="text-xs font-semibold text-green-700 bg-green-100 px-2 py-1 rounded-md",
        )
    if any(
        row.get(key) for key in ("Website", "Instagram URL", "X URL", "TikTok URL", "LinkedIn URL")
    ):
        return Span(
            "Social only",
            cls="text-xs font-semibold text-blue-700 bg-blue-100 px-2 py-1 rounded-md",
        )
    return Span(
        "No contact", cls="text-xs font-semibold text-gray-600 bg-gray-100 px-2 py-1 rounded-md"
    )


def _row(row: dict[str, str]) -> Tr:
    return Tr(
        Td(
            Div(
                A(
                    row.get("Company") or "Unknown creator",
                    href=row.get("ViralVibes Profile URL", "#").replace(
                        "https://www.viralvibes.fyi", ""
                    ),
                    cls="font-semibold text-sm text-foreground hover:text-red-600 no-underline",
                ),
                P(
                    row.get("YouTube URL") or "",
                    cls="text-xs text-muted-foreground truncate max-w-xs",
                ),
            )
        ),
        Td(Span(row.get("Email") or "-", cls="text-sm text-muted-foreground")),
        Td(_contact_status(row)),
        Td(Span(format_number(int(row.get("Subscribers") or 0)), cls="text-sm tabular-nums")),
        Td(Span(row.get("Category") or "-", cls="text-sm text-muted-foreground")),
        cls="hover:bg-accent/30 transition-colors",
    )


def _empty_state() -> Div:
    return Div(
        UkIcon("send", cls="w-14 h-14 text-muted-foreground/30 mx-auto mb-4"),
        H2("No saved creators yet", cls="text-xl font-semibold text-foreground mb-2"),
        P(
            "Save creators first, then export the ones with public emails for outreach.",
            cls="text-muted-foreground text-center max-w-sm",
        ),
        A(
            UkIcon("users", cls="w-4 h-4 mr-2"),
            "Browse Creators",
            href="/creators",
            cls="mt-6 inline-flex items-center px-5 py-2.5 bg-red-500 hover:bg-red-600 text-white font-semibold rounded-lg no-underline transition-colors",
        ),
        cls="flex flex-col items-center justify-center py-20 text-center",
    )


def render_outreach_page(rows: list[dict[str, str]], user_name: str = "") -> Div:
    email_rows = filter_email_ready_rows(rows)
    social_rows = [
        r
        for r in rows
        if not r.get("Email")
        and any(
            r.get(k) for k in ("Website", "Instagram URL", "X URL", "TikTok URL", "LinkedIn URL")
        )
    ]

    return Container(
        Div(
            Div(
                H1(
                    f"{user_name}'s Outreach" if user_name else "Outreach",
                    cls="text-3xl font-bold text-foreground mb-1",
                ),
                P(
                    "Export saved creators with public emails into your outreach tool.",
                    cls="text-muted-foreground text-sm",
                ),
            ),
            Div(
                (
                    A(
                        UkIcon("download", cls="w-4 h-4 mr-2"),
                        "Export email CSV",
                        href="/me/outreach/export",
                        cls=(
                            "inline-flex items-center px-4 py-2 bg-green-600 hover:bg-green-700 "
                            "text-white text-sm font-semibold rounded-lg no-underline transition-colors"
                        ),
                    )
                    if email_rows
                    else None
                ),
                A(
                    UkIcon("heart", cls="w-4 h-4 mr-2"),
                    "Saved Creators",
                    href="/me/favourites",
                    cls="inline-flex items-center px-4 py-2 bg-accent hover:bg-accent/80 text-foreground text-sm font-semibold rounded-lg no-underline transition-colors",
                ),
                cls="flex items-center gap-2",
            ),
            cls="flex items-start justify-between mt-8 mb-6 gap-4",
        ),
        (
            Div(
                _stat_card("Saved", len(rows), "heart"),
                _stat_card("With email", len(email_rows), "mail"),
                _stat_card("Social only", len(social_rows), "share-2"),
                cls="grid grid-cols-1 sm:grid-cols-3 gap-3 mb-6",
            )
            if rows
            else None
        ),
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
                                "Email",
                                cls="text-left text-xs font-semibold text-muted-foreground py-3",
                            ),
                            Th(
                                "Status",
                                cls="text-left text-xs font-semibold text-muted-foreground py-3",
                            ),
                            Th(
                                "Subscribers",
                                cls="text-left text-xs font-semibold text-muted-foreground py-3",
                            ),
                            Th(
                                "Category",
                                cls="text-left text-xs font-semibold text-muted-foreground py-3",
                            ),
                        ),
                        cls="border-b border-border",
                    ),
                    Tbody(*[_row(r) for r in rows]),
                    cls="w-full text-sm",
                ),
                cls="bg-background border border-border rounded-xl overflow-hidden shadow-sm",
            )
            if rows
            else _empty_state()
        ),
        cls=ContainerT.xl,
    )
