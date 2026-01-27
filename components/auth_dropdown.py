"""
Auth dropdown component using MonsterUI.

Shows login/logout options based on auth state.
Uses MonsterUI DropDownNavContainer for the dropdown.
"""

from fasthtml.common import *
from monsterui.all import *


def AuthDropdown(user: dict = None, avatar_url: str = None):
    """
    Authentication dropdown menu.

    ✅ MonsterUI DropDownNavContainer pattern
    ✅ Shows different UI for logged in/out users
    ✅ Reusable component

    Args:
        user: User dict from session (None if not logged in)
        avatar_url: Optional custom avatar URL

    Returns:
        Nav dropdown component
    """

    # --- LOGGED OUT STATE ---
    if not user:
        return Div(
            A(
                Button(
                    "Login",
                    cls=ButtonT.primary,
                ),
                href="/login",
            ),
            cls="flex items-center gap-2",
        )

    # --- LOGGED IN STATE ---
    user_name = user.get("user_name") or user.get("given_name") or "User"
    user_email = user.get("user_email", "")
    user_id = user.get("user_id", "")

    # Use provided avatar or fetch from storage
    avatar = avatar_url or (f"/avatar/{user_id}" if user_id else "/static/favicon.jpeg")

    # Build dropdown items
    dropdown_items = [
        # Header with user info
        Li(
            Div(
                P(user_name, cls="font-semibold text-gray-900"),
                P(user_email, cls="text-xs text-gray-500"),
                cls="px-4 py-2 border-b",
            ),
            cls="uk-nav-header",  # MonsterUI styling
        ),
        # My Dashboards
        Li(
            A(
                Span(cls="uk-icon", uk_icon="icon: grid"),
                Span("My Dashboards", cls="ml-2"),
                href="/me/dashboards",
                cls="flex items-center px-4 py-2 hover:bg-gray-100",
            )
        ),
        # Divider
        Li(cls="uk-nav-divider"),
        # Settings (optional)
        Li(
            A(
                Span(cls="uk-icon", uk_icon="icon: cog"),
                Span("Settings", cls="ml-2"),
                href="/me/settings",
                cls="flex items-center px-4 py-2 hover:bg-gray-100",
            )
        ),
        # Logout
        Li(
            A(
                Span(cls="uk-icon", uk_icon="icon: sign-out"),
                Span("Logout", cls="ml-2"),
                href="/logout",
                cls="flex items-center px-4 py-2 hover:bg-gray-100 text-gray-700",
            )
        ),
        # Revoke Access (danger zone)
        Li(
            A(
                Span(cls="uk-icon", uk_icon="icon: trash"),
                Span("Revoke Access", cls="ml-2"),
                href="/revoke",
                cls="flex items-center px-4 py-2 hover:bg-red-50 text-red-600",
                onclick="return confirm('This will fully disconnect your Google account. Continue?')",
            )
        ),
    ]

    # ✅ Use MonsterUI DropDownNavContainer
    return Div(
        # Trigger button (avatar + name)
        Button(
            Img(
                src=avatar,
                alt=user_name,
                cls="w-8 h-8 rounded-full object-cover",
                onerror="this.src='/static/favicon.jpeg'",
            ),
            Span(user_name, cls="ml-2 hidden md:inline"),
            Span(cls="uk-icon ml-1", uk_icon="icon: chevron-down"),
            cls="flex items-center gap-2 bg-white border rounded-lg px-3 py-2 hover:bg-gray-50",
        ),
        # ✅ MonsterUI DropDownNavContainer
        DropDownNavContainer(
            *dropdown_items,
            cls="uk-nav uk-dropdown-nav min-w-[240px]",
        ),
        cls="relative",
        uk_dropdown="mode: click; pos: bottom-right",  # ✅ UIKit dropdown behavior
    )
