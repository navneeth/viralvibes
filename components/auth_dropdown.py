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
        user: User dict from session with keys:
              - user_id
              - user_name (full name)
              - user_given_name (first name)
              - user_email
        avatar_url: Avatar URL from Google (stored in session)

    Returns:
        Dropdown component or login button
    """

    # --- LOGGED OUT STATE ---
    if not user or not user.get("user_id"):
        return A(
            "Log in",
            href="/login",
            cls=f"{ButtonT.primary} text-sm",
        )

    # --- LOGGED IN STATE ---

    # Extract user info
    user_id = user.get("user_id", "")
    user_name = user.get("user_name") or user.get("user_given_name") or "User"
    user_given_name = (
        user.get("user_given_name") or user_name.split()[0] if user_name else "User"
    )
    user_email = user.get("user_email", "")

    # Create avatar element
    if avatar_url:
        # Use Google avatar URL
        avatar = Img(
            src=avatar_url,
            alt=user_given_name,
            cls="w-8 h-8 rounded-full object-cover border border-gray-300",
            onerror="this.src='/static/favicon.jpeg'",  # Fallback
        )
    else:
        # Fallback: initials
        initial = user_given_name[0].upper() if user_given_name else "U"
        avatar = Div(
            initial,
            cls="w-8 h-8 rounded-full bg-gradient-to-br from-red-400 to-red-600 flex items-center justify-center text-white text-sm font-semibold border border-red-700",
        )

    # Build dropdown menu items
    dropdown_items = [
        # User info header
        Li(
            Div(
                P(user_given_name, cls="font-semibold text-sm text-gray-900"),
                P(
                    user_email,
                    cls="text-xs text-gray-500 truncate",
                    style="max-width: 200px",
                ),
                cls="px-4 py-3 border-b bg-gray-50",
            ),
        ),
        # My Dashboards
        Li(
            A(
                UkIcon("grid", width=16, height=16),
                Span("My Dashboards", cls="ml-2"),
                href="/me/dashboards",
                cls="flex items-center px-4 py-2 text-sm hover:bg-gray-100 transition-colors",
            )
        ),
        # Divider
        Li(cls="uk-nav-divider"),
        # Logout
        Li(
            A(
                UkIcon("sign-out", width=16, height=16),
                Span("Log out", cls="ml-2"),
                href="/logout",
                cls="flex items-center px-4 py-2 text-sm hover:bg-gray-100 transition-colors",
            )
        ),
        # Revoke Access (danger zone)
        Li(
            A(
                UkIcon("trash", width=16, height=16),
                Span("Revoke Access", cls="ml-2"),
                href="/revoke",
                cls="flex items-center px-4 py-2 text-sm text-red-600 hover:bg-red-50 transition-colors",
                onclick="return confirm('This will fully disconnect your Google account. Continue?')",
            )
        ),
    ]

    # ✅ Return dropdown with MonsterUI components
    return Div(
        # Trigger button
        Button(
            avatar,
            Span(
                user_given_name,
                cls="ml-2 text-sm font-medium text-gray-700 hidden md:inline",  # Hide on mobile
            ),
            UkIcon("chevron-down", width=14, height=14, cls="ml-1"),
            cls="flex items-center gap-1 bg-transparent border-none hover:opacity-80 cursor-pointer p-0",
        ),
        # Dropdown container
        DropDownNavContainer(
            *dropdown_items,
            cls="uk-nav uk-dropdown-nav min-w-[220px]",
        ),
        cls="relative",
        uk_dropdown="mode: click; pos: bottom-right; offset: 5",
    )
