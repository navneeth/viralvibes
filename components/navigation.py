"""
Navigation component with auth-aware rendering and user profile display.
Shows personalized content when user is logged in (avatar, name, dropdown menu).
"""

from fasthtml.common import *
from monsterui.all import *


# ============================================================================
# ⭐ NEW: Helper Function for Dropdown Items
# ============================================================================


def _build_auth_dropdown_items(user_given_name: str, user_id: str):
    """
    Build dropdown menu items for authenticated users.

    Returns:
        List of Li elements for MonsterUI DropDownNavContainer
    """
    return [
        # User info header
        Li(
            Div(
                P(user_given_name, cls="font-semibold text-gray-900 text-sm"),
                P("Logged in", cls="text-xs text-gray-500"),
                cls="px-4 py-2 border-b",
            ),
        ),
        # My Dashboards (NEW)
        Li(
            A(
                UkIcon("grid", width=16, height=16),
                Span("My Dashboards", cls="ml-2"),
                href="/me/dashboards",
                cls="flex items-center px-4 py-2 hover:bg-gray-100 text-sm",
            )
        ),
        # Divider
        Li(cls="uk-nav-divider"),
        # Log out (moved from inline)
        Li(
            A(
                UkIcon("sign-out", width=16, height=16),
                Span("Log out", cls="ml-2"),
                href="/logout",
                cls="flex items-center px-4 py-2 hover:bg-gray-100 text-sm",
            )
        ),
        # Revoke (moved from inline)
        Li(
            A(
                UkIcon("trash", width=16, height=16),
                Span("Revoke Access", cls="ml-2"),
                href="/revoke",
                cls="flex items-center px-4 py-2 hover:bg-red-50 text-red-600 text-sm",
                onclick="return confirm('This will disconnect your Google account. Continue?')",
            )
        ),
    ]


# ============================================================================
# Main Navigation Component
# ============================================================================


def NavComponent(oauth, req=None, sess=None):
    """
    Reusable navigation component with auth-aware rendering.

    When logged in:
    - Shows user's avatar from Google (stored in avatar_url)
    - Shows user's first name
    - Shows dropdown menu with: My Dashboards, Log out, Revoke

    When logged out:
    - Shows Log in button
    - Or CTA to scroll to analysis form if OAuth not configured

    Args:
        oauth: OAuth instance for login link generation
        req: Request object (needed for login link generation)
        sess: Session dict (contains auth status and user info)

    Returns:
        NavBar component with appropriate links based on auth status
    """
    # Base navigation links (always visible)
    base_links = [
        A("Why ViralVibes", href="/#home-section", cls="text-sm hover:text-blue-600"),
        A("Product", href="/#analyze-section", cls="text-sm hover:text-blue-600"),
        A("About", href="/#explore-section", cls="text-sm hover:text-blue-600"),
    ]

    # Check auth from session - normalize to strict boolean
    is_authenticated = bool(sess and sess.get("auth"))

    # ============================================================================
    # Build auth section (right side of navbar)
    # ============================================================================
    if is_authenticated:
        # User is logged in - show personalized dropdown
        user_id = sess.get("user_id")
        user_given_name = sess.get("user_given_name", "User")
        avatar_url = sess.get("avatar_url")  # ✅ Get avatar URL from session

        # Create avatar element - either image or initials
        if avatar_url:
            # ✅ Use Google avatar URL directly (stored in avatar_url field)
            avatar = Img(
                src=avatar_url,
                alt=f"{user_given_name}'s avatar",
                cls="w-8 h-8 rounded-full object-cover border border-gray-300",
                style="width: 32px; height: 32px; min-width: 32px;",
            )
        else:
            # Fallback: show first initial in colored circle
            initial = user_given_name[0].upper() if user_given_name else "U"
            avatar = Div(
                initial,
                cls="w-8 h-8 rounded-full bg-gradient-to-br from-red-400 to-red-600 flex items-center justify-center text-white text-sm font-semibold border border-red-700",
                style="width: 32px; height: 32px; min-width: 32px; flex-shrink: 0;",
            )

        # ⭐ CHANGED: Build dropdown menu instead of inline links
        auth_section = Div(
            # Trigger button (avatar + name + dropdown icon)
            Button(
                avatar,
                Span(
                    f"Welcome back, {user_given_name}!",
                    cls="text-sm font-medium text-gray-700 hidden sm:inline ml-2",
                ),
                UkIcon("chevron-down", width=16, height=16, cls="ml-1"),
                cls="flex items-center gap-2 bg-transparent border-none hover:opacity-80 cursor-pointer p-0",
            ),
            # ⭐ NEW: Dropdown menu container
            DropDownNavContainer(
                *_build_auth_dropdown_items(user_given_name, user_id),
                cls="uk-nav uk-dropdown-nav min-w-[200px]",
            ),
            cls="relative",
            uk_dropdown="mode: click; pos: bottom-right",  # UIKit dropdown behavior
        )
    else:
        # ❌ User is not logged in - show login/CTA
        if oauth and req:
            # OAuth configured - show login link
            login_url = oauth.login_link(req)
            auth_section = A(
                "Log in",
                href=login_url if isinstance(login_url, str) else "/login",
                cls=f"{ButtonT.primary} text-sm",
            )
        else:
            # No OAuth - show CTA to scroll to analysis form
            auth_section = Button(
                "Try ViralVibes",
                cls=f"{ButtonT.primary} text-sm",
                onclick="document.querySelector('#analysis-form')?.scrollIntoView({behavior:'smooth'})",
            )

    # ============================================================================
    # Combine all nav items and build navbar
    # ============================================================================
    return NavBar(
        *base_links,
        auth_section,
        brand=DivLAligned(
            H3("ViralVibes", cls="text-lg font-bold"),
            UkIcon("chart-line", height=30, width=30),
        ),
        sticky=True,
        uk_scrollspy_nav=True,
        scrollspy_cls=ScrollspyT.bold,
        cls="backdrop-blur bg-white/60 shadow-sm px-4 py-3 border-b border-gray-200 z-50",
    )
