"""
Navigation component with auth-aware rendering and user profile display.
Shows personalized content when user is logged in (avatar, name, logout/revoke links).
"""

from fasthtml.common import *
from monsterui.all import *


def NavComponent(oauth, req=None, sess=None):
    """
    Reusable navigation component with auth-aware rendering.

    When logged in:
    - Shows user's avatar from Google (stored in avatar_url)
    - Shows user's first name
    - Shows Log out and Revoke links

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
        # ✅ User is logged in - show personalized section
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
            # ✅ Fallback: show first initial in colored circle
            initial = user_given_name[0].upper() if user_given_name else "U"
            avatar = Div(
                initial,
                cls="w-8 h-8 rounded-full bg-gradient-to-br from-red-400 to-red-600 flex items-center justify-center text-white text-sm font-semibold border border-red-700",
                style="width: 32px; height: 32px; min-width: 32px; flex-shrink: 0;",
            )

        # ✅ Build auth links section
        auth_section = Div(
            avatar,
            Span(
                f"Welcome back, {user_given_name}!",
                cls="text-sm font-medium text-gray-700 hidden sm:inline",
            ),
            Span("|", cls="text-gray-400 hidden sm:inline"),
            A(
                "Log out",
                href="/logout",
                cls="text-sm hover:text-blue-600 transition-colors font-medium",
            ),
            Span("|", cls="text-gray-400 hidden sm:inline"),
            A(
                "Revoke",
                href="/revoke",
                cls="text-sm hover:text-red-600 transition-colors font-medium",
            ),
            cls="flex items-center gap-3",
        )
    else:
        # ❌ User is not logged in - show login/CTA
        # User is NOT logged in

        # ⭐ DEFENSIVE: Always show login, with graceful fallbacks
        login_href = "/login"  # Safe default

        # Try to get OAuth login URL
        if oauth and req:
            try:
                login_url = oauth.login_link(req)
                if login_url and isinstance(login_url, str):
                    login_href = login_url
            except Exception as e:
                # Log but don't fail
                import logging

                logging.warning(f"Failed to generate OAuth login link: {e}")

        # ✅ ALWAYS render login button
        auth_section = A(
            "Log in",
            href=login_href,
            cls=f"{ButtonT.primary} text-sm",
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
