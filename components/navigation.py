"""
Navigation component with auth-aware rendering and user profile display.
Shows personalized content when user is logged in (avatar dropdown with menu).
"""

from fasthtml.common import *
from monsterui.all import *

from .auth_dropdown import AuthDropdown  # ✅ Import the dropdown


def NavComponent(oauth, req=None, sess=None):
    """
    Reusable navigation component with auth-aware rendering.

    When logged in:
    - Shows avatar dropdown with My Dashboards, Settings, Logout, Revoke

    When logged out:
    - Shows "Try It Free" CTA + "Log in" button

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

    # Check auth from session
    is_authenticated = bool(sess and sess.get("auth"))

    # ============================================================================
    # Build auth section (right side of navbar)
    # ============================================================================
    if is_authenticated:
        # ✅ LOGGED IN: Use AuthDropdown component

        # Extract user data from session (populated by auth_service.py)
        user_data = {
            "user_id": sess.get("user_id"),
            "user_name": sess.get("user_name"),
            "user_given_name": sess.get("user_given_name"),
            "user_email": sess.get("user_email"),
        }

        # Get avatar URL from session (set by auth_service.py)
        avatar_url = sess.get("avatar_url")

        # ✅ Use the dropdown component
        auth_section = AuthDropdown(user=user_data, avatar_url=avatar_url)

    else:
        # ❌ LOGGED OUT: Show "Try It Free" + "Log in"

        # Get login URL with fallback
        login_href = "/login"
        if oauth and req:
            try:
                login_url = oauth.login_link(req)
                if login_url and isinstance(login_url, str):
                    login_href = login_url
            except Exception as e:
                import logging

                logging.warning(f"Failed to generate OAuth login link: {e}")

        # ✅ Show BOTH buttons (progressive disclosure)
        auth_section = Div(
            # Primary CTA: Try the product
            Button(
                "Try It Free",
                cls=f"{ButtonT.secondary} text-sm",
                onclick="document.querySelector('#analyze-section')?.scrollIntoView({behavior:'smooth'})",
            ),
            # Secondary: Login
            A(
                "Log in",
                href=login_href,
                cls=f"{ButtonT.primary} text-sm",
            ),
            cls="flex items-center gap-2",
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
