"""
Navigation component with auth-aware rendering and user profile display.
Shows personalized content when user is logged in (avatar dropdown with menu).
"""

import logging

from fasthtml.common import *
from monsterui.all import *

from .auth_dropdown import AuthDropdown


def _nav_link_cls(req, path):
    """Return Tailwind classes for a nav link, highlighting the active route."""
    if req and req.url.path == path:
        return "text-sm font-semibold text-red-600"
    return "text-sm text-gray-700 hover:text-red-600 transition-colors duration-150"


def TopAlertBar():
    """
    Full-width announcement bar displayed above the navbar on the homepage.
    Uses the CSS 100vw escape trick to break out of any containing element.
    Dismissable via close button with no dependencies.
    """
    return Div(
        Div(
            Span("🚀", cls="mr-1"),
            Span("Now tracking "),
            Span("1 Mil+ YouTube creators", cls="font-semibold"),
            Span(" across 150+ countries — "),
            A(
                "Explore free →",
                href="/creators",
                cls="font-semibold underline underline-offset-2 hover:no-underline",
            ),
            cls="flex items-center justify-center gap-x-1 text-sm text-white flex-wrap px-8",
        ),
        Button(
            "×",
            onclick="document.getElementById('vv-alert-bar').style.display='none'",
            cls="absolute right-4 top-1/2 -translate-y-1/2 text-white/70 hover:text-white text-xl leading-none transition-colors cursor-pointer",
            type="button",
            aria_label="Dismiss",
        ),
        cls="vv-alert-bar",
        id="vv-alert-bar",
    )


def NavComponent(oauth, req=None, sess=None):
    """
    Reusable navigation component with auth-aware rendering.

    Product-pillar order: Creators → Lists → Analyze → Why ViralVibes
    Active route is highlighted in red.

    When logged in:
    - Shows avatar dropdown with My Dashboards, Settings, Logout, Revoke
    - If user is admin: Shows Admin Dashboard link in dropdown

    When logged out:
    - Primary CTA "Explore Creators" (no friction, goes straight to product)
    - Secondary "Sign in" text link

    Args:
        oauth: OAuth instance for login link generation
        req: Request object (needed for login link generation and active state)
        sess: Session dict (contains auth status and user info)

    Returns:
        NavBar component with appropriate links based on auth status
    """
    # Product-pillar first, discovery links secondary
    base_links = [
        A("Creators", href="/creators", cls=_nav_link_cls(req, "/creators")),
        A("Lists", href="/lists", cls=_nav_link_cls(req, "/lists")),
        A("Analyze", href="/analysis", cls=_nav_link_cls(req, "/analysis")),
        A(
            "Why ViralVibes",
            href="/#home-section",
            cls="text-sm text-gray-500 hover:text-gray-800 transition-colors duration-150",
        ),
    ]

    # Check auth from session
    is_authenticated = bool(sess and sess.get("auth"))

    # ============================================================================
    # Build auth section (right side of navbar)
    # ============================================================================

    # Get login URL with fallback (used by both AuthDropdown and fallback button)
    login_href = "/login"
    if oauth and req:
        try:
            login_url = oauth.login_link(req)
            if login_url and isinstance(login_url, str):
                login_href = login_url
        except Exception as e:
            logging.warning(f"Failed to generate OAuth login link: {e}")

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

        # is_admin is cached in session at login — no DB call here
        is_admin = bool(sess.get("is_admin"))

        # Use the dropdown component with OAuth-aware login URL for consistency
        auth_section = AuthDropdown(
            user=user_data,
            avatar_url=avatar_url,
            login_href=login_href,
            is_admin=is_admin,
        )

    else:
        # ❌ LOGGED OUT: Single conversion action — Sign in.
        # Nav links already cover all discovery destinations. The only thing
        # missing for a guest is a way in. Mirroring Linear/Vercel/Notion pattern:
        # nav handles "where to go", CTA handles "what to do" — they never overlap.
        # Wrapped in a Div to match the structural weight of AuthDropdown so
        # NavBar's internal flex layout treats both auth states consistently.
        auth_section = Div(
            A(
                "Sign in",
                href=login_href,
                cls="btn-cta-primary text-sm",
            ),
        )

    # ============================================================================
    # Combine all nav items and build navbar
    # ============================================================================
    return NavBar(
        *base_links,
        auth_section,
        brand=DivLAligned(
            H3("ViralVibes", cls="text-lg font-bold"),
            UkIcon("chart-line", height=28, width=28),
            Span(
                "1M+ creators",
                cls="hidden lg:inline-flex ml-2 px-2 py-0.5 text-xs font-medium bg-red-50 text-red-600 border border-red-100 rounded-full",
            ),
        ),
        sticky=True,
        uk_scrollspy_nav=True,
        scrollspy_cls=ScrollspyT.bold,
        cls="backdrop-blur bg-white/60 shadow-sm px-4 py-3 border-b border-gray-200 z-50",
    )
