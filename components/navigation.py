"""
Navigation component with auth-aware rendering and user profile display.
Shows personalized content when user is logged in (avatar dropdown with menu).
"""

import logging
from urllib.parse import parse_qs, urlencode

from fasthtml.common import *
from monsterui.all import *

from routes.admin import _is_admin  # ✅ Import admin check

from .auth_dropdown import AuthDropdown


def _current_return_url(req) -> str:
    """Return the current same-site path for post-login continuation."""
    if not req:
        return "/"
    path = req.url.path or "/"
    if path in {"/login", "/login/new", "/login/onetap", "/signup", "/redirect"}:
        query = parse_qs(getattr(req.url, "query", ""))
        return (query.get("return_url") or query.get("return_to") or ["/"])[0]
    query = getattr(req.url, "query", "")
    return f"{path}?{query}" if query else path


def _branded_login_href(req) -> str:
    return f"/login?{urlencode({'return_url': _current_return_url(req)})}"


def _branded_signup_href(req) -> str:
    return f"/signup?{urlencode({'return_url': _current_return_url(req)})}"


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
    - Provides a clear "Create free account" action and a separate login link.
    - Discovery links remain available without signing in.

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

    # Keep account creation and returning-user sign-in as distinct entry points.
    login_href = _branded_login_href(req)
    signup_href = _branded_signup_href(req)

    if is_authenticated:
        # ✅ LOGGED IN: Use AuthDropdown component

        # Extract user data from session (populated by auth_service.py)
        user_id = sess.get("user_id")
        user_data = {
            "user_id": user_id,
            "user_name": sess.get("user_name"),
            "user_given_name": sess.get("user_given_name"),
            "user_email": sess.get("user_email"),
        }

        # Get avatar URL from session (set by auth_service.py)
        avatar_url = sess.get("avatar_url")

        # Check admin status — try session cache first (fast), then DB (fresh)
        is_admin = bool(sess.get("is_admin"))  # Cached at login
        if not is_admin and user_id:
            # Fallback: check DB directly (in case they were just granted access)
            try:
                is_admin = _is_admin(user_id)
                if is_admin:
                    logging.info(
                        f"[NavComponent] ✅ Admin link will show for {sess.get('user_email')}"
                    )
            except Exception as e:
                logging.warning(f"[NavComponent] Failed to check admin status for {user_id}: {e}")
                is_admin = False

        if not is_admin:
            logging.debug(
                f"[NavComponent] No admin link for {sess.get('user_email')} (is_admin={is_admin})"
            )

        # Use the dropdown component with OAuth-aware login URL for consistency
        auth_section = AuthDropdown(
            user=user_data,
            avatar_url=avatar_url,
            login_href=login_href,
            is_admin=is_admin,
        )

    else:
        # Discovery stays open; account actions are explicit and distinct.
        auth_section = Div(
            A(
                "Log in",
                href=login_href,
                rel="nofollow",
                cls=(
                    "text-sm font-semibold text-gray-700 hover:text-red-600 "
                    "transition-colors duration-150"
                ),
            ),
            A(
                "Create free account",
                href=signup_href,
                rel="nofollow",
                cls="btn-cta-primary btn-nav-signup",
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
