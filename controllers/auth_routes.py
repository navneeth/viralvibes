"""
Authentication route handlers and utilities.
Note: @rt decorators stay in main.py, but business logic goes here.
"""

import logging
import os

from fasthtml.common import *
from monsterui.all import ButtonT, AlertT


logger = logging.getLogger(__name__)


def build_login_page(oauth, req):
    """Build login page content - ORIGINAL STABLE VERSION"""
    if oauth:
        login_link = oauth.login_link(req)
    else:
        login_link = Div(P("Google OAuth not configured", cls="text-red-600"))

    return Titled(
        "ViralVibes - Login",
        Container(
            Div(
                H1("Login to ViralVibes", cls="text-3xl font-bold mb-6"),
                A(
                    "Sign in with Google",
                    href=login_link if isinstance(login_link, str) else "#",
                    cls=f"{ButtonT.primary} inline-block",
                ),
                cls="max-w-md mx-auto mt-20",
            ),
        ),
    )


def build_onetap_login_page(
    oauth,
    req,
    sess=None,
    return_url: str = None,
    remembered_email: str = None,
):
    """Build One-Tap login page - NEW OPTIONAL ENHANCEMENT

    This is the new Material Design 3 login UI.
    Use this instead of build_login_page() when ready to migrate.

    Args:
        oauth: OAuth instance
        req: Request object
        sess: Session object (for remembered email)
        return_url: URL to redirect after login
        remembered_email: Last logged-in email (optional)

    Returns:
        OneTapLoginCard component
    """
    from components.auth_components import OneTapLoginCard

    # Get remembered user info from session/cookie
    if sess and not remembered_email:
        remembered_email = sess.get("last_email")

    remembered_avatar = None
    remembered_user_id = None

    if remembered_email and sess:
        remembered_user_id = sess.get("last_user_id")
        if remembered_user_id:
            remembered_avatar = f"/avatar/{remembered_user_id}"

    # Get OAuth login link
    if oauth:
        login_link = oauth.login_link(req)
    else:
        login_link = "#"
        logger.warning("OAuth not configured - login will not work")

    # Build One-Tap card
    return OneTapLoginCard(
        oauth_login_link=login_link,
        site_name="ViralVibes",
        logo_src="/static/ViralVibes_Logo.png",
        return_url=return_url,
        remembered_email=remembered_email,
        remembered_avatar=remembered_avatar,
        remembered_user_id=remembered_user_id,
    )


def build_logout_response():
    """Build logout response - always redirect to homepage"""
    response = RedirectResponse("/", status_code=303)  # Changed from "/login" to "/"
    response.delete_cookie("auth")
    return response


def require_auth(auth, error_message="Please log in.", skip_in_tests=True):
    """
    Check if user is authenticated.
    Returns alert if not authenticated.

    Args:
        auth: Authentication data from session
        error_message: Custom error message
        skip_in_tests: If True, skip auth check when TESTING=1 (default: True)

    Returns:
        Alert if not authenticated, None if authenticated
    """
    # ✅ In test mode, bypass auth check
    if skip_in_tests and os.getenv("TESTING") == "1":
        return None  # Auth passed

    if not auth:
        return Alert(P(error_message), cls=AlertT.warning)
    return None
