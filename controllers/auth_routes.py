"""
Authentication route handlers and utilities.
Note: @rt decorators stay in main.py, but business logic goes here.
"""

import logging
from fasthtml.common import RedirectResponse, Div, P, A, H1, Alert, AlertT
from monsterui.all import ButtonT
from fasthtml.common import Titled, Container

logger = logging.getLogger(__name__)


def build_login_page(oauth, req):
    """Build login page content"""
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


def build_logout_response():
    """Build logout response"""
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie("auth")
    return response


def require_auth(auth, error_message="Please log in."):
    """
    Check if user is authenticated.
    Returns alert if not authenticated.
    """
    if not auth:
        return Alert(P(error_message), cls=AlertT.warning)
    return None
