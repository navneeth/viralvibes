"""
Authentication route handlers and utilities.
Note: @rt decorators stay in main.py, but business logic goes here.
"""

import logging
import os
from urllib.parse import urlsplit

from fasthtml.common import *
from monsterui.all import ButtonT, AlertT

from components import NavComponent
from components.auth_components import OneTapLoginCard

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Login page value-proposition splash
# ---------------------------------------------------------------------------

_LOGIN_SPLASH_FEATURES = [
    ("trending-up", "Growth Blueprint", "Personalised action plan for any channel"),
    ("users", "1M+ Creators", "Filter by category, country, language & grade"),
    ("bar-chart-2", "Deep Analytics", "VPV, viral coefficient, peer benchmarks"),
    ("download", "Outreach Export", "Contact CSV for your saved creator list"),
]


def _login_value_splash() -> Div:
    """Compact feature strip shown above the login card to encourage sign-up."""
    chips = [
        Div(
            UkIcon(icon, cls="w-4 h-4 text-red-500 shrink-0 mt-0.5"),
            Div(
                Span(title, cls="text-sm font-semibold text-foreground block leading-tight"),
                Span(desc, cls="text-xs text-muted-foreground leading-snug"),
            ),
            cls="flex items-start gap-2.5",
        )
        for icon, title, desc in _LOGIN_SPLASH_FEATURES
    ]
    return Div(
        P(
            "Free — no credit card required",
            cls="text-xs font-semibold text-muted-foreground uppercase tracking-widest mb-4 text-center",
        ),
        Div(*chips, cls="grid grid-cols-2 gap-x-8 gap-y-3"),
        cls="w-full max-w-sm mx-auto px-4 pb-6",
    )


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
    subheadline: str = None,
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
        subheadline: Optional context message shown under the headline.

    Returns:
        OneTapLoginCard component
    """
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
        subheadline=subheadline,
    )


def build_logout_response():
    """Build logout response - always redirect to homepage"""
    response = RedirectResponse("/", status_code=303)  # Changed from "/login" to "/"
    response.delete_cookie("auth")
    return response


def require_auth(
    auth, context_label="Sign in to continue", return_url="/login", skip_in_tests=True
):
    """
    Check if user is authenticated.
    Returns a SignInNudge card if not authenticated, None if authenticated.

    Args:
        auth:           Authentication data from session.
        context_label:  Short phrase shown at the top of the nudge card,
                        e.g. "Sign in to analyse playlists".
        return_url:     After login the user is redirected here.
        skip_in_tests:  If True, bypass auth check when TESTING=1 (default True).

    Returns:
        SignInNudge Div if not authenticated, None if authenticated.
    """
    # ✅ In test mode, bypass auth check
    if skip_in_tests and os.getenv("TESTING") == "1":
        return None  # Auth passed

    if not auth:
        from components.modals import SignInNudge  # lazy — avoids circular import

        return SignInNudge(context_label=context_label, return_url=return_url)
    return None


# =============================================================================
# Session normalization for login flows
# =============================================================================


def normalize_intended_url(sess):
    """Normalize stored intended_url for manual login visits.

    If the user manually visits a login route (no intended_url previously set),
    clear any stored intended_url so they get redirected to the homepage
    after login instead of a stale URL.

    Args:
        sess: Session object (optional)
    """
    if sess and not sess.get("intended_url"):
        sess.pop("intended_url", None)


def safe_local_return_url(raw: str | None, default: str = "/") -> str:
    """Return a same-site redirect target, falling back to default.

    Login links can carry ?return_url=/path so OAuth lands users back at the
    moment of intent. Keep this deliberately local to avoid open redirects.
    """
    if not raw:
        return default

    value = str(raw).strip()
    if not value or any(ch in value for ch in "\r\n"):
        return default

    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc:
        return default
    if not parsed.path.startswith("/") or parsed.path.startswith("//"):
        return default
    if parsed.path in {"/login", "/login/new", "/login/onetap", "/redirect"}:
        return default

    return value


def login_subheadline_for_return_url(return_url: str) -> str | None:
    """Contextual login copy for known product-intent redirects."""
    path = urlsplit(return_url or "").path
    if path == "/creators":
        return "Sign in to start free. No credit card required."
    if path == "/me/dashboards":
        return "Sign in to continue to your saved dashboards."
    if path == "/me/favourites":
        return "Sign in to save creators and build shortlists."
    if path == "/me/outreach":
        return "Sign in to export creator contacts for outreach."
    return None


# =============================================================================
# STEP 3: Unified Auth Redirect Page Builder
# =============================================================================


def build_auth_redirect_page(
    oauth,
    req,
    sess=None,
    return_url: str = "/",
    use_new_ui: bool = None,
    subheadline: str = None,
):
    """
    UNIFIED AUTH PAGE BUILDER - Single source of truth for login UI

    Use this everywhere you need to show a login page. Automatically
    includes navbar and handles UI switching.

    Args:
        oauth: OAuth instance
        req: Request object
        sess: Session object (optional, for remembered user data)
        return_url: Where to redirect after login (default: "/")
        use_new_ui: Force UI choice (None = use env var, True/False = override)
        subheadline: Optional context message (e.g. "Sign in to start your Pro trial").

    Returns:
        Full page with NavComponent + auth card (new UI) or button (old UI)

    Environment:
        USE_NEW_LOGIN_UI: Accepts "true", "1", "yes", "on" (case-insensitive)
                         Any other value → Use simple button (legacy)
                         Default: "true" → Use One-Tap card

    Usage:
        # In any route that needs auth redirect:
        if not (sess and sess.get("auth")):
            return build_auth_redirect_page(oauth, req, sess, return_url="/my-path")
    """

    # Respect session's intended_url if present (set by previous routes like /validate/url)
    # Otherwise use the provided return_url parameter, default to "/"
    final_return_url = return_url
    if sess and sess.get("intended_url"):
        final_return_url = sess["intended_url"]

    # Determine which UI to use
    if use_new_ui is None:
        # Flexible env var parsing: accept multiple truthy values
        env_val = os.getenv("USE_NEW_LOGIN_UI", "true").strip().lower()
        use_new_ui = env_val in {"1", "true", "yes", "on"}

    # Build the auth card/button
    if use_new_ui:
        # NEW: One-Tap Material Design card (full-page with centering)
        auth_content = build_onetap_login_page(
            oauth, req, sess, final_return_url, subheadline=subheadline
        )

        # Return full page: navbar + centered card
        # Don't use Container wrapper for new UI, let auth-container handle layout
        return Titled(
            "Sign in to ViralVibes",
            Div(
                NavComponent(oauth, req, sess),
                _login_value_splash(),
                auth_content,  # Already has full-page layout in .auth-container
                cls="auth-page-wrapper",
            ),
        )
    else:
        # OLD: Simple button (legacy) - wrapped in container with navbar
        return Titled(
            "Sign in to ViralVibes",
            Container(
                NavComponent(oauth, req, sess),
                build_login_page(oauth, req),
            ),
        )
