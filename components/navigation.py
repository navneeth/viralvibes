"""
Navigation component with auth-aware rendering.
"""

from fasthtml.common import *
from monsterui.all import *


def NavComponent(auth, oauth, req=None):
    """
    Reusable navigation component with auth-aware rendering.

    Args:
        auth: Current user auth object (None if not logged in)
        oauth: OAuth instance for login link generation
        req: Request object (needed for login link generation)

    Returns:
        NavBar component with appropriate links based on auth status
    """
    # Base navigation links (always visible)
    base_links = [
        A("Why ViralVibes", href="/#home-section"),
        A("Product", href="/#analyze-section"),
        A("About", href="/#explore-section"),
    ]

    # Auth-dependent link
    if auth:
        # User is logged in - show logout
        auth_link = A("Log out", href="/logout", cls=f"{ButtonT.primary}")
    else:
        # User not logged in
        if oauth and req:
            # OAuth configured - show login link
            login_url = oauth.login_link(req)
            auth_link = A("Log in", href=login_url, cls=f"{ButtonT.primary}")
        else:
            # No OAuth - show CTA to scroll to analysis form
            auth_link = Button(
                "Try ViralVibes",
                cls=ButtonT.primary,
                onclick="document.querySelector('#analysis-form')?.scrollIntoView({behavior:'smooth'})",
            )

    # Combine all nav items
    nav_items = base_links + [auth_link]

    return NavBar(
        *nav_items,
        brand=DivLAligned(H3("ViralVibes"), UkIcon("chart-line", height=30, width=30)),
        sticky=True,
        uk_scrollspy_nav=True,
        scrollspy_cls=ScrollspyT.bold,
        cls="backdrop-blur bg-white/60 shadow-sm px-4 py-3",
    )
