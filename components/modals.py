"""
Reusable modal components with modern glass effects and animations.
"""

from urllib.parse import urlencode

from fasthtml.common import *
from monsterui.all import *


def Modal(title: str, *content, modal_id: str = "modal", show_close: bool = True):
    """
    Reusable modal component with backdrop blur.

    Args:
        title: Modal header title
        *content: Child elements for modal body
        modal_id: Unique ID for the modal (default: "modal")
        show_close: Whether to show the X close button (default: True)

    Example:
        >>> Modal(
        ...     "Share Dashboard",
        ...     P("Share this analysis with your team:"),
        ...     Input(value="https://viralvibes.com/d/abc123", readonly=True),
        ...     modal_id="share-modal"
        ... )
    """

    return Div(
        # Backdrop with blur effect
        Div(
            # Modal container
            Div(
                # Header
                Div(
                    H2(title, cls="text-xl font-semibold text-gray-900"),
                    # Close button (optional)
                    (
                        Button(
                            UkIcon("x", cls="w-5 h-5"),
                            onclick=f"document.getElementById('{modal_id}').classList.add('hidden')",
                            cls="text-gray-400 hover:bg-gray-200 hover:text-gray-900 rounded-lg p-1.5 transition-colors",
                            type="button",
                            aria_label="Close modal",
                        )
                        if show_close
                        else None
                    ),
                    cls="flex items-center justify-between p-4 md:p-5 border-b border-gray-200",
                ),
                # Content
                Div(*content, cls="p-4 md:p-6"),
                cls="relative bg-white rounded-lg shadow-xl max-w-2xl w-full mx-4 max-h-[90vh] overflow-y-auto animate-in fade-in zoom-in-95 duration-300",
            ),
            # Click outside to close
            onclick=f"if(event.target === this) document.getElementById('{modal_id}').classList.add('hidden')",
            cls="fixed inset-0 z-50 flex items-center justify-center bg-gray-900/50 backdrop-blur-sm animate-in fade-in duration-300",
        ),
        # Auto-show modal on render
        Script(f"document.getElementById('{modal_id}').classList.remove('hidden')"),
        id=modal_id,
        cls="modal-container hidden",  # ✅ Start hidden
    )


def ShareModal(dashboard_url: str, playlist_name: str, modal_id: str = "share-modal"):
    """
    Pre-built share modal for dashboards.

    Args:
        dashboard_url: Full URL to the dashboard
        playlist_name: Name of the playlist being shared
        modal_id: Unique ID for this modal instance

    Example:
        >>> ShareModal(
        ...     dashboard_url="https://viralvibes.com/d/abc123",
        ...     playlist_name="My Awesome Playlist"
        ... )
    """

    return Modal(
        "Share Dashboard",
        # Description
        P(
            f"Share this analysis of '{playlist_name}' with your team:",
            cls="text-gray-600 mb-4",
        ),
        # Copy link section
        Div(
            Label("Dashboard Link", cls="block text-sm font-medium text-gray-700 mb-2"),
            Div(
                Input(
                    value=dashboard_url,
                    readonly=True,
                    id="share-url-input",
                    cls="flex-1 px-4 py-2 border border-gray-300 rounded-l-lg focus:outline-none focus:ring-2 focus:ring-red-500 bg-gray-50",
                ),
                Button(
                    UkIcon("clipboard", cls="mr-2 w-4 h-4"),
                    Span("Copy", id="copy-btn-text"),
                    onclick=f"""
                        const input = document.getElementById('share-url-input');
                        const btn = document.getElementById('copy-btn-text');

                        navigator.clipboard.writeText(input.value).then(() => {{
                            btn.textContent = '✓ Copied!';
                            setTimeout(() => {{ btn.textContent = 'Copy'; }}, 2000);
                        }});
                    """,
                    cls="px-4 py-2 bg-red-600 text-white rounded-r-lg hover:bg-red-700 transition-colors flex items-center",
                    type="button",
                ),
                cls="flex",
            ),
            cls="mb-6",
        ),
        # Social sharing buttons (optional)
        Div(
            P("Or share via:", cls="text-sm font-medium text-gray-700 mb-3"),
            Div(
                # Twitter
                A(
                    UkIcon("twitter", cls="w-5 h-5 mr-2"),
                    "Twitter",
                    href=f"https://twitter.com/intent/tweet?url={dashboard_url}&text=Check%20out%20this%20YouTube%20playlist%20analysis!",
                    target="_blank",
                    cls="flex items-center px-4 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors",
                ),
                # LinkedIn
                A(
                    UkIcon("linkedin", cls="w-5 h-5 mr-2"),
                    "LinkedIn",
                    href=f"https://www.linkedin.com/sharing/share-offsite/?url={dashboard_url}",
                    target="_blank",
                    cls="flex items-center px-4 py-2 bg-blue-700 text-white rounded-lg hover:bg-blue-800 transition-colors",
                ),
                # Email
                A(
                    UkIcon("mail", cls="w-5 h-5 mr-2"),
                    "Email",
                    href=f"mailto:?subject=YouTube%20Playlist%20Analysis&body=Check%20out%20this%20analysis:%20{dashboard_url}",
                    cls="flex items-center px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors",
                ),
                cls="flex flex-wrap gap-3",
            ),
        ),
        modal_id=modal_id,
    )


def ExportModal(dashboard_id: str, playlist_name: str, modal_id: str = "export-modal"):
    """
    Pre-built export modal for downloading data.

    Args:
        dashboard_id: Dashboard ID for export endpoints
        playlist_name: Name of the playlist
        modal_id: Unique ID for this modal instance
    """

    return Modal(
        "Export Data",
        P(
            f"Download data for '{playlist_name}' in your preferred format:",
            cls="text-gray-600 mb-6",
        ),
        # Export options
        Div(
            # CSV Export
            A(
                Div(
                    UkIcon("file-text", cls="w-8 h-8 text-green-600 mb-2"),
                    H3("CSV", cls="text-lg font-semibold text-gray-900"),
                    P("Spreadsheet-friendly format", cls="text-sm text-gray-500"),
                    cls="text-center",
                ),
                href=f"/export/{dashboard_id}/csv",
                download=f"{playlist_name}.csv",
                cls="block p-6 border-2 border-gray-200 rounded-lg hover:border-green-500 hover:shadow-lg transition-all cursor-pointer",
            ),
            # JSON Export
            A(
                Div(
                    UkIcon("code", cls="w-8 h-8 text-blue-600 mb-2"),
                    H3("JSON", cls="text-lg font-semibold text-gray-900"),
                    P("Developer-friendly format", cls="text-sm text-gray-500"),
                    cls="text-center",
                ),
                href=f"/export/{dashboard_id}/json",
                download=f"{playlist_name}.json",
                cls="block p-6 border-2 border-gray-200 rounded-lg hover:border-blue-500 hover:shadow-lg transition-all cursor-pointer",
            ),
            # PDF Export (future)
            Div(
                Div(
                    UkIcon("file", cls="w-8 h-8 text-gray-400 mb-2"),
                    H3("PDF", cls="text-lg font-semibold text-gray-400"),
                    P("Coming soon", cls="text-sm text-gray-400"),
                    cls="text-center",
                ),
                cls="p-6 border-2 border-gray-200 rounded-lg opacity-50 cursor-not-allowed",
            ),
            cls="grid grid-cols-1 md:grid-cols-3 gap-4",
        ),
        modal_id=modal_id,
    )


# ---------------------------------------------------------------------------
# AuthModal — soft sign-in gate
# ---------------------------------------------------------------------------
# Shown in-place (HTMX OOB) when an anonymous user tries a protected action
# such as saving a creator.  Replaces the hard 401 redirect with a contextual
# "here is what you unlock" overlay that lets them stay on the page.
# ---------------------------------------------------------------------------

_AUTH_BENEFITS = [
    ("heart", "Save creators to your personal watchlist"),
    ("mail", "Access extracted contact emails"),
    ("download", "Export CSV for outreach campaigns"),
    ("git-compare", "Compare any two creators side by side"),
]


def AuthModal(
    *,
    modal_id: str = "auth-modal",
    return_url: str = "/creators",
    context_label: str = "Sign in to save creators",
):
    """Soft auth gate modal — shown instead of a hard /login redirect.

    Displays the four key benefits of a free ViralVibes account, a Google
    sign-in CTA, and a "Maybe later" dismiss link.  Designed to be injected
    via HTMX ``hx-swap-oob`` into a ``<div id="auth-modal-mount">`` already
    present on the page so no full reload is needed.

    Args:
        modal_id:      DOM id for the modal wrapper (default ``auth-modal``).
        return_url:    After successful login, redirect back to this URL.
        context_label: Short phrase shown above the headline, e.g.
                       "Sign in to save this creator".
    """
    from components.auth_components import GoogleGLogo  # lazy — avoids circular import

    login_href = f"/login?{urlencode({'return_url': return_url})}"
    close_js = f"document.getElementById('{modal_id}').classList.add('hidden')"

    def _benefit(icon: str, text: str):
        return Div(
            Div(
                UkIcon(icon, cls="w-4 h-4 text-blue-600"),
                cls="flex-shrink-0 w-8 h-8 rounded-full bg-blue-50 flex items-center justify-center",
            ),
            P(text, cls="text-sm text-foreground leading-snug"),
            cls="flex items-center gap-3",
        )

    return Div(
        # ── Backdrop ──────────────────────────────────────────────────────────
        Div(
            # ── Panel ─────────────────────────────────────────────────────────
            Div(
                # Header band — gradient accent
                Div(
                    Div(
                        # Eyebrow
                        P(
                            context_label,
                            cls="text-xs font-mono uppercase tracking-[0.16em] text-blue-200 mb-2",
                        ),
                        H2(
                            "One click away.",
                            id=f"{modal_id}-heading",
                            cls="text-2xl font-bold text-white leading-tight",
                        ),
                        P(
                            "Free forever — no credit card required.",
                            cls="text-sm text-blue-100 mt-1",
                        ),
                        cls="",
                    ),
                    # Close button (top-right)
                    Button(
                        UkIcon("x", cls="w-4 h-4"),
                        onclick=close_js,
                        type="button",
                        aria_label="Close",
                        cls=(
                            "absolute top-4 right-4 p-1.5 rounded-lg "
                            "text-blue-200 hover:text-white hover:bg-white/10 transition-colors"
                        ),
                    ),
                    cls=(
                        "relative px-6 py-6 "
                        "bg-gradient-to-br from-blue-600 via-blue-700 to-indigo-700 "
                        "rounded-t-xl"
                    ),
                ),
                # Body
                Div(
                    # What you unlock
                    P(
                        "After signing in you can:",
                        cls="text-xs font-semibold uppercase tracking-widest text-muted-foreground mb-4",
                    ),
                    Div(
                        *[_benefit(icon, text) for icon, text in _AUTH_BENEFITS],
                        cls="flex flex-col gap-3 mb-6",
                    ),
                    # Google CTA
                    A(
                        GoogleGLogo(18),
                        Span("Continue with Google", cls="ml-2 text-sm font-semibold"),
                        href=login_href,
                        cls=(
                            "flex items-center justify-center w-full px-4 py-3 "
                            "bg-white border border-gray-200 hover:border-gray-300 "
                            "hover:shadow-md text-gray-800 rounded-lg transition-all "
                            "no-underline"
                        ),
                    ),
                    # Footer row
                    Div(
                        Span(
                            UkIcon("lock", cls="w-3 h-3 mr-1 inline"),
                            "Secure Google sign-in",
                            cls="text-xs text-muted-foreground",
                        ),
                        Button(
                            "Maybe later",
                            onclick=close_js,
                            type="button",
                            cls="text-xs text-muted-foreground hover:text-foreground underline underline-offset-2 bg-transparent border-0 cursor-pointer",
                        ),
                        cls="flex items-center justify-between mt-4",
                    ),
                    cls="px-6 py-5",
                ),
                cls=(
                    "relative bg-background rounded-xl shadow-2xl "
                    "w-full max-w-sm mx-4 "
                    "animate-in fade-in zoom-in-95 duration-200"
                ),
                role="dialog",
                aria_modal="true",
                aria_labelledby=f"{modal_id}-heading",
            ),
            onclick=f"if(event.target===this){{{close_js}}}",
            cls=(
                "fixed inset-0 z-50 flex items-center justify-center "
                "bg-gray-900/60 backdrop-blur-sm animate-in fade-in duration-200"
            ),
        ),
        Script(
            f"""
(function() {{
    var el = document.getElementById('{modal_id}');
    el.classList.remove('hidden');
    var closeBtn = el.querySelector('button[aria-label="Close"]');
    if (closeBtn) {{ closeBtn.focus(); }}
    function _onKey(e) {{
        if (e.key === 'Escape') {{
            {close_js};
            document.removeEventListener('keydown', _onKey);
        }}
    }}
    document.addEventListener('keydown', _onKey);
}})();
"""
        ),
        id=modal_id,
        cls="modal-container hidden",
    )


# ---------------------------------------------------------------------------
# SignInNudge — inline sign-in invitation card
# ---------------------------------------------------------------------------
# Used wherever an unauthenticated user tries to take a protected action
# (playlist analysis, suggest a creator, etc.).  Unlike AuthModal it is not
# a floating overlay — it renders inline inside the HTMX swap target so the
# rest of the page stays visible and the prompt feels like a natural next step
# rather than a hard gate.
# ---------------------------------------------------------------------------


def SignInNudge(
    *,
    context_label: str = "Sign in to continue",
    return_url: str = "/login",
) -> Div:
    """Compact inline sign-in card returned by HTMX action endpoints.

    Args:
        context_label: Short phrase describing why sign-in is needed, e.g.
                       ``"Sign in to analyse playlists"``.
        return_url:    URL the login flow redirects back to after auth.
    """
    from components.auth_components import GoogleGLogo  # lazy — avoids circular import

    login_href = f"/login?{urlencode({'return_url': return_url})}"

    return Div(
        # Thin gradient header strip
        Div(
            P(
                context_label,
                cls="text-xs font-mono uppercase tracking-[0.14em] text-white/90",
            ),
            cls=("px-4 py-2.5 " "bg-gradient-to-r from-blue-600 to-indigo-600 " "rounded-t-xl"),
        ),
        # Body
        Div(
            P(
                "Free forever — no credit card required.",
                cls="text-sm text-muted-foreground mb-4",
            ),
            A(
                GoogleGLogo(18),
                Span("Continue with Google", cls="ml-2 text-sm font-semibold"),
                href=login_href,
                cls=(
                    "flex items-center justify-center w-full px-4 py-2.5 "
                    "bg-white border border-gray-200 hover:border-gray-300 "
                    "hover:shadow-sm text-gray-800 rounded-lg transition-all no-underline"
                ),
            ),
            cls="px-4 py-4",
        ),
        cls=(
            "rounded-xl border border-border bg-background shadow-sm "
            "max-w-xs mx-auto my-4 overflow-hidden"
        ),
    )
