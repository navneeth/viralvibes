"""
Reusable modal components with modern glass effects and animations.
"""

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
