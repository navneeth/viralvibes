"""
Unified error and alert components.
Standardizes error display across the application.
"""

from fasthtml.common import *
from monsterui.all import *


def ErrorAlert(
    title: str,
    message: str,
    type: str = "error",
    show_home_link: bool = True,
) -> Div:
    """
    Standardized error/alert display component.

    Args:
        title: Error title (e.g., "Dashboard Not Found")
        message: Detailed error message
        type: Alert type - "error", "warning", "info", "success"
        show_home_link: Whether to show link back to homepage

    Returns:
        Styled alert Div component

    Example:
        >>> ErrorAlert("Not Found", "Dashboard does not exist")
    """
    color_schemes = {
        "error": {
            "bg": "bg-red-50",
            "border": "border-red-200",
            "text": "text-red-900",
            "icon": "⚠️",
        },
        "warning": {
            "bg": "bg-yellow-50",
            "border": "border-yellow-200",
            "text": "text-yellow-900",
            "icon": "⚠️",
        },
        "info": {
            "bg": "bg-blue-50",
            "border": "border-blue-200",
            "text": "text-blue-900",
            "icon": "ℹ️",
        },
        "success": {
            "bg": "bg-green-50",
            "border": "border-green-200",
            "text": "text-green-900",
            "icon": "✓",
        },
    }

    scheme = color_schemes.get(type, color_schemes["error"])

    content = [
        # Icon + Title
        Div(
            Span(scheme["icon"], cls="text-3xl mb-3"),
            H2(title, cls=f"text-2xl font-bold mb-2 {scheme['text']}"),
            cls="flex flex-col items-center",
        ),
        # Message
        P(message, cls=f"text-sm mb-4 {scheme['text']}"),
    ]

    # Optional home link
    if show_home_link:
        content.append(
            Div(
                A(
                    Button("← Back to Home", cls=ButtonT.secondary),
                    href="/",
                    cls="no-underline",
                ),
                cls="mt-4",
            )
        )

    return Div(
        *content,
        cls=f"max-w-xl mx-auto mt-24 text-center p-8 rounded-lg border {scheme['bg']} {scheme['border']}",
    )


def get_user_friendly_error(error_text: str) -> dict:
    """
    Convert technical error messages to user-friendly display data.

    Args:
        error_text: Technical error message from exception

    Returns:
        Dict with title, message, and suggestions for ErrorAlert

    Example:
        >>> error_data = get_user_friendly_error("Network timeout")
        >>> ErrorAlert(**error_data)
    """
    error_lower = (error_text or "").lower()

    error_mappings = {
        "network": {
            "title": "Network Connection Issue",
            "message": "We had trouble connecting to YouTube. This might be a temporary network issue or YouTube rate limiting.",
            "suggestions": [
                "Check your internet connection",
                "Try a different playlist",
                "Wait a few minutes and retry",
            ],
        },
        "timeout": {
            "title": "Request Timed Out",
            "message": "YouTube took too long to respond. This usually happens with very large playlists.",
            "suggestions": [
                "Try analyzing a smaller playlist first",
                "Wait and try again later",
                "Check if the playlist is public",
            ],
        },
        "blocked": {
            "title": "YouTube Bot Protection",
            "message": "YouTube detected our request as bot activity. This is common with large playlist analyses.",
            "suggestions": [
                "Wait 5-10 minutes before trying again",
                "Try a smaller playlist",
                "Enable JavaScript for better compatibility",
            ],
        },
        "bot challenge": {
            "title": "YouTube Bot Protection",
            "message": "YouTube detected our request as bot activity. This is common with large playlist analyses.",
            "suggestions": [
                "Wait 5-10 minutes before trying again",
                "Try a smaller playlist",
            ],
        },
        "not found": {
            "title": "Playlist Not Found",
            "message": "We couldn't find or access this playlist. It might be private or deleted.",
            "suggestions": [
                "Verify the playlist URL is correct",
                "Make sure the playlist is public",
                "Try a different playlist",
            ],
        },
        "private": {
            "title": "Private Playlist",
            "message": "This playlist is private and cannot be analyzed.",
            "suggestions": [
                "Make sure the playlist is public",
                "Try a different playlist",
            ],
        },
    }

    # Match error type (check all patterns)
    for pattern, error_data in error_mappings.items():
        if pattern in error_lower:
            return {
                "title": error_data["title"],
                "message": error_data["message"],
                "type": "warning",
            }

    # Default error
    return {
        "title": "Analysis Failed",
        "message": error_text or "An unexpected error occurred. Please try again.",
        "type": "error",
    }
