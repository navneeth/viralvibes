"""
Controller functions for route handlers.
"""

from .auth_routes import (
    build_login_page,
    build_logout_response,
    require_auth,
)

# ✅ Re-export for convenience
__all__ = [
    "build_login_page",
    "build_logout_response",
    "require_auth",
]
