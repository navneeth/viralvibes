"""
Controller functions for route handlers.

All controllers are pure functions that:
- Take request data as parameters
- Return FT components or Response objects
- Have no side effects except logging
- Don't use @rt decorators (those stay in main.py)
"""

from .auth_routes import (
    build_login_page,
    build_logout_response,
    require_auth,
)

from .dashboard import (
    view_dashboard_controller,
    list_user_dashboards_controller,
)

# ✅ Re-export for convenience
__all__ = [
    # Auth
    "build_login_page",
    "build_logout_response",
    "require_auth",
    # Dashboard
    "view_dashboard_controller",
    "list_user_dashboards_controller",
]
