"""
OAuth initialization and authentication service.
Handles Google OAuth setup and user storage.
"""

import logging
import os
from datetime import datetime

from fasthtml.common import RedirectResponse
from fasthtml.oauth import GoogleAppClient, OAuth

logger = logging.getLogger(__name__)


class ViralVibesAuth(OAuth):
    """Custom OAuth handler for ViralVibes"""

    def __init__(self, app, client, supabase_client=None, skip=None):
        # ✅ Pass skip to parent OAuth class
        super().__init__(app, client, skip=skip or [])
        self.supabase_client = supabase_client

    def get_auth(self, info, ident, session, state):
        """Handle successful Google authentication"""
        email = info.email or ""
        name = info.name or "User"

        logger.info(f"✅ User authenticated: {email}")

        # Store user in Supabase
        if self.supabase_client:
            try:
                self.supabase_client.table("users").upsert(
                    {
                        "email": email,
                        "name": name,
                        "google_id": ident,
                        "picture": info.picture,
                        "updated_at": datetime.utcnow().isoformat(),
                    }
                ).execute()
                logger.info(f"✅ Stored user {email} in database")
            except Exception as e:
                logger.error(f"Failed to store user {email}: {e}")

        return RedirectResponse("/", status_code=303)


def init_google_oauth(app, supabase_client=None):
    """
    Initialize Google OAuth.

    Returns:
        (google_client, oauth_instance) tuple or (None, None)
    """
    client_id = os.getenv("GOOGLE_AUTH_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_AUTH_CLIENT_SECRET")

    if not client_id or not client_secret:
        logger.warning("⚠️  Google OAuth not configured")
        return None, None

    try:
        google_client = GoogleAppClient(client_id, client_secret)

        # ✅ Define public routes that skip authentication
        skip_routes = [
            "/",  # Homepage - public
            "/login",  # Login page - must be public
            "/redirect",  # OAuth callback - required by FastHTML
            "/validate/url",  # Allow previews without login
            "/validate/preview",  # Allow previews without login
            "/newsletter",  # Public newsletter signup
        ]

        oauth = ViralVibesAuth(
            app, google_client, supabase_client, skip=skip_routes  # ✅ Pass skip routes
        )

        logger.info(
            f"✅ Google OAuth initialized with {len(skip_routes)} public routes"
        )
        return google_client, oauth
    except Exception as e:
        logger.error(f"❌ Failed to initialize OAuth: {e}")
        return None, None
