"""
OAuth initialization and authentication service.
Handles Google OAuth setup, user storage, and avatar management.
Integrates with Supabase users and auth_providers tables.
"""

import logging
import os
from datetime import datetime

import requests
from fasthtml.common import RedirectResponse
from fasthtml.oauth import GoogleAppClient, OAuth

logger = logging.getLogger(__name__)


class ViralVibesAuth(OAuth):
    """Custom OAuth handler for ViralVibes"""

    def __init__(self, app, client, supabase_client=None, skip=None):
        # ✅ Pass skip to parent OAuth class
        super().__init__(app, client, skip=skip or [])
        self.supabase_client = supabase_client

    def _download_avatar(self, avatar_url: str) -> bytes:
        """
        Download and process user avatar from Google.

        Args:
            avatar_url: URL to the avatar image

        Returns:
            Avatar image bytes or None if download fails
        """
        if not avatar_url:
            return None

        try:
            # Fix Google avatar URL for better compatibility
            if "googleusercontent.com" in avatar_url and "=" in avatar_url:
                base_url = avatar_url.split("=")[0]
                avatar_url = f"{base_url}=s256"  # Request 256x256 size

            response = requests.get(avatar_url, timeout=10)
            if response.status_code == 200:
                logger.info(f"✅ Avatar downloaded successfully from {avatar_url}")
                return response.content
            else:
                logger.warning(
                    f"⚠️  Avatar download failed: HTTP {response.status_code}"
                )
                return None

        except Exception as e:
            logger.warning(f"⚠️  Avatar download error: {e}")
            return None

    def get_auth(self, info, ident, session, state):
        """
        Handle successful Google authentication.

        Stores user info in users table, auth provider in auth_providers table,
        and avatar as blob in storage. Also updates session with user data.

        Args:
            info: User info from Google OAuth
            ident: User identifier from Google
            session: FastHTML session dict
            state: State parameter from OAuth flow
        """
        # Extract user info from Google
        email = info.email or ""
        name = info.name or "User"
        given_name = getattr(info, "given_name", name.split()[0])
        picture_url = getattr(info, "picture", None)
        email_verified = getattr(info, "email_verified", False)

        logger.info(f"✅ User authenticated: {email} (verified: {email_verified})")

        # Only proceed if email is verified
        if not email_verified:
            logger.warning(f"⚠️  Email not verified for {email}")
            return RedirectResponse("/login?error=email_not_verified", status_code=303)

        # Download avatar as blob
        avatar_data = None
        if picture_url:
            avatar_data = self._download_avatar(picture_url)

        # Store user in Supabase
        user_id = None
        avatar_uploaded = False  # Track successful upload, not just download

        if self.supabase_client:
            try:
                # 1. Check if user already exists
                existing_user = self._get_user_by_email(email)

                if existing_user:
                    user_id = existing_user["id"]
                    # Update existing user - only update fields that exist in schema
                    user_data = {
                        "name": name,
                        "email_verified": email_verified,
                        "avatar_url": picture_url,
                        "last_login_at": datetime.utcnow().isoformat(),
                    }
                    self.supabase_client.table("users").update(user_data).eq(
                        "id", user_id
                    ).execute()
                    logger.info(f"✅ Updated existing user {email}")
                else:
                    # Create new user - only insert fields that exist in schema
                    user_data = {
                        "email": email,
                        "name": name,
                        "email_verified": email_verified,
                        "avatar_url": picture_url,
                    }
                    user_response = (
                        self.supabase_client.table("users").insert(user_data).execute()
                    )
                    if user_response.data:
                        user_id = user_response.data[0]["id"]
                        logger.info(f"✅ Created new user {email} with ID {user_id}")

                # 2. Upload avatar blob to storage if available
                if avatar_data and user_id:
                    try:
                        avatar_path = f"avatars/{user_id}/avatar.jpg"
                        self.supabase_client.storage.from_("users").upload(
                            avatar_path, avatar_data
                        )
                        logger.info(f"✅ Uploaded avatar for user {email}")
                        avatar_uploaded = True  # ✅ ONLY set true if upload succeeds
                    except Exception as e:
                        logger.warning(f"⚠️  Failed to upload avatar: {e}")
                        avatar_uploaded = False  # ✅ Track failure explicitly

                # 3. Store or update auth provider info
                if user_id:
                    auth_provider_data = {
                        "user_id": user_id,
                        "provider": "google",
                        "provider_user_id": ident,
                        "provider_email": email,
                        "access_token": session.get("access_token"),
                        "refresh_token": session.get("refresh_token"),
                        "token_expires_at": session.get("token_expires_at"),
                    }

                    self.supabase_client.table("auth_providers").upsert(
                        auth_provider_data, on_conflict="user_id,provider"
                    ).execute()

                    logger.info(f"✅ Stored auth provider info for {email}")

            except Exception as e:
                logger.error(f"❌ Failed to store user {email}: {e}")
                # Don't fail auth if storage fails - user can still proceed

        # ✅ Store user info in session for quick access
        if user_id:
            session["user_id"] = user_id
            session["user_email"] = email
            session["user_name"] = name
            session["user_given_name"] = given_name
            session["avatar_url"] = picture_url  # ✅ Store Google avatar URL for navbar
            logger.info(f"✅ Stored session data for user {email}")

        # ✅ SMART REDIRECT LOGIC
        # 1. Check if there's a stored intended destination
        intended_url = session.get("intended_url")
        if intended_url and intended_url != "/login":
            session.pop("intended_url", None)  # Remove after using
            logger.info(f"Redirecting to intended URL: {intended_url}")
            return RedirectResponse(intended_url, status_code=303)

        # 2. If user logged in from /login route, redirect to homepage
        if state and state.endswith("/login"):
            logger.info("Login from /login route, redirecting to homepage")
            return RedirectResponse("/", status_code=303)

        # 3. Otherwise use state parameter (current page) or default to homepage
        redirect_url = state if state else "/"
        logger.info(f"Redirecting to: {redirect_url}")
        return RedirectResponse(redirect_url, status_code=303)

    def logout(self, session):
        """
        Handle logout.
        Clears session and redirects to homepage.
        """
        logger.info("User logged out")

        # Clear all user-related session data
        session_keys_to_clear = [
            "auth",
            "intended_url",
            "access_token",
            "refresh_token",
            "user_id",
            "user_email",
            "user_name",
            "user_given_name",
            "user_has_avatar",
            "last_login_at",
        ]

        for key in session_keys_to_clear:
            session.pop(key, None)

        # ✅ Always redirect to homepage after logout
        return RedirectResponse("/", status_code=303)

    def _get_user_by_email(self, email: str) -> dict:
        """
        Fetch user from Supabase by email.

        Args:
            email: User email

        Returns:
            User dict or None
        """
        if not self.supabase_client:
            return None

        try:
            response = (
                self.supabase_client.table("users")
                .select("*")
                .eq("email", email)
                .single()
                .execute()
            )
            return response.data if response.data else None
        except Exception as e:
            logger.debug(f"User {email} not found in database (first login)")
            return None

    def _get_auth_provider(self, user_id: str, provider: str = "google") -> dict:
        """
        Fetch auth provider info for a user.

        Args:
            user_id: User ID from users table
            provider: OAuth provider (default: "google")

        Returns:
            Auth provider dict or None
        """
        if not self.supabase_client:
            return None

        try:
            response = (
                self.supabase_client.table("auth_providers")
                .select("*")
                .eq("user_id", user_id)
                .eq("provider", provider)
                .single()
                .execute()
            )
            return response.data if response.data else None
        except Exception as e:
            logger.debug(f"Auth provider not found for user {user_id}: {e}")
            return None


def get_user_by_email(supabase_client, email: str) -> dict:
    """
    Fetch user from Supabase by email.
    Helper function for use outside of Auth class.

    Args:
        supabase_client: Supabase client instance
        email: User email

    Returns:
        User dict or None
    """
    if not supabase_client:
        return None

    try:
        response = (
            supabase_client.table("users")
            .select("*")
            .eq("email", email)
            .single()
            .execute()
        )
        return response.data if response.data else None
    except Exception as e:
        logger.debug(f"Failed to fetch user {email}: {e}")
        return None


def get_auth_provider(supabase_client, user_id: str, provider: str = "google") -> dict:
    """
    Fetch auth provider info for a user.
    Helper function for use outside of Auth class.

    Args:
        supabase_client: Supabase client instance
        user_id: User ID from users table
        provider: OAuth provider (default: "google")

    Returns:
        Auth provider dict or None
    """
    if not supabase_client:
        return None

    try:
        response = (
            supabase_client.table("auth_providers")
            .select("*")
            .eq("user_id", user_id)
            .eq("provider", provider)
            .single()
            .execute()
        )
        return response.data if response.data else None
    except Exception as e:
        logger.debug(f"Failed to fetch auth provider for user {user_id}: {e}")
        return None


def init_google_oauth(app, supabase_client=None):
    """
    Initialize Google OAuth.

    Args:
        app: FastHTML app instance
        supabase_client: Optional Supabase client

    Returns:
        (google_client, oauth_instance) tuple or (None, None)
    """
    client_id = os.getenv("GOOGLE_AUTH_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_AUTH_CLIENT_SECRET")

    if not client_id or not client_secret:
        logger.warning("⚠️  Google OAuth not configured - credentials missing")
        return None, None

    try:
        google_client = GoogleAppClient(client_id, client_secret)

        # ✅ Define public routes that skip authentication
        skip_routes = [
            "/",  # Homepage - public
            "/login",  # Login page - must be public
            "/redirect",  # OAuth callback - required by FastHTML
            "/validate/url",  # Allow URL validation without login
            "/validate/preview",  # Allow previews without login
            "/newsletter",  # Public newsletter signup
            "/debug/supabase",  # Debug endpoint - public
            "/avatar",  # Avatar serving - public
        ]

        oauth = ViralVibesAuth(app, google_client, supabase_client, skip=skip_routes)

        logger.info(
            f"✅ Google OAuth initialized successfully with {len(skip_routes)} public routes"
        )
        return google_client, oauth

    except Exception as e:
        logger.error(f"❌ Failed to initialize OAuth: {e}")
        return None, None
