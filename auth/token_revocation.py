"""
Token revocation utilities for Google OAuth.
Allows revoking access tokens for complete logout/disconnection.
"""

import logging

import requests

logger = logging.getLogger(__name__)

GOOGLE_REVOKE_URL = "https://accounts.google.com/o/oauth2/revoke"


def revoke_google_token(access_token: str) -> bool:
    """
    Revoke a Google OAuth access token.

    This disconnects the user from the app at the OAuth provider level,
    requiring full re-authentication on next login.

    Args:
        access_token: The access token to revoke

    Returns:
        True if revocation successful, False otherwise
    """
    if not access_token:
        logger.warning("⚠️  No access token provided for revocation")
        return False

    try:
        response = requests.post(
            GOOGLE_REVOKE_URL, params={"token": access_token}, timeout=10
        )

        if response.status_code == 200:
            logger.info("✅ Google access token revoked successfully")
            return True
        else:
            logger.warning(
                f"⚠️  Token revocation returned status {response.status_code}"
            )
            return False

    except Exception as e:
        logger.error(f"❌ Failed to revoke token: {e}")
        return False


def clear_auth_session(sess: dict) -> None:
    """
    Clear all authentication-related session data.

    Args:
        sess: FastHTML session dictionary
    """
    session_keys_to_clear = [
        "auth",
        "access_token",
        "refresh_token",
        "intended_url",
        "user_id",
        "user_email",
        "user_name",
        "user_given_name",
        "user_has_avatar",
        "last_login_at",
    ]

    for key in session_keys_to_clear:
        sess.pop(key, None)

    logger.info("✅ Session cleared")
