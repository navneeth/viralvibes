import os, secrets
from urllib.parse import urlparse
from dotenv import load_dotenv
from fasthtml.oauth import GoogleAppClient, OAuth
from starlette.responses import RedirectResponse
import requests
from db import upsert_user

load_dotenv()

client = GoogleAppClient(
    os.getenv("GOOGLE_AUTH_CLIENT_ID"),
    os.getenv("GOOGLE_AUTH_CLIENT_SECRET"),
    redirect_uri=os.getenv("GOOGLE_AUTH_REDIRECT"),
)

ALLOWED_PATH_PREFIX = "/"


def _safe_state_target(raw: str | None) -> str:
    if not raw:
        return "/"
    p = urlparse(raw)
    if p.netloc or not p.path.startswith(ALLOWED_PATH_PREFIX):
        return "/"
    return p.path or "/"


def _fetch_avatar(url: str | None) -> bytes | None:
    if not url:
        return None
    if "googleusercontent.com" in url and "=" in url:
        url = url.split("=")[0] + "=s64"
    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200 and len(r.content) <= 50_000:
            return r.content
    except Exception:
        return None
    return None


class Auth(OAuth):
    def get_auth(self, info, ident, session, state):
        if not getattr(info, "email_verified", False):
            return RedirectResponse("/login", status_code=303)
        email = info.email
        display_name = (
            getattr(info, "name", None)
            or getattr(info, "given_name", None)
            or email.split("@")[0]
        )
        avatar_bytes = _fetch_avatar(getattr(info, "picture", None))
        user_row = upsert_user(
            email, display_name, getattr(info, "given_name", None), avatar_bytes
        )
        if user_row:
            session["auth"] = True
            session["user_email"] = email
            session["user_id"] = user_row.get("id")
            session["user_name"] = user_row.get("name")
            session["user_has_avatar"] = user_row.get("has_avatar")
            session["avatar_url"] = user_row.get("avatar_url")
        if hasattr(self, "cli") and hasattr(self.cli, "token"):
            session["access_token"] = self.cli.token.get("access_token")
        # Safe relative redirect
        target = "/"
        if state:
            from urllib.parse import urlparse

            p = urlparse(state)
            if not p.netloc and p.path.startswith("/"):
                target = p.path or "/"
        return RedirectResponse(target, status_code=303)

    def logout(self, session):
        for k in [
            "auth",
            "user_email",
            "user_id",
            "user_name",
            "user_has_avatar",
            "avatar_url",
            "access_token",
        ]:
            session.pop(k, None)
        return RedirectResponse("/", status_code=303)
