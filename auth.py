import os, secrets
from urllib.parse import urlparse
from dotenv import load_dotenv
from fasthtml.oauth import GoogleAppClient, OAuth
from starlette.responses import RedirectResponse

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


class Auth(OAuth):
    def get_auth(self, info, ident, session, state):
        # Validate nonce
        nonce = session.pop("auth_nonce", None)
        if not nonce or nonce != ident.nonce:
            return RedirectResponse("/login", status_code=303)

        if not getattr(info, "email_verified", False):
            return RedirectResponse("/login", status_code=303)

        if hasattr(self, "cli") and hasattr(self.cli, "token"):
            session["access_token"] = self.cli.token.get("access_token")

        target = _safe_state_target(state)
        return RedirectResponse(target, status_code=303)

    def logout(self, session):
        session.pop("access_token", None)
        session.pop("auth", None)
        return RedirectResponse("/", status_code=303)
