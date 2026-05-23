"""
Saved creator outreach routes.
"""

from __future__ import annotations

import os

from fasthtml.common import RedirectResponse
from starlette.responses import Response as StarletteResponse

from db import get_user_favourite_creators
from services.outreach import build_outreach_rows, filter_email_ready_rows, render_outreach_csv
from views.outreach import render_outreach_page

_IS_TESTING = os.getenv("TESTING") == "1"


def _base_url(request) -> str:
    url = getattr(request, "url", None)
    if not url:
        return "https://www.viralvibes.fyi"
    return f"{url.scheme}://{url.netloc}"


def outreach_route(req, sess):
    """GET /me/outreach — export-focused outreach workspace."""
    user_id = sess.get("user_id") if sess else None
    auth = sess.get("auth") if sess else None
    user_name = sess.get("user_name", "User") if sess else "User"

    if not auth or not user_id:
        if _IS_TESTING:
            user_id = user_id or "test-user-id"
        else:
            sess["intended_url"] = "/me/outreach"
            return RedirectResponse("/login", status_code=303)

    creators = get_user_favourite_creators(user_id, limit=500)
    rows = build_outreach_rows(creators, base_url=_base_url(req))
    return render_outreach_page(rows, user_name=user_name)


def outreach_export_route(req, sess):
    """GET /me/outreach/export.csv — email-tool-friendly saved creator export."""
    user_id = sess.get("user_id") if sess else None
    auth = sess.get("auth") if sess else None

    if not auth or not user_id:
        if _IS_TESTING:
            user_id = user_id or "test-user-id"
        else:
            sess["intended_url"] = "/me/outreach"
            return RedirectResponse("/login", status_code=303)

    creators = get_user_favourite_creators(user_id, limit=500)
    rows = filter_email_ready_rows(build_outreach_rows(creators, base_url=_base_url(req)))

    return StarletteResponse(
        content=render_outreach_csv(rows),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="saved-creators-outreach.csv"'},
    )
