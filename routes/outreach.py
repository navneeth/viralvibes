"""
Saved creator outreach routes.
"""

from __future__ import annotations

import os
import csv
import io

from fasthtml.common import A, Div, P, RedirectResponse, Span
from starlette.responses import Response as StarletteResponse

from db import add_favourite_creators_bulk, get_user_favourite_creators, get_user_favourite_lists
from services.contact_extractor import ContactExtractorService
from services.outreach_lists import clamp_import_limit, get_creators_for_outreach_list
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
    saved_lists = get_user_favourite_lists(user_id, limit=50)

    # Build rows using unified service
    rows = [
        ContactExtractorService.build_creator_contact_row(c, base_url=_base_url(req))
        for c in creators
    ]

    return render_outreach_page(rows, saved_lists=saved_lists, user_name=user_name)


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

    # Build rows using unified service
    rows = [
        ContactExtractorService.build_creator_contact_row(c, base_url=_base_url(req))
        for c in creators
    ]

    # Filter email-ready to keep file size down
    rows = ContactExtractorService.filter_email_ready_rows(rows)

    # Render CSV
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf, fieldnames=ContactExtractorService.EMAIL_EXPORT_HEADERS, extrasaction="ignore"
    )
    writer.writeheader()
    writer.writerows(rows)

    return StarletteResponse(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="saved-creators-outreach.csv"'},
    )


def outreach_import_list_route(req, sess, list_key: str = "", limit: int | str = 25):
    """
    POST /me/outreach/import-list — bulk-save creators from a saved list.

    The import target is the existing saved-creators table. That keeps this MVP
    small and makes the existing outreach export immediately useful.
    """
    user_id = sess.get("user_id") if sess else None
    auth = sess.get("auth") if sess else None

    if not auth or not user_id:
        if _IS_TESTING:
            user_id = user_id or "test-user-id"
        else:
            sess["intended_url"] = "/me/outreach"
            return RedirectResponse("/login", status_code=303)

    limit_int = clamp_import_limit(limit)
    creators = get_creators_for_outreach_list(list_key, limit=limit_int)

    if not creators:
        return Div(
            P(
                "No importable creators found for this list.",
                cls="text-sm font-medium text-foreground",
            ),
            cls="p-3 rounded-lg bg-yellow-50 border border-yellow-200 text-yellow-800",
        )

    creator_ids = [str(c.get("id") or "") for c in creators if c.get("id")]
    saved_count = add_favourite_creators_bulk(user_id, creator_ids)

    return Div(
        P(
            Span(f"Added up to {saved_count} creators", cls="font-semibold"),
            " to your outreach pool (already-saved creators were skipped).",
            cls="text-sm text-foreground",
        ),
        A("Refresh outreach", href="/me/outreach", cls="text-xs text-red-600 hover:underline"),
        cls="p-3 rounded-lg bg-green-50 border border-green-200 text-green-800",
    )
