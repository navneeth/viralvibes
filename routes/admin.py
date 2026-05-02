"""
routes/admin.py -- Thin handler for the admin dashboard.

Auth:
    1. OAuth login -- user_id in admin_users table (reads from session, no DB call here)
    2. Static ADMIN_TOKEN query param or cookie (fallback)

Rendering lives entirely in views/admin.py.
DB helpers (is_admin) live in db.py.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from fasthtml.common import *

import db as _db
from views.admin import AdminPage, _JobsSection

logger = logging.getLogger(__name__)

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")


# -- Auth helpers ─────────────────────────────────────────────────────────────


def _is_admin(user_id: str | None) -> bool:
    """
    Check if a user_id exists in the admin_users table.
    Used by NavComponent to determine if admin link should be shown.
    """
    client = _db.supabase_client  # always read the live module-level client
    if not user_id or not client:
        return False
    try:
        resp = client.table("admin_users").select("id").eq("user_id", user_id).limit(1).execute()
        return bool(resp.data)
    except Exception as e:
        logger.warning("[Admin] Error checking admin status for %s: %s", user_id, e)
        return False


def _is_authorised(req, sess) -> bool:
    # Priority 1: is_admin cached in session at login (zero DB cost)
    if sess and sess.get("is_admin"):
        return True
    # Priority 2: static token fallback
    if not ADMIN_TOKEN:
        return False
    qs_token = req.query_params.get("token", "")
    cookie_token = req.cookies.get("admin_token", "")
    return bool(qs_token == ADMIN_TOKEN or cookie_token == ADMIN_TOKEN)


def _auth_response():
    return Response(
        content=str(
            Titled(
                "Admin — Unauthorized",
                Div(
                    H2("401 Unauthorized", cls="text-2xl font-bold text-red-600 mb-3"),
                    P(
                        "You are not authorized to access this page. "
                        "Log in with an admin account or supply ?token=<ADMIN_TOKEN> in the URL.",
                        cls="text-muted-foreground",
                    ),
                    cls="py-24 text-center",
                ),
            )
        ),
        status_code=401,
        media_type="text/html",
    )


# -- Data fetching ------------------------------------------------------------


def _fetch_admin_data() -> tuple[dict, dict, dict, list[dict]]:
    """Fetch all data needed for the admin page from Supabase."""
    stats = {"total": 0, "synced_today": 0, "pending": 0, "failed": 0, "processing": 0}
    breakdown = {"synced": 0, "pending": 0, "failed": 0, "invalid": 0}
    throughput = {
        "jobs_per_hour": "--",
        "avg_duration": "--",
        "last_job_ago": "--",
        "quota_used": 0,
        "quota_total": 10000,
    }
    jobs: list[dict] = []

    if not _db.supabase_client:
        return stats, breakdown, throughput, jobs

    try:
        # Total creators
        r = _db.supabase_client.table("creators").select("id", count="exact").execute()
        stats["total"] = r.count or 0

        # Breakdown by sync_status
        for status_key in ("synced", "pending", "failed", "invalid"):
            r = (
                _db.supabase_client.table("creators")
                .select("id", count="exact")
                .eq("sync_status", status_key)
                .execute()
            )
            breakdown[status_key] = r.count or 0

        # Synced today (last 24h)
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        r = (
            _db.supabase_client.table("creators")
            .select("id", count="exact")
            .gte("last_synced_at", cutoff)
            .execute()
        )
        stats["synced_today"] = r.count or 0
        stats["pending"] = breakdown["pending"]
        stats["failed"] = breakdown["failed"]

        # Processing jobs count
        r = (
            _db.supabase_client.table("creator_sync_jobs")
            .select("id", count="exact")
            .eq("status", "processing")
            .execute()
        )
        stats["processing"] = r.count or 0

        # Recent 50 jobs for the table
        r = (
            _db.supabase_client.table("creator_sync_jobs")
            .select("id, creator_id, status, retry_count, created_at, updated_at")
            .order("created_at", desc=True)
            .limit(50)
            .execute()
        )
        jobs = r.data or []

        # Throughput: last job time
        if jobs:
            last_ts = jobs[0].get("updated_at") or jobs[0].get("created_at")
            if last_ts:
                try:
                    last_dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                    diff = datetime.now(timezone.utc) - last_dt
                    secs = int(diff.total_seconds())
                    if secs < 60:
                        throughput["last_job_ago"] = f"{secs}s ago"
                    elif secs < 3600:
                        throughput["last_job_ago"] = f"{secs // 60}m ago"
                    else:
                        throughput["last_job_ago"] = f"{secs // 3600}h ago"
                except Exception:
                    pass

    except Exception as e:
        logger.exception("[Admin] Error fetching admin data: %s", e)

    return stats, breakdown, throughput, jobs


# -- Route handlers -----------------------------------------------------------


def admin_get(req, sess) -> Response | FT:
    """GET /admin -- full admin dashboard."""
    if not _is_authorised(req, sess):
        return _auth_response()

    stats, breakdown, throughput, jobs = _fetch_admin_data()
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    page = AdminPage(
        stats=stats, breakdown=breakdown, throughput=throughput, jobs=jobs, refreshed_at=now
    )

    # After token auth: set cookie then redirect to clean URL so the token
    # doesn't remain in browser history, logs, or Referrer headers.
    if req.query_params.get("token") == ADMIN_TOKEN and ADMIN_TOKEN:
        resp = RedirectResponse("/admin", status_code=303)
        resp.set_cookie("admin_token", ADMIN_TOKEN, httponly=True, samesite="strict")
        return resp

    resp = Response(content=str(page), media_type="text/html")
    return resp


def admin_jobs_fragment(req, sess) -> Response | FT:
    """GET /admin/jobs -- HTMX fragment: jobs panel only."""
    if not _is_authorised(req, sess):
        return Response("Unauthorised", status_code=401)

    _, _, _, jobs = _fetch_admin_data()
    return _JobsSection(jobs)
