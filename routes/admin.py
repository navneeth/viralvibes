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


def _parse_iso_utc(s: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp (with or without trailing Z) to a UTC-aware datetime."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


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
    # Priority 2: static token — checked before the DB so programmatic / CI
    # access via ?token= or admin_token cookie is never blocked by a DB call.
    if ADMIN_TOKEN:
        qs_token = req.query_params.get("token", "")
        cookie_token = req.cookies.get("admin_token", "")
        if qs_token == ADMIN_TOKEN or cookie_token == ADMIN_TOKEN:
            return True
    # Priority 3: live DB check — covers stale sessions (established before the
    # admin row was added, or before is_admin was correctly cached at login).
    # Self-heals the session cache so subsequent requests are free.
    user_id = sess.get("user_id") if sess else None
    if user_id and _is_admin(user_id):
        if sess is not None:
            sess["is_admin"] = True
        return True
    return False


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


def _fetch_admin_data() -> dict:
    """
    Fetch all data for the admin dashboard. Returns a flat dict; every key has
    a safe default so the view never crashes on a missing value.

    Queries are grouped into independent try/except blocks so a single failing
    query (e.g. missing column) degrades that section only, not the whole page.
    """
    now = datetime.now(timezone.utc)
    cutoff_24h = (now - timedelta(hours=24)).isoformat()
    cutoff_1h = (now - timedelta(hours=1)).isoformat()
    cutoff_7d = (now - timedelta(days=7)).isoformat()
    cutoff_30d = (now - timedelta(days=30)).isoformat()

    data: dict = {
        # Creator inventory
        "total_creators": 0,
        "synced": 0,
        "pending_creators": 0,
        "failed_creators": 0,
        "invalid_creators": 0,
        "visible": 0,
        "fresh_7d": 0,
        "stale_30d": 0,
        "never_synced": 0,
        # Job queue
        "queue_pending": 0,
        "queue_processing": 0,
        "queue_failed": 0,
        "oldest_pending_secs": None,
        # Throughput
        "completed_24h": 0,
        "completed_1h": 0,
        "avg_job_secs": None,
        "last_completed_secs": None,
        # Recent jobs table
        "recent_jobs": [],
    }

    if not _db.supabase_client:
        return data

    sc = _db.supabase_client

    # ── Creator inventory ──────────────────────────────────────────────────────
    try:
        data["total_creators"] = (
            sc.table("creators").select("id", count="exact").execute().count or 0
        )
        for col, status in [
            ("synced", "synced"),
            ("pending_creators", "pending"),
            ("failed_creators", "failed"),
            ("invalid_creators", "invalid"),
        ]:
            data[col] = (
                sc.table("creators")
                .select("id", count="exact")
                .eq("sync_status", status)
                .execute()
                .count
                or 0
            )
        data["visible"] = (
            sc.table("creators")
            .select("id", count="exact")
            .eq("sync_status", "synced")
            .not_.is_("channel_name", "null")
            .gt("current_subscribers", 0)
            .execute()
            .count
            or 0
        )
        data["fresh_7d"] = (
            sc.table("creators")
            .select("id", count="exact")
            .eq("sync_status", "synced")
            .gte("last_synced_at", cutoff_7d)
            .execute()
            .count
            or 0
        )
        data["stale_30d"] = (
            sc.table("creators")
            .select("id", count="exact")
            .eq("sync_status", "synced")
            .lt("last_synced_at", cutoff_30d)
            .execute()
            .count
            or 0
        )
        data["never_synced"] = (
            sc.table("creators")
            .select("id", count="exact")
            .is_("last_synced_at", "null")
            .execute()
            .count
            or 0
        )
    except Exception:
        logger.exception("[Admin] Creator inventory queries failed")

    # ── Job queue ──────────────────────────────────────────────────────────────
    try:
        for col, status in [
            ("queue_pending", "pending"),
            ("queue_processing", "processing"),
            ("queue_failed", "failed"),
        ]:
            data[col] = (
                sc.table("creator_sync_jobs")
                .select("id", count="exact")
                .eq("status", status)
                .execute()
                .count
                or 0
            )
        oldest = (
            sc.table("creator_sync_jobs")
            .select("created_at")
            .eq("status", "pending")
            .is_("next_retry_at", "null")
            .order("created_at", desc=False)
            .limit(1)
            .execute()
            .data
        )
        if oldest:
            dt = _parse_iso_utc(oldest[0]["created_at"])
            if dt:
                data["oldest_pending_secs"] = int((now - dt).total_seconds())
    except Exception:
        logger.exception("[Admin] Job queue queries failed")

    # ── Throughput counts ──────────────────────────────────────────────────────
    try:
        data["completed_24h"] = (
            sc.table("creator_sync_jobs")
            .select("id", count="exact")
            .eq("status", "completed")
            .gte("completed_at", cutoff_24h)
            .execute()
            .count
            or 0
        )
        data["completed_1h"] = (
            sc.table("creator_sync_jobs")
            .select("id", count="exact")
            .eq("status", "completed")
            .gte("completed_at", cutoff_1h)
            .execute()
            .count
            or 0
        )
    except Exception:
        logger.exception("[Admin] Throughput count queries failed")

    # ── Avg duration + last heartbeat (from recent completed jobs) ─────────────
    try:
        rows = (
            sc.table("creator_sync_jobs")
            .select("started_at, completed_at")
            .eq("status", "completed")
            .order("completed_at", desc=True)
            .limit(50)
            .execute()
            .data
            or []
        )
        durations = []
        for row in rows:
            s_dt = _parse_iso_utc(row.get("started_at"))
            c_dt = _parse_iso_utc(row.get("completed_at"))
            if s_dt and c_dt:
                dur = (c_dt - s_dt).total_seconds()
                if 0 < dur < 300:
                    durations.append(dur)
        if durations:
            data["avg_job_secs"] = round(sum(durations) / len(durations), 1)
        if rows:
            last_dt = _parse_iso_utc(rows[0].get("completed_at"))
            if last_dt:
                data["last_completed_secs"] = int((now - last_dt).total_seconds())
    except Exception:
        logger.exception("[Admin] Duration/heartbeat queries failed")

    # ── Recent jobs table ──────────────────────────────────────────────────────
    try:
        data["recent_jobs"] = (
            sc.table("creator_sync_jobs")
            .select(
                "id, creator_id, job_type, source, status, retry_count, "
                "started_at, completed_at, created_at, error_message"
            )
            .neq("status", "pending")  # exclude 500k+ pending rows; show activity
            .order("completed_at", desc=True)
            .limit(20)
            .execute()
            .data
            or []
        )
    except Exception:
        logger.exception("[Admin] Recent jobs query failed")

    return data


# -- Route handlers -----------------------------------------------------------


def _fetch_recent_jobs() -> list[dict]:
    """Lightweight fetch for the HTMX jobs-panel fragment (30s poll)."""
    if not _db.supabase_client:
        return []
    try:
        return (
            _db.supabase_client.table("creator_sync_jobs")
            .select(
                "id, creator_id, job_type, source, status, retry_count, "
                "started_at, completed_at, created_at, error_message"
            )
            .neq("status", "pending")
            .order("completed_at", desc=True)
            .limit(20)
            .execute()
            .data
            or []
        )
    except Exception:
        logger.exception("[Admin] Recent jobs fragment query failed")
        return []


def admin_get(req, sess) -> Response | FT:
    """GET /admin -- full admin dashboard. Returns FT for main.py to wrap."""
    if not _is_authorised(req, sess):
        return _auth_response()

    # After token auth: set cookie then redirect to clean URL so the token
    # doesn't remain in browser history, logs, or Referrer headers.
    if req.query_params.get("token") == ADMIN_TOKEN and ADMIN_TOKEN:
        resp = RedirectResponse("/admin", status_code=303)
        resp.set_cookie("admin_token", ADMIN_TOKEN, httponly=True, samesite="strict")
        return resp

    data = _fetch_admin_data()
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    return AdminPage(data=data, refreshed_at=now)


def admin_jobs_fragment(req, sess) -> Response | FT:
    """GET /admin/jobs — HTMX fragment; refreshes only the recent-jobs panel."""
    if not _is_authorised(req, sess):
        return _auth_response()
    return _JobsSection(_fetch_recent_jobs())
