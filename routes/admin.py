"""
routes/admin.py -- Thin handler for the admin dashboard.

Auth:
    1. OAuth login -- user_id in admin_users table (reads from session, no DB call here)
    2. Static ADMIN_TOKEN query param or cookie (fallback)

Rendering lives entirely in views/admin.py.
DB helpers (is_admin) live in db.py.
"""

from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timedelta, timezone

from fasthtml.common import *
from starlette.responses import Response as StarletteResponse

import db as _db
from constants import BROWSEABLE_SYNC_STATUSES
from services.contact_extractor import ContactExtractorService
from utils.dates import parse_iso_utc
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

# Freshness tier boundaries (days since last_synced_at).
# Each entry is (data_key, label, ge_days, lt_days_or_None).
# Boundaries are exclusive-lower / inclusive-upper so tiers are contiguous
# with no overlap or gap: [0,7) → [7,30) → [30,90) → [90,∞).
_FRESHNESS_TIERS: tuple[tuple[str, int, int | None], ...] = (
    ("fresh_7d", 0, 7),
    ("fresh_7_30d", 7, 30),
    ("stale_30_90d", 30, 90),
    ("stale_90d", 90, None),
)


def _count(sc, table: str, extra_filters=None) -> int:
    """Generic COUNT(*) helper for any table.

    Args:
        sc: Supabase client.
        table: Table name.
        extra_filters: Optional callable(query) -> query for additional filters.
    """
    q = sc.table(table).select("id", count="exact")
    if extra_filters is not None:
        q = extra_filters(q)
    return q.execute().count or 0


def _count_creators(sc, *, status: str | None = None, extra_filters=None) -> int:
    """Return a COUNT(*) for the creators table with optional sync_status filter."""

    def _f(q):
        if status is not None:
            q = q.eq("sync_status", status)
        if extra_filters is not None:
            q = extra_filters(q)
        return q

    return _count(sc, "creators", _f)


def _fetch_admin_data() -> dict:
    """
    Fetch all data for the admin dashboard. Returns a flat dict; every key has
    a safe default so the view never crashes on a missing value.

    Queries are grouped into independent try/except blocks so a single failing
    query (e.g. missing column) degrades that section only, not the whole page.
    """
    now = datetime.now(timezone.utc)
    cutoff_1h = (now - timedelta(hours=1)).isoformat()
    cutoff_24h = (now - timedelta(hours=24)).isoformat()
    cutoff_7d = (now - timedelta(days=7)).isoformat()
    cutoff_30d = (now - timedelta(days=30)).isoformat()

    # Freshness cutoffs derived from _FRESHNESS_TIERS so the two are always in sync.
    _tier_days = sorted({d for _, lo, hi in _FRESHNESS_TIERS for d in (lo, hi) if d is not None})
    _cutoffs: dict[int, str] = {d: (now - timedelta(days=d)).isoformat() for d in _tier_days}

    data: dict = {
        # Creator inventory
        "total_creators": 0,
        "synced": 0,
        "synced_partial": 0,
        "pending_creators": 0,
        "failed_creators": 0,
        "invalid_creators": 0,
        "visible": 0,
        "fresh_7d": 0,
        "fresh_7_30d": 0,
        "stale_30_90d": 0,
        "stale_90d": 0,
        "stale_30d": 0,
        "never_synced": 0,
        # Data quality
        "creators_with_engagement": 0,
        "creators_with_grade": 0,
        "creators_with_recent_perf": 0,
        "distinct_categories": 0,
        "distinct_countries": 0,
        # Job queue
        "queue_pending": 0,
        "queue_processing": 0,
        "queue_failed": 0,
        "failed_quota": 0,
        "failed_invalid": 0,
        "failed_other": 0,
        "oldest_pending_secs": None,
        # Throughput
        "completed_24h": 0,
        "completed_1h": 0,
        "avg_job_secs": None,
        "last_completed_secs": None,
        # Contact signals / Outreach
        "creators_with_contact": 0,
        "creators_with_email": 0,
        "creators_with_instagram": 0,
        "last_contact_extracted_at": None,
        # Users & growth
        "total_users": 0,
        "users_new_7d": 0,
        "users_new_30d": 0,
        "users_completed_oauth": 0,
        "users_active_30d": 0,
        "users_never_returned": 0,
        # Revenue & plans
        "plan_active_paid": 0,
        "plan_pro_monthly": 0,
        "plan_pro_annual": 0,
        "plan_trialing": 0,
        "plan_past_due": 0,
        # Engagement
        "total_favourites": 0,
        # Contact inquiries inbox
        "inquiries_new_7d": 0,
        "inquiries_unforwarded": 0,
        # Recent jobs table
        "recent_jobs": [],
    }

    if not _db.supabase_client:
        return data

    sc = _db.supabase_client

    # ── Creator inventory ──────────────────────────────────────────────────────
    try:
        data["total_creators"] = _count_creators(sc)
        for col, status in [
            ("synced", "synced"),
            ("synced_partial", "synced_partial"),
            ("pending_creators", "pending"),
            ("failed_creators", "failed"),
            ("invalid_creators", "invalid"),
        ]:
            data[col] = _count_creators(sc, status=status)

        data["visible"] = _count_creators(
            sc,
            extra_filters=lambda q: (
                q.in_("sync_status", list(BROWSEABLE_SYNC_STATUSES))
                .not_.is_("channel_name", "null")
                .gt("current_subscribers", 0)
            ),
        )

        # Freshness tiers — driven by _FRESHNESS_TIERS so boundaries stay consistent.
        for key, lo_days, hi_days in _FRESHNESS_TIERS:

            def _freshness_filter(q, lo=lo_days, hi=hi_days):
                q = q.eq("sync_status", "synced")
                if lo > 0:
                    q = q.lt("last_synced_at", _cutoffs[lo])
                if hi is not None:
                    q = q.gte("last_synced_at", _cutoffs[hi])
                return q

            data[key] = _count_creators(sc, extra_filters=_freshness_filter)

        # keep stale_30d as total stale (>30d) for backward compat with existing view
        data["stale_30d"] = data["stale_30_90d"] + data["stale_90d"]

        data["never_synced"] = _count_creators(
            sc, extra_filters=lambda q: q.is_("last_synced_at", "null")
        )
    except Exception:
        logger.exception("[Admin] Creator inventory queries failed")

    # ── Data quality (synced pool) ─────────────────────────────────────────────
    try:
        data["creators_with_engagement"] = _count_creators(
            sc, status="synced", extra_filters=lambda q: q.gt("engagement_score", 0)
        )
        data["creators_with_grade"] = _count_creators(
            sc, status="synced", extra_filters=lambda q: q.not_.is_("quality_grade", "null")
        )
        data["creators_with_recent_perf"] = _count_creators(
            sc, status="synced", extra_filters=lambda q: q.not_.is_("avg_views_10", "null")
        )
        # Approximate category / country coverage via top-N RPCs
        # (exact DISTINCT counts live in mv_lists_meta but may be stale)
        try:
            cat_resp = sc.rpc("get_top_categories_with_counts", {"p_limit": 200}).execute()
            data["distinct_categories"] = len(cat_resp.data or [])
        except Exception:
            data["distinct_categories"] = 0
        try:
            cty_resp = sc.rpc("get_top_countries_with_counts", {"p_limit": 300}).execute()
            data["distinct_countries"] = len(cty_resp.data or [])
        except Exception:
            data["distinct_countries"] = 0
    except Exception:
        logger.exception("[Admin] Data quality queries failed")

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
            .order("created_at", desc=False)  # absolute oldest, regardless of retry state
            .limit(1)
            .execute()
            .data
        )
        if oldest:
            dt = parse_iso_utc(oldest[0]["created_at"])
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
            s_dt = parse_iso_utc(row.get("started_at"))
            c_dt = parse_iso_utc(row.get("completed_at"))
            if s_dt and c_dt:
                dur = (c_dt - s_dt).total_seconds()
                if 0 < dur < 300:
                    durations.append(dur)
        if durations:
            data["avg_job_secs"] = round(sum(durations) / len(durations), 1)
        if rows:
            last_dt = parse_iso_utc(rows[0].get("completed_at"))
            if last_dt:
                data["last_completed_secs"] = int((now - last_dt).total_seconds())
    except Exception:
        logger.exception("[Admin] Duration/heartbeat queries failed")

    # ── Contact signals / Outreach ────────────────────────────────────────────
    try:
        data["creators_with_contact"] = (
            sc.table("creators")
            .select("id", count="exact")
            .eq("has_contact_info", True)
            .execute()
            .count
            or 0
        )
        data["creators_with_email"] = (
            sc.table("creators")
            .select("id", count="exact")
            .not_.is_("extracted_email", "null")
            .execute()
            .count
            or 0
        )
        data["creators_with_instagram"] = (
            sc.table("creators")
            .select("id", count="exact")
            .not_.is_("extracted_instagram", "null")
            .execute()
            .count
            or 0
        )
        # Get most recent extraction timestamp
        last_extracted = (
            sc.table("creators")
            .select("contact_signals_extracted_at")
            .order("contact_signals_extracted_at", desc=True)
            .limit(1)
            .execute()
            .data
        )
        if last_extracted and last_extracted[0].get("contact_signals_extracted_at"):
            dt = parse_iso_utc(last_extracted[0]["contact_signals_extracted_at"])
            if dt:
                data["last_contact_extracted_at"] = int((now - dt).total_seconds())
    except Exception:
        logger.exception("[Admin] Contact signals queries failed")

    # ── Recent jobs table ──────────────────────────────────────────────────────
    try:
        data["recent_jobs"] = (
            sc.table("creator_sync_jobs")
            .select(
                "id, creator_id, job_type, source, status, retry_count, "
                "started_at, completed_at, created_at, error_message"
            )
            .neq("status", "pending")  # exclude 500k+ pending rows; show activity
            .order(
                "created_at", desc=True
            )  # completed_at is NULL on failed rows; created_at is always set
            .limit(20)
            .execute()
            .data
            or []
        )
    except Exception:
        logger.exception("[Admin] Recent jobs query failed")

    # ── Failed job breakdown (quota vs invalid vs other) ───────────────────────
    try:
        data["failed_quota"] = (
            sc.table("creator_sync_jobs")
            .select("id", count="exact")
            .eq("status", "failed")
            .ilike("error_message", "%quota%")
            .execute()
            .count
            or 0
        )
        data["failed_invalid"] = (
            sc.table("creator_sync_jobs")
            .select("id", count="exact")
            .eq("status", "failed")
            .or_("error_message.ilike.%zero%,error_message.ilike.%suspicious%")
            .execute()
            .count
            or 0
        )
        # Other = total failed minus the two classified buckets
        data["failed_other"] = max(
            0, data["queue_failed"] - data["failed_quota"] - data["failed_invalid"]
        )
    except Exception:
        logger.exception("[Admin] Failed job breakdown query failed")

    # ── Users & Growth ────────────────────────────────────────────────────────
    try:
        data["total_users"] = _count(sc, "users")
        data["users_new_7d"] = _count(sc, "users", lambda q: q.gte("created_at", cutoff_7d))
        data["users_new_30d"] = _count(sc, "users", lambda q: q.gte("created_at", cutoff_30d))
        data["users_completed_oauth"] = _count(sc, "auth_providers")
        data["users_active_30d"] = _count(sc, "users", lambda q: q.gte("last_login_at", cutoff_30d))
        data["users_never_returned"] = _count(sc, "users", lambda q: q.is_("last_login_at", "null"))
    except Exception:
        logger.exception("[Admin] Users & growth queries failed")

    # ── Revenue & Plans ───────────────────────────────────────────────────────
    try:
        data["plan_active_paid"] = _count(
            sc, "subscriptions", lambda q: q.eq("status", "active").neq("plan", "free")
        )
        data["plan_pro_monthly"] = _count(
            sc,
            "subscriptions",
            lambda q: q.eq("status", "active").neq("plan", "free").eq("interval", "month"),
        )
        data["plan_pro_annual"] = _count(
            sc,
            "subscriptions",
            lambda q: q.eq("status", "active").neq("plan", "free").eq("interval", "year"),
        )
        data["plan_trialing"] = _count(sc, "subscriptions", lambda q: q.eq("status", "trialing"))
        data["plan_past_due"] = _count(sc, "subscriptions", lambda q: q.eq("status", "past_due"))
    except Exception:
        logger.exception("[Admin] Revenue & plans queries failed")

    # ── Engagement ────────────────────────────────────────────────────────────
    try:
        data["total_favourites"] = _count(sc, "user_favourite_creators")
    except Exception:
        logger.exception("[Admin] Engagement queries failed")

    # ── Contact inquiries inbox ───────────────────────────────────────────────
    try:
        data["inquiries_new_7d"] = _count(
            sc, "contact_inquiries", lambda q: q.gte("created_at", cutoff_7d)
        )
        data["inquiries_unforwarded"] = _count(
            sc,
            "contact_inquiries",
            lambda q: q.is_("forwarded_at", "null").is_("forward_error", "null"),
        )
    except Exception:
        logger.exception("[Admin] Contact inquiries queries failed")

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
            .order("created_at", desc=True)  # completed_at is NULL on failed rows
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


def admin_rescue_quota_jobs(req, sess) -> Response | FT:
    """POST /admin/rescue-quota-jobs — reset quota-failed jobs back to pending."""
    if not _is_authorised(req, sess):
        return _auth_response()

    sc = _db.supabase_client
    if not sc:
        return Response("DB unavailable", status_code=503)

    try:
        # Single filter-based update — no ID fetch needed, avoids URL length limits
        # with 952 IDs. The ilike filter matches "YouTube quota exceeded" rows exactly.
        result = (
            sc.table("creator_sync_jobs")
            .update(
                {
                    "status": "pending",
                    "retry_count": 0,
                    "retry_at": None,
                    "error_message": None,
                }
            )
            .eq("status", "failed")
            .ilike("error_message", "%quota%")
            .execute(count="exact")
        )
        rescued = result.count or 0
        if rescued == 0:
            return P("No quota-failed jobs found.", cls="text-sm text-muted-foreground")

        logger.info("[Admin] Rescued %d quota-failed jobs", rescued)
        return P(
            f"✅ Reset {rescued:,} quota-failed jobs to pending.",
            cls="text-sm text-green-600 font-medium",
        )
    except Exception:
        logger.exception("[Admin] rescue_quota_jobs failed")
        return P("❌ Rescue failed — check server logs.", cls="text-sm text-red-600")


# -- Admin Outreach Export ---------------------------------------------------


def admin_outreach_export_route(req, sess) -> Response:
    """
    GET /admin/outreach/export — bulk export of all creators with contact info.

    ADMIN-ONLY: Authorized via _is_authorised() (OAuth admin_users OR static token).

    Returns CSV with columns:
      Email, First Name, Last Name, Company, Website, YouTube URL,
      Instagram URL, X URL, TikTok URL, LinkedIn URL, Tags, Notes, etc.

    Filters to creators with:
      - has_contact_info = TRUE (denormalized, indexed)
      - sync_status = 'synced' (valid stats)
      - Email present (keeps file size down per user requirement)
    """
    if not _is_authorised(req, sess):
        return StarletteResponse(
            "Unauthorized",
            status_code=401,
            media_type="text/plain",
        )

    sc = _db.supabase_client
    if not sc:
        return StarletteResponse("DB unavailable", status_code=503, media_type="text/plain")

    try:
        # Query creators with contact info, synced status only
        creators = (
            sc.table("creators")
            .select("*")
            .eq("has_contact_info", True)
            .eq("sync_status", "synced")
            .order("current_subscribers", desc=True)
            .execute()
            .data
            or []
        )

        if not creators:
            return StarletteResponse(
                "No creators with contact information found.",
                status_code=200,
                media_type="text/plain",
            )

        logger.info(f"[Admin] Generating outreach export for {len(creators)} creators")

        # Build rows using unified service
        rows = [ContactExtractorService.build_creator_contact_row(c) for c in creators]

        # Filter email-ready (keep file size down)
        rows = ContactExtractorService.filter_email_ready_rows(rows)

        if not rows:
            return StarletteResponse(
                "No creators with email addresses found.",
                status_code=200,
                media_type="text/plain",
            )

        logger.info(f"[Admin] Exporting {len(rows)} email-ready creators")

        # Render CSV
        buf = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames=ContactExtractorService.EMAIL_EXPORT_HEADERS,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)

        return StarletteResponse(
            content=buf.getvalue(),
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="viralvibes-creators-{datetime.now().strftime("%Y%m%d")}.csv"'
            },
        )

    except Exception as e:
        logger.exception("[Admin] Outreach export failed")
        return StarletteResponse(
            f"Export failed: {str(e)[:200]}",
            status_code=500,
            media_type="text/plain",
        )
