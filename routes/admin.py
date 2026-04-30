"""
routes/admin.py — Admin dashboard for ViralVibes ops.

Protected by OAuth user ID (admin_users table) or static ADMIN_TOKEN.

Access methods:
    1. OAuth login (recommended) — if user_id is in admin_users table
    2. Static token — ?token=<ADMIN_TOKEN> or admin_token cookie

Usage in main.py:
    from routes.admin import admin_get, admin_jobs_fragment

    @rt("/admin")
    def admin(req, sess): return admin_get(req, sess, supabase_client)

    @rt("/admin/jobs")
    def admin_jobs(req, sess): return admin_jobs_fragment(req, sess, supabase_client)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from fasthtml.common import *
from monsterui.all import *

logger = logging.getLogger(__name__)

# ── Auth ──────────────────────────────────────────────────────────────────────

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")


def _is_admin(user_id: str | None, supabase_client) -> bool:
    """
    Check if user is an admin by looking up their ID in the admin_users table.

    Args:
        user_id: User UUID from session
        supabase_client: Supabase client for DB queries

    Returns:
        True if user_id exists in admin_users table, False otherwise
    """
    if not user_id or not supabase_client:
        return False

    try:
        resp = (
            supabase_client.table("admin_users")
            .select("id")
            .eq("user_id", user_id)
            .limit(1)
            .execute()
        )
        return bool(resp.data)
    except Exception as e:
        logger.exception(f"[Admin] Error checking admin status for {user_id}: {e}")
        return False


def _is_authorised(req, sess=None, supabase_client=None) -> bool:
    """
    Check admin access via:
    1. OAuth user ID in admin_users table (preferred — requires login)
    2. Static ADMIN_TOKEN query param/cookie (fallback)

    Args:
        req: FastHTML request
        sess: Session dict (optional)
        supabase_client: Supabase client for DB queries (optional)

    Returns:
        True if user is authorized for admin access, False otherwise
    """
    # Priority 1: Check OAuth-based access (user must be logged in)
    if sess and sess.get("user_id"):
        if _is_admin(sess.get("user_id"), supabase_client):
            return True

    # Priority 2: Fall back to static token
    if not ADMIN_TOKEN:
        return False

    qs_token = req.query_params.get("token", "")
    cookie_token = req.cookies.get("admin_token", "")
    return bool(qs_token == ADMIN_TOKEN or cookie_token == ADMIN_TOKEN)


def _auth_response(req):
    """Return 401 HTML page when auth fails."""
    return Response(
        content=str(
            Titled(
                "Admin — Unauthorised",
                Container(
                    Div(
                        H2("401 Unauthorised", cls="text-2xl font-bold text-red-600 mb-3"),
                        P("Supply ?token=<ADMIN_TOKEN> in the URL.", cls="text-gray-600"),
                        cls="py-24 text-center",
                    )
                ),
            )
        ),
        status_code=401,
        media_type="text/html",
    )


# ── Palette helpers ───────────────────────────────────────────────────────────

_STAT_COLORS = {
    "total": "text-blue-600",
    "synced": "text-green-600",
    "pending": "text-yellow-600",
    "failed": "text-red-600",
    "processing": "text-purple-600",
}

_STATUS_BAR_COLORS = {
    "synced": "bg-green-500",
    "pending": "bg-yellow-400",
    "failed": "bg-red-500",
    "invalid": "bg-gray-400",
}


# ── Sub-components ────────────────────────────────────────────────────────────


def _StatCard(label: str, value: str, sublabel: str, color_key: str) -> Div:
    """Stat card with label, value, and sublabel."""
    color = _STAT_COLORS.get(color_key, "text-gray-700")
    return Div(
        P(label, cls="text-xs font-mono uppercase tracking-widest text-gray-400 mb-1"),
        P(value, cls=f"text-3xl font-bold {color} font-mono"),
        P(sublabel, cls="text-xs text-gray-400 mt-1"),
        cls=(
            "bg-white rounded-lg border border-gray-200 px-5 py-4 "
            "shadow-sm hover:shadow-md transition-shadow"
        ),
    )


def _StatusBar(label: str, count: int, total: int, color_cls: str) -> Div:
    """Horizontal bar chart row."""
    pct = round(count / total * 100, 1) if total else 0.0
    bar_w = max(int(pct), 2)
    return Div(
        Div(
            Span(label, cls="text-sm font-medium text-gray-700 w-20 inline-block"),
            Div(
                Div(cls=f"{color_cls} h-3 rounded-full", style=f"width:{bar_w}%"),
                cls="flex-1 bg-gray-100 rounded-full mx-3 h-3",
            ),
            Span(f"{count:,}", cls="text-sm font-mono text-gray-500 w-20 text-right"),
            Span(f"{pct}%", cls="text-sm font-mono text-gray-400 w-14 text-right"),
            cls="flex items-center",
        ),
        cls="py-1",
    )


def _JobRow(job: dict) -> Tr:
    """Single job table row."""
    status = job.get("status", "unknown")
    status_colors = {
        "completed": "text-green-600 bg-green-50",
        "synced": "text-green-600 bg-green-50",
        "processing": "text-purple-600 bg-purple-50",
        "pending": "text-yellow-600 bg-yellow-50",
        "failed": "text-red-600 bg-red-50",
        "blocked": "text-orange-600 bg-orange-50",
    }
    status_cls = status_colors.get(status, "text-gray-600 bg-gray-50")
    icon = "✅" if status in ("completed", "synced") else "❌" if status == "failed" else "⏳"

    # Calculate duration from start/finish timestamps
    started = job.get("started_at") or job.get("created_at")
    finished = job.get("finished_at") or job.get("updated_at")
    duration = "—"

    if started and finished:
        try:
            s = datetime.fromisoformat(started.split(".")[0])
            f = datetime.fromisoformat(finished.split(".")[0])
            secs = int((f - s).total_seconds())
            duration = f"{secs}s" if secs >= 0 else "—"
        except Exception:
            pass

    creator_id = str(job.get("creator_id") or "")[:8]
    retry_count = job.get("retry_count", 0)
    job_id = job.get("id", "")

    return Tr(
        Td(Code(f"#{job_id}", cls="text-xs"), cls="px-3 py-2"),
        Td(Code(creator_id + "…", cls="text-xs text-blue-500"), cls="px-3 py-2"),
        Td(
            Span(
                status,
                cls=f"text-xs font-medium px-2 py-0.5 rounded-full {status_cls}",
            ),
            cls="px-3 py-2",
        ),
        Td(duration, cls="px-3 py-2 text-xs font-mono text-gray-500"),
        Td(
            Span(f"retry:{retry_count}", cls="text-xs text-gray-400"),
            cls="px-3 py-2",
        ),
        Td(icon, cls="px-3 py-2 text-center"),
        cls="border-b border-gray-100 hover:bg-gray-50",
    )


def _JobTable(jobs: list[dict]) -> Div:
    """Jobs table wrapper."""
    if not jobs:
        return Div(
            P("No jobs found.", cls="text-sm text-gray-400 py-6 text-center"),
        )
    return Div(
        Table(
            Thead(
                Tr(
                    *[
                        Th(
                            h,
                            cls="px-3 py-2 text-left text-xs font-mono uppercase tracking-wide text-gray-400",
                        )
                        for h in ["ID", "Creator", "Status", "Duration", "Retries", ""]
                    ]
                ),
                cls="border-b border-gray-200",
            ),
            Tbody(*[_JobRow(j) for j in jobs]),
            cls="w-full text-sm",
        ),
        cls="overflow-x-auto",
    )


# ── Section builders ──────────────────────────────────────────────────────────


def _HeroSection(stats: dict) -> Div:
    """5 stat cards across the top."""
    return Div(
        _StatCard("Total Creators", f"{stats.get('total', 0):,}", "in database", "total"),
        _StatCard("Synced Today", f"{stats.get('synced_today', 0):,}", "last 24h", "synced"),
        _StatCard("Pending", f"{stats.get('pending', 0):,}", "in queue", "pending"),
        _StatCard("Failed", f"{stats.get('failed', 0):,}", "need attention", "failed"),
        _StatCard(
            "Processing", f"{stats.get('processing', 0):,}", "active right now", "processing"
        ),
        cls="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6",
        id="hero-stats",
    )


def _BreakdownSection(breakdown: dict, total: int) -> Div:
    """Sync status bar chart."""
    rows = [
        _StatusBar(k, breakdown.get(k, 0), total, _STATUS_BAR_COLORS.get(k, "bg-gray-300"))
        for k in ("synced", "pending", "failed", "invalid")
    ]
    return Div(
        H3("Sync Status", cls="text-sm font-mono uppercase tracking-widest text-gray-400 mb-4"),
        *rows,
        cls=("bg-white rounded-lg border border-gray-200 shadow-sm " "px-5 py-4 space-y-1"),
    )


def _ThroughputSection(throughput: dict) -> Div:
    """Worker performance panel."""

    def _row(label, value):
        return Div(
            Span(label, cls="text-sm text-gray-500"),
            Span(value, cls="text-sm font-mono font-medium text-gray-800"),
            cls="flex justify-between items-center py-1.5 border-b border-gray-50",
        )

    quota_used = throughput.get("quota_used", 0)
    quota_total = throughput.get("quota_total", 10000)
    quota_pct = round(quota_used / quota_total * 100, 1) if quota_total else 0

    return Div(
        H3("Worker Health", cls="text-sm font-mono uppercase tracking-widest text-gray-400 mb-4"),
        _row("Jobs / hour (est.)", str(throughput.get("jobs_per_hour", "—"))),
        _row("Avg duration", throughput.get("avg_duration", "—")),
        _row("Last job", throughput.get("last_job_ago", "—")),
        _row("YT Quota", f"{quota_used:,} / {quota_total:,} ({quota_pct}%)"),
        Div(
            Div(
                cls="bg-orange-400 h-2 rounded-full transition-all",
                style=f"width:{min(quota_pct, 100)}%",
            ),
            cls="mt-3 bg-gray-100 rounded-full h-2",
        ),
        cls=("bg-white rounded-lg border border-gray-200 shadow-sm " "px-5 py-4"),
    )


def _JobsSection(jobs: list[dict]) -> Div:
    """Recent jobs table with refresh button."""
    return Div(
        Div(
            H3("Recent Jobs", cls="text-sm font-mono uppercase tracking-widest text-gray-400"),
            A(
                "↻ Refresh",
                href="/admin/jobs",
                hx_get="/admin/jobs",
                hx_target="#jobs-panel",
                hx_swap="outerHTML",
                cls="text-xs text-blue-500 hover:underline cursor-pointer",
            ),
            cls="flex items-center justify-between mb-4",
        ),
        Div(_JobTable(jobs), id="jobs-panel"),
        cls=("bg-white rounded-lg border border-gray-200 shadow-sm " "px-5 py-5"),
    )


# ── Full page assembly ────────────────────────────────────────────────────────


def AdminPage(
    stats: dict,
    breakdown: dict,
    throughput: dict,
    jobs: list[dict],
    refreshed_at: str = "",
) -> FT:
    """
    Full admin page. All data is passed in as plain dicts —
    the route handler fetches the data, this function only renders.
    """
    total = sum(breakdown.values()) or 1

    return Titled(
        "Admin — ViralVibes",
        # ── Top bar ───────────────────────────────────────────────────────────
        Div(
            Div(
                Span("⚙️", cls="mr-2"),
                Span("Admin", cls="font-bold text-gray-800"),
                Span(" · ViralVibes", cls="text-gray-400 text-sm"),
                cls="flex items-center",
            ),
            Div(
                Span(f"Refreshed {refreshed_at}", cls="text-xs text-gray-400 mr-3"),
                A(
                    "↻ Refresh",
                    href="/admin",
                    cls="text-xs text-blue-500 hover:underline",
                ),
                cls="flex items-center",
            ),
            cls=(
                "flex items-center justify-between "
                "bg-white border-b border-gray-200 px-6 py-3 mb-6 "
                "sticky top-0 z-40 shadow-sm"
            ),
        ),
        # ── Main content ──────────────────────────────────────────────────────
        Container(
            _HeroSection(stats),
            Grid(
                _BreakdownSection(breakdown, total),
                _ThroughputSection(throughput),
                cols_md=2,
                gap=4,
                cls="mb-6",
            ),
            _JobsSection(jobs),
            cls="max-w-6xl mx-auto px-4 pb-16",
        ),
        # ── HTMX auto-refresh every 30s ───────────────────────────────────────
        Script(
            """
            setTimeout(function() { window.location.reload(); }, 30000);
        """
        ),
    )


# ── Data fetching ─────────────────────────────────────────────────────────────


def _fetch_admin_data() -> tuple[dict, dict, dict, list[dict]]:
    """
    Fetch all data needed for the admin page.
    Returns (stats, breakdown, throughput, jobs).

    STUB — returns zeroed data. Replace each section with real
    Supabase queries as we implement them step by step.
    """
    stats = {
        "total": 0,
        "synced_today": 0,
        "pending": 0,
        "failed": 0,
        "processing": 0,
    }
    breakdown = {
        "synced": 0,
        "pending": 0,
        "failed": 0,
        "invalid": 0,
    }
    throughput = {
        "jobs_per_hour": "—",
        "avg_duration": "—",
        "last_job_ago": "—",
        "quota_used": 0,
        "quota_total": 10000,
    }
    jobs: list[dict] = []

    return stats, breakdown, throughput, jobs


# ── Route handlers ────────────────────────────────────────────────────────────


def admin_get(req, sess=None, supabase_client=None) -> Response | FT:
    """
    GET /admin — Full admin dashboard.

    Protected by OAuth user ID or static ADMIN_TOKEN.

    Args:
        req: FastHTML request
        sess: Session dict (optional)
        supabase_client: Supabase client (optional)

    Access methods:
        1. Log in with Google OAuth — if user_id is in admin_users table
        2. Query param — ?token=<ADMIN_TOKEN>
        3. Cookie — admin_token=<ADMIN_TOKEN>
    """
    if not _is_authorised(req, sess, supabase_client):
        return _auth_response(req)

    stats, breakdown, throughput, jobs = _fetch_admin_data()

    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    page = AdminPage(
        stats=stats, breakdown=breakdown, throughput=throughput, jobs=jobs, refreshed_at=now
    )

    # Set auth cookie so token doesn't need to be in URL on every refresh
    resp = Response(content=str(page), media_type="text/html")
    if req.query_params.get("token") == ADMIN_TOKEN and ADMIN_TOKEN:
        resp.set_cookie("admin_token", ADMIN_TOKEN, httponly=True, samesite="strict")
    return resp


def admin_jobs_fragment(req, sess=None, supabase_client=None) -> Response | Div:
    """
    GET /admin/jobs — HTMX fragment endpoint.

    Returns just the jobs table panel for HTMX refresh (no full page).
    Protected by OAuth user ID or static ADMIN_TOKEN (same as /admin).

    Args:
        req: FastHTML request
        sess: Session dict (optional)
        supabase_client: Supabase client (optional)
    """
    if not _is_authorised(req, sess, supabase_client):
        return Response("Unauthorised", status_code=401)

    _, _, _, jobs = _fetch_admin_data()
    return Div(_JobTable(jobs), id="jobs-panel")
