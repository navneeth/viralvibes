"""
views/admin.py — Admin dashboard rendering.

All FT components for the /admin page. No auth logic here — that lives in
routes/admin.py. Data is passed in as a flat dict from _fetch_admin_data().
"""

from __future__ import annotations

from datetime import datetime, timezone

from fasthtml.common import *
from monsterui.all import *

from constants import (
    CREATOR_SYNC_COVERAGE_MAX_AGE_HOURS,
    CREATOR_SYNC_PROGRESS_GRACE_HOURS,
    CREATOR_SYNC_READY_WARNING_HOURS,
    CREATOR_SYNC_STALL_HOURS,
)
from utils.dates import parse_iso_utc


# ── Colour maps ───────────────────────────────────────────────────────────────

_COLOR = {
    "blue": "text-blue-600",
    "green": "text-green-600",
    "yellow": "text-yellow-500",
    "red": "text-red-600",
    "purple": "text-purple-600",
    "orange": "text-orange-500",
    "gray": "text-gray-400",
}

_BAR_COLOR = {
    "synced": "bg-green-500",
    "synced_partial": "bg-blue-400",
    "pending": "bg-yellow-400",
    "failed": "bg-red-500",
    "invalid": "bg-gray-400",
}

_JOB_BADGE = {
    "completed": "text-green-700 bg-green-100",
    "processing": "text-purple-700 bg-purple-100",
    "pending": "text-yellow-700 bg-yellow-100",
    "failed": "text-red-700 bg-red-100",
}


# ── Formatting helpers ────────────────────────────────────────────────────────


def _fmt_ago(secs: int | None) -> str:
    if secs is None:
        return "—"
    if secs < 60:
        return f"{secs}s ago"
    if secs < 3600:
        return f"{secs // 60}m ago"
    if secs < 86400:
        return f"{secs // 3600}h {(secs % 3600) // 60}m ago"
    return f"{secs // 86400}d ago"


def _fmt_dur(secs: float | None) -> str:
    if secs is None:
        return "—"
    s = int(secs)
    if s < 60:
        return f"{s}s"
    return f"{s // 60}m {s % 60}s"


def _fmt_age(secs: int | None) -> str:
    """Human-readable age (no 'ago' suffix — for queue wait times)."""
    if secs is None:
        return "—"
    if secs < 3600:
        return f"{secs // 60}m"
    if secs < 86400:
        return f"{secs // 3600}h {(secs % 3600) // 60}m"
    return f"{secs // 86400}d {(secs % 86400) // 3600}h"


def _fmt_drain(pending: int, rate_per_hour: float) -> str:
    if rate_per_hour <= 0 or pending <= 0:
        return "—"
    hours = pending / rate_per_hour
    if hours < 1:
        return f"~{int(hours * 60)}m"
    if hours < 48:
        return f"~{hours:.1f}h"
    return f"~{hours / 24:.1f} days"


# ── Atomic components ─────────────────────────────────────────────────────────


def _StatCard(
    label: str,
    value: int | str,
    sublabel: str = "",
    color: str = "blue",
    warn: bool = False,
) -> Div:
    val_str = f"{value:,}" if isinstance(value, int) else value
    color_cls = _COLOR.get(color, "text-foreground")
    warn_cls = " border-l-4 border-orange-400" if warn else ""
    return Card(
        P(label, cls="text-xs font-mono uppercase tracking-widest text-muted-foreground mb-1"),
        P(val_str, cls=f"text-3xl font-bold font-mono {color_cls}"),
        (P(sublabel, cls="text-xs text-muted-foreground mt-1") if sublabel else None),
        body_cls=f"p-5{warn_cls}",
        cls="hover:shadow-md transition-shadow",
    )


def _KVRow(label: str, value: str, warn: bool = False) -> Div:
    val_cls = "text-sm font-mono font-medium"
    val_cls += " text-orange-500" if warn else " text-foreground"
    return DivFullySpaced(
        Span(label, cls="text-sm text-muted-foreground"),
        Span(value, cls=val_cls),
        cls="py-2 border-b border-border last:border-0",
    )


def _StatusBar(label: str, count: int, total: int, color_cls: str) -> Div:
    pct = round(count / total * 100, 1) if total else 0.0
    bar_w = max(int(pct), 2) if count > 0 else 0
    return Div(
        Span(label, cls="text-sm font-medium w-24 inline-block capitalize"),
        Div(
            Div(cls=f"{color_cls} h-3 rounded-full", style=f"width:{bar_w}%"),
            cls="flex-1 bg-accent rounded-full mx-3 h-3",
        ),
        Span(f"{count:,}", cls="text-sm font-mono text-muted-foreground w-24 text-right"),
        Span(f"{pct}%", cls="text-sm font-mono text-muted-foreground w-14 text-right"),
        cls="flex items-center py-1",
    )


def _JobRow(job: dict) -> Tr:
    status = job.get("status", "unknown")
    badge_cls = _JOB_BADGE.get(status, "text-gray-600 bg-gray-100")
    job_type = job.get("job_type") or "sync_stats"
    creator = str(job.get("creator_id") or "")[:8]
    retries = job.get("retry_count", 0)

    # Per-job duration
    duration = "—"
    s_dt = parse_iso_utc(job.get("started_at"))
    c_dt = parse_iso_utc(job.get("completed_at"))
    if s_dt and c_dt:
        dur = (c_dt - s_dt).total_seconds()
        if dur >= 0:
            duration = _fmt_dur(dur)
    elif s_dt and not c_dt:
        # Job was picked up by the worker but never completed (failed mid-run)
        duration = Span("started", cls="text-xs text-orange-400 italic")

    # Age of last update
    age_ts = job.get("completed_at") or job.get("created_at")
    age = "—"
    age_dt = parse_iso_utc(age_ts)
    if age_dt:
        secs = int((datetime.now(timezone.utc) - age_dt).total_seconds())
        age = _fmt_ago(secs)

    error = job.get("error_message") or ""
    error_cell = Td(
        (
            Span(error[:45] + "…" if len(error) > 45 else error, cls="text-xs text-red-500")
            if error
            else Span("—", cls="text-xs text-muted-foreground")
        ),
        cls="px-3 py-2 max-w-xs",
    )

    return Tr(
        Td(Code(f"#{job.get('id', '')}", cls="text-xs"), cls="px-3 py-2 whitespace-nowrap"),
        Td(Code(creator + "…" if creator else "—", cls="text-xs text-primary"), cls="px-3 py-2"),
        Td(Span(job_type, cls="text-xs text-muted-foreground"), cls="px-3 py-2"),
        Td(
            Span(status, cls=f"text-xs font-medium px-2 py-0.5 rounded-full {badge_cls}"),
            cls="px-3 py-2",
        ),
        Td(duration, cls="px-3 py-2 text-xs font-mono text-muted-foreground whitespace-nowrap"),
        Td(
            Span(
                f"×{retries}",
                cls=f"text-xs {'text-orange-500 font-medium' if retries > 0 else 'text-muted-foreground'}",
            ),
            cls="px-3 py-2",
        ),
        error_cell,
        Td(age, cls="px-3 py-2 text-xs text-muted-foreground whitespace-nowrap"),
        cls="border-b border-border hover:bg-accent/50",
    )


# ── Section builders ──────────────────────────────────────────────────────────


def _UsersSection(data: dict) -> Div:
    total = data.get("total_users", 0)
    completed_oauth = data.get("users_completed_oauth", 0)
    never_returned = data.get("users_never_returned", 0)
    oauth_pct = f"{completed_oauth / total * 100:.0f}% of signups" if total else "—"
    now = datetime.now(timezone.utc)
    recent_users = data.get("recent_active_users", [])

    if recent_users:
        user_rows = []
        for user in recent_users:
            last_login = parse_iso_utc(user.get("last_login_at"))
            created_at = parse_iso_utc(user.get("created_at"))
            last_login_ago = (
                _fmt_ago(max(0, int((now - last_login).total_seconds()))) if last_login else "—"
            )
            email = user.get("email") or ""
            user_rows.append(
                Tr(
                    Td(user.get("name") or "Unnamed user", cls="px-3 py-2"),
                    Td(
                        (
                            A(email, href=f"mailto:{email}", cls="text-primary hover:underline")
                            if email
                            else Span("—", cls="text-muted-foreground")
                        ),
                        cls="px-3 py-2",
                    ),
                    Td(last_login_ago, cls="px-3 py-2 text-xs font-mono whitespace-nowrap"),
                    Td(
                        created_at.strftime("%Y-%m-%d") if created_at else "—",
                        cls="px-3 py-2 text-xs font-mono whitespace-nowrap",
                    ),
                    cls="border-b border-border last:border-0 hover:bg-accent/50",
                )
            )
        users_body = Tbody(*user_rows)
    else:
        users_body = Tbody(
            Tr(
                Td(
                    "No users logged in during the last 30 days.",
                    colspan=4,
                    cls="px-3 py-6 text-center text-muted-foreground",
                )
            )
        )

    return Div(
        H3(
            "Users & Growth",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        Grid(
            _StatCard("Total Users", total, "all-time signups", "blue"),
            _StatCard("Completed OAuth", completed_oauth, oauth_pct, "green"),
            _StatCard(
                "Active (30d)",
                data.get("users_active_30d", 0),
                "logged in last 30 days",
                "purple",
            ),
            _StatCard("New (7d)", data.get("users_new_7d", 0), "signed up this week", "blue"),
            _StatCard("New (30d)", data.get("users_new_30d", 0), "signed up this month", "blue"),
            _StatCard(
                "Never Returned",
                never_returned,
                "first OAuth only, never logged in again",
                "orange",
                warn=never_returned > 0,
            ),
            cols_md=3,
            gap=4,
        ),
        Card(
            H3(
                "Recently Active for Outreach",
                cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-1",
            ),
            P(
                "Top 20 by latest login in the last 30 days; this is login recency, not login frequency.",
                cls="text-xs text-muted-foreground mb-3",
            ),
            Div(
                Table(
                    Thead(
                        Tr(
                            *[
                                Th(
                                    label,
                                    cls="px-3 py-2 text-left text-xs font-mono uppercase tracking-wide text-muted-foreground",
                                )
                                for label in ("Name", "Email", "Last login", "Joined")
                            ]
                        ),
                        cls="border-b border-border",
                    ),
                    users_body,
                    cls="w-full text-sm",
                ),
                cls="overflow-x-auto",
            ),
            body_cls="p-5",
            cls="mt-4",
        ),
        cls="mb-6",
    )


def _RevenueSection(data: dict) -> Div:
    total_users = data.get("total_users", 0)
    active_paid = data.get("plan_active_paid", 0)
    monthly = data.get("plan_pro_monthly", 0)
    annual = data.get("plan_pro_annual", 0)
    trialing = data.get("plan_trialing", 0)
    past_due = data.get("plan_past_due", 0)
    total_favs = data.get("total_favourites", 0)
    inquiries_new = data.get("inquiries_new_7d", 0)
    inquiries_unfwd = data.get("inquiries_unforwarded", 0)
    conversion = f"{active_paid / total_users * 100:.1f}% conversion" if total_users else "—"

    billing_card = Card(
        H3(
            "Revenue & Plans",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        _KVRow("Paid (active)", f"{active_paid:,}  ({conversion})"),
        _KVRow("Pro Monthly", f"{monthly:,}"),
        _KVRow("Pro Annual", f"{annual:,}"),
        _KVRow("Trialing", f"{trialing:,}", warn=trialing > 0),
        _KVRow("Past Due", f"{past_due:,}", warn=past_due > 0),
        body_cls="p-5",
    )

    signals_card = Card(
        H3(
            "Engagement & Inbox",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        _KVRow("Favourites saved (total)", f"{total_favs:,}"),
        _KVRow("Contact inquiries (7d)", f"{inquiries_new:,}"),
        _KVRow("Pending forward", f"{inquiries_unfwd:,}", warn=inquiries_unfwd > 0),
        body_cls="p-5",
    )

    return Grid(billing_card, signals_card, cols_md=2, gap=4, cls="mb-6")


def _QueueSection(data: dict) -> Div:
    pending = data["queue_pending"]
    oldest_str = _fmt_age(data.get("oldest_pending_secs"))
    warn_pending = pending > 100_000
    warn_failed = data["queue_failed"] > 0

    failed_quota = data.get("failed_quota", 0)
    failed_invalid = data.get("failed_invalid", 0)
    failed_other = data.get("failed_other", 0)

    failed_detail = Card(
        H3(
            "Failed Jobs",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        _KVRow("Quota exceeded", f"{failed_quota:,}", warn=failed_quota > 0),
        _KVRow("Invalid channel", f"{failed_invalid:,}"),
        _KVRow("Other", f"{failed_other:,}"),
        Div(
            (
                Button(
                    f"Rescue {failed_quota:,} quota jobs",
                    cls="text-xs px-3 py-1.5 rounded bg-green-600 hover:bg-green-700 text-white font-medium mt-3",
                    **{
                        "hx-post": "/admin/rescue-quota-jobs",
                        "hx-target": "#rescue-result",
                        "hx-swap": "innerHTML",
                        "hx-confirm": f"Reset {failed_quota:,} quota-failed jobs to pending?",
                    },
                )
                if failed_quota > 0
                else None
            ),
            Div(id="rescue-result", cls="mt-2"),
        ),
        body_cls="p-5",
        cls="border-l-4 border-red-400" if warn_failed else "",
    )

    return Div(
        H3(
            "Job Queue",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        Grid(
            _StatCard(
                "Pending", pending, f"oldest waiting: {oldest_str}", "yellow", warn=warn_pending
            ),
            _StatCard("Processing", data["queue_processing"], "active right now", "purple"),
            _StatCard(
                "Failed", data["queue_failed"], "need manual review", "red", warn=warn_failed
            ),
            cols_md=3,
            gap=4,
        ),
        (failed_detail if warn_failed else None),
        cls="mb-6",
    )


def _CreatorSyncHealthSection(data: dict) -> Div:
    available = data.get("creator_sync_health_available", False)
    ready_jobs = data.get("creator_sync_ready_jobs")
    delayed_jobs = data.get("creator_sync_delayed_jobs")
    processing_jobs = data.get("creator_sync_processing_jobs")
    ready_secs = data.get("creator_sync_oldest_ready_secs")
    processing_secs = data.get("creator_sync_oldest_processing_secs")
    unqueued_stale = data.get("creator_sync_unqueued_stale")
    oldest_secs = data.get("creator_sync_oldest_unqueued_secs")
    last_success_secs = data.get("creator_sync_last_success_secs")
    coverage_available = data.get("creator_sync_coverage_available", False)
    coverage_age = data.get("creator_sync_coverage_age_secs")
    coverage_stale = (
        coverage_available and coverage_age > CREATOR_SYNC_COVERAGE_MAX_AGE_HOURS * 3600
    )
    waiting_secs = max(ready_secs or 0, processing_secs or 0)

    if not available:
        status, status_cls = "Unavailable", "text-gray-600 bg-gray-100"
        summary = "Health metrics unavailable; check server logs."
    elif ready_jobs > 0 or processing_jobs > 0:
        stalled = waiting_secs > CREATOR_SYNC_STALL_HOURS * 3600 and (
            last_success_secs is None
            or last_success_secs > CREATOR_SYNC_PROGRESS_GRACE_HOURS * 3600
        )
        status = "Stalled" if stalled else "Behind"
        if not stalled and waiting_secs <= CREATOR_SYNC_READY_WARNING_HOURS * 3600:
            status = "Healthy"
        status_cls = "text-red-700 bg-red-100" if stalled else "text-orange-700 bg-orange-100"
        if status == "Healthy":
            status_cls = "text-green-700 bg-green-100"
        summary = (
            "Overdue sync work has no recent successful completions."
            if stalled
            else (
                "Sync work has exceeded its waiting-time target."
                if status == "Behind"
                else "Sync work is within the daily schedule's waiting-time target."
            )
        )
    else:
        status, status_cls = "Idle", "text-green-700 bg-green-100"
        summary = "No runnable or processing sync jobs."

    def _count_text(value):
        return f"{value:,}" if available else "Unavailable"

    oldest_text = (
        _fmt_age(oldest_secs)
        if coverage_available and unqueued_stale > 0 and oldest_secs is not None
        else ("—" if coverage_available else "Unavailable")
    )
    last_success_text = (
        _fmt_ago(last_success_secs)
        if last_success_secs is not None
        else ("Never" if available else "Unavailable")
    )

    return Card(
        DivFullySpaced(
            H3(
                "Creator Sync Health",
                cls="text-sm font-mono uppercase tracking-widest text-muted-foreground",
            ),
            Span(
                status,
                cls=f"px-2.5 py-1 rounded-full text-xs font-semibold uppercase tracking-wide {status_cls}",
            ),
            cls="mb-2",
        ),
        P(summary, cls="text-sm text-muted-foreground mb-3"),
        _KVRow("Ready jobs", _count_text(ready_jobs)),
        _KVRow("Retry-delayed jobs", _count_text(delayed_jobs)),
        _KVRow("Processing jobs", _count_text(processing_jobs)),
        _KVRow("Oldest ready wait", _fmt_age(ready_secs) if available else "Unavailable"),
        _KVRow("Oldest processing age", _fmt_age(processing_secs) if available else "Unavailable"),
        _KVRow(
            "Eligible creators without a job",
            f"{unqueued_stale:,}" if coverage_available else "Unavailable",
        ),
        _KVRow("Oldest eligible wait", oldest_text),
        _KVRow(
            "Coverage updated",
            (
                _fmt_ago(coverage_age) + (" (stale)" if coverage_stale else "")
                if coverage_available
                else "Unavailable"
            ),
            warn=coverage_stale,
        ),
        _KVRow(
            "Coverage minimum subscribers",
            (
                f"{data['creator_sync_coverage_min_subscribers']:,}"
                if coverage_available
                else "Unavailable"
            ),
        ),
        _KVRow(
            "Last successful sync",
            last_success_text,
            warn=status == "Stalled",
        ),
        body_cls="p-5",
        cls=(
            "mb-6 border-l-4 border-gray-300"
            if not available
            else (
                "mb-6 border-l-4 border-red-400"
                if status == "Stalled"
                else (
                    "mb-6 border-l-4 border-orange-400"
                    if status == "Behind"
                    else "mb-6 border-l-4 border-green-400"
                )
            )
        ),
    )


def _InventorySection(data: dict) -> Div:
    total = data["total_creators"]
    invisible = total - data["visible"]

    return Div(
        H3(
            "Creator Inventory",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        Grid(
            _StatCard("Total in DB", total, "all records", "blue"),
            _StatCard(
                "Visible", data["visible"], "synced + synced_partial + named + subs", "green"
            ),
            _StatCard("Invisible", invisible, "not yet synced, failed, or invalid", "gray"),
            _StatCard("Fresh (<7d)", data["fresh_7d"], "synced within last 7 days", "green"),
            _StatCard(
                "Stale (>30d)",
                data["stale_30d"],
                "synced but overdue refresh",
                "orange",
                warn=data["stale_30d"] > 0,
            ),
            _StatCard(
                "Never Synced",
                data["never_synced"],
                "last_synced_at is NULL",
                "red",
                warn=data["never_synced"] > 0,
            ),
            cols_md=3,
            gap=4,
        ),
        cls="mb-6",
    )


def _WorkerSection(data: dict) -> Div:
    completed_1h = data["completed_1h"]
    completed_24h = data["completed_24h"]
    pending = data["queue_pending"]

    rate_24h = completed_24h / 24.0  # jobs/hour (24h window — authoritative for batch worker)

    last_secs = data.get("last_completed_secs")
    # Stale = nothing completed in last 24h (covers idle Kaggle workers between runs)
    worker_stale = completed_24h == 0

    # 1h row: distinguish "idle this hour but worked today" from "truly silent"
    if completed_1h > 0:
        completed_1h_str = f"{completed_1h:,}"
    elif completed_24h > 0:
        completed_1h_str = "0 (idle this hour)"
    else:
        completed_1h_str = "0"

    throughput_card = Card(
        H3(
            "Worker Throughput",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        _KVRow("Jobs completed (last 1h)", completed_1h_str),
        _KVRow("Jobs completed (last 24h)", f"{completed_24h:,}"),
        _KVRow("Avg job duration", _fmt_dur(data.get("avg_job_secs"))),
        _KVRow("Last completed", _fmt_ago(last_secs), warn=worker_stale),
        body_cls="p-5",
    )

    drain_card = Card(
        H3(
            "Queue Drain Estimate",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        _KVRow(
            "At 24h avg rate",
            _fmt_drain(pending, rate_24h),
            warn=pending > 0 and rate_24h > 0 and (pending / rate_24h) > 24 * 14,
        ),
        _KVRow("Rate (jobs/hr)", f"{rate_24h:.0f}" if rate_24h > 0 else "—"),
        _KVRow("Queue depth", f"{pending:,} jobs"),
        _KVRow("Oldest job waiting", _fmt_age(data.get("oldest_pending_secs"))),
        body_cls="p-5",
    )

    return Grid(throughput_card, drain_card, cols_md=2, gap=4, cls="mb-6")


def _FreshnessSection(data: dict) -> Div:
    """Four-tier freshness breakdown for synced creators."""
    synced = data.get("synced", 0)
    fresh_7d = data.get("fresh_7d", 0)
    fresh_7_30d = data.get("fresh_7_30d", 0)
    stale_30_90d = data.get("stale_30_90d", 0)
    stale_90d = data.get("stale_90d", 0)

    def _bar(label, count, color):
        pct = round(count / synced * 100, 1) if synced else 0.0
        bar_w = max(int(pct), 2) if count > 0 else 0
        return Div(
            Span(label, cls="text-sm font-medium w-36 inline-block"),
            Div(
                Div(cls=f"{color} h-3 rounded-full", style=f"width:{bar_w}%"),
                cls="flex-1 bg-accent rounded-full mx-3 h-3",
            ),
            Span(f"{count:,}", cls="text-sm font-mono text-muted-foreground w-24 text-right"),
            Span(f"{pct}%", cls="text-sm font-mono text-muted-foreground w-14 text-right"),
            cls="flex items-center py-1",
        )

    return Card(
        H3(
            "Sync Freshness (synced creators)",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        _bar("< 7 days", fresh_7d, "bg-green-500"),
        _bar("7 – 30 days", fresh_7_30d, "bg-lime-400"),
        _bar("30 – 90 days", stale_30_90d, "bg-orange-400"),
        _bar("> 90 days", stale_90d, "bg-red-500"),
        body_cls="p-5 space-y-1",
        cls="mb-6",
    )


def _DataQualitySection(data: dict) -> Div:
    """Data completeness metrics for the synced creator pool."""
    synced = data.get("synced", 0)

    def _pct(n):
        return f"{n / synced * 100:.1f}%" if synced else "—"

    engagement = data.get("creators_with_engagement", 0)
    grade = data.get("creators_with_grade", 0)
    recent_perf = data.get("creators_with_recent_perf", 0)
    categories = data.get("distinct_categories", 0)
    countries = data.get("distinct_countries", 0)

    quality_card = Card(
        H3(
            "Data Completeness (synced)",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        _KVRow(
            "Engagement score",
            f"{engagement:,}  ({_pct(engagement)})",
            warn=engagement < synced * 0.5,
        ),
        _KVRow("Quality grade", f"{grade:,}  ({_pct(grade)})", warn=grade < synced * 0.5),
        _KVRow(
            "Recent perf (last 10 vids)",
            f"{recent_perf:,}  ({_pct(recent_perf)})",
            warn=recent_perf < synced * 0.3,
        ),
        body_cls="p-5",
    )

    coverage_card = Card(
        H3(
            "Taxonomy Coverage",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        _KVRow("Distinct categories", f"{categories:,}"),
        _KVRow("Distinct countries", f"{countries:,}"),
        body_cls="p-5",
    )

    return Grid(quality_card, coverage_card, cols_md=2, gap=4, cls="mb-6")


def _OutreachSection(data: dict) -> Div:
    """
    Contact signals & outreach stats.
    Shows coverage of extracted contact info (email, Instagram, etc.)
    and provides direct download link to admin export.
    """
    total = data.get("total_creators", 0)
    with_contact = data.get("creators_with_contact", 0)
    with_email = data.get("creators_with_email", 0)
    with_instagram = data.get("creators_with_instagram", 0)
    last_extracted_secs = data.get("last_contact_extracted_at")

    contact_pct = (with_contact / total * 100) if total > 0 else 0
    email_pct = (with_email / total * 100) if total > 0 else 0
    instagram_pct = (with_instagram / total * 100) if total > 0 else 0

    coverage_card = Card(
        H3(
            "Contact Signal Coverage",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        _KVRow("Creators with any contact", f"{with_contact:,} ({contact_pct:.1f}%)"),
        _KVRow("Creators with email", f"{with_email:,} ({email_pct:.1f}%)"),
        _KVRow("Creators with Instagram", f"{with_instagram:,} ({instagram_pct:.1f}%)"),
        _KVRow("Last extracted", _fmt_ago(last_extracted_secs) if last_extracted_secs else "—"),
        body_cls="p-5",
    )

    export_card = Card(
        H3(
            "Bulk Outreach",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        Div(
            P(
                "Export all creators with email addresses for mass outreach campaigns.",
                cls="text-sm text-muted-foreground mb-4",
            ),
            A(
                "📥 Export All Creators (CSV)",
                href="/admin/outreach/export",
                cls="inline-block px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 font-medium text-sm",
            ),
            cls="flex flex-col gap-3",
        ),
        body_cls="p-5",
    )

    return Grid(coverage_card, export_card, cols_md=2, gap=4, cls="mb-6")


def _BreakdownSection(data: dict) -> Div:
    total = data["total_creators"]
    breakdown = {
        "synced": data["synced"],
        "synced_partial": data.get("synced_partial", 0),
        "pending": data["pending_creators"],
        "failed": data["failed_creators"],
        "invalid": data["invalid_creators"],
    }
    return Card(
        H3(
            "Sync Status Breakdown",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        *[
            _StatusBar(k, breakdown[k], total, _BAR_COLOR.get(k, "bg-gray-300"))
            for k in ("synced", "synced_partial", "pending", "failed", "invalid")
        ],
        body_cls="p-5 space-y-1",
        cls="mb-6",
    )


def _JobsSection(jobs: list[dict]) -> Div:
    if not jobs:
        body = P("No recent jobs.", cls="text-sm text-muted-foreground py-6 text-center")
    else:
        headers = ["ID", "Creator", "Type", "Status", "Duration", "Retries", "Error", "Age"]
        body = Div(
            Table(
                Thead(
                    Tr(
                        *[
                            Th(
                                h,
                                cls="px-3 py-2 text-left text-xs font-mono uppercase tracking-wide text-muted-foreground",
                            )
                            for h in headers
                        ]
                    ),
                    cls="border-b border-border",
                ),
                Tbody(*[_JobRow(j) for j in jobs]),
                cls="w-full text-sm",
            ),
            cls="overflow-x-auto",
        )

    return Card(
        H3(
            "Recent Jobs (20)",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        body,
        body_cls="p-5",
        id="jobs-panel",
        hx_get="/admin/jobs",
        hx_trigger="every 5m",
        hx_swap="outerHTML",
    )


# ── Full page ─────────────────────────────────────────────────────────────────


def AdminPage(data: dict, refreshed_at: str = "") -> FT:
    """Admin page content. Caller (main.py) wraps with Titled + NavComponent."""
    return Div(
        # Sticky top bar
        Div(
            Div(
                Span("⚙️", cls="mr-2"),
                Span("Admin", cls="font-bold text-foreground"),
                Span(" · ViralVibes", cls="text-muted-foreground text-sm"),
                cls="flex items-center",
            ),
            Div(
                Span(f"Refreshed {refreshed_at}", cls="text-xs text-muted-foreground mr-3"),
                A("↻ Refresh", href="/admin", cls="text-xs text-primary hover:underline"),
                cls="flex items-center",
            ),
            cls="flex items-center justify-between bg-background border-b border-border px-6 py-3 mb-6 sticky top-0 z-40 shadow-sm",
        ),
        Container(
            _UsersSection(data),
            _RevenueSection(data),
            _QueueSection(data),
            _CreatorSyncHealthSection(data),
            _InventorySection(data),
            _FreshnessSection(data),
            _DataQualitySection(data),
            _WorkerSection(data),
            _OutreachSection(data),
            _BreakdownSection(data),
            _JobsSection(data["recent_jobs"]),
            cls="max-w-6xl mx-auto px-4 pb-16",
        ),
    )
