"""
views/admin.py — Admin dashboard rendering.

All FT components for the /admin page. No auth logic here — that lives in
routes/admin.py. Data is passed in as a flat dict from _fetch_admin_data().
"""

from __future__ import annotations

from datetime import datetime, timezone

from fasthtml.common import *
from monsterui.all import *


def _parse_iso_utc(s: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp (with or without trailing Z) to a UTC-aware datetime."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


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
    s_dt = _parse_iso_utc(job.get("started_at"))
    c_dt = _parse_iso_utc(job.get("completed_at"))
    if s_dt and c_dt:
        dur = (c_dt - s_dt).total_seconds()
        if dur >= 0:
            duration = _fmt_dur(dur)

    # Age of last update
    age_ts = job.get("completed_at") or job.get("created_at")
    age = "—"
    age_dt = _parse_iso_utc(age_ts)
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


def _QueueSection(data: dict) -> Div:
    pending = data["queue_pending"]
    oldest_str = _fmt_age(data.get("oldest_pending_secs"))
    warn_pending = pending > 100_000
    warn_failed = data["queue_failed"] > 0

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
        cls="mb-6",
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
            _StatCard("Visible", data["visible"], "synced + named + has subscribers", "green"),
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


def _BreakdownSection(data: dict) -> Div:
    total = data["total_creators"]
    breakdown = {
        "synced": data["synced"],
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
            for k in ("synced", "pending", "failed", "invalid")
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
        **{"hx-get": "/admin/jobs", "hx-trigger": "every 30s", "hx-swap": "outerHTML"},
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
            _QueueSection(data),
            _InventorySection(data),
            _WorkerSection(data),
            _BreakdownSection(data),
            _JobsSection(data["recent_jobs"]),
            cls="max-w-6xl mx-auto px-4 pb-16",
        ),
    )
