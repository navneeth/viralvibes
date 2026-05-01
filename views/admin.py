"""
views/admin.py — Admin dashboard rendering.

All FT components for the /admin page. No auth logic here — that lives in
routes/admin.py. Data is passed in as plain dicts from the route handler.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fasthtml.common import *
from monsterui.all import *

# ── Palette constants ─────────────────────────────────────────────────────────

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

_JOB_STATUS_CLS = {
    "completed": "text-green-600 bg-green-50",
    "synced": "text-green-600 bg-green-50",
    "processing": "text-purple-600 bg-purple-50",
    "pending": "text-yellow-600 bg-yellow-50",
    "failed": "text-red-600 bg-red-50",
    "blocked": "text-orange-600 bg-orange-50",
}


# ── Atomic components ─────────────────────────────────────────────────────────


def _StatCard(label: str, value: str, sublabel: str, color_key: str) -> Div:
    color = _STAT_COLORS.get(color_key, "text-gray-700")
    return Card(
        P(label, cls="text-xs font-mono uppercase tracking-widest text-muted-foreground mb-1"),
        P(value, cls=f"text-3xl font-bold {color} font-mono"),
        P(sublabel, cls="text-xs text-muted-foreground mt-1"),
        body_cls="p-5",
        cls="hover:shadow-md transition-shadow",
    )


def _StatusBar(label: str, count: int, total: int, color_cls: str) -> Div:
    pct = round(count / total * 100, 1) if total else 0.0
    bar_w = max(int(pct), 2) if count > 0 else 0
    return Div(
        Span(label, cls="text-sm font-medium text-foreground w-20 inline-block"),
        Div(
            Div(cls=f"{color_cls} h-3 rounded-full", style=f"width:{bar_w}%"),
            cls="flex-1 bg-accent rounded-full mx-3 h-3",
        ),
        Span(f"{count:,}", cls="text-sm font-mono text-muted-foreground w-20 text-right"),
        Span(f"{pct}%", cls="text-sm font-mono text-muted-foreground w-14 text-right"),
        cls="flex items-center py-1",
    )


def _JobRow(job: dict) -> Tr:
    status = job.get("status", "unknown")
    status_cls = _JOB_STATUS_CLS.get(status, "text-muted-foreground bg-accent")
    icon = "✅" if status in ("completed", "synced") else "❌" if status == "failed" else "⏳"

    started = job.get("started_at") or job.get("created_at")
    finished = job.get("finished_at") or job.get("updated_at")
    duration = "—"
    if started and finished:
        try:
            s = datetime.fromisoformat(started.replace("Z", "+00:00"))
            f = datetime.fromisoformat(finished.replace("Z", "+00:00"))
            secs = int((f - s).total_seconds())
            duration = f"{secs}s" if secs >= 0 else "—"
        except Exception:
            pass

    creator_id = str(job.get("creator_id") or "")[:8]
    return Tr(
        Td(Code(f"#{job.get('id', '')}", cls="text-xs"), cls="px-3 py-2"),
        Td(Code(creator_id + "…", cls="text-xs text-primary"), cls="px-3 py-2"),
        Td(
            Span(status, cls=f"text-xs font-medium px-2 py-0.5 rounded-full {status_cls}"),
            cls="px-3 py-2",
        ),
        Td(duration, cls="px-3 py-2 text-xs font-mono text-muted-foreground"),
        Td(
            Span(f"retry:{job.get('retry_count', 0)}", cls="text-xs text-muted-foreground"),
            cls="px-3 py-2",
        ),
        Td(icon, cls="px-3 py-2 text-center"),
        cls="border-b border-border hover:bg-accent/50",
    )


# ── Section builders ──────────────────────────────────────────────────────────


def _HeroSection(stats: dict) -> Div:
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
    return Card(
        H3(
            "Sync Status",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        *[
            _StatusBar(k, breakdown.get(k, 0), total, _STATUS_BAR_COLORS.get(k, "bg-gray-300"))
            for k in ("synced", "pending", "failed", "invalid")
        ],
        body_cls="p-5 space-y-1",
    )


def _ThroughputSection(throughput: dict) -> Div:
    def _row(label, value):
        return DivFullySpaced(
            Span(label, cls="text-sm text-muted-foreground"),
            Span(value, cls="text-sm font-mono font-medium text-foreground"),
            cls="py-1.5 border-b border-border last:border-0",
        )

    quota_used = throughput.get("quota_used", 0)
    quota_total = throughput.get("quota_total", 10000)
    quota_pct = round(quota_used / quota_total * 100, 1) if quota_total else 0
    return Card(
        H3(
            "Worker Health",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        _row("Jobs / hour (est.)", str(throughput.get("jobs_per_hour", "—"))),
        _row("Avg duration", throughput.get("avg_duration", "—")),
        _row("Last job", throughput.get("last_job_ago", "—")),
        _row("YT Quota", f"{quota_used:,} / {quota_total:,} ({quota_pct}%)"),
        Div(
            Div(
                cls="bg-orange-400 h-2 rounded-full transition-all",
                style=f"width:{min(quota_pct, 100)}%",
            ),
            cls="w-full bg-accent rounded-full h-2 mt-3",
        ),
        body_cls="p-5",
    )


def _JobsSection(jobs: list[dict]) -> Div:
    if not jobs:
        body = P("No recent jobs.", cls="text-sm text-muted-foreground py-6 text-center")
    else:
        body = Div(
            Table(
                Thead(
                    Tr(
                        *[
                            Th(
                                h,
                                cls="px-3 py-2 text-left text-xs font-mono uppercase tracking-wide text-muted-foreground",
                            )
                            for h in ["ID", "Creator", "Status", "Duration", "Retries", ""]
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
            "Recent Jobs",
            cls="text-sm font-mono uppercase tracking-widest text-muted-foreground mb-4",
        ),
        # HTMX auto-refresh every 30s — only the jobs panel, no full reload
        body,
        body_cls="p-5",
        cls="mb-6",
        id="jobs-panel",
        **{"hx-get": "/admin/jobs", "hx-trigger": "every 30s", "hx-swap": "outerHTML"},
    )


# ── Full page ─────────────────────────────────────────────────────────────────


def AdminPage(
    stats: dict,
    breakdown: dict,
    throughput: dict,
    jobs: list[dict],
    refreshed_at: str = "",
) -> FT:
    """Full admin page. Data passed in as plain dicts from route handler."""
    total = sum(breakdown.values()) or 1

    return Titled(
        "Admin — ViralVibes",
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
    )
