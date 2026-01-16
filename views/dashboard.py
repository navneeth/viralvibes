from datetime import date, datetime

from fasthtml.common import *
from fasthtml.common import RedirectResponse
from fasthtml.core import HtmxHeaders
from monsterui.all import *
from starlette.responses import StreamingResponse

from components.modals import ExportModal, ShareModal
from components.steps import StepProgress
from components.tables import VideoExtremesSection
from constants import PLAYLIST_STEPS_CONFIG
from services.playlist_loader import load_cached_or_stub
from ui_components import (
    AnalyticsDashboardSection,
    AnalyticsHeader,
)
from views.table import render_playlist_table


def PersistentDashboardMetaBar(*, dashboard_id: str, interest: dict | None):
    interest = interest or {}

    return Div(
        Div(
            A(
                "🔗 Permanent link",
                href=f"/d/{dashboard_id}",
                cls="text-sm text-blue-600 hover:underline font-medium",
            ),
            Div(
                Span(
                    f"{interest.get('view', 0)} views",
                    cls="text-xs text-gray-500",
                ),
                Span(
                    f"{interest.get('share', 0)} shares",
                    cls="text-xs text-gray-500",
                ),
                cls="flex gap-3",
            ),
            cls="flex items-center justify-between",
        ),
        cls="p-3 bg-gray-50 border border-gray-200 rounded-lg",
    )


def EmbeddedDashboardBadge(*, dashboard_id: str):
    """Badge shown on embedded dashboards linking to the dedicated page."""
    return A(
        Span("Open full dashboard", cls="mr-1"),
        UkIcon("external-link", width=14, height=14),
        href=f"/d/{dashboard_id}",
        target="_blank",
        cls=(
            "inline-flex items-center gap-1 "
            "px-3 py-1 rounded-full text-xs font-semibold "
            "bg-indigo-100 text-indigo-700 "
            "hover:bg-indigo-200 transition"
        ),
    )


def render_full_dashboard(
    *,
    df,
    summary_stats,
    playlist_name,
    channel_name,
    channel_thumbnail,
    playlist_url,
    valid_sort,
    valid_order,
    next_order,
    cached_stats=None,
    mode: str = "embedded",
    dashboard_id: str | None = None,
    interest: dict | None = None,  # {"view": int, "share": int}
):
    """Render the full dashboard view."""

    return Div(
        # Modal container (at the top, before existing content)
        Div(id="modal-container", cls="relative z-50"),
        # 🔽 Persistent dashboard meta
        (
            PersistentDashboardMetaBar(
                dashboard_id=dashboard_id,
                interest=interest,
            )
            if mode == "persistent" and dashboard_id
            else None
        ),
        # Row 1: Steps + Header side by side
        Div(
            Div(
                StepProgress(len(PLAYLIST_STEPS_CONFIG)),
                cls="flex-1 p-4 bg-white rounded-xl shadow-sm border border-gray-100",
            ),
            Div(
                AnalyticsHeader(
                    playlist_title=playlist_name,
                    channel_name=channel_name,
                    total_videos=summary_stats.get("actual_playlist_count", 0),
                    processed_videos=df.height,
                    playlist_thumbnail=(
                        cached_stats.get("playlist_thumbnail") if cached_stats else None
                    ),
                    channel_url=None,  # optional
                    channel_thumbnail=channel_thumbnail,
                    processed_date=date.today().strftime("%b %d, %Y"),
                    engagement_rate=summary_stats.get("avg_engagement"),
                    total_views=summary_stats.get("total_views"),
                ),
                # 🔗 Add Share/Export buttons HERE
                (
                    Div(
                        Button(
                            UkIcon("share-2", cls="mr-2 w-4 h-4"),
                            "Share",
                            hx_get=(
                                f"/modal/share/{dashboard_id}" if dashboard_id else "#"
                            ),
                            hx_target="#modal-container",
                            hx_swap="innerHTML",
                            cls="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center",
                            type="button",
                            disabled=(dashboard_id is None),
                        ),
                        Button(
                            UkIcon("download", cls="mr-2 w-4 h-4"),
                            "Export",
                            hx_get=(
                                f"/modal/export/{dashboard_id}" if dashboard_id else "#"
                            ),
                            hx_target="#modal-container",
                            hx_swap="innerHTML",
                            cls="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors flex items-center",
                            type="button",
                            disabled=(dashboard_id is None),
                        ),
                        cls="flex gap-3 mt-4",
                    )
                    if dashboard_id
                    else None
                ),
                # 🔗 Dedicated dashboard link (ONLY in embedded mode)
                (
                    Div(
                        EmbeddedDashboardBadge(dashboard_id=dashboard_id),
                        cls="mt-3",
                    )
                    if mode == "embedded" and dashboard_id
                    else None
                ),
                cls="flex-1",
            ),
            cls="grid grid-cols-1 md:grid-cols-2 gap-6 items-start mb-8",
        ),
        # Row 2: Table
        render_playlist_table(
            df=df,
            summary_stats=summary_stats,
            playlist_url=playlist_url,
            valid_sort=valid_sort,
            valid_order=valid_order,
            next_order=next_order,
        ),
        # Row 2.5: Video Extremes Section
        VideoExtremesSection(df),
        # Row 3: Analytics dashboard / plots
        Div(
            AnalyticsDashboardSection(
                df,
                summary_stats,
                playlist_name,
                A(
                    channel_name,
                    href=playlist_url,
                    target="_blank",
                    cls="text-blue-600 hover:underline",
                ),
            ),
            cls="mt-6",
        ),
        cls="space-y-8",
    )
