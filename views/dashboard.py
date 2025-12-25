from datetime import date, datetime

from fasthtml.common import *
from fasthtml.common import RedirectResponse
from fasthtml.core import HtmxHeaders
from starlette.responses import StreamingResponse

from components.tables import VideoExtremesSection
from constants import PLAYLIST_STEPS_CONFIG
from services.playlist_loader import load_cached_or_stub
from step_components import StepProgress
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
    mode: str = "session",  # "session" | "persistent"
    dashboard_id: str | None = None,
    interest: dict | None = None,  # {"view": int, "share": int}
):
    """Render the full dashboard view."""

    return Div(
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
