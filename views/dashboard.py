from datetime import date, datetime

from fasthtml.common import *

from components import (
    AnalyticsDashboardSection,
    AnalyticsHeader,
    VideoExtremesSection,
)
from constants import PLAYLIST_STEPS_CONFIG
from step_components import StepProgress
from views.table import render_playlist_table


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
):
    return Div(
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
