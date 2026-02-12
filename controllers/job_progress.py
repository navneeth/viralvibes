# controllers/job_progress.py

import logging
from dataclasses import dataclass
from urllib.parse import quote_plus

from fasthtml.common import *
from monsterui.all import *

from components.errors import get_user_friendly_error
from components.processing_tips import get_tip_for_progress
from constants import JobStatus
from db import (
    get_estimated_stats,
    get_job_progress,
    get_playlist_preview_info,
)
from utils import (
    clamp,
    compute_batches,
    compute_dashboard_id,
    create_redirect_script,
    estimate_remaining_time,
    format_number,
    format_seconds,
)
from views.job_progress import render_job_progress_view
from views.job_progress_state import JobProgressViewState

logger = logging.getLogger(__name__)


def job_progress_controller(playlist_url: str):
    """
    Show job progress with auto-redirect on completion.

    Polls job status every 2 seconds and:
    - Redirects to dashboard when complete
    - Shows progress UI while processing
    - Shows error/warning for failed/blocked jobs

    Args:
        playlist_url: YouTube playlist URL

    Returns:
        HTML component (progress UI or redirect script)
    """
    # ✅ Reduced to debug level (polled every 2s)
    logger.debug(f"Job progress check for: {playlist_url}")

    # Get job progress data
    job_data = get_job_progress(playlist_url)

    if not job_data:
        logger.warning(f"No job found for {playlist_url}")
        return Div(
            Alert(
                P("No analysis job found for this playlist."),
                A("Try analyzing again", href="/", cls=ButtonT.primary),
                cls=AlertT.warning,
            ),
            cls="p-6 max-w-2xl mx-auto",
        )

    status = job_data.get("status")
    progress = job_data.get("progress", 0)
    error = job_data.get("error")

    # ✅ Only log on status transitions (not every poll)
    # This could be enhanced with a global state tracker if needed
    logger.debug(f"Job status: {status}, progress: {progress}%")

    # ========================================================
    # ✅ CHECK FOR COMPLETION → REDIRECT
    # ========================================================
    if status in JobStatus.SUCCESS:
        # ✅ Only log INFO for important state transitions
        logger.info(f"✅ Job complete for {playlist_url}, redirecting to dashboard")

        dashboard_id = compute_dashboard_id(playlist_url)
        redirect_url = f"/d/{dashboard_id}"

        return Div(
            # Show completion message briefly
            Alert(
                P("✅ Analysis complete! Redirecting to dashboard..."),
                cls=AlertT.success,
            ),
            # ✅ Use safe redirect helper
            Script(
                create_redirect_script(
                    url=redirect_url,
                    delay_ms=500,
                    message=f"Job complete, redirecting to {redirect_url}",
                )
            ),
            cls="p-6 max-w-2xl mx-auto",
        )

    # ========================================================
    # HANDLE FAILED JOB
    # ========================================================
    if status == JobStatus.FAILED:
        error = job_data.get("error", "Unknown error")
        error_info = get_user_friendly_error(error)

        logger.error(f"❌ Job failed for {playlist_url}: {error}")
        return Div(
            Alert(
                H3(error_info["title"], cls="text-lg font-bold mb-3"),
                P(error_info["message"], cls="text-sm text-gray-700 mb-4"),
                # Suggestions
                (
                    Div(
                        P("💡 Try:", cls="font-semibold text-sm mb-2"),
                        Ul(
                            *[
                                Li(s, cls="text-xs text-gray-600")
                                for s in error_info.get("suggestions", [])
                            ],
                            cls="list-disc list-inside space-y-1",
                        ),
                        cls="bg-blue-50 p-3 rounded mb-4",
                    )
                    if error_info.get("suggestions")
                    else None
                ),
                # Actions
                Div(
                    A(
                        Button("Try Again", cls=ButtonT.primary),
                        href="/#analyze-section",
                    ),
                    A(
                        Button("Go Home", cls=ButtonT.secondary),
                        href="/",
                    ),
                    cls="flex gap-3",
                ),
                cls=AlertT.error,
            ),
            cls="p-6 max-w-2xl mx-auto",
        )

    # ========================================================
    # HANDLE BLOCKED JOB
    # ========================================================
    if status == JobStatus.BLOCKED:
        logger.warning(f"⚠️  Job blocked for {playlist_url}")
        return Div(
            Alert(
                H3("Analysis Blocked", cls="text-lg font-bold mb-2"),
                P(
                    "YouTube's bot protection blocked this analysis. "
                    "This usually happens with very large playlists or rate limiting."
                ),
                P("Please try again in a few minutes.", cls="text-sm mt-2"),
                A("Go back", href="/", cls=f"{ButtonT.primary} mt-4"),
                cls=AlertT.warning,
            ),
            cls="p-6 max-w-2xl mx-auto",
        )

    # ========================================================
    # SHOW PROGRESS UI (processing/pending/queued)
    # ========================================================
    preview_info = get_playlist_preview_info(playlist_url) or {}

    return render_job_progress_ui(
        playlist_url=playlist_url,
        status=status,
        progress=progress,
        preview_info=preview_info,
    )


def render_job_progress_ui(
    playlist_url: str,
    status: str | None,
    progress: float,
    preview_info: dict,
):
    """
    Render the progress UI with preview data.

    Shows:
    - Playlist preview (thumbnail, title, etc)
    - Progress bar
    - Status message
    - Estimated time remaining
    """
    # Extract preview fields
    title = preview_info.get("title", "YouTube Playlist")
    channel = preview_info.get("channel_name", "Unknown Channel")
    thumbnail = preview_info.get("thumbnail", "/static/favicon.jpeg")
    video_count = preview_info.get("video_count", 0)

    # ✅ Use centralized time estimation utility
    _, time_label = estimate_remaining_time(video_count, progress)

    # ✅ Clamp progress to valid range [0, 100]
    clamped_progress = clamp(progress, 0.0, 100.0)

    # ✅ Handle missing/unknown status gracefully
    if not status or status not in JobStatus.ACTIVE:
        status = JobStatus.QUEUED  # Default to queued if unknown

    # ✅ Status messages using constants
    status_messages = {
        JobStatus.QUEUED: "Waiting in queue...",
        JobStatus.PENDING: "Starting analysis...",
        JobStatus.PROCESSING: f"Analyzing videos... {int(clamped_progress)}% complete",
    }
    status_message = status_messages.get(status, "Preparing analysis...")

    return Div(
        # Header
        Div(
            Img(
                src=thumbnail,
                alt="Playlist thumbnail",
                cls="w-24 h-24 rounded-xl shadow object-cover border",
                onerror="this.src='/static/favicon.jpeg'",
            ),
            Div(
                H2(title, cls="text-2xl font-bold text-gray-900"),
                P(channel, cls="text-sm text-gray-600"),
                Span(
                    "Analysis in Progress",
                    cls="inline-block mt-2 px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800",
                ),
            ),
            cls="flex gap-4 mb-6 items-start",
        ),
        # Progress bar
        Div(
            Div(
                Div(
                    # ✅ Use clamped value for width
                    style=f"width: {clamped_progress}%",
                    cls="bg-blue-600 h-full rounded-full transition-all duration-500",
                ),
                cls="w-full bg-gray-200 rounded-full h-4 overflow-hidden",
            ),
            Div(
                Span(status_message, cls="text-sm font-medium text-gray-700"),
                Span(time_label, cls="text-sm text-gray-500"),
                cls="flex justify-between mt-2",
            ),
            cls="mb-6",
        ),
        # Stats
        Div(
            Div(
                Div(f"{video_count:,}", cls="text-2xl font-bold text-gray-900"),
                Div("Videos", cls="text-sm text-gray-500"),
                cls="bg-red-50 border border-red-200 p-4 rounded-lg text-center",
            ),
            Div(
                # ✅ Use clamped value for display
                Div(
                    f"{int(clamped_progress)}%", cls="text-2xl font-bold text-blue-600"
                ),
                Div("Complete", cls="text-sm text-gray-500"),
                cls="bg-blue-50 border border-blue-200 p-4 rounded-lg text-center",
            ),
            cls="grid grid-cols-2 gap-4 mb-6",
        ),
        # Info
        Div(
            H3("What's happening?", cls="text-sm font-semibold text-gray-700 mb-2"),
            Ul(
                Li("Fetching video metadata"),
                Li("Calculating engagement metrics"),
                Li("Analyzing trends and patterns"),
                cls="list-disc list-inside text-sm text-gray-700 space-y-1",
            ),
            cls="bg-gradient-to-br from-purple-50 to-blue-50 p-4 rounded-lg border",
        ),
        cls="p-6 bg-white rounded-xl shadow-lg border max-w-3xl mx-auto",
        id="preview-box",
        # 🔄 Continue polling
        hx_get=f"/job-progress?playlist_url={quote_plus(playlist_url)}",
        hx_trigger="every 2s",
        hx_swap="outerHTML",
    )
