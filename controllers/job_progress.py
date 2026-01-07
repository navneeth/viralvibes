# controllers/job_progress.py

from dataclasses import dataclass
from urllib.parse import quote_plus

from fasthtml.common import *
from monsterui.all import *

from components.processing_tips import get_tip_for_progress
from db import (
    get_estimated_stats,
    get_job_progress,
    get_playlist_preview_info,
)
from utils import (
    compute_batches,
    compute_time_metrics,
    format_number,
    format_seconds,
)
from views.job_progress import render_job_progress_view
from views.job_progress_state import JobProgressViewState


def job_progress_controller(playlist_url: str):
    # Get job status
    job = get_job_progress(playlist_url)
    if not job:
        return Div(P("Job not found", cls="text-red-600"))

    progress = job.get("progress") or 0.0
    status = job.get("status")
    started_at = job.get("started_at")
    error = job.get("error")

    # Estimate remaining time (based on progress rate)
    elapsed, remaining = compute_time_metrics(started_at, progress)
    elapsed_display = format_seconds(elapsed)
    remaining_display = format_seconds(remaining)

    # Batch calculation (assume ~5 batches for smooth progress)
    current_batch, batch_count = compute_batches(progress)
    # Get tip for current progress
    tip = get_tip_for_progress(progress)

    # Get estimated stats for preview
    preview_info = get_playlist_preview_info(playlist_url)
    video_count = preview_info.get("video_count", 0) if preview_info else 0
    estimated_stats = get_estimated_stats(video_count)

    # Determine if we should show completion
    is_complete = status == "done"

    state = JobProgressViewState(
        progress=progress,
        status=status,
        elapsed_display=elapsed_display,
        remaining_display=remaining_display,
        current_batch=current_batch,
        batch_count=batch_count,
        tip=tip,
        video_count=video_count,
        estimated_stats=estimated_stats,
        error=error,
        is_complete=is_complete,
        playlist_url=playlist_url,
    )

    return render_job_progress_view(state)
