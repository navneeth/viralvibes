import logging

from constants import JobStatus
from db import (
    get_cached_playlist_stats,
    get_playlist_job_status,
    get_playlist_preview_info,
)
from views.preview import (
    render_blocked_preview,
    render_preview_card,
    render_queuing_placeholder,
    render_redirect_to_full,
)

logger = logging.getLogger(__name__)


def preview_playlist_controller(playlist_url: str, *, authenticated: bool = False):
    """
    Orchestrate the preview flow without starting full analysis for guests.

    Data sources:
    - Supabase only (frontend-safe)
    - YouTube enrichment happens asynchronously in workers
    - authenticated users may auto-submit or skip directly to existing results
    - guests see the preview before choosing whether to request full analysis
    """
    logger.info(f"Received request to preview playlist: {playlist_url}")

    # Authenticated users can go straight to full analysis for existing data.
    # Guests should see the preview first and decide whether to request full analysis.
    cached = get_cached_playlist_stats(playlist_url, check_date=True)
    if cached and authenticated:
        logger.info("Preview: cache hit → redirect to full")
        return render_redirect_to_full(playlist_url)

    # 2. Job status check
    job_status = get_playlist_job_status(playlist_url)

    # ✅ Use constants instead of string literals
    if job_status == JobStatus.COMPLETE and authenticated:
        logger.info("Preview: job done → redirect to full")
        return render_redirect_to_full(playlist_url)

    # ⚠️ BLOCKED job → show error
    if job_status == JobStatus.BLOCKED:
        logger.warning("Preview: job blocked")
        return render_blocked_preview()

    # ✅ Auto-submit if no job exists or job failed — return before the
    # preview-info DB query; that data is unused by the placeholder.
    if (job_status is None or job_status == JobStatus.FAILED) and authenticated:
        logger.info(f"Auto-submitting job for {playlist_url}")
        return render_queuing_placeholder(playlist_url=playlist_url)

    # 3. Preview data (DB stub or API fallback) — only needed for the card
    preview_info = get_playlist_preview_info(playlist_url) or {}

    return render_preview_card(
        playlist_url=playlist_url,
        job_status=job_status,
        preview_info=preview_info,
        auto_submit=False,
        needs_auth=not authenticated,
    )
