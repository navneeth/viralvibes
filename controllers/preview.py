import logging

from db import (
    get_cached_playlist_stats,
    get_playlist_job_status,
    get_playlist_preview_info,
)
from views.preview import (
    render_blocked_preview,
    render_preview_card,
    render_redirect_to_full,
)

logger = logging.getLogger(__name__)


def preview_playlist_controller(playlist_url: str):
    """
    Orchestrates playlist preview  flow (frontend-safe).
    - cache → redirect to full analysis
    - job state → redirect / blocked
    - fallback preview → render a preview card
    (guaranteed UI safe; no youutube analysis here)

    Data sources:
    - Supabase only (frontend-safe)
    - YouTube enrichment happens asynchronously in workers
    - job state → redirect / blocked / auto-submit
    - fallback preview → render a preview card WITH auto-submit
    """
    logger.info(f"Received request to preview playlist: {playlist_url}")

    # 1. Cache hit → skip preview entirely
    cached = get_cached_playlist_stats(playlist_url, check_date=True)
    if cached:
        logger.info("Preview: cache hit → redirect to full")
        return render_redirect_to_full(playlist_url)

    # 2. Job status check
    job_status = get_playlist_job_status(playlist_url)

    if job_status == "done":
        logger.info("Preview: job done → redirect to full")
        return render_redirect_to_full(playlist_url)

    if job_status == "blocked":
        logger.warning("Preview: job blocked")
        return render_blocked_preview()

    # 3. Preview data (DB stub or API fallback)
    preview_info = get_playlist_preview_info(playlist_url) or {}

    # 🆕 Determine if we should auto-submit
    # Auto-submit if no job exists OR job failed (retry)
    auto_submit = job_status is None or job_status == "failed"

    return render_preview_card(
        playlist_url=playlist_url,
        job_status=job_status,
        preview_info=preview_info,
        auto_submit=auto_submit,
    )
