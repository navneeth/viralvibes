"""
routes/mentions.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Single endpoint: GET /creator/{creator_id}/mentions

Called by the HTMX placeholder in the profile page after it renders.
Fetches both feeds, renders the card, and returns the HTML fragment.

Wire into main.py:
    from routes.mentions import mentions_route
    rt("/creator/{creator_id}/mentions")(mentions_route)
"""

from __future__ import annotations

import logging

from db import get_creator_stats
from services.mentions import get_mentions
from views.mentions import render_mentions_card, render_mentions_error

logger = logging.getLogger(__name__)


def mentions_route(req, sess, creator_id: str):
    """GET /creator/{creator_id}/mentions — HTMX lazy-load fragment."""
    try:
        creator = get_creator_stats(creator_id)
        if not creator:
            return render_mentions_error()

        channel_id = creator.get("channel_id", "")
        channel_name = creator.get("channel_name", "")

        if not channel_id or not channel_name:
            return render_mentions_error()

        bundle = get_mentions(channel_id, channel_name)
        return render_mentions_card(bundle)

    except Exception as exc:
        # Fail silently — mentions are supplementary, not critical
        logger.error(
            "mentions_route error while rendering mentions card",
            extra={
                "creator_id": creator_id,
                "exception": repr(exc),
            },
        )
        return render_mentions_error()
