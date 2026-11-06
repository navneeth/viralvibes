"""Default-off admission requests for bounded video snapshot enrollment."""

import logging
import os
from datetime import datetime, timedelta, timezone
from uuid import UUID

logger = logging.getLogger(__name__)


def video_snapshot_demand_enabled() -> bool:
    return os.environ.get("VIDEO_SNAPSHOT_DEMAND_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def needs_video_snapshot(creator: dict) -> bool:
    if creator.get("archived_at") is not None:
        return False
    if creator.get("recent_videos") is None:
        return True
    try:
        fetched = datetime.fromisoformat(creator["recent_videos_fetched_at"].replace("Z", "+00:00"))
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return fetched > now or fetched <= now - timedelta(days=7)
    except (KeyError, AttributeError, TypeError, ValueError):
        return True


def request_video_snapshot(creator_id: str, user_id: str) -> str:
    if not video_snapshot_demand_enabled():
        return "disabled"
    try:
        creator_id = str(UUID(creator_id))
        user_id = str(UUID(user_id))
    except (AttributeError, TypeError, ValueError):
        return "invalid"
    try:
        from db import supabase_client

        result = supabase_client.rpc(
            "request_creator_video_snapshot",
            {
                "p_creator_id": creator_id,
                "p_user_id": user_id,
            },
        ).execute()
        return result.data if isinstance(result.data, str) else "unavailable"
    except Exception:
        logger.warning("Video snapshot demand admission unavailable", exc_info=True)
        return "unavailable"
