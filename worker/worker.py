# worker/worker.py
import asyncio
import logging

from db import supabase_client  # Supabase/Postgres client
from worker.jobs import process_playlist  # Your playlist processing logic

# ------------------------
# Logging setup
# ------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")

# ------------------------
# Asyncio queue for tasks
# ------------------------
playlist_queue = asyncio.Queue()


# ------------------------
# Worker task: process one playlist
# ------------------------
async def worker_task():
    """
    Process a single playlist from the queue.
    Returns True if processed, False if queue is empty.
    """
    try:
        playlist_url = await playlist_queue.get()
    except asyncio.CancelledError:
        return False

    if not playlist_url:
        playlist_queue.task_done()
        return False

    try:
        logger.info("Processing playlist: %s", playlist_url)

        # Fetch playlist stats
        stats = await process_playlist(playlist_url)

        # Prepare DB row matching playlist_stats schema
        db_row = {
            "playlist_url": playlist_url,
            "title": stats.get("playlist_name"),
            "view_count": stats.get("view_count"),
            "like_count": stats.get("like_count"),
            "dislike_count": stats.get("dislike_count"),
            "comment_count": stats.get("comment_count"),
            "video_count": stats.get("video_count"),
            "avg_duration": stats.get("avg_duration"),
            "engagement_rate": stats.get("engagement_rate"),
            "controversy_score": stats.get("controversy_score"),
        }

        # Insert into DB
        if supabase_client:
            supabase_client.table("playlist_stats").insert(db_row).execute()
            logger.info("Inserted playlist stats into DB: %s", playlist_url)
        else:
            logger.warning(
                "Supabase client not initialized. Skipping DB insert.")

    except Exception as e:
        logger.exception("Error processing playlist %s: %s", playlist_url, e)
    finally:
        playlist_queue.task_done()
    return True


# ------------------------
# Add playlist URL to queue
# ------------------------
async def add_playlist(playlist_url: str):
    """Add a playlist URL to the processing queue."""
    await playlist_queue.put(playlist_url)


# ------------------------
# Continuous worker loop
# ------------------------
async def worker_loop():
    """Continuously process playlists from the queue."""
    while True:
        await worker_task()


# ------------------------
# Entrypoint for Render
# ------------------------
if __name__ == "__main__":
    try:
        logger.info("Starting ViralVibes worker...")
        asyncio.run(worker_loop())
    except KeyboardInterrupt:
        logger.info("Worker stopped manually")
