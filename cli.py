import asyncio
import logging
import os
from datetime import datetime

import click

from db import (
    init_supabase,
    setup_logging,
    supabase_client,
    upsert_playlist_stats,
)
from worker.worker import (
    fetch_pending_jobs,
    handle_job,
    mark_job_status,
    worker_loop,
)
from youtube_service import YoutubePlaylistService

# --- Setup logging once for CLI ---
setup_logging()
logger = logging.getLogger("vv_cli")

# --- Default backend if not passed ---
DEFAULT_BACKEND = os.getenv("VV_BACKEND", "youtubeapi")


# --- Dry-run monkey patch helpers ---
async def _dry_run_mark_job_status(job_id, status, meta=None):
    safe_meta = {k: v for k, v in (meta or {}).items() if k != "error_trace"}
    logger.info(f"[DryRun Job {job_id}] Would mark status={status}, meta={safe_meta}")


def _dry_run_upsert_playlist_stats(stats):
    safe_stats = {k: v for k, v in stats.items() if k not in ["df", "summary_stats"]}
    logger.info(f"[DryRun] Would upsert stats={safe_stats}")
    return {
        "source": "dry-run",
        "df": stats.get("df"),
        "summary_stats": stats.get("summary_stats"),
    }


def enable_dry_run():
    global mark_job_status, upsert_playlist_stats
    mark_job_status = _dry_run_mark_job_status
    upsert_playlist_stats = _dry_run_upsert_playlist_stats
    logger.warning("Dry-run monkey patch enabled: no DB writes will occur")


@click.group()
def cli():
    """ViralVibes Worker CLI for local job processing."""


@cli.command()
@click.option(
    "--poll-interval", default=10, show_default=True, help="Polling interval in seconds"
)
@click.option(
    "--batch-size", default=3, show_default=True, help="Max jobs to process per batch"
)
@click.option(
    "--max-runtime", default=300, show_default=True, help="Max runtime in minutes"
)
@click.option(
    "--backend",
    type=click.Choice(["yt-dlp", "youtubeapi"]),
    default=DEFAULT_BACKEND,
    show_default=True,
)
@click.option("--dry-run", is_flag=True, help="Run without updating Supabase DB")
def run(poll_interval, batch_size, max_runtime, backend, dry_run):
    """Run the worker loop locally (like on Render)."""
    os.environ["WORKER_POLL_INTERVAL"] = str(poll_interval)
    os.environ["WORKER_BATCH_SIZE"] = str(batch_size)
    os.environ["WORKER_MAX_RUNTIME"] = str(max_runtime)
    os.environ["VV_BACKEND"] = backend

    if dry_run:
        os.environ["VV_DRY_RUN"] = "1"
        enable_dry_run()

    async def _run():
        if not dry_run:
            init_supabase()
        global yt_service
        yt_service = YoutubePlaylistService(backend=backend)
        jobs_processed = await worker_loop()
        logger.info(f"Worker completed. Jobs processed: {jobs_processed}")

    asyncio.run(_run())


@cli.command()
@click.argument("playlist_url")
@click.option(
    "--backend",
    type=click.Choice(["yt-dlp", "youtubeapi"]),
    default=DEFAULT_BACKEND,
    show_default=True,
)
@click.option("--dry-run", is_flag=True, help="Run without updating Supabase DB")
def process(playlist_url, backend, dry_run):
    """Process a single playlist job locally (no scheduler loop)."""
    os.environ["VV_BACKEND"] = backend

    if dry_run:
        os.environ["VV_DRY_RUN"] = "1"
        enable_dry_run()

    async def _process():
        """Process a single playlist with proper async handling."""
        if not dry_run:
            client = init_supabase()
            if not client:
                logger.error("Failed to initialize Supabase client. Exiting.")
                return

        global yt_service
        yt_service = YoutubePlaylistService(backend=backend)

        # Fake job dict for local run
        job = {
            "id": int(datetime.utcnow().timestamp()),
            "playlist_url": playlist_url,
            "status": "pending",
        }

        try:
            await mark_job_status(
                job["id"], "processing", {"started_at": datetime.utcnow().isoformat()}
            )
            await handle_job(job)
            click.echo(f"✅ Processed playlist: {playlist_url}")
        except Exception as e:
            logger.error(f"Failed to process job: {e}")
            if not dry_run:
                # Properly await status update
                await mark_job_status(
                    job["id"],
                    "failed",
                    {"error": str(e), "finished_at": datetime.utcnow().isoformat()},
                )

    asyncio.run(_process())


@cli.command()
def pending():
    """List pending jobs from the DB."""

    async def _list():
        init_supabase()
        jobs = await fetch_pending_jobs()
        if not jobs:
            click.echo("No pending jobs found.")
            return
        for job in jobs:
            click.echo(f"Job {job['id']} - {job['playlist_url']} - {job['status']}")

    asyncio.run(_list())


if __name__ == "__main__":
    cli()
