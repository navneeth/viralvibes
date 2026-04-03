"""
worker/render_worker.py — Render.com-compatible entrypoint for the ViralVibes creator worker.

ARCHITECTURE
────────────
This file is the long-running "supervisor" process. It:

  1. Binds an HTTP health-check server on $PORT so Render's web service
     health check passes (the existing service is type:web and cannot be
     changed after creation).

  2. Polls Supabase for pending jobs.

  3. For each job, spawns worker/run_one_job.py as a *subprocess* and waits
     for it to finish before fetching the next job.

WHY SUBPROCESS PER JOB
───────────────────────
httplib2 (used inside the YouTube Data API client) corrupts its own C-level
heap after one use — `malloc(): corrupted top size` — and the only reliable
fix is a fresh OS process per job. This is exactly what the original bash
respawn loop did. We replicate that here in Python so the supervisor never
has to exit.

The RESOLVER_RESET_INTERVAL trick in kaggle_worker.py works only because
Kaggle links a different httplib2 build. On Render's Debian container the
corruption is deterministic on job 1.

DEPLOYMENT
──────────
  worker/render_worker.py   ← this file (supervisor)
  worker/run_one_job.py     ← single-job runner (spawned per job)

render.yaml startCommand:
  python -m worker.render_worker
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

# ── Secrets + logging ────────────────────────────────────────────────────────
from secrets_loader import load_secrets

_env_source = load_secrets()

from db import setup_logging

setup_logging()
logger = logging.getLogger("render_worker")
logger.info("render_worker (supervisor) starting — secrets loaded via: %s", _env_source)

# ── Worker internals (for queue polling and maintenance only) ─────────────────
import worker.creator_worker as _cw

# ── Config ────────────────────────────────────────────────────────────────────

# Render injects $PORT; default 8080 matches Render's expected port.
PORT: int = int(os.getenv("PORT", "8080"))

# How long to sleep between polls when the queue is empty (seconds).
IDLE_POLL_INTERVAL: int = int(os.getenv("RENDER_IDLE_POLL_INTERVAL", "60"))

# How often to run periodic maintenance (queue unsynced / stale creators).
PERIODIC_CHECK_INTERVAL: int = int(os.getenv("RENDER_PERIODIC_CHECK_INTERVAL", "300"))

# Log a warning (not exit) after this many consecutive empty polls.
EMPTY_WARN_THRESHOLD: int = int(os.getenv("RENDER_EMPTY_WARN_THRESHOLD", "10"))

# Subprocess timeout per job: sync timeout + generous buffer for DB ops.
JOB_SUBPROCESS_TIMEOUT: int = _cw.SYNC_TIMEOUT + 60

# ── Graceful shutdown ─────────────────────────────────────────────────────────

_shutdown = asyncio.Event()


def _handle_signal(sig, _frame):
    logger.info("Received signal %s — initiating graceful shutdown", sig)
    _shutdown.set()
    _cw.stop_event.set()


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ── Health-check HTTP server ──────────────────────────────────────────────────


class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler — returns 200 OK on all GET requests."""

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, fmt, *args):
        pass  # suppress per-request access logs


def _start_health_server(port: int) -> None:
    """Start health-check server in a daemon thread (never blocks the event loop)."""
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    logger.info("Health-check server listening on port %d", port)
    server.serve_forever()


# ── Job subprocess runner ─────────────────────────────────────────────────────


async def _run_job_subprocess(
    job_id: int, creator_id: str, job_number: int, retry_count: int = 0
) -> bool:
    """
    Spawn worker/run_one_job.py in a fresh subprocess for full httplib2 isolation.

    Returns True on success (exit code 0), False otherwise.
    """
    cmd = [
        sys.executable,
        "-m",
        "worker.run_one_job",
        "--job-id",
        str(job_id),
        "--creator-id",
        str(creator_id),
        "--job-number",
        str(job_number),
        "--retry-count",
        str(retry_count),
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,  # merge stderr → stdout
        )

        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=JOB_SUBPROCESS_TIMEOUT)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            logger.warning(
                "⏱  Job #%d (id=%s) subprocess timed out after %ds — killed",
                job_number,
                job_id,
                JOB_SUBPROCESS_TIMEOUT,
            )
            return False

        # Forward subprocess output to our log (it uses the same log format)
        if stdout:
            for line in stdout.decode(errors="replace").splitlines():
                if line.strip():
                    logger.info("[subprocess] %s", line)

        success = proc.returncode == 0
        icon = "✅" if success else "❌"
        logger.info(
            "%s Job #%d (id=%s) subprocess exited with code %d",
            icon,
            job_number,
            job_id,
            proc.returncode,
        )
        return success

    except Exception:
        logger.exception("❌ Failed to spawn subprocess for job #%d (id=%s)", job_number, job_id)
        return False


# ── Supervisor loop ───────────────────────────────────────────────────────────


async def _supervisor_loop() -> None:
    """
    Poll for jobs and dispatch each one to an isolated subprocess.
    Never exits unless _shutdown is set.
    """
    await _cw.init()  # sets up Supabase client

    jobs_processed: int = 0
    empty_polls: int = 0
    start_time: float = time.time()
    last_periodic_check: float = 0.0  # force check on first iteration

    while not _shutdown.is_set():

        # ── 1. Time-gated periodic maintenance ───────────────────────────────
        if time.time() - last_periodic_check >= PERIODIC_CHECK_INTERVAL:
            logger.info("⏰ Periodic maintenance check...")
            try:
                n = _cw.queue_invalid_creators_for_retry(hours_since_last_sync=24)
                logger.info("  queue_invalid_creators_for_retry: %d queued", n)
            except Exception:
                logger.debug("queue_invalid_creators_for_retry raised (non-fatal)", exc_info=True)
            try:
                n = _cw._queue_unsynced_creators(batch_size=500)
                logger.info("  _queue_unsynced_creators: %d queued", n)
            except Exception:
                logger.debug("_queue_unsynced_creators raised (non-fatal)", exc_info=True)
            last_periodic_check = time.time()

        # ── 2. Fetch next job ─────────────────────────────────────────────────
        jobs = _cw._fetch_pending_jobs(batch_size=1)

        if not jobs:
            empty_polls += 1
            if empty_polls == 1:
                logger.info("Queue empty — sleeping %ds before next poll", IDLE_POLL_INTERVAL)
            elif empty_polls % EMPTY_WARN_THRESHOLD == 0:
                logger.warning(
                    "Queue has been empty for %d consecutive polls (~%.0f min idle)",
                    empty_polls,
                    (time.time() - start_time) / 60,
                )
            await asyncio.sleep(IDLE_POLL_INTERVAL)
            continue

        if empty_polls > 0:
            logger.info("Queue active again after %d idle polls — resuming", empty_polls)
            empty_polls = 0

        job = jobs[0]
        jobs_processed += 1

        # ── 3. Run job in isolated subprocess ─────────────────────────────────
        logger.info(
            "→ Dispatching job #%d (id=%s, creator=%s) to subprocess",
            jobs_processed,
            job["id"],
            job["creator_id"],
        )
        await _run_job_subprocess(
            job_id=job["id"],
            creator_id=job["creator_id"],
            job_number=jobs_processed,
            retry_count=int(job.get("retry_count") or 0),
        )

    # ── Shutdown summary ──────────────────────────────────────────────────────
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("render_worker supervisor shutting down")
    logger.info("  Uptime              : %.0fs (%.1f min)", elapsed, elapsed / 60)
    logger.info("  Jobs dispatched     : %d", jobs_processed)
    logger.info("=" * 60)


# ── Entry point ───────────────────────────────────────────────────────────────


async def main() -> None:
    logger.info("=" * 60)
    logger.info("🚀 ViralVibes Creator Worker — Render Supervisor")
    logger.info("   PORT                    : %d", PORT)
    logger.info("   IDLE_POLL_INTERVAL      : %ds", IDLE_POLL_INTERVAL)
    logger.info("   PERIODIC_CHECK_INTERVAL : %ds", PERIODIC_CHECK_INTERVAL)
    logger.info("   JOB_SUBPROCESS_TIMEOUT  : %ds", JOB_SUBPROCESS_TIMEOUT)
    logger.info("=" * 60)

    # Health-check server in a background daemon thread
    Thread(target=_start_health_server, args=(PORT,), daemon=True).start()

    await _supervisor_loop()


if __name__ == "__main__":
    asyncio.run(main())
