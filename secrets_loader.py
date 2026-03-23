"""
secrets_loader.py — Unified credential bootstrap.

Auto-detects the runtime environment and injects secrets into os.environ so
that every downstream module (db.py, youtube_config.py, …) can read them
from os.environ / os.getenv() without knowing where they came from.

Supported environments
─────────────────────
• Local dev / GitHub Actions  → python-dotenv reads .env (or env vars already set)
• Kaggle Notebooks            → kaggle_secrets.UserSecretsClient

Usage — call ONCE at the very top of your entry-point before any other import
that relies on environment variables:

    from secrets_loader import load_secrets
    load_secrets()                  # auto-detects environment

The function is idempotent: calling it a second time is a no-op if secrets
are already present in os.environ.

Kaggle setup
────────────
In your notebook go to  Add-ons → Secrets  and add:

    NEXT_PUBLIC_SUPABASE_URL      (required)
    SUPABASE_SERVICE_KEY          (required — worker needs broader perms than anon key)
    NEXT_PUBLIC_SUPABASE_ANON_KEY (optional — kept for web-app compat)
    YOUTUBE_API_KEY               (required)
    YOUTUBE_DAILY_QUOTA           (optional — defaults to 10000)
    CREATOR_WORKER_BATCH_SIZE     (optional)
    CREATOR_WORKER_POLL_INTERVAL  (optional)
    CREATOR_WORKER_MAX_RUNTIME    (optional)
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Secrets to pull from Kaggle → os.environ
# Add any new env-var your worker needs here; missing keys are silently skipped
# so optional values don't break the loader.
# ──────────────────────────────────────────────────────────────────────────────
_KAGGLE_SECRET_KEYS: list[str] = [
    # Supabase
    "NEXT_PUBLIC_SUPABASE_URL",
    "SUPABASE_SERVICE_KEY",  # worker context — broader permissions
    "NEXT_PUBLIC_SUPABASE_ANON_KEY",  # web-app / fallback compat
    # YouTube
    "YOUTUBE_API_KEY",
    "YOUTUBE_DAILY_QUOTA",
    # Worker tuning (optional — constants.py defaults are used when absent)
    "CREATOR_WORKER_BATCH_SIZE",
    "CREATOR_WORKER_POLL_INTERVAL",
    "CREATOR_WORKER_MAX_RUNTIME",
    "CREATOR_WORKER_SYNC_TIMEOUT",
    "CREATOR_WORKER_EMPTY_BACKOFF_BASE",
    "CREATOR_WORKER_EMPTY_BACKOFF_MAX",
    "CREATOR_WORKER_SKIP_DIAGNOSIS",
]

# Sentinel: set to True after the first successful load to make subsequent
# calls instant no-ops (safe to call from multiple modules).
_secrets_loaded: bool = False


# ──────────────────────────────────────────────────────────────────────────────
# Environment detection
# ──────────────────────────────────────────────────────────────────────────────


def _is_kaggle() -> bool:
    """
    Return True when running inside a Kaggle Notebook kernel.

    Kaggle injects KAGGLE_DATA_PROXY_TOKEN into every kernel environment;
    it is the most reliable signal that we're on their platform.
    """
    return "KAGGLE_DATA_PROXY_TOKEN" in os.environ


# ──────────────────────────────────────────────────────────────────────────────
# Loaders
# ──────────────────────────────────────────────────────────────────────────────


def _load_kaggle_secrets() -> None:
    """
    Fetch every key in _KAGGLE_SECRET_KEYS from Kaggle's UserSecretsClient
    and inject the non-empty values into os.environ.

    Keys that haven't been added to the notebook's secret store are skipped
    with a DEBUG log — they may be optional or deliberately omitted.
    """
    try:
        from kaggle_secrets import UserSecretsClient  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "kaggle_secrets module not found. "
            "This loader expects to run inside a Kaggle Notebook kernel."
        ) from exc

    client = UserSecretsClient()
    loaded: list[str] = []
    skipped: list[str] = []

    for key in _KAGGLE_SECRET_KEYS:
        # Only overwrite if the key isn't already set — lets notebook cells
        # override secrets by exporting env vars directly if needed.
        if key in os.environ:
            loaded.append(f"{key} (already set)")
            continue
        try:
            value = client.get_secret(key)
            if value:
                os.environ[key] = value
                loaded.append(key)
            else:
                skipped.append(f"{key} (empty)")
        except KeyError:
            # get_secret() raises KeyError when the key doesn't exist in the
            # notebook's secret store — treat as optional and move on.
            skipped.append(key)
        except Exception:
            # Unexpected client/runtime error — log clearly and abort.
            # Do NOT silently treat this as a missing key.
            logger.error(
                "Unexpected error retrieving Kaggle secret '%s' — "
                "check UserSecretsClient connectivity",
                key,
            )
            raise

    logger.info(
        "Kaggle secrets processed. Total keys: %d, loaded/already set: %d, " "not found/empty: %d",
        len(_KAGGLE_SECRET_KEYS),
        len(loaded),
        len(skipped),
    )
    if skipped:
        logger.debug("Kaggle secrets not found / empty (may be optional): %d", len(skipped))


def _load_dotenv_secrets() -> None:
    """
    Load .env file into os.environ via python-dotenv.

    In GitHub Actions / CI the variables are already in the environment, so
    load_dotenv() is effectively a no-op — which is the desired behaviour.
    """
    try:
        from dotenv import load_dotenv  # type: ignore[import]

        load_dotenv()
        logger.info("Secrets loaded from .env file / pre-set environment variables")
    except ImportError:
        # dotenv not installed — rely entirely on whatever is already in the env.
        logger.debug(
            "python-dotenv not installed; " "relying on pre-set environment variables only"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────


def load_secrets() -> str:
    """
    Auto-detect the runtime and load credentials into os.environ.

    This is the only function callers need.  Call it once — typically at the
    very top of your entry-point module — before importing anything that reads
    from os.environ.

    Returns:
        "kaggle"   if running inside a Kaggle Notebook (UserSecretsClient used)
        "dotenv"   if running locally or in GitHub Actions (.env / env vars used)

    The return value is informational; most callers can ignore it.
    """
    global _secrets_loaded

    if _secrets_loaded:
        logger.debug("load_secrets() called again — already loaded, skipping")
        return "already_loaded"

    if _is_kaggle():
        logger.info("Kaggle kernel detected — loading secrets via UserSecretsClient")
        _load_kaggle_secrets()
        loader_used = "kaggle"
    else:
        logger.debug("Non-Kaggle environment — loading secrets via dotenv / env vars")
        _load_dotenv_secrets()
        loader_used = "dotenv"

    _secrets_loaded = True
    return loader_used
