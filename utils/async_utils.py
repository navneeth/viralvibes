"""
Async utilities and decorators.
"""

import asyncio
import functools
import logging

logger = logging.getLogger(__name__)


def with_retries(
    max_retries=3,
    base_delay=5.0,
    backoff=2.0,
    retry_exceptions=(Exception,),
    jitter=0.0,
):
    """
    Decorator to retry an async function on specified exceptions.

    Args:
        max_retries (int): Maximum number of retry attempts.
        base_delay (float): Initial delay between retries (seconds).
        backoff (float): Multiplicative backoff factor.
        retry_exceptions (tuple): Exception types that trigger a retry.
        jitter (float): Random jitter added/subtracted to delay, in seconds.

    Example:
        @with_retries(max_retries=3, base_delay=2.0)
        async def fetch_data(url):
            ...
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except retry_exceptions as e:
                    last_exc = e
                    if attempt == max_retries - 1:
                        logger.error(
                            f"{func.__name__} failed after {max_retries} attempts: {e}"
                        )
                        raise
                    delay = base_delay * (backoff**attempt)
                    if jitter:
                        import random

                        delay += random.uniform(-jitter, jitter)
                    logger.warning(
                        f"{func.__name__} attempt {attempt + 1}/{max_retries} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    await asyncio.sleep(delay)
            # Should not reach here, but fallback in case loop exits unexpectedly
            raise last_exc or RuntimeError(f"{func.__name__} failed after retries")

        return wrapper

    return decorator
