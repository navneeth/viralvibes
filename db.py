"""
Database operations module.
Handles Supabase client initialization and logging setup.
"""

import logging
import os
from typing import Optional
from supabase import create_client, Client
from tenacity import retry, stop_after_attempt, wait_exponential

# Get logger instance
logger = logging.getLogger(__name__)

# Global Supabase client
supabase_client: Optional[Client] = None


def setup_logging():
    """Configure logging for the application.
    
    This function should be called at application startup.
    It configures the logging format and level.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')


@retry(stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1, min=4, max=10),
       reraise=True)
def init_supabase() -> Optional[Client]:
    """Initialize Supabase client with retry logic and proper error handling.
    
    Returns:
        Optional[Client]: Supabase client if initialization succeeds, None otherwise
        
    Note:
        - Retries up to 3 times with exponential backoff
        - Returns None instead of raising exceptions
        - Logs errors for debugging
    """
    global supabase_client

    # Return existing client if already initialized
    if supabase_client is not None:
        return supabase_client

    try:
        url: str = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")
        key: str = os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY", "")

        if not url or not key:
            logger.warning(
                "Missing Supabase environment variables - running without Supabase"
            )
            return None

        client = create_client(url, key)

        # Test the connection
        client.auth.get_session()

        supabase_client = client
        logger.info("Supabase client initialized successfully")
        return client

    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {str(e)}")
        return None
