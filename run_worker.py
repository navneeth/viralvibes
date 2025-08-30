#!/usr/bin/env python3
"""
Worker launcher script for ViralVibes.
Run this from the project root directory to avoid import issues.
"""

import asyncio
import logging
import os
import sys

# Ensure we're in the right directory
if not os.path.exists('worker'):
    print("❌ Error: Please run this script from the project root directory")
    print("   Current directory:", os.getcwd())
    print("   Expected to find: worker/ directory")
    sys.exit(1)

# Add current directory to Python path
sys.path.insert(0, os.getcwd())

try:
    from worker.worker import worker_loop
    print("✅ Worker imported successfully")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print(
        "   Make sure all dependencies are installed: pip install -r requirements.txt"
    )
    sys.exit(1)

if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("worker")
        logger.info("Starting ViralVibes worker...")
        asyncio.run(worker_loop())
    except KeyboardInterrupt:
        logger.info("Worker stopped manually")
    except Exception as e:
        logger.exception("Worker failed with error: %s", e)
