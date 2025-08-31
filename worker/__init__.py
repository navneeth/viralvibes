# worker/__init__.py
"""
Worker package for ViralVibes background processing.
"""

# Import the main functions for easy access
from worker.jobs import process_playlist
from worker.worker import add_playlist, worker_loop, worker_task

__all__ = ['worker_task', 'add_playlist', 'worker_loop', 'process_playlist']

# Version info
__version__ = "1.0.0"
