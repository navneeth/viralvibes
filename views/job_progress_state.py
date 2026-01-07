# views/job_progress_state.py

from dataclasses import dataclass
from typing import List, Optional

from fasthtml.common import Div


@dataclass
class JobProgressViewState:
    progress: float
    status: str | None
    elapsed_display: str
    remaining_display: str
    current_batch: int
    batch_count: int
    tip: dict
    video_count: int
    estimated_stats: dict
    error: str | None
    is_complete: bool
    playlist_url: str
