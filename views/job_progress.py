# views/job_progress.py
from urllib.parse import quote_plus

from fasthtml.common import *
from monsterui.all import *

from utils import format_number
from views.job_progress_state import JobProgressViewState


def render_job_progress_view(state: JobProgressViewState) -> Div:
    """Render the job progress view with real-time updates."""
    progress = state.progress

    # Build inner content as a list
    inner_content = [
        # Header
        Div(
            H2("Processing Your Playlist", cls="text-2xl font-bold text-gray-900 mb-2"),
            P(
                f"Analyzing {format_number(state.video_count) if state.video_count else '?'} videos",
                cls="text-gray-600",
            ),
            cls="mb-8",
        ),
        # Main progress section
        Div(
            # Progress percentage and time
            Div(
                Div(
                    Span(
                        f"{int(progress * 100)}%",
                        cls="text-5xl font-bold text-blue-600",
                    ),
                    Span(
                        "Complete",
                        cls="text-lg text-gray-600 ml-3",
                    ),
                    cls="flex items-baseline",
                ),
                Div(
                    Div(
                        Span("⏱️ ", cls="mr-1"),
                        Span(
                            f"Elapsed: {state.elapsed_display}",
                            cls="text-gray-700",
                        ),
                        cls="",
                    ),
                    (
                        Div(
                            Span("⏳ ", cls="mr-1"),
                            Span(
                                f"Remaining: {state.remaining_display}",
                                cls="text-gray-700",
                            ),
                            cls="mt-1",
                        )
                        if progress > 0 and progress < 1.0
                        else None
                    ),
                    cls="space-y-1 text-sm",
                ),
                cls="flex justify-between items-start mb-4",
            ),
            # Progress bar
            Div(
                cls="w-full bg-gray-200 rounded-full h-3 overflow-hidden",
                style=f"background: linear-gradient(to right, #3b82f6 0%, #3b82f6 {progress * 100}%, #e5e7eb {progress * 100}%, #e5e7eb 100%)",
            ),
            # Batch indicators
            Div(
                *[
                    Div(
                        cls=(
                            "h-2 flex-1 rounded-sm transition-colors duration-300 "
                            + (
                                "bg-blue-600"
                                if i < state.current_batch
                                else "bg-gray-300"
                            )
                        ),
                    )
                    for i in range(state.batch_count)
                ],
                cls="flex gap-2 mt-4",
            ),
            Div(
                P(
                    f"Processing batch {state.current_batch} of {state.batch_count}",
                    cls="text-sm text-gray-600 mt-2",
                ),
                cls="text-center",
            ),
            cls="bg-white p-6 rounded-lg border border-gray-200 mb-6",
        ),
        # Stats preview section
        Div(
            H3("What's Being Analyzed", cls="text-lg font-semibold text-gray-900 mb-4"),
            Div(
                Div(
                    Div(
                        UkIcon(
                            "play-circle", width=24, height=24, cls="text-red-600 mb-2"
                        ),
                        Div(
                            Div(
                                format_number(state.video_count),
                                cls="text-2xl font-bold text-gray-900",
                            ),
                            Div("Videos", cls="text-xs text-gray-500"),
                        ),
                    ),
                    cls="bg-red-50 p-4 rounded-lg border border-red-200",
                ),
                Div(
                    Div(
                        UkIcon("eye", width=24, height=24, cls="text-blue-600 mb-2"),
                        Div(
                            Div(
                                format_number(
                                    state.estimated_stats["estimated_total_views"]
                                ),
                                cls="text-lg font-bold text-gray-900",
                            ),
                            Div("Est. Views", cls="text-xs text-gray-500"),
                        ),
                    ),
                    cls="bg-blue-50 p-4 rounded-lg border border-blue-200",
                ),
                Div(
                    Div(
                        UkIcon("heart", width=24, height=24, cls="text-pink-600 mb-2"),
                        Div(
                            Div(
                                format_number(
                                    state.estimated_stats["estimated_total_likes"]
                                ),
                                cls="text-lg font-bold text-gray-900",
                            ),
                            Div("Est. Likes", cls="text-xs text-gray-500"),
                        ),
                    ),
                    cls="bg-pink-50 p-4 rounded-lg border border-pink-200",
                ),
                Div(
                    Div(
                        UkIcon(
                            "message-circle",
                            width=24,
                            height=24,
                            cls="text-green-600 mb-2",
                        ),
                        Div(
                            Div(
                                format_number(
                                    state.estimated_stats["estimated_total_comments"]
                                ),
                                cls="text-lg font-bold text-gray-900",
                            ),
                            Div("Est. Comments", cls="text-xs text-gray-500"),
                        ),
                    ),
                    cls="bg-green-50 p-4 rounded-lg border border-green-200",
                ),
                cls="grid grid-cols-2 sm:grid-cols-4 gap-3",
            ),
            Div(
                P(
                    "📌 These are estimates based on typical playlist patterns. Actual metrics will be calculated once processing completes.",
                    cls="text-xs text-gray-500 mt-3",
                ),
                cls="text-center",
            ),
            cls="bg-white p-6 rounded-lg border border-gray-200 mb-6",
        ),
        # Tips section
        Div(
            Div(
                UkIcon(
                    state.tip["icon"], width=24, height=24, cls="text-blue-600 mb-2"
                ),
                H4(state.tip["title"], cls="font-semibold text-gray-900 mb-1"),
                P(state.tip["content"], cls="text-sm text-gray-600"),
                cls="",
            ),
            cls="bg-gradient-to-r from-blue-50 to-indigo-50 p-6 rounded-lg border border-blue-200 mb-6",
        ),
        # Status messages for edge cases
        (
            Div(
                P(
                    f"⚠️ Error: {state.error}",
                    cls="text-red-600 text-sm",
                ),
                cls="bg-red-50 p-4 rounded-lg border border-red-200",
            )
            if state.status == "failed" and state.error
            else None
        ),
        # Completion message (shown when done)
        (
            Div(
                P(
                    "✅ Processing complete! Loading results...",
                    cls="text-green-600 font-semibold",
                ),
                Script(
                    f"setTimeout(() => {{ htmx.ajax('POST', '/validate/full', {{target: '#preview-box', values: {{playlist_url: '{state.playlist_url}'}}}}); }}, 1000);"
                ),
            )
            if state.status == "complete"
            else None
        ),
    ]

    # Return outer container with HTMX attributes
    # HTMX will replace the entire outer div on each poll
    return Div(
        *inner_content,
        id="progress-container",
        hx_get=(
            None
            if state.is_complete
            else f"/job-progress?playlist_url={quote_plus(state.playlist_url)}"
        ),
        hx_trigger=None if state.is_complete else "every 2s",
        hx_swap=None if state.is_complete else "outerHTML",
        cls="max-w-2xl mx-auto",
    )
