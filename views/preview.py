from fasthtml.common import *
from monsterui.all import *

# from monsterui.all import Loading, LoadingT


def render_redirect_to_full(playlist_url: str):
    return Script(
        f"""
        htmx.ajax('POST', '/validate/full', {{
            target: '#preview-box',
            values: {{ playlist_url: '{playlist_url}' }}
        }});
        """
    )


def render_blocked_preview():
    return Div(
        UkIcon("ban", width=48, height=48, cls="text-red-500 mb-4"),
        H2("YouTube Bot Challenge Detected", cls="text-xl font-bold text-red-700"),
        P(
            "YouTube temporarily blocked automated access. Try again in a few minutes.",
            cls="text-gray-600",
        ),
        cls="p-8 bg-red-50 border border-red-200 rounded-xl text-center max-w-md mx-auto",
    )


def render_preview_card(
    *,
    playlist_url: str,
    job_status: str | None,
    preview_info: dict,
    auto_submit: bool = False,
):
    """
    Render preview card with optional auto-submit.

    If auto_submit=True, HTMX will automatically trigger /submit-job
    on page load, eliminating the need for user to click button.
    """
    # ---- Guaranteed-safe preview fields ----
    title = preview_info.get("title", "YouTube Playlist")
    channel = preview_info.get("channel_name", "Unknown Channel")
    thumbnail = preview_info.get("thumbnail", "/static/favicon.jpeg")
    video_count = preview_info.get("video_count", 0)
    description = preview_info.get("description", "")

    # ---- State derivations (UI-only) ----
    is_processing = job_status in ("pending", "processing", "queued")
    has_previous_analysis = bool(preview_info.get("processed_video_count"))

    # ---- Time estimate (cheap + honest) ----
    estimated_seconds = int(video_count * 2.5) if video_count else 60
    estimated_minutes = max(1, estimated_seconds // 60)
    estimate_label = f"~{estimated_minutes} min"

    return Div(
        # 🆕 Auto-submit trigger (hidden, fires on page load)
        (
            Div(
                hx_post="/submit-job",
                hx_vals={"playlist_url": playlist_url},
                hx_trigger="load",  # Triggers immediately when page loads
                hx_target="#preview-box",
                hx_swap="outerHTML",
                style="display:none;",
            )
            if auto_submit
            else None
        ),
        # =========================================================
        # Header: thumbnail + identity
        # =========================================================
        Div(
            Img(
                src=thumbnail,
                alt="Playlist thumbnail",
                cls="w-24 h-24 rounded-xl shadow object-cover border",
                onerror="this.src='/static/favicon.jpeg'",
            ),
            Div(
                H2(title, cls="text-2xl font-bold text-gray-900"),
                P(
                    channel,
                    cls="text-sm text-gray-600 truncate max-w-sm",
                ),
                Span(
                    "Previously Analyzed" if has_previous_analysis else "New Analysis",
                    cls=(
                        "inline-block mt-2 px-3 py-1 rounded-full text-xs font-semibold "
                        + (
                            "bg-blue-100 text-blue-800"
                            if has_previous_analysis
                            else "bg-green-100 text-green-800"
                        )
                    ),
                ),
            ),
            cls="flex gap-4 mb-6 items-start",
        ),
        # =========================================================
        # Stats row
        # =========================================================
        Div(
            Div(
                Div(
                    f"{video_count:,}" if video_count else "—",
                    cls="text-3xl font-bold text-gray-900",
                ),
                Div("Videos", cls="text-sm text-gray-500"),
                cls="bg-red-50 border border-red-200 p-4 rounded-lg text-center",
            ),
            Div(
                Div(
                    estimate_label,
                    cls="text-lg font-bold text-gray-900",
                ),
                Div("Estimated Time", cls="text-sm text-gray-500"),
                cls="bg-blue-50 border border-blue-200 p-4 rounded-lg text-center",
            ),
            cls="grid grid-cols-2 gap-4 mb-6",
        ),
        # =========================================================
        # Optional description
        # =========================================================
        (
            Div(
                P(
                    description[:200] + ("…" if len(description) > 200 else ""),
                    cls="text-sm text-gray-700 italic",
                ),
                cls="bg-gray-50 p-4 rounded-lg border mb-6",
            )
            if description
            else None
        ),
        # =========================================================
        # What will be analyzed (trust builder)
        # =========================================================
        Div(
            H3("What we're analyzing", cls="text-sm font-semibold text-gray-700 mb-2"),
            Ul(
                Li("Views, likes, and engagement metrics"),
                Li("Engagement rate & distribution"),
                Li("Trends across videos in the playlist"),
                cls="list-disc list-inside text-sm text-gray-700 space-y-1",
            ),
            cls="bg-gradient-to-br from-purple-50 to-blue-50 p-4 rounded-lg border mb-6",
        ),
        # =========================================================
        # Status indicator (shows when auto-submit triggers)
        # =========================================================
        (
            Div(
                Loading(cls=(LoadingT.ring, LoadingT.sm, "text-blue-600")),
                Span(
                    (
                        "Starting analysis..."
                        if auto_submit
                        else f"Status: {job_status.title()}"
                    ),
                    cls="text-sm text-gray-700 ml-3",
                ),
                cls="flex items-center bg-blue-50 p-3 rounded-lg border mb-4",
            )
            if (is_processing or auto_submit)
            else None
        ),
        # =========================================================
        # Action button (hidden when auto-submitting)
        # =========================================================
        (
            Button(
                "Analysis in Progress…",
                disabled=True,
                cls="mt-6 px-6 py-3 rounded-xl font-semibold shadow bg-gray-400 cursor-not-allowed text-white w-full",
                type="button",
            )
            if (is_processing or auto_submit)
            else Button(
                "Start Deep Analysis",
                hx_post="/submit-job",
                hx_vals={"playlist_url": playlist_url},
                hx_target="#preview-box",
                hx_indicator="#loading-bar",
                cls="mt-6 px-6 py-3 rounded-xl font-semibold shadow bg-blue-600 hover:bg-blue-700 text-white w-full transition",
                type="button",
            )
        ),
        Div(
            Loading(
                id="loading-bar",
                cls=(LoadingT.bars, LoadingT.lg),
                style="margin-top:1rem;",
            ),
            id="results-box",
        ),
        cls="p-6 bg-white rounded-xl shadow-lg border max-w-3xl mx-auto",
        id="preview-box",
    )
